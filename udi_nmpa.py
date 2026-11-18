# -*- coding: utf-8 -*-
"""
udi_nmpa —— 国家药监局「医疗器械唯一标识（UDI）数据库」纯后台查询库

不依赖浏览器、不依赖第三方包（仅标准库），可直接被其它项目 import。

快速开始
--------
    from udi_nmpa import UdiClient

    c = UdiClient()                                  # 默认 2.5 秒/条（实测安全极限）
    print(c.get_by_di("06971714900754"))              # 按产品标识查，返回中文字段 dict
    print(c.search("主动脉瓣"))                       # 按产品名/企业名模糊查

接口档案（2026-11-19 逆向 + 实测）
--------------------------------
    POST https://udi.nmpa.gov.cn/getDeviceList.html
    Content-Type: application/x-www-form-urlencoded
    query=<关键词>&searchType=1&page=1&rows=50&sidx=&sord=asc&_search=false

    * 无需登录 / Cookie / token / Referer —— 实测以 curl/8.0 的裸请求（不带 Referer、
      不带 Content-Type）同样返回 200 正常 JSON。请求头只是"像浏览器"，非必需
    * searchType=1 普通搜索：只覆盖 产品标识 / 产品名称 / 企业名称
      —— 实测：**用注册证号做普通搜索会返回 0 条**，搜不到
    * searchType=2 高级搜索：query 语法是 `字段:(值)`，**括号和引号缺一不可**，
      缺了直接 HTTP 500（不是空结果）。例：
        等于   REGCARD_NAME:("国械注准20223131010")
        包含   PRODUCT_NAME:(*支架*)
        组合   COMPANY_NAME:("某公司") and PRODUCT_NAME:(*支架*)
      可用字段见 https://udi.nmpa.gov.cn/deviceField.html （33 个，REGCARD_NAME = 注册证/备案证号）
    * 单页条数上限 **15**（请求 rows=50 也只返回 15），多结果必须翻页
    * 返回 JSON：{records, total, rows:[...], page, ...}

限流实测（关键，别跳过）
----------------------
    * 硬极限 = 每 2.5 秒 1 条；2.4 秒时每 5 条必掉第 3 条
    * 触发限流返回 HTTP 429 + {"message":"搜索过于频繁，请稍后再试"}
    * 429 属"软拒绝"，约 1.2 秒即恢复，短退避重试即可
    * 并发无意义：同时最多放行 2 个请求，第 3 个起 429；开 50 个线程也只过 2 个
      （注：早前"并发 10~50 每档只过 1 个"是探测请求吃掉 1 个名额导致的测法污染，已更正）
    * 链路前置阿里云盾 WAF（CNAME → *.yundunwaf5.com），但拦截由应用层限流完成

作者备注：限流参数集中在 RateLimiter 里，服务端策略若变化只需改那一处。
"""

import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque

__version__ = "1.1.0"
__all__ = ["UdiClient", "RateLimiter", "FIELDS", "FIELD_LABELS", "UdiError", "RateLimitError"]

API_URL = "https://udi.nmpa.gov.cn/getDeviceList.html"
REFERER = "https://udi.nmpa.gov.cn/showListXXCX.html"
HOME_URL = "https://udi.nmpa.gov.cn/showListXXCX.html"

DEFAULT_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/157.0.0.0 Safari/537.36"),
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Referer": REFERER,
    "X-Requested-With": "XMLHttpRequest",
    "Accept": "application/json, text/javascript, */*; q=0.01",
}

# 接口原始字段 -> 界面中文列名（顺序与官网表格一致）
FIELDS = [
    ("primaryDeviceId", "最小销售单元产品标识"),
    ("agencyName", "医疗器械唯一标识编码体系名称"),
    ("deviceCount", "最小销售单元中使用单元的数量"),
    ("companyName", "医疗器械注册人/备案人名称"),
    ("productName", "产品名称/通用名称"),
    ("specification", "型号规格/包装规格"),
    ("deviceEndDateStatus", "是否退市"),
]
FIELD_LABELS = dict(FIELDS)

# 详情页跳转用，官方详情接口需另做限流预检（/checkDetailRateLimit.html），本库未实现
RECORD_KEY = "deviceRecordKey"


class UdiError(Exception):
    """查询失败（网络异常、非 429 的 HTTP 错误、返回体无法解析等）"""


class RateLimitError(UdiError):
    """重试若干次后仍被限流（HTTP 429）"""


class RateLimiter:
    """
    按实测策略限速：任意 5 秒窗口内最多 2 次，且两次间隔不小于 min_interval 秒。

    这与实测现象吻合：
      - 2.5s 间隔 = 恰好卡在 2 条/5s 的边界，全过
      - 2.4s 间隔 = 每 5 条掉 1 条
    线程安全，可被多线程共享。
    """

    def __init__(self, min_interval=2.5, window=5.0, max_calls=2):
        self.min_interval = float(min_interval)
        self.window = float(window)
        self.max_calls = int(max_calls)
        self._lock = threading.Lock()
        self._history = deque()          # 最近若干次请求的发出时刻
        self._last = 0.0

    def acquire(self):
        with self._lock:
            while True:
                now = time.monotonic()
                # 清理滑窗外的历史
                while self._history and now - self._history[0] >= self.window:
                    self._history.popleft()
                wait = 0.0
                gap = self.min_interval - (now - self._last)
                if gap > 0:
                    wait = gap
                if len(self._history) >= self.max_calls:
                    wait = max(wait, self.window - (now - self._history[0]) + 0.01)
                if wait <= 0:
                    self._history.append(now)
                    self._last = now
                    return
                time.sleep(wait)


class UdiClient:
    """
    UDI 数据库客户端。

    参数
    ----
    min_interval : 两次请求最小间隔（秒）。2.5 = 实测极限；3.0 = 保守；1.0 = 自担被限风险
    rows         : 单页条数，默认 50
    timeout      : 单次请求超时（秒）
    retries      : 遇 429 的重试次数
    proxy        : 是否使用系统代理。默认 False —— 本机若设了 http_proxy，
                   走代理会把状态码/内容搞脏，务必直连
    """

    def __init__(self, min_interval=2.5, rows=50, timeout=25, retries=5, proxy=False):
        self.rows = int(rows)
        self.timeout = float(timeout)
        self.retries = int(retries)
        self.limiter = RateLimiter(min_interval=min_interval)
        handlers = [] if proxy else [urllib.request.ProxyHandler({})]
        self._opener = urllib.request.build_opener(*handlers)

    # ------------------------------------------------------------------ 底层
    def _post(self, params):
        """发一次请求，返回解析后的 JSON dict；429 自动退避重试"""
        body = urllib.parse.urlencode(params).encode("utf-8")
        last = None
        for attempt in range(self.retries):
            self.limiter.acquire()
            req = urllib.request.Request(API_URL, data=body,
                                         headers=DEFAULT_HEADERS, method="POST")
            try:
                with self._opener.open(req, timeout=self.timeout) as r:
                    raw = r.read().decode("utf-8", "replace")
                    return json.loads(raw)
            except urllib.error.HTTPError as e:
                last = "HTTP %s" % e.code
                if e.code == 429:
                    # 实测 1.2 秒即恢复，退避 1.5/3/4.5... 秒
                    time.sleep(1.5 * (attempt + 1))
                    continue
                raise UdiError("%s（%s）" % (last, API_URL))
            except json.JSONDecodeError as e:
                raise UdiError("返回体不是 JSON：%s" % e)
            except Exception as e:
                last = "%s: %s" % (type(e).__name__, e)
                time.sleep(1.5 * (attempt + 1))
        raise RateLimitError("重试 %d 次仍被限流（%s）" % (self.retries, last))

    # ------------------------------------------------------------------ 查询
    def search(self, keyword, search_type=1, page=1, rows=None):
        """
        查一页，返回接口原始 rows 列表（键为英文字段名）。

        keyword     : 关键词。search_type=1 时为产品标识/产品名称/企业名称
        search_type : 1=普通搜索，2=高级搜索（Lucene 语法）
        """
        data = self._post({
            "query": keyword,
            "searchType": str(search_type),
            "page": str(page),
            "rows": str(rows or self.rows),
            "sidx": "",
            "sord": "asc",
            "_search": "false",
        })
        return data.get("rows") or []

    def search_all(self, keyword, search_type=1, max_records=500):
        """自动翻页，取回全部结果（受 max_records 与限流速度约束）"""
        out, page = [], 1
        while len(out) < max_records:
            params = {"query": keyword, "searchType": str(search_type),
                      "page": str(page), "rows": str(self.rows),
                      "sidx": "", "sord": "asc", "_search": "false"}
            data = self._post(params)
            chunk = data.get("rows") or []
            out.extend(chunk)
            total = int(data.get("records") or 0)
            if not chunk or len(out) >= total:
                break
            page += 1
        return out[:max_records]

    def get_by_di(self, di):
        """按产品标识（DI）精确查一条，返回中文字段 dict；查不到返回 None"""
        rows = self.search(di, search_type=1, rows=50)
        if not rows:
            return None
        # 精确优先：接口是模糊匹配，可能带回多条，筛出 primaryDeviceId 完全相等的
        for r in rows:
            if str(r.get("primaryDeviceId", "")).strip() == str(di).strip():
                return self.to_cn(r)
        return self.to_cn(rows[0])

    # ---------------------------------------------------- 按注册证/备案证号查
    @staticmethod
    def build_regcard_query(regcard, mode="eq"):
        """
        构造按注册证号检索的高级查询串。

        官方页面 buildQuery 的真实语法（必须带括号，否则 HTTP 500）：
            等于   REGCARD_NAME:("国械注准20223131010")
            包含   REGCARD_NAME:(*20223131010*)
            开头是 REGCARD_NAME:(国械注准2022*)

        mode: "eq" 等于 / "like" 包含 / "prefix" 开头是
        """
        v = str(regcard).strip()
        if mode == "eq":
            return 'REGCARD_NAME:("%s")' % v
        if mode == "like":
            return "REGCARD_NAME:(*%s*)" % v
        if mode == "prefix":
            return "REGCARD_NAME:(%s*)" % v
        raise ValueError("mode 只能是 eq / like / prefix")

    def get_by_regcard(self, regcard, mode="eq", max_records=500):
        """
        按注册证/备案证号查，返回该证号下的**全部** UDI 记录（自动翻页）。

        注意：
          - 一个注册证号通常对应多条 UDI（不同规格/型号），本例 25 条
          - 返回体里**不含**注册证号本身，只有 8 个标准字段
          - 服务端单页上限 15 条（请求 rows=50 也只会返回 15），必须翻页
        """
        q = self.build_regcard_query(regcard, mode)
        rows = self.search_all(q, search_type=2, max_records=max_records)
        return [self.to_cn(r) for r in rows]

    def batch(self, di_list, progress=None, stop_on_error=False):
        """
        批量按 DI 查询。progress(i, total, di, record) 可用于打进度。
        返回 list[dict|None]，与输入一一对应。
        """
        items = list(di_list)
        out = []
        for i, di in enumerate(items, 1):
            rec = None
            try:
                rec = self.get_by_di(di)
            except RateLimitError:
                if stop_on_error:
                    raise
            except UdiError:
                if stop_on_error:
                    raise
            out.append(rec)
            if progress:
                progress(i, len(items), di, rec)
        return out

    # ------------------------------------------------------------------ 工具
    @staticmethod
    def to_cn(row):
        """英文字段 -> 中文列名字典（保留原始键，加 'raw' 便于取 deviceRecordKey）"""
        d = {label: row.get(key, "") for key, label in FIELDS}
        d["raw"] = row
        return d

    @staticmethod
    def csv_fields():
        """导出 CSV 时的列顺序（中文表头）"""
        return [label for _, label in FIELDS]


# ---------------------------------------------------------------- 命令行自测
if __name__ == "__main__":
    c = UdiClient()
    for di in ["06971714900754", "06971714901171"]:
        rec = c.get_by_di(di)
        print("-" * 46)
        if rec:
            for label in c.csv_fields():
                print("%-22s %s" % (label + ":", rec[label]))
        else:
            print("%s 未查到" % di)
