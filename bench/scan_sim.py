# -*- coding: utf-8 -*-
"""
UDI 接口 · 扫码节奏模拟测试

模拟真实扫码：每隔 0.3~1.0 秒随机"扫"到一个新码，立刻新开一个线程发出请求，
不等待上一个返回。检验这种节奏下接口能否支撑。

两轮：
  轮A 裸测   —— 每条只发一次，不重试。看"扫完立刻有没有结果"
  轮B 带重试 —— 每条失败就退避重试，直到拿到正确内容。看"最终能不能都补齐、要多久"

输出：bench/results/scan_sim_result.csv
"""
import csv
import json
import os
import random
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8") if hasattr(sys.stdout, "reconfigure") else None

API = "https://udi.nmpa.gov.cn/getDeviceList.html"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/157.0.0.0 Safari/537.36"),
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Referer": "https://udi.nmpa.gov.cn/showListXXCX.html",
    "X-Requested-With": "XMLHttpRequest",
}
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

HERE = os.path.dirname(os.path.abspath(__file__))
LIST = os.path.join(HERE, "..", "data", "10个测试码.txt")
RESULT_CSV = os.path.join(HERE, "results", "scan_sim_result.csv")

INTERVAL_RANGE = (0.3, 1.0)
MAX_ATTEMPTS = 8

_lock = threading.Lock()
_inflight = 0
_peak = 0


def one_call(code):
    """发一次请求。返回 (状态, 是否内容正确, 耗时ms, 产品名)"""
    body = urllib.parse.urlencode({"query": code, "searchType": "1",
                                   "page": "1", "rows": "50"}).encode()
    req = urllib.request.Request(API, data=body, headers=HEADERS, method="POST")
    t0 = time.time()
    global _inflight, _peak
    with _lock:
        _inflight += 1
        _peak = max(_peak, _inflight)
    try:
        with OPENER.open(req, timeout=25) as r:
            d = json.loads(r.read().decode("utf-8"))
            rows = d.get("rows") or []
            good = any(str(x.get("primaryDeviceId", "")) == code
                       and x.get("companyName") and x.get("productName") for x in rows)
            name = rows[0].get("productName", "") if rows else ""
            return r.status, good, round((time.time() - t0) * 1000), name
    except urllib.error.HTTPError as e:
        return e.code, False, round((time.time() - t0) * 1000), ""
    except Exception as e:
        return "ERR:%s" % type(e).__name__, False, round((time.time() - t0) * 1000), ""
    finally:
        with _lock:
            _inflight -= 1


def run_round(codes, retry, tag):
    global _peak
    _peak = 0
    results = {}
    threads = []
    t_start = time.time()

    print("\n" + "=" * 70)
    print("%s：%d 个码，扫码间隔 0.3~1.0 秒随机，每次开新线程" % (tag, len(codes)))
    print("=" * 70)

    for i, code in enumerate(codes, 1):
        gap = random.uniform(*INTERVAL_RANGE)
        time.sleep(gap)
        t_scan = time.time() - t_start

        def worker(cd=code, idx=i, ts=t_scan):
            attempts, detail = 0, []
            while True:
                attempts += 1
                st, ok, ms, name = one_call(cd)
                detail.append("%s/%dms%s" % (st, ms, "" if ok else "✗"))
                if ok:
                    results[cd] = {"序号": idx, "扫码时刻s": round(ts, 2), "条码": cd,
                                   "尝试次数": attempts, "最终状态": st,
                                   "耗时ms": ms, "产品名称": name,
                                   "成本秒": round(time.time() - t_start - ts, 2),
                                   "过程": " > ".join(detail)}
                    print("  [%2d] %s 扫码@%.1fs → ✓ %s（第%d次尝试，用时%.1fs）"
                          % (idx, cd, ts, name[:24], attempts, time.time() - t_start - ts))
                    return
                if not retry or attempts >= MAX_ATTEMPTS:
                    results[cd] = {"序号": idx, "扫码时刻s": round(ts, 2), "条码": cd,
                                   "尝试次数": attempts, "最终状态": st,
                                   "耗时ms": ms, "产品名称": "",
                                   "成本秒": round(time.time() - t_start - ts, 2),
                                   "过程": " > ".join(detail)}
                    print("  [%2d] %s 扫码@%.1fs → ✗ %s（试了%d次，放弃）"
                          % (idx, cd, ts, st, attempts))
                    return
                time.sleep(1.5 * attempts)

        th = threading.Thread(target=worker, name="scan-%d" % i, daemon=True)
        th.start()
        threads.append(th)
        print("  [%2d] %s 扫码 @%.1fs（间隔%.2fs，此刻在途 %d 个）"
              % (i, code, t_scan, gap, _inflight))

    for th in threads:
        th.join(timeout=120)

    wall = time.time() - t_start
    ok_n = sum(1 for v in results.values() if v["产品名称"])
    print("\n  小计：成功 %d/%d，总耗时 %.1f 秒，峰值并发 %d"
          % (ok_n, len(codes), wall, _peak))
    return results, wall, ok_n


def main():
    with open(LIST, encoding="utf-8-sig") as f:
        codes = [ln.strip() for ln in f if ln.strip()][:10]
    print("载入 %d 个真实产品标识，开始时间 %s" % (len(codes), datetime.now().strftime("%H:%M:%S")))

    a, wall_a, ok_a = run_round(codes, retry=False, tag="轮A · 裸测（不重试）")

    print("\n冷却 25 秒，清空限流计数…")
    time.sleep(25)

    b, wall_b, ok_b = run_round(codes, retry=True, tag="轮B · 带重试（直到拿到内容）")

    rows = []
    for stage, data, wall, ok in (("A-裸测", a, wall_a, ok_a), ("B-重试", b, wall_b, ok_b)):
        for cd in codes:
            v = data.get(cd, {})
            rows.append({"轮次": stage, "序号": v.get("序号", ""), "条码": cd,
                         "扫码时刻s": v.get("扫码时刻s", ""), "尝试次数": v.get("尝试次数", ""),
                         "最终状态": v.get("最终状态", ""), "产品名称": v.get("产品名称", ""),
                         "从扫码到拿到内容s": v.get("成本秒", ""), "过程": v.get("过程", "")})

    os.makedirs(os.path.dirname(RESULT_CSV), exist_ok=True)
    with open(RESULT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    print("\n" + "=" * 70)
    print("汇总")
    print("=" * 70)
    print("轮A 裸测   ：成功 %d/10，总耗时 %.1fs" % (ok_a, wall_a))
    print("轮B 带重试 ：成功 %d/10，总耗时 %.1fs" % (ok_b, wall_b))
    print("已保存：%s" % RESULT_CSV)


if __name__ == "__main__":
    main()
