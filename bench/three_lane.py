# -*- coding: utf-8 -*-
"""
UDI 接口 · A/B/C 三通道测试（毫秒级时间戳）

规则：
  - 三个通道 A/B/C，**首次出发依次间隔 0.3 秒**（A@0ms，B@300ms，C@600ms）
  - 任意通道拿到【正确数据】后，立刻从 FIFO 队列取下一个码再次出发
  - 遇 429 等 1.0 秒再试（与双通道测试保持一致，便于对比）
  - 全程记录毫秒级时间戳 + 每次请求的在途并发数

关注：
  - 10 个码能否全部拿到、总耗时（ms）
  - 第 3 条通道是否长期被拒（3 通道 > 2 名额）
  - 实际在途并发是否始终被压到 2

输出：bench/results/three_lane_result.csv
"""
import csv
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
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
RESULT_CSV = os.path.join(HERE, "results", "three_lane_result.csv")

LANES = [("A", 0.0), ("B", 0.3), ("C", 0.6)]
RETRY_GAP = 1.0
MAX_ATTEMPTS = 15


def ms_str(epoch):
    return datetime.fromtimestamp(epoch).strftime("%H:%M:%S.") + "%03d" % (int(epoch * 1000) % 1000)


def one_call(code):
    body = urllib.parse.urlencode({"query": code, "searchType": "1",
                                   "page": "1", "rows": "50"}).encode()
    req = urllib.request.Request(API, data=body, headers=HEADERS, method="POST")
    t0 = time.perf_counter()
    try:
        with OPENER.open(req, timeout=25) as r:
            d = json.loads(r.read().decode("utf-8"))
            rows = d.get("rows") or []
            good = any(str(x.get("primaryDeviceId", "")) == code
                       and x.get("companyName") and x.get("productName") for x in rows)
            name = rows[0].get("productName", "") if rows else ""
            return r.status, good, (time.perf_counter() - t0) * 1000, name
    except urllib.error.HTTPError as e:
        return e.code, False, (time.perf_counter() - t0) * 1000, ""
    except Exception as e:
        return "ERR:%s" % type(e).__name__, False, (time.perf_counter() - t0) * 1000, ""


class Hub:
    def __init__(self, codes):
        self.queue = deque(codes)
        self.lock = threading.Lock()
        self.log = []
        self.done = {}
        self.inflight = 0
        self.peak = 0
        self.t0 = None

    def take(self):
        with self.lock:
            return self.queue.popleft() if self.queue else None

    def enter(self):
        with self.lock:
            self.inflight += 1
            self.peak = max(self.peak, self.inflight)
            return self.inflight

    def leave(self):
        with self.lock:
            self.inflight -= 1

    def record(self, lane, code, st, ms, ok, t_send, t_recv, inflight):
        with self.lock:
            self.log.append({
                "相对ms": int((t_send - self.t0) * 1000),
                "发出时刻": ms_str(t_send),
                "返回时刻": ms_str(t_recv),
                "通道": lane, "条码": code, "状态": str(st),
                "耗时ms": int(ms), "在途并发": inflight,
                "结果": "正确" if ok else "",
            })


def lane_worker(hub, name, delay):
    time.sleep(delay)                      # 首次出发的错峰
    while True:
        code = hub.take()
        if code is None:
            return
        attempts = 0
        while True:
            attempts += 1
            t_send = time.time()
            inflight = hub.enter()
            st, ok, ms, pname = one_call(code)
            t_recv = time.time()
            hub.leave()
            hub.record(name, code, st, ms, ok, t_send, t_recv, inflight)
            if ok:
                with hub.lock:
                    hub.done[code] = {"通道": name, "条码": code, "尝试次数": attempts,
                                      "完成相对ms": int((t_recv - hub.t0) * 1000),
                                      "产品名称": pname}
                print("  +%6dms [%s] %s ✓ %s（第%d次，耗时%dms，在途%d）"
                      % ((t_recv - hub.t0) * 1000, name, code, pname[:22], attempts, ms, inflight))
                break
            if attempts >= MAX_ATTEMPTS:
                with hub.lock:
                    hub.done[code] = {"通道": name, "条码": code, "尝试次数": attempts,
                                      "完成相对ms": int((t_recv - hub.t0) * 1000), "产品名称": ""}
                print("  +%6dms [%s] %s ✗ 放弃（%d次）"
                      % ((t_recv - hub.t0) * 1000, name, code, attempts))
                break
            time.sleep(RETRY_GAP)


def main():
    with open(os.path.join(HERE, "..", "data", "10个测试码.txt"), encoding="utf-8-sig") as f:
        codes = [ln.strip() for ln in f if ln.strip()][:10]

    print("冷却 20 秒，让名额回满…")
    time.sleep(20)

    print("\n" + "=" * 78)
    print("A/B/C 三通道 · 10 个码 · 首次出发间隔 0.3s · 毫秒级时间戳")
    print("开始 %s" % ms_str(time.time()))
    print("=" * 78)

    hub = Hub(codes)
    hub.t0 = time.time()
    threads = [threading.Thread(target=lane_worker, args=(hub, n, d), name=n)
               for n, d in LANES]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=200)
    wall = time.time() - hub.t0

    ok_n = sum(1 for v in hub.done.values() if v["产品名称"])
    dist = {n: sum(1 for v in hub.done.values() if v["通道"] == n) for n, _ in LANES}
    c429 = sum(1 for r in hub.log if r["状态"] == "429")

    print("\n" + "=" * 78)
    print("完整请求流水（毫秒级）")
    print("=" * 78)
    print("  %-10s %-13s %-13s %-4s %-16s %-4s %-7s %-4s" %
          ("相对ms", "发出时刻", "返回时刻", "通道", "条码", "状态", "耗时ms", "在途"))
    for r in hub.log:
        print("  %-10d %-13s %-13s %-4s %-16s %-4s %-7d %-4d"
              % (r["相对ms"], r["发出时刻"], r["返回时刻"], r["通道"],
                 r["条码"], r["状态"], r["耗时ms"], r["在途并发"]))

    print("\n" + "=" * 78)
    print("结果")
    print("=" * 78)
    print("  成功拿到内容 ：%d / %d" % (ok_n, len(codes)))
    print("  总耗时       ：%.3f 秒（%d ms）" % (wall, wall * 1000))
    print("  平均         ：%.0f ms/条" % (wall * 1000 / len(codes)))
    print("  实际请求数   ：%d（429 共 %d）  有效率 %.0f%%"
          % (len(hub.log), c429, ok_n * 100.0 / max(len(hub.log), 1)))
    print("  通道分布     ：" + "，".join("%s %d 条" % (k, v) for k, v in dist.items()))
    print("  峰值在途并发 ：%d" % hub.peak)
    print("  实测吞吐     ：%.1f 条/分钟" % (ok_n / wall * 60))

    print("\n  成功时刻（毫秒，按完成先后）：")
    for code, v in sorted(hub.done.items(), key=lambda x: x[1]["完成相对ms"]):
        print("    +%6dms  [%s]  %s  %s（%d次尝试）"
              % (v["完成相对ms"], v["通道"], code, v["产品名称"][:24], v["尝试次数"]))

    os.makedirs(os.path.dirname(RESULT_CSV), exist_ok=True)
    with open(RESULT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["相对ms", "发出时刻", "返回时刻", "通道",
                                          "条码", "状态", "耗时ms", "在途并发", "结果"])
        w.writeheader()
        w.writerows(hub.log)
    print("\n已保存：%s" % RESULT_CSV)


if __name__ == "__main__":
    main()
