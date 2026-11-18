# -*- coding: utf-8 -*-
"""
UDI 接口 · A/B 双通道消费模型测试

模型：
  - 10 个码进 FIFO 队列（严格按扫码顺序，避免饥饿）
  - A、B 两条通道各是一个线程
  - 任意通道拿到【正确数据】后，立刻取队首下一个码再次出发，不加人为等待
  - 遇 429 不立刻扑上去重打，等 1.0 秒再试（避免把 429 打成雪崩）

观测：
  - 10 个码是否全部拿到、总耗时
  - 实际发出去多少个请求（有效率 = 10 / 总请求）
  - 两条通道各消化几个、是否有饥饿
  - 全程请求时间线

输出：bench/results/dual_lane_result.csv
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
RESULT_CSV = os.path.join(HERE, "results", "dual_lane_result.csv")
LANES = 2
RETRY_GAP = 1.0          # 429 后的重试间隔
MAX_ATTEMPTS = 12


def one_call(code):
    body = urllib.parse.urlencode({"query": code, "searchType": "1",
                                   "page": "1", "rows": "50"}).encode()
    req = urllib.request.Request(API, data=body, headers=HEADERS, method="POST")
    t0 = time.time()
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


class Hub:
    def __init__(self, codes):
        self.queue = deque(codes)
        self.lock = threading.Lock()
        self.log = []            # 每次请求的记录
        self.done = {}           # code -> 记录
        self.codes = codes
        self.t0 = None

    def take(self):
        with self.lock:
            return self.queue.popleft() if self.queue else None

    def log_req(self, lane, code, st, ms, ok):
        with self.lock:
            self.log.append({"t": round(time.time() - self.t0, 2), "lane": lane,
                             "code": code, "status": str(st), "ms": ms, "ok": ok})


def lane_worker(hub, name):
    while True:
        code = hub.take()
        if code is None:
            return
        attempts = 0
        while True:
            attempts += 1
            st, ok, ms, pname = one_call(code)
            hub.log_req(name, code, st, ms, ok)
            if ok:
                with hub.lock:
                    hub.done[code] = {
                        "通道": name, "条码": code, "尝试次数": attempts,
                        "完成时刻s": round(time.time() - hub.t0, 2),
                        "产品名称": pname,
                    }
                print("  [%s] %s ✓ %s（第%d次尝试，%.1fs 完成）"
                      % (name, code, pname[:26], attempts, time.time() - hub.t0))
                break
            if attempts >= MAX_ATTEMPTS:
                with hub.lock:
                    hub.done[code] = {"通道": name, "条码": code, "尝试次数": attempts,
                                      "完成时刻s": round(time.time() - hub.t0, 2),
                                      "产品名称": ""}
                print("  [%s] %s ✗ 放弃（%d次）" % (name, code, attempts))
                break
            time.sleep(RETRY_GAP)


def main():
    with open(os.path.join(HERE, "..", "data", "10个测试码.txt"), encoding="utf-8-sig") as f:
        codes = [ln.strip() for ln in f if ln.strip()][:10]

    print("冷却 20 秒，让闸机名额回满…")
    time.sleep(20)

    print("\n" + "=" * 72)
    print("A/B 双通道 · %d 个码 · 谁拿到正确数据谁立刻再接一个" % len(codes))
    print("开始 %s" % datetime.now().strftime("%H:%M:%S"))
    print("=" * 72)

    hub = Hub(codes)
    hub.t0 = time.time()
    threads = [threading.Thread(target=lane_worker, args=(hub, n), name=n)
               for n in ("A", "B")]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=180)
    wall = time.time() - hub.t0

    ok_n = sum(1 for v in hub.done.values() if v["产品名称"])
    total_req = len(hub.log)
    c429 = sum(1 for r in hub.log if r["status"] == "429")
    a_n = sum(1 for v in hub.done.values() if v["通道"] == "A")
    b_n = sum(1 for v in hub.done.values() if v["通道"] == "B")

    print("\n" + "=" * 72)
    print("结果")
    print("=" * 72)
    print("  成功拿到内容 ：%d / %d" % (ok_n, len(codes)))
    print("  总耗时       ：%.1f 秒（平均 %.1f 秒/条）" % (wall, wall / len(codes)))
    print("  实际发请求   ：%d 个（其中 429 共 %d 个）" % (total_req, c429))
    print("  有效率       ：%.0f%%（%d 条内容 / %d 次请求）"
          % (ok_n * 100.0 / max(total_req, 1), ok_n, total_req))
    print("  通道分布     ：A 消化 %d 条，B 消化 %d 条" % (a_n, b_n))
    print("  实测吞吐     ：%.1f 条/分钟" % (ok_n / wall * 60))

    print("\n  订单时间线（按完成顺序）：")
    for code, v in sorted(hub.done.items(), key=lambda x: x[1]["完成时刻s"]):
        print("    %5.1fs  [%s]  %s  %s  （%d次尝试）"
              % (v["完成时刻s"], v["通道"], code, v["产品名称"][:24], v["尝试次数"]))

    print("\n  请求明细（前 40 条）：")
    for r in hub.log[:40]:
        print("    %5.1fs [%s] %s → %-3s %4dms %s"
              % (r["t"], r["lane"], r["code"], r["status"], r["ms"],
                 "✓" if r["ok"] else ""))

    os.makedirs(os.path.dirname(RESULT_CSV), exist_ok=True)
    with open(RESULT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["记录类型", "时刻s", "通道", "条码", "状态", "耗时ms", "尝试次数", "产品名称"])
        for r in hub.log:
            w.writerow(["请求", r["t"], r["lane"], r["code"], r["status"], r["ms"],
                        "", "✓" if r["ok"] else ""])
        for code, v in hub.done.items():
            w.writerow(["结果", v["完成时刻s"], v["通道"], code, "", "",
                        v["尝试次数"], v["产品名称"]])
    print("\n已保存：%s" % RESULT_CSV)


if __name__ == "__main__":
    main()
