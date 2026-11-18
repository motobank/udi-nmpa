# -*- coding: utf-8 -*-
"""
UDI 接口 · 「卡点开闸」自适应测试（对比实验八/九）

思路：
  实测发现名额是【按窗口整批放】的（成功时刻成对出现）。
  所以不该"每秒撞一次 429"，而应该：
    1. 维护一个"开闸周期"，每周期一次性放 2 个名额给 A/B 两条通道
    2. 周期长度自适应：遇 429 拉长 +0.5s，连续 4 次干净成功则收紧 -0.3s
    3. 名额用完后，两条通道都安静地等到下一个开闸时刻，绝不在空档里乱撞

目标：10 个码全部拿到，且请求数从 44（双通道乱撞）压到 12 左右。

输出：bench/results/pacer_result.csv
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
RESULT_CSV = os.path.join(HERE, "results", "pacer_result.csv")

INIT_INTERVAL = 5.5
MIN_INTERVAL = 4.0
MAX_INTERVAL = 15.0
SLOTS_PER_CYCLE = 2
MAX_ATTEMPTS = 10


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


class Pacer:
    """按周期发放名额。一个周期放 2 个，周期长度自适应。"""

    def __init__(self, interval=INIT_INTERVAL):
        self.interval = interval
        self.lock = threading.Lock()
        self.next_open = time.monotonic()      # 首轮立即开闸
        self.slots_left = SLOTS_PER_CYCLE
        self.clean_streak = 0
        self.cycle_no = 1
        self.history = []

    def wait_turn(self):
        """阻塞直到拿到一个名额，返回 (是否新周期, 当时周期长度)"""
        while True:
            with self.lock:
                now = time.monotonic()
                if now >= self.next_open:
                    if self.slots_left > 0:
                        self.slots_left -= 1
                        new_cycle = (self.slots_left == SLOTS_PER_CYCLE - 1)
                        self.history.append({"周期": self.cycle_no, "间隔": round(self.interval, 2),
                                             "时刻": round((now - T0) * 1000)})
                        return new_cycle, self.interval
                    # ★ 关键修正：按"上次开闸时刻 + 周期"推进，而不是"现在 + 周期"
                    #   否则每个周期都会被请求耗时（~0.7s）拖长，5 轮就白丢 3.5 秒
                    self.next_open += self.interval
                    while self.next_open <= now:
                        self.next_open += self.interval
                    self.slots_left = SLOTS_PER_CYCLE
                    self.cycle_no += 1
                    continue
                sleep_for = self.next_open - now
            time.sleep(max(min(sleep_for, 1.0), 0.002))

    def feedback(self, ok):
        with self.lock:
            if ok:
                self.clean_streak += 1
                if self.clean_streak >= 4:
                    self.interval = max(MIN_INTERVAL, round(self.interval - 0.3, 2))
                    self.clean_streak = 0
            else:
                self.clean_streak = 0
                self.interval = min(MAX_INTERVAL, round(self.interval + 0.5, 2))


T0 = time.time()


class Hub:
    def __init__(self, codes, pacer):
        self.queue = deque(codes)
        self.pacer = pacer
        self.lock = threading.Lock()
        self.log = []
        self.done = {}
        self.t0 = None

    def take(self):
        with self.lock:
            return self.queue.popleft() if self.queue else None

    def record(self, lane, code, st, ms, ok, t_send, inflight):
        with self.lock:
            self.log.append({"相对ms": int((t_send - self.t0) * 1000),
                             "发出时刻": ms_str(t_send), "通道": lane, "条码": code,
                             "状态": str(st), "耗时ms": int(ms),
                             "周期间隔s": self.pacer.interval, "结果": "正确" if ok else ""})


def lane_worker(hub, name):
    while True:
        code = hub.take()
        if code is None:
            return
        attempts = 0
        while True:
            attempts += 1
            hub.pacer.wait_turn()                  # ★ 只在开闸时刻出手
            t_send = time.time()
            st, ok, ms, pname = one_call(code)
            hub.pacer.feedback(ok)
            hub.record(name, code, st, ms, ok, t_send, 0)
            if ok:
                with hub.lock:
                    hub.done[code] = {"通道": name, "条码": code, "尝试次数": attempts,
                                      "完成相对ms": int((time.time() - hub.t0) * 1000),
                                      "产品名称": pname}
                print("  +%6dms [%s] %s ✓ %s（第%d次，耗时%dms，当时周期%.1fs）"
                      % ((time.time() - hub.t0) * 1000, name, code, pname[:20],
                         attempts, ms, hub.pacer.interval))
                break
            if attempts >= MAX_ATTEMPTS:
                with hub.lock:
                    hub.done[code] = {"通道": name, "条码": code, "尝试次数": attempts,
                                      "完成相对ms": int((time.time() - hub.t0) * 1000), "产品名称": ""}
                break


def main(init=INIT_INTERVAL, min_i=MIN_INTERVAL, max_i=MAX_INTERVAL, tag=""):
    global MIN_INTERVAL, MAX_INTERVAL
    MIN_INTERVAL, MAX_INTERVAL = min_i, max_i
    with open(os.path.join(HERE, "..", "data", "10个测试码.txt"), encoding="utf-8-sig") as f:
        codes = [ln.strip() for ln in f if ln.strip()][:10]

    print("冷却 20 秒…")
    time.sleep(20)

    print("\n" + "=" * 74)
    print("卡点开闸 · A/B 双通道 · 10 个码 · 自适应周期（初始 %.1fs，下限 %.1fs%s）"
          % (init, min_i, "，" + tag if tag else ""))
    print("开始 %s" % ms_str(time.time()))
    print("=" * 74)

    pacer = Pacer(interval=init)
    hub = Hub(codes, pacer)
    hub.t0 = time.time()
    threads = [threading.Thread(target=lane_worker, args=(hub, n), name=n) for n in ("A", "B")]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=200)
    wall = time.time() - hub.t0

    ok_n = sum(1 for v in hub.done.values() if v["产品名称"])
    c429 = sum(1 for r in hub.log if r["状态"] == "429")

    print("\n" + "=" * 74)
    print("结果对比")
    print("=" * 74)
    print("  成功         ：%d / %d" % (ok_n, len(codes)))
    print("  总耗时       ：%.3f 秒" % wall)
    print("  请求数       ：%d（429 共 %d）" % (len(hub.log), c429))
    print("  有效率       ：%.0f%%" % (ok_n * 100.0 / max(len(hub.log), 1)))
    print("  最终周期长度 ：%.1f 秒" % pacer.interval)
    print("  通道分布     ：A %d / B %d"
          % (sum(1 for v in hub.done.values() if v["通道"] == "A"),
             sum(1 for v in hub.done.values() if v["通道"] == "B")))
    print("\n  —— 与前面几轮对比 ——")
    print("  %-18s %-10s %-10s %-10s" % ("方案", "耗时", "请求数", "有效率"))
    print("  %-18s %-10s %-10s %-10s" % ("10 线程乱抢", "26.9s", "?", "?"))
    print("  %-18s %-10s %-10s %-10s" % ("A/B 双通道(1s重试)", "22.6s", "44", "23%"))
    print("  %-18s %-10s %-10s %-10s" % ("A/B/C 三通道", "22.4s", "59", "17%"))
    print("  %-18s %-10s %-10s %-10s" % ("卡点开闸(本次)", "%.1fs" % wall,
                                         str(len(hub.log)), "%.0f%%" % (ok_n * 100.0 / max(len(hub.log), 1))))

    print("\n  完整请求流水：")
    for r in hub.log:
        print("    +%6dms [%s] %s %-4s %4dms 周期%.1fs %s"
              % (r["相对ms"], r["通道"], r["条码"], r["状态"],
                 r["耗时ms"], r["周期间隔s"], "✓" if r["结果"] else ""))

    os.makedirs(os.path.dirname(RESULT_CSV), exist_ok=True)
    with open(RESULT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["相对ms", "发出时刻", "通道", "条码",
                                          "状态", "耗时ms", "周期间隔s", "结果"])
        w.writeheader()
        w.writerows(hub.log)
    print("\n已保存：%s" % RESULT_CSV)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--init", type=float, default=INIT_INTERVAL, help="初始周期秒数")
    ap.add_argument("--min", type=float, default=MIN_INTERVAL, help="周期下限")
    ap.add_argument("--max", type=float, default=MAX_INTERVAL, help="周期上限")
    ap.add_argument("--tag", default="", help="标签，用于结果文件名")
    a = ap.parse_args()
    if a.tag:
        RESULT_CSV = os.path.join(HERE, "results", "pacer_result_%s.csv" % a.tag)
    main(init=a.init, min_i=a.min, max_i=a.max, tag=a.tag)
