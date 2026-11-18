# -*- coding: utf-8 -*-
"""
NMPA UDI · 单条串行「间隔极限」测试
方法：严格一次一条（无线程、无并发），请求发出时刻之间的间隔 = 设定值
阶段A：1.5s→2.6s 每 0.1s 一档，每档连发 5 条，记录成败样貌
阶段B：临界档位 3 轮复测（每轮 8 条），排除偶然
阶段C：打爆后测恢复时间（多久能重新拿到 200）
"""
import sys, json, time, csv
import urllib.request, urllib.parse, urllib.error
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8") if hasattr(sys.stdout, "reconfigure") else None

API = "https://udi.nmpa.gov.cn/getDeviceList.html"
IDS = ["06971714901171", "06971714901195", "06971714900426"]
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/157.0.0.0 Safari/537.36",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Referer": "https://udi.nmpa.gov.cn/showListXXCX.html",
    "X-Requested-With": "XMLHttpRequest",
}
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

GAPS = [1.5, 1.6, 1.7, 1.8, 1.9, 2.0, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6]
PER_GAP = 5
ROUND_COOLDOWN = 10


def hit(kw):
    body = urllib.parse.urlencode({"query": kw, "searchType": "1", "page": "1",
                                   "rows": "50"}).encode()
    req = urllib.request.Request(API, data=body, headers=HEADERS, method="POST")
    t0 = time.time()
    try:
        with OPENER.open(req, timeout=25) as r:
            d = json.loads(r.read().decode())
            return r.status, len(d.get("rows") or []), round((time.time() - t0) * 1000)
    except urllib.error.HTTPError as e:
        return e.code, 0, round((time.time() - t0) * 1000)
    except Exception as e:
        return "ERR:%s" % type(e).__name__, 0, round((time.time() - t0) * 1000)


def serial_run(gap, count, tag=""):
    """严格串行：保持「请求发出时刻」间隔 = gap"""
    seq, ok = [], 0
    next_at = time.time()
    for i in range(count):
        now = time.time()
        if now < next_at:
            time.sleep(next_at - now)
        st, rows, ms = hit(IDS[i % len(IDS)])
        next_at = time.time() + gap          # 以下一次「发出」时刻为基准
        good = (st == 200 and rows > 0)
        if good:
            ok += 1
        seq.append(str(st))
        print("     #%d %-4s %4dms" % (i + 1, st, ms))
    print("   %s=> 成功 %d/%d  序列[%s]" % (tag, ok, count, ",".join(seq)))
    return ok, seq


def main():
    result_rows = []
    print("=" * 68)
    print("阶段A：细粒度扫描（严格串行，一次一条）")
    print("=" * 68)
    for gap in GAPS:
        print("\n【间隔 %.1fs】%s" % (gap, datetime.now().strftime("%H:%M:%S")))
        ok, seq = serial_run(gap, PER_GAP)
        result_rows.append({"阶段": "A-扫描", "间隔s": gap, "条数": PER_GAP,
                            "成功": ok, "成功率": "%d%%" % (ok * 100 // PER_GAP),
                            "序列": ",".join(seq)})
        if gap != GAPS[-1]:
            print("   冷却 %ds…" % ROUND_COOLDOWN)
            time.sleep(ROUND_COOLDOWN)

    # 找临界：第一档全成功的
    full = [r["间隔s"] for r in result_rows if r["成功"] == PER_GAP]
    edge = full[0] if full else GAPS[-1]
    print("\n" + "=" * 68)
    print("阶段B：临界值 %.1fs 复测 ×3 轮（每轮 8 条）" % edge)
    print("=" * 68)
    b_ok = 0
    for r in range(3):
        print("\n  第 %d 轮 / 共 3 轮" % (r + 1))
        ok, seq = serial_run(edge, 8, tag="轮%d " % (r + 1))
        b_ok += ok
        result_rows.append({"阶段": "B-复测", "间隔s": edge, "条数": 8,
                            "成功": ok, "成功率": "%d%%" % (ok * 100 // 8),
                            "序列": ",".join(seq)})
        if r < 2:
            print("   冷却 %ds…" % ROUND_COOLDOWN)
            time.sleep(ROUND_COOLDOWN)

    print("\n" + "=" * 68)
    print("阶段C：打爆后恢复时间")
    print("=" * 68)
    for i in range(5):                      # 连发 5 条打爆
        hit(IDS[i % 3])
    print("  已连发 5 条制造限流，开始每 0.3s 探测一次恢复…")
    t0 = time.time()
    recovered = None
    probes = []
    while time.time() - t0 < 60:
        st, rows, ms = hit(IDS[0])
        el = round(time.time() - t0, 1)
        probes.append("%.1fs:%s" % (el, st))
        print("   +%.1fs  %s" % (el, st))
        if st == 200 and rows > 0:
            recovered = el
            break
        time.sleep(0.3)
    print("  => 恢复用时：%s 秒" % (recovered if recovered is not None else ">60"))
    result_rows.append({"阶段": "C-恢复", "间隔s": "-", "条数": 5,
                        "成功": "", "成功率": "", "序列": ";".join(probes)})

    with open("serial_test_result.csv", "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["阶段", "间隔s", "条数", "成功", "成功率", "序列"])
        w.writeheader(); w.writerows(result_rows)
    print("\n已保存 serial_test_result.csv")


if __name__ == "__main__":
    main()
