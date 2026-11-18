# -*- coding: utf-8 -*-
"""
精度测试：找出连续请求的最小安全间隔
每组：以固定间隔连发 5 次，统计成功次数；组间冷却 12 秒
"""
import sys, json, time
import urllib.request, urllib.parse, urllib.error

sys.stdout.reconfigure(encoding="utf-8") if hasattr(sys.stdout, "reconfigure") else None

API = "https://udi.nmpa.gov.cn/getDeviceList.html"
IDS = ["06971714901171", "06971714901195", "06971714900426"]
GAPS = [0.2, 0.5, 0.8, 1.0, 1.5, 2.0]
PER = 5

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/157.0.0.0 Safari/537.36",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Referer": "https://udi.nmpa.gov.cn/showListXXCX.html",
    "X-Requested-With": "XMLHttpRequest",
}
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


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
        return "ERR", 0, round((time.time() - t0) * 1000)


print("目标：每分钟允许几次？连续请求间隔多少才不被拒？")
print("=" * 66)
for gap in GAPS:
    print("\n间隔 %.1f 秒 × %d 次：" % (gap, PER))
    ok = 0
    codes = []
    for i in range(PER):
        st, rows, ms = hit(IDS[i % len(IDS)])
        codes.append(str(st))
        if st == 200 and rows > 0:
            ok += 1
        print("    #%d %s  %dms" % (i + 1, st, ms))
        if i < PER - 1:
            time.sleep(gap)
    print("  => 成功 %d/%d  [%s]" % (ok, PER, ",".join(codes)))
    if gap != GAPS[-1]:
        print("  冷却 12 秒…")
        time.sleep(12)
