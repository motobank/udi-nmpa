# -*- coding: utf-8 -*-
"""
UDI 接口 · 用「注册证号」检索的可行性与效果测试

背景：此前一直用产品标识（DI）检索。本次改用注册证号 `国械注准20223131010`。
字段清单接口 /deviceField.html 显示第 29 个字段是 REGCARD_NAME（注册证/备案证号），
页面高级搜索的查询语法为 `字段名:值`（字段名用英文 CONFIG_NAME）。

测试四种写法，看哪种能命中，以及返回内容是否带注册证号。
输出：bench/results/regcard_result.csv
"""
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

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
RESULT_CSV = os.path.join(HERE, "results", "regcard_result.csv")
GAP = 5.8          # 实测安全周期

REGCARD = "国械注准20223131010"

CASES = [
    ("① 普通搜索", REGCARD, 1),
    ("② 高级·英文段名", "REGCARD_NAME:%s" % REGCARD, 2),
    ("③ 高级·英文段名+引号", 'REGCARD_NAME:"%s"' % REGCARD, 2),
    ("④ 高级·中文段名", "注册证/备案证号:%s" % REGCARD, 2),
]


def call(query, search_type, rows=50):
    body = urllib.parse.urlencode({"query": query, "searchType": str(search_type),
                                   "page": "1", "rows": str(rows),
                                   "sidx": "", "sord": "asc", "_search": "false"}).encode()
    req = urllib.request.Request(API, data=body, headers=HEADERS, method="POST")
    t0 = time.time()
    try:
        with OPENER.open(req, timeout=25) as r:
            d = json.loads(r.read().decode("utf-8"))
            return (r.status, d.get("records", 0), d.get("rows") or [],
                    round((time.time() - t0) * 1000))
    except urllib.error.HTTPError as e:
        return e.code, 0, [], round((time.time() - t0) * 1000)
    except Exception as e:
        return "ERR:%s" % type(e).__name__, 0, [], round((time.time() - t0) * 1000)


def main():
    print("=" * 76)
    print("用注册证号检索：%s" % REGCARD)
    print("=" * 76)

    out = []
    for name, query, st in CASES:
        time.sleep(GAP)
        status, records, rows, ms = call(query, st)
        print("\n%s  [searchType=%d]" % (name, st))
        print("   查询串 : %s" % query)
        print("   结果   : HTTP=%s  records=%s  返回行数=%d  %dms"
              % (status, records, len(rows), ms))
        if rows:
            for r in rows[:3]:
                print("     %s | %s | %s" % (r.get("primaryDeviceId"),
                                             r.get("companyName", "")[:20],
                                             r.get("productName", "")[:28]))
            print("   返回字段: %s" % ", ".join(sorted(rows[0].keys())))
        out.append({"用例": name, "searchType": st, "查询串": query,
                    "HTTP": status, "records": records, "返回行数": len(rows),
                    "首行产品标识": rows[0].get("primaryDeviceId", "") if rows else "",
                    "首行注册人": rows[0].get("companyName", "") if rows else "",
                    "首行产品名": rows[0].get("productName", "") if rows else "",
                    "耗时ms": ms})

    # 补充：用命中的产品再去普通搜索，看返回是否含注册证号字段
    hit = [o for o in out if o["返回行数"] > 0]
    if hit:
        di = hit[0]["首行产品标识"]
        time.sleep(GAP)
        status, records, rows, ms = call(di, 1)
        print("\n【交叉验证】用命中的 DI 反查：%s" % di)
        print("   records=%s  字段=%s" % (records, ", ".join(sorted(rows[0].keys())) if rows else "无"))
        print("   → 返回体里%s注册证号字段" % ("含" if rows and any("regcard" in k.lower() for k in rows[0]) else "不含"))
        out.append({"用例": "⑤ 反查DI", "searchType": 1, "查询串": di, "HTTP": status,
                    "records": records, "返回行数": len(rows),
                    "首行产品标识": di,
                    "首行注册人": rows[0].get("companyName", "") if rows else "",
                    "首行产品名": rows[0].get("productName", "") if rows else "",
                    "耗时ms": ms})

    os.makedirs(os.path.dirname(RESULT_CSV), exist_ok=True)
    with open(RESULT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out[0].keys()))
        w.writeheader()
        w.writerows(out)
    print("\n已保存：%s" % RESULT_CSV)


if __name__ == "__main__":
    main()
