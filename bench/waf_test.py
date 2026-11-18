# -*- coding: utf-8 -*-
"""
NMPA UDI 接口 · 并发/防火墙行为探测
目的：分别以 10/20/30/40/50 路并发请求，观察是否触发 WAF、限流或封禁
注意：单档请求量极小（<=50），属正常压测范畴；档间强制冷却，避免污染
输出：waf_test_result.csv + waf_test_raw.json
"""
import sys, os, json, time, csv, threading
import urllib.request, urllib.parse, urllib.error
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8") if hasattr(sys.stdout, "reconfigure") else None

API = "https://udi.nmpa.gov.cn/getDeviceList.html"
IDS = ["06971714901171", "06971714901195", "06971714900426"]
LEVELS = [10, 20, 30, 40, 50]
COOLDOWN = 30          # 每档之间冷却秒数

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/157.0.0.0 Safari/537.36",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Referer": "https://udi.nmpa.gov.cn/showListXXCX.html",
    "X-Requested-With": "XMLHttpRequest",
}

# 关键：绕过系统代理，直连
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def one_request(keyword, barrier, idx):
    """发一次请求，返回观测结果"""
    body = urllib.parse.urlencode({
        "query": keyword, "searchType": "1", "page": "1",
        "rows": "50", "sidx": "", "sord": "asc", "_search": "false",
    }).encode("utf-8")
    req = urllib.request.Request(API, data=body, headers=HEADERS, method="POST")

    barrier.wait()                      # 所有线程在此对齐，同一瞬间发出
    t0 = time.time()
    res = {"idx": idx, "keyword": keyword, "status": None, "ms": None,
           "ok": False, "rows": 0, "snippet": "", "server": "", "cookie": "",
           "retry_after": "", "ctype": "", "err": ""}
    try:
        with OPENER.open(req, timeout=30) as r:
            raw = r.read()
            res["status"] = r.status
            res["ms"] = round((time.time() - t0) * 1000)
            res["server"] = r.headers.get("Server", "")
            res["ctype"] = r.headers.get("Content-Type", "")
            res["retry_after"] = r.headers.get("Retry-After", "")
            sc = r.headers.get_all("Set-Cookie") or []
            res["cookie"] = " | ".join(sc)[:120]
            try:
                data = json.loads(raw.decode("utf-8"))
                rows = data.get("rows") or []
                res["rows"] = len(rows)
                res["ok"] = len(rows) > 0
                res["snippet"] = (rows[0].get("productName", "") if rows else "")[:30]
            except Exception:
                res["snippet"] = raw.decode("utf-8", "ignore")[:120].replace("\n", " ")
    except urllib.error.HTTPError as e:
        res["status"] = e.code
        res["ms"] = round((time.time() - t0) * 1000)
        res["retry_after"] = e.headers.get("Retry-After", "") if e.headers else ""
        res["server"] = e.headers.get("Server", "") if e.headers else ""
        try:
            res["snippet"] = e.read().decode("utf-8", "ignore")[:150].replace("\n", " ")
        except Exception:
            pass
    except Exception as e:
        res["ms"] = round((time.time() - t0) * 1000)
        res["err"] = type(e).__name__ + ": " + str(e)[:80]
    return res


def run_level(n):
    """一档并发：n 个线程同时发，请求在 3 个标识间轮转"""
    barrier = threading.Barrier(n)
    tasks = [(IDS[i % len(IDS)], barrier, i) for i in range(n)]
    with ThreadPoolExecutor(max_workers=n) as ex:
        futs = [ex.submit(one_request, kw, b, i) for kw, b, i in tasks]
        return [f.result() for f in futs]


def summarize(results):
    st = {}
    for r in results:
        key = str(r["status"]) if r["status"] else ("ERR:" + r["err"].split(":")[0])
        st[key] = st.get(key, 0) + 1
    ms = sorted([r["ms"] for r in results if r["ms"] is not None])
    succ = sum(1 for r in results if r["ok"])
    p50 = ms[len(ms) // 2] if ms else 0
    p95 = ms[int(len(ms) * 0.95) - 1] if len(ms) > 1 else (ms[0] if ms else 0)
    return {"success": succ, "total": len(results), "codes": st,
            "min": ms[0] if ms else 0, "max": ms[-1] if ms else 0, "p50": p50, "p95": p95}


def probe_once():
    """冷却后探一次，判断是否已恢复"""
    try:
        req = urllib.request.Request(API, data=urllib.parse.urlencode(
            {"query": IDS[0], "searchType": "1", "page": "1", "rows": "50"}).encode(),
            headers=HEADERS, method="POST")
        with OPENER.open(req, timeout=25) as r:
            d = json.loads(r.read().decode())
            return r.status, len(d.get("rows") or [])
    except urllib.error.HTTPError as e:
        return e.code, 0
    except Exception as e:
        return "ERR", 0


def main():
    all_data, rows_out = [], []
    print("基线探测：", probe_once(), "\n" + "=" * 60)

    for n in LEVELS:
        print("\n【并发 %d】%s 起跑…" % (n, datetime.now().strftime("%H:%M:%S")))
        res = run_level(n)
        s = summarize(res)
        print("  状态码分布 : %s" % s["codes"])
        print("  成功取数   : %d/%d" % (s["success"], s["total"]))
        print("  耗时 ms    : min=%d p50=%d p95=%d max=%d" % (s["min"], s["p50"], s["p95"], s["max"]))
        bad = [r for r in res if not r["ok"]]
        if bad:
            print("  首个异常   : status=%s server=%s retry_after=%s body=%s"
                  % (bad[0]["status"], bad[0]["server"], bad[0]["retry_after"], bad[0]["snippet"][:90]))
        all_data.append({"level": n, "summary": s, "detail": res})
        rows_out.append({
            "并发数": n, "请求数": s["total"], "成功数": s["success"],
            "成功率": "%.0f%%" % (s["success"] * 100.0 / s["total"]),
            "状态码分布": json.dumps(s["codes"], ensure_ascii=False),
            "最小ms": s["min"], "P50ms": s["p50"], "P95ms": s["p95"], "最大ms": s["max"],
            "异常样本": (bad[0]["snippet"][:100] if bad else ""),
        })

        if n != LEVELS[-1]:
            print("  冷却 %d 秒…" % COOLDOWN, end="", flush=True)
            for _ in range(COOLDOWN // 5):
                time.sleep(5)
                print(".", end="", flush=True)
            print(" 冷却后探测=%s" % (probe_once(),))

    with open("waf_test_result.csv", "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows_out[0].keys()))
        w.writeheader(); w.writerows(rows_out)
    with open("waf_test_raw.json", "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=1)
    print("\n" + "=" * 60 + "\n已保存 waf_test_result.csv / waf_test_raw.json")


if __name__ == "__main__":
    main()
