# -*- coding: utf-8 -*-
"""
udi_nmpa 命令行入口

    python cli.py 06971714900754                     查看单个条码
    python cli.py 06971714900754 --json              输出 JSON
    python cli.py -f data/条码列表.txt -o out.csv    批量查并导出 CSV
    python cli.py 主动脉瓣 --limit 20 --interval 3   模糊查，最多 20 条，间隔 3 秒
    python cli.py -f list.txt --interval 2.5 --resume 极限模式 + 断点续跑
"""

import argparse
import csv
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8") if hasattr(sys.stdout, "reconfigure") else None

from udi_nmpa import UdiClient, UdiError, RateLimitError


def do_single(client, keyword, as_json, limit):
    rows = client.search_all(keyword, max_records=limit)
    if as_json:
        print(json.dumps([client.to_cn(r) for r in rows], ensure_ascii=False, indent=2))
        return
    if not rows:
        print("未查到「%s」" % keyword)
        return
    for i, r in enumerate(rows, 1):
        if len(rows) > 1:
            print("\n--- 第 %d 条 ---" % i)
        rec = client.to_cn(r)
        for label in client.csv_fields():
            print("%-22s %s" % (label + ":", rec[label]))


def do_batch(client, path, out_csv, resume):
    with open(path, encoding="utf-8-sig") as f:
        keys = [ln.strip() for ln in f if ln.strip()]
    if not keys:
        print("清单为空")
        return

    done = {}
    if resume and out_csv and os.path.exists(out_csv):
        with open(out_csv, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                done[row.get("查询标识", "")] = row
        print("断点续跑：已有 %d 条结果，跳过" % len(done))

    todo = [k for k in keys if k not in done]
    print("待查 %d 条 / 共 %d 条，间隔 %.1f 秒，预计 %.1f 分钟"
          % (len(todo), len(keys), client.limiter.min_interval,
             len(todo) * client.limiter.min_interval / 60.0))

    results = dict(done)
    fail = 0
    for i, k in enumerate(todo, 1):
        try:
            rec = client.get_by_di(k)
        except RateLimitError as e:
            print("[%d/%d] %s ✗ %s" % (i, len(todo), k, e))
            fail += 1
            continue
        except UdiError as e:
            print("[%d/%d] %s ✗ %s" % (i, len(todo), k, e))
            fail += 1
            continue
        if rec:
            row = {"查询标识": k}
            row.update({lb: rec[lb] for lb in client.csv_fields()})
            print("[%d/%d] %s ✓ %s" % (i, len(todo), k, rec["产品名称/通用名称"]))
        else:
            row = {"查询标识": k}
            row.update({lb: "" for lb in client.csv_fields()})
            print("[%d/%d] %s ✗ 未查到" % (i, len(todo), k))
        results[k] = row
        if out_csv:
            write_csv(out_csv, keys, results)      # 每条落地，断电也不丢

    hit = sum(1 for v in results.values() if v.get("产品名称/通用名称"))
    print("\n完成：命中 %d / %d，失败 %d" % (hit, len(keys), fail))
    if out_csv:
        write_csv(out_csv, keys, results)
        print("已保存：%s" % os.path.abspath(out_csv))


def write_csv(path, order, results):
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["查询标识"] + UdiClient.csv_fields())
        w.writeheader()
        for k in order:
            if k in results:
                w.writerow(results[k])


def main():
    ap = argparse.ArgumentParser(description="NMPA UDI 纯后台查询")
    ap.add_argument("keyword", nargs="?", help="产品标识 / 产品名称 / 企业名称")
    ap.add_argument("-f", "--file", help="批量：每行一个标识的文本文件")
    ap.add_argument("-o", "--output", default="", help="批量结果 CSV 路径")
    ap.add_argument("--interval", type=float, default=2.5,
                    help="请求间隔秒数，默认 2.5（实测极限）；保守可用 3")
    ap.add_argument("--limit", type=int, default=50, help="模糊查最多取几条，默认 50")
    ap.add_argument("--json", action="store_true", help="以 JSON 输出")
    ap.add_argument("--resume", action="store_true", help="断点续跑（跳过已存在的标识）")
    a = ap.parse_args()

    client = UdiClient(min_interval=a.interval)
    try:
        if a.file:
            do_batch(client, a.file, a.output, a.resume)
        elif a.keyword:
            do_single(client, a.keyword, a.json, a.limit)
        else:
            ap.print_help()
    except KeyboardInterrupt:
        print("\n已中断（用 --resume 可续跑）")


if __name__ == "__main__":
    main()
