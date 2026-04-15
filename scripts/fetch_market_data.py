#!/usr/bin/env python3
"""
市场数据验证与格式化脚本（Hard Gate）

本脚本不直接调用行情 API，而是验证 LLM 通过 stock-price-query / stock-info-explorer
等技能获取并保存的原始数据文件，输出一份带时间戳的标准化行情 Markdown。

用法：
    python scripts/fetch_market_data.py --date YYYY-MM-DD

流程：
1. 检查 data/raw_stock_{TICKER}.json 是否存在且非空
2. 解析收盘价、涨跌幅、时间戳
3. 输出 data/market_data_YYYY-MM-DD.md
4. 若有缺失，返回非零退出码并打印缺失列表
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

DEFAULT_INPUT_DIR = Path("data")
DEFAULT_OUTPUT_DIR = Path("data")

# 必须获取的基准资产池
REQUIRED_ASSETS = [
    {"ticker": "000001.SS", "name": "上证指数", "tz": "Asia/Shanghai"},
    {"ticker": "399001.SZ", "name": "深证成指", "tz": "Asia/Shanghai"},
    {"ticker": "^HSI", "name": "恒生指数", "tz": "Asia/Shanghai"},
    {"ticker": "^DJI", "name": "道琼斯指数", "tz": "America/New_York"},
    {"ticker": "^IXIC", "name": "纳斯达克指数", "tz": "America/New_York"},
    {"ticker": "BZ=F", "name": "布伦特原油", "tz": "America/New_York"},
    {"ticker": "GC=F", "name": "伦敦金现(COMEX黄金期货)", "tz": "America/New_York"},
]


def _extract_value(data: dict, keys: list, default=None):
    """从嵌套 dict 中按候选 key 列表提取值。"""
    for key in keys:
        if key in data:
            return data[key]
        # 尝试 snake_case / camelCase 变体
        alt_key = key[0].lower() + key[1:] if key else key
        if alt_key in data:
            return data[alt_key]
        alt_key2 = "".join(
            [w.capitalize() if i > 0 else w for i, w in enumerate(key.split("_"))]
        )
        if alt_key2 in data:
            return data[alt_key2]
    return default


def _parse_raw_json(path: Path) -> dict:
    """解析单个 raw_stock_*.json 文件，提取核心字段。"""
    try:
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as exc:
        return {"error": f"读取失败: {exc}"}

    # 支持两种常见结构：
    # 1) 直接是 dict {"close": 123, "change": 1.2, ...}
    # 2) 嵌套在 {"data": {...}} 或 {"result": {...}} 中
    data = raw
    for wrapper in ("data", "result", "response", "payload"):
        if isinstance(data, dict) and wrapper in data:
            data = data[wrapper]
            break

    if not isinstance(data, dict):
        return {"error": f"无法解析的结构: {type(data).__name__}"}

    close = _extract_value(data, ["close", "Close", "price", "Price", "latestPrice", "current_price"])
    prev = _extract_value(data, ["previousClose", "prev_close", "previous_close", "yesterday_close", "open"])
    change = _extract_value(data, ["change", "Change", "priceChange"])
    change_pct = _extract_value(data, ["changePercent", "change_percent", "changesPercentage", "pct_change", "涨跌幅"])
    ts = _extract_value(data, ["timestamp", "Timestamp", "date", "Date", "time", "Time", "updated_at", "last_updated"])

    # 若没给 change 但给了 close 和 prev，计算 change
    if change is None and close is not None and prev is not None:
        try:
            change = float(close) - float(prev)
        except Exception:
            pass

    # 若没给 change_pct 但给了 close 和 prev，计算 pct
    if change_pct is None and close is not None and prev is not None:
        try:
            c, p = float(close), float(prev)
            if p != 0:
                change_pct = (c - p) / p * 100
        except Exception:
            pass

    return {
        "close": close,
        "previous_close": prev,
        "change": change,
        "change_percent": change_pct,
        "timestamp_raw": ts,
        "raw_keys": list(data.keys())[:20],
    }


def validate_and_generate(date_str: str, input_dir: Path, output_dir: Path) -> int:
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    missing = []
    records = []

    for asset in REQUIRED_ASSETS:
        ticker = asset["ticker"]
        filename = f"raw_stock_{ticker.replace('/', '-')}.json"
        path = input_dir / filename

        if not path.exists() or path.stat().st_size == 0:
            missing.append(ticker)
            records.append({"ticker": ticker, "name": asset["name"], "status": "API暂无数据"})
            continue

        parsed = _parse_raw_json(path)
        if "error" in parsed:
            missing.append(f"{ticker}({parsed['error']})")
            records.append({"ticker": ticker, "name": asset["name"], "status": "API暂无数据", "error": parsed["error"]})
            continue

        if parsed["close"] is None:
            missing.append(f"{ticker}(缺少收盘价)")
            records.append({"ticker": ticker, "name": asset["name"], "status": "API暂无数据", "error": "缺少收盘价"})
            continue

        records.append({
            "ticker": ticker,
            "name": asset["name"],
            "status": "OK",
            "close": parsed["close"],
            "change": parsed["change"],
            "change_percent": parsed["change_percent"],
            "timestamp_raw": parsed["timestamp_raw"],
            "tz": asset["tz"],
        })

    # 生成 Markdown
    lines = [
        f"# 基准资产行情数据 ({date_str})",
        f"> 验证时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "> 数据来源：stock-price-query / stock-info-explorer",
        "",
        "## 强制使用说明",
        "- 以下数据是撰写《核心资产与市场异动》模块的**唯一数值基准**。**",
        "- 每条行情数据必须原样附带时间戳写入日报。**",
        "- 若某项状态为 'API暂无数据'，日报中必须写 'API暂无数据'，绝不允许捏造。**",
        "",
        "## 行情汇总",
        "",
    ]

    for r in records:
        if r["status"] != "OK":
            lines.append(f"### {r['name']} ({r['ticker']})")
            lines.append(f"- 状态：**{r['status']}**")
            if "error" in r:
                lines.append(f"- 错误详情：{r['error']}")
            lines.append("")
            continue

        close_str = f"{float(r['close']):,.2f}" if isinstance(r['close'], (int, float, str)) else str(r['close'])
        change_str = "N/A"
        if r["change"] is not None:
            change_val = float(r["change"])
            sign = "+" if change_val >= 0 else ""
            change_str = f"{sign}{change_val:,.2f}"

        pct_str = "N/A"
        if r["change_percent"] is not None:
            pct_val = float(r["change_percent"])
            sign = "+" if pct_val >= 0 else ""
            pct_str = f"{sign}{pct_val:.2f}%"

        ts_str = str(r["timestamp_raw"]) if r["timestamp_raw"] else "时间戳未提供"

        lines.append(f"### {r['name']} ({r['ticker']})")
        lines.append(f"- 收盘价：{close_str}")
        lines.append(f"- 涨跌额：{change_str}")
        lines.append(f"- 涨跌幅：{pct_str}")
        lines.append(f"- 时间戳：{ts_str} ({r['tz']})")
        lines.append("")

    output_path = output_dir / f"market_data_{date_str}.md"
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    if missing:
        print(f"[WARN] 以下资产数据缺失或解析失败：{', '.join(missing)}")
        print(f"[INFO] 已生成占位文件：{output_path}")
        return 1

    print(f"[OK] 所有基准资产数据验证通过。输出：{output_path}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="验证并格式化行情数据")
    parser.add_argument("--date", required=True, help="日期格式 YYYY-MM-DD")
    parser.add_argument("--input-dir", default=str(DEFAULT_INPUT_DIR), help="原始 JSON 文件目录")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="输出 Markdown 目录")
    args = parser.parse_args()

    return validate_and_generate(args.date, Path(args.input_dir), Path(args.output_dir))


if __name__ == "__main__":
    raise SystemExit(main())
