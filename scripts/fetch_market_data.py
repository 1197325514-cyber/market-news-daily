#!/usr/bin/env python3
"""
市场数据真实性门禁脚本（Hard Gate v2）

目标：
1. 防止“有文件但假数据”的形式主义验证。
2. 强制校验行情数据来源、标的匹配、时间新鲜度、数值有效性。
3. 对 ^DJI / ^IXIC 执行双源交叉验证，差异超阈值直接阻塞。

用法：
    python scripts/fetch_market_data.py --date YYYY-MM-DD

输入文件：
    data/raw_stock_*.json      # 主数据源（stock-price-query）
    data/raw_stock2_*.json     # 交叉数据源（stock-info-explorer，仅 ^DJI / ^IXIC 强制）

输出文件：
    data/market_data_YYYY-MM-DD.md
    data/market_data_YYYY-MM-DD.json
"""

import argparse
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

DEFAULT_INPUT_DIR = Path("data")
DEFAULT_OUTPUT_DIR = Path("data")
ALLOWED_SOURCES = ("stock-price-query", "stock-info-explorer")
MAX_STALE_HOURS = 72
CROSS_CHECK_MAX_GAP_PCT = 0.5
MAX_ABS_PCT_CHANGE = 30.0


def _clean_old_files(input_dir: Path) -> None:
    """删除旧的 raw_stock_*.json，防止缓存污染导致读取历史错误数据。"""
    if not input_dir.exists():
        return
    for p in input_dir.glob("raw_stock*.json"):
        try:
            p.unlink()
            print(f"[CLEAN] 已清理旧缓存：{p.name}")
        except Exception as exc:
            print(f"[WARN] 清理失败 {p.name}: {exc}")


def _check_report_date(ticker: str, ts: datetime, report_date_str: str, asset_tz: str) -> Optional[str]:
    """检测 timestamp 是否对齐报告日期的物理交易日定义。"""
    from datetime import date
    report_date = date.fromisoformat(report_date_str)
    ts_local = ts.astimezone(timezone.utc)
    # 根据资产时区做偏移近似（不用 ZoneInfo 保持兼容性）
    tz_offsets = {
        "Asia/Shanghai": 8,
        "Asia/Hong_Kong": 8,
        "America/New_York": -4,  # EDT 简化；冬令时-5，误差在允许范围内
        "Europe/London": 1,
    }
    offset_hours = tz_offsets.get(asset_tz, 0)
    ts_asset = ts_local + timedelta(hours=offset_hours)
    actual_date = ts_asset.date()

    if ticker in ("000001.SS", "399001.SZ", "^HSI"):
        # A股/港股：收盘日期必须是 report_date 当天
        if actual_date != report_date:
            return (
                f"日期对齐失败：{ticker} 收盘日期为 {actual_date}，"
                f"但报告要求 {report_date} 的收盘数据"
            )

    elif ticker in ("^DJI", "^IXIC"):
        # 美股：北京时间 report_date 早8点时，最新收盘是北京时间 report_date 凌晨（美东 report_date-1）
        expected = report_date - timedelta(days=1)
        if actual_date != expected:
            return (
                f"日期对齐失败：{ticker} 数据日期为 {actual_date}（美东），"
                f"但报告要求 {expected}（对应北京时间 {report_date} 凌晨收盘）"
            )

    elif ticker == "BZ=F":
        # 布伦特原油：主要参考伦敦收盘，允许 report_date 或 report_date-1
        if actual_date not in (report_date, report_date - timedelta(days=1)):
            return (
                f"日期对齐失败：{ticker} 数据日期为 {actual_date}，"
                f"与报告日期 {report_date} 偏差超过1天"
            )

    elif ticker == "GC=F":
        # COMEX黄金：美东收盘，对应北京时间 report_date 凌晨，应为 report_date-1
        expected = report_date - timedelta(days=1)
        if actual_date != expected:
            return (
                f"日期对齐失败：{ticker} 数据日期为 {actual_date}（美东），"
                f"但报告要求 {expected}（对应北京时间 {report_date} 凌晨收盘）"
            )

    return None

# 必须获取的基准资产池
REQUIRED_ASSETS = [
    {
        "ticker": "000001.SS",
        "name": "上证指数",
        "tz": "Asia/Shanghai",
        "accepted": ("000001.SS", "000001.SH", "SSE:000001"),
        "min_close": 1000.0,
        "max_close": 10000.0,
    },
    {
        "ticker": "399001.SZ",
        "name": "深证成指",
        "tz": "Asia/Shanghai",
        "accepted": ("399001.SZ", "399001.SZSE", "SZSE:399001"),
        "min_close": 1000.0,
        "max_close": 30000.0,
    },
    {
        "ticker": "^HSI",
        "name": "恒生指数",
        "tz": "Asia/Hong_Kong",
        "accepted": ("^HSI", "HSI", "HKHSI"),
        "min_close": 10000.0,
        "max_close": 50000.0,
    },
    {
        "ticker": "^DJI",
        "name": "道琼斯指数",
        "tz": "America/New_York",
        "accepted": ("^DJI", "DJI", "DJIA"),
        "min_close": 15000.0,
        "max_close": 70000.0,
    },
    {
        "ticker": "^IXIC",
        "name": "纳斯达克指数",
        "tz": "America/New_York",
        "accepted": ("^IXIC", "IXIC", "NASDAQ"),
        "min_close": 5000.0,
        "max_close": 30000.0,
    },
    {
        "ticker": "BZ=F",
        "name": "布伦特原油",
        "tz": "Europe/London",
        "accepted": ("BZ=F", "BRENT", "Brent"),
        "min_close": 10.0,
        "max_close": 300.0,
    },
    {
        "ticker": "GC=F",
        "name": "COMEX黄金期货",
        "tz": "America/New_York",
        "accepted": ("GC=F", "COMEX_GC", "Gold"),
        "min_close": 500.0,
        "max_close": 10000.0,
    },
]


@dataclass
class ParsedQuote:
    ticker_found: Optional[str]
    close: float
    previous_close: Optional[float]
    change: Optional[float]
    change_percent: Optional[float]
    timestamp: datetime
    timestamp_raw: str
    source: str
    source_file: str


def _norm_symbol(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9=._^]+", "", str(text)).upper()


def _safe_ticker(ticker: str) -> str:
    return re.sub(r"[^A-Za-z0-9=._-]+", "_", ticker)


def _candidate_files(prefix: str, ticker: str, base_dir: Path) -> List[Path]:
    variants = [
        ticker,
        ticker.replace("/", "-"),
        _safe_ticker(ticker),
        ticker.replace("^", ""),
        ticker.replace("^", "_"),
    ]
    unique = []
    seen = set()
    for v in variants:
        filename = f"{prefix}{v}.json"
        if filename not in seen:
            seen.add(filename)
            unique.append(base_dir / filename)
    return unique


def _locate_file(prefix: str, ticker: str, base_dir: Path) -> Optional[Path]:
    for p in _candidate_files(prefix, ticker, base_dir):
        if p.exists() and p.stat().st_size > 0:
            return p
    return None


def _find_by_keys(obj: Any, keys: Tuple[str, ...]) -> Optional[Any]:
    keyset = {k.lower() for k in keys}
    if isinstance(obj, dict):
        for k, v in obj.items():
            if str(k).lower() in keyset:
                return v
        for v in obj.values():
            found = _find_by_keys(v, keys)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _find_by_keys(item, keys)
            if found is not None:
                return found
    return None


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if math.isfinite(float(value)):
            return float(value)
        return None
    if isinstance(value, str):
        text = value.strip().replace(",", "")
        text = text.replace("%", "")
        if not text:
            return None
        try:
            num = float(text)
            if math.isfinite(num):
                return num
        except ValueError:
            return None
    return None


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        if v > 1e12:  # ms
            v /= 1000.0
        try:
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except Exception:
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        # 兼容 "2026-04-16T04:00:00Z"
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        # 兼容纯日期
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(text, fmt)
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                pass
        try:
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            return None
    return None


def _extract_source(raw: Dict[str, Any]) -> Optional[str]:
    value = _find_by_keys(raw, ("source_tool", "source", "tool", "provider"))
    if value is None:
        return None
    source = str(value).strip().lower()
    for allowed in ALLOWED_SOURCES:
        if allowed in source:
            return allowed
    return source if source else None


def _parse_raw_json(path: Path) -> Dict[str, Any]:
    """解析单个 raw_stock_*.json 文件并执行基础格式校验。"""
    try:
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as exc:
        return {"error": f"读取失败: {exc}"}

    if not isinstance(raw, dict):
        return {"error": "原始 JSON 顶层必须为对象"}

    source = _extract_source(raw)
    if not source:
        return {"error": "缺少来源字段(source_tool/source/tool/provider)"}

    # 优先读包装结构中的 raw/payload/data
    payload = raw
    for key in ("raw", "payload", "data", "result", "response", "quote"):
        nested = raw.get(key) if isinstance(raw, dict) else None
        if isinstance(nested, dict):
            payload = nested
            break

    ticker_found = _find_by_keys(raw, ("ticker", "symbol", "code", "instrument"))
    if ticker_found is None:
        ticker_found = _find_by_keys(payload, ("ticker", "symbol", "code", "instrument"))
    ticker_found = str(ticker_found).strip() if ticker_found is not None else None

    close = _to_float(_find_by_keys(payload, ("close", "last", "price", "latestPrice", "settle")))
    prev = _to_float(
        _find_by_keys(payload, ("previousClose", "prev_close", "previous_close", "preClose", "yesterday_close"))
    )
    change = _to_float(_find_by_keys(payload, ("change", "priceChange", "delta")))
    change_pct = _to_float(
        _find_by_keys(payload, ("changePercent", "change_percent", "changesPercentage", "pct_change", "pct"))
    )
    ts_raw = _find_by_keys(raw, ("fetched_at", "timestamp", "time", "datetime", "last_updated"))
    if ts_raw is None:
        ts_raw = _find_by_keys(payload, ("timestamp", "time", "datetime", "last_updated", "date"))
    ts = _parse_datetime(ts_raw)

    if close is None:
        return {"error": "缺少收盘价/最新价字段(close/last/price)"}
    if ts is None:
        return {"error": "缺少可解析时间戳(timestamp/fetched_at/...)"}

    if change is None and close is not None and prev is not None:
        change = close - prev
    if prev is None and change is not None and close is not None:
        prev = close - change
    if change_pct is None and close is not None and prev not in (None, 0):
        change_pct = (close - prev) / prev * 100.0

    if prev is None and change_pct is None:
        return {"error": "缺少 previous_close 或 change_percent，无法验证涨跌逻辑"}

    return {
        "ticker_found": ticker_found,
        "close": close,
        "previous_close": prev,
        "change": change,
        "change_percent": change_pct,
        "timestamp": ts,
        "timestamp_raw": str(ts_raw),
        "source": source,
        "source_file": str(path),
    }


def _check_time_paradox(ticker: str, ts: datetime, now_utc: datetime) -> Optional[str]:
    """基于当前系统时间检测明显的时间悖论（伪造数据特征）。"""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    ts_utc = ts.astimezone(timezone.utc)
    delta = now_utc - ts_utc

    # 任何未来时间都是红线
    if delta < timedelta(minutes=-5):
        # 给出更具体的伪造判定
        bj_now = now_utc.astimezone(timezone(timedelta(hours=8)))
        est_now = now_utc.astimezone(timezone(timedelta(hours=-4)))  # 简化用EDT
        if ticker in ("000001.SS", "399001.SZ"):
            ts_bj = ts_utc.astimezone(timezone(timedelta(hours=8)))
            if ts_bj.hour == 15:
                return f"时间悖论/伪造判定：A股数据标为 {ts_bj.strftime('%Y-%m-%d %H:%M')} CST，但当前北京时间仅 {bj_now.strftime('%Y-%m-%d %H:%M')}，A股尚未收盘"
        if ticker in ("^DJI", "^IXIC"):
            ts_est = ts_utc.astimezone(timezone(timedelta(hours=-4)))
            if ts_est.hour >= 16:
                return f"时间悖论/伪造判定：美股数据标为 {ts_est.strftime('%Y-%m-%d %H:%M')} EDT，但当前美东时间仅 {est_now.strftime('%Y-%m-%d %H:%M')}，美股尚未收盘"
        return "时间戳在未来，疑似伪造或时区错误"
    return None


def _validate_quote(asset: Dict[str, Any], parsed: Dict[str, Any], now_utc: datetime, report_date_str: str) -> Tuple[Optional[ParsedQuote], List[str]]:
    errors: List[str] = []
    ticker = asset["ticker"]
    accepted = {_norm_symbol(x) for x in asset["accepted"]}

    ticker_found = parsed.get("ticker_found")
    if ticker_found:
        found_norm = _norm_symbol(ticker_found)
        if found_norm not in accepted:
            errors.append(
                f"标的代码不匹配：期望 {ticker}，文件中为 {ticker_found}"
            )
    else:
        errors.append("缺少 ticker/symbol 字段")

    close = parsed["close"]
    if not (asset["min_close"] <= close <= asset["max_close"]):
        errors.append(
            f"价格越界：{close:.4f} 不在合理范围 [{asset['min_close']}, {asset['max_close']}]"
        )

    ts: datetime = parsed["timestamp"]
    paradox_err = _check_time_paradox(ticker, ts, now_utc)
    if paradox_err:
        errors.append(paradox_err)
    else:
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        delta = now_utc - ts.astimezone(timezone.utc)
        if delta > timedelta(hours=MAX_STALE_HOURS):
            errors.append(f"时间戳过旧（>{MAX_STALE_HOURS}h），不允许写入日报")

    date_align_err = _check_report_date(ticker, ts, report_date_str, asset["tz"])
    if date_align_err:
        errors.append(date_align_err)

    source = parsed["source"]
    if source not in ALLOWED_SOURCES:
        errors.append(f"来源字段异常：{source}，必须来自 {ALLOWED_SOURCES}")

    prev = parsed["previous_close"]
    chg = parsed["change"]
    pct = parsed["change_percent"]
    if prev not in (None, 0) and pct is not None:
        derived_pct = (close - prev) / prev * 100.0
        if abs(derived_pct - pct) > 0.35:
            errors.append(
                f"涨跌幅不一致：字段值 {pct:.4f}%，按 close/prev 推导 {derived_pct:.4f}%"
            )
    if pct is not None and abs(pct) > MAX_ABS_PCT_CHANGE:
        errors.append(
            f"触发极值熔断：涨跌幅 {pct:.4f}% 超过 {MAX_ABS_PCT_CHANGE:.1f}%（疑似错码/复权异常/API错误）"
        )
    if prev is not None and chg is not None:
        derived_chg = close - prev
        if abs(derived_chg - chg) > max(0.01, abs(chg) * 0.1):
            errors.append(
                f"涨跌额不一致：字段值 {chg:.4f}，按 close/prev 推导 {derived_chg:.4f}"
            )

    if errors:
        return None, errors

    return ParsedQuote(
        ticker_found=ticker_found,
        close=close,
        previous_close=prev,
        change=chg,
        change_percent=pct,
        timestamp=ts,
        timestamp_raw=parsed["timestamp_raw"],
        source=source,
        source_file=parsed["source_file"],
    ), []


def _compare_quotes(primary: ParsedQuote, secondary: ParsedQuote) -> Tuple[bool, str]:
    base = (primary.close + secondary.close) / 2.0
    if base == 0:
        return False, "交叉验证失败：价格为零，无法比较"
    gap_pct = abs(primary.close - secondary.close) / base * 100.0
    if gap_pct > CROSS_CHECK_MAX_GAP_PCT:
        return False, f"交叉验证失败：双源价格差 {gap_pct:.3f}% > {CROSS_CHECK_MAX_GAP_PCT:.3f}%"
    return True, f"交叉验证通过：双源价格差 {gap_pct:.3f}%"


def validate_and_generate(date_str: str, input_dir: Path, output_dir: Path, clean: bool = True) -> int:
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if clean:
        _clean_old_files(input_dir)

    errors: List[str] = []
    records: List[Dict[str, Any]] = []
    now_utc = datetime.now(timezone.utc)

    for asset in REQUIRED_ASSETS:
        ticker = asset["ticker"]
        primary_file = _locate_file("raw_stock_", ticker, input_dir)
        if primary_file is None:
            err = f"{ticker}: 缺少主数据文件 raw_stock_*.json"
            errors.append(err)
            records.append({"ticker": ticker, "name": asset["name"], "status": "BLOCKED", "errors": [err]})
            continue

        parsed = _parse_raw_json(primary_file)
        if "error" in parsed:
            err = f"{ticker}: 主数据解析失败 - {parsed['error']}"
            errors.append(err)
            records.append({"ticker": ticker, "name": asset["name"], "status": "BLOCKED", "errors": [err]})
            continue

        primary, errs = _validate_quote(asset, parsed, now_utc, date_str)
        if errs:
            errors.extend([f"{ticker}: {x}" for x in errs])
            records.append({"ticker": ticker, "name": asset["name"], "status": "BLOCKED", "errors": errs})
            continue

        cross_msg = None
        if ticker in ("^DJI", "^IXIC"):
            secondary_file = _locate_file("raw_stock2_", ticker, input_dir)
            if secondary_file is None:
                err = f"{ticker}: 缺少交叉验证文件 raw_stock2_*.json"
                errors.append(err)
                records.append({"ticker": ticker, "name": asset["name"], "status": "BLOCKED", "errors": [err]})
                continue
            second_parsed = _parse_raw_json(secondary_file)
            if "error" in second_parsed:
                err = f"{ticker}: 交叉源解析失败 - {second_parsed['error']}"
                errors.append(err)
                records.append({"ticker": ticker, "name": asset["name"], "status": "BLOCKED", "errors": [err]})
                continue
            secondary, second_errs = _validate_quote(asset, second_parsed, now_utc, date_str)
            if second_errs:
                errors.extend([f"{ticker}(cross): {x}" for x in second_errs])
                records.append({"ticker": ticker, "name": asset["name"], "status": "BLOCKED", "errors": second_errs})
                continue
            ok, cross_msg = _compare_quotes(primary, secondary)
            if not ok:
                errors.append(f"{ticker}: {cross_msg}")
                records.append({"ticker": ticker, "name": asset["name"], "status": "BLOCKED", "errors": [cross_msg]})
                continue

        records.append(
            {
                "ticker": ticker,
                "name": asset["name"],
                "status": "OK",
                "close": primary.close,
                "change": primary.change,
                "change_percent": primary.change_percent,
                "timestamp": primary.timestamp.isoformat(),
                "timestamp_raw": primary.timestamp_raw,
                "tz": asset["tz"],
                "source": primary.source,
                "source_file": primary.source_file,
                "cross_check": cross_msg,
            }
        )

    # 生成 Markdown 与 JSON
    lines = [
        f"# 基准资产行情数据（真实性门禁）({date_str})",
        f"> 验证时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
        "> 数据来源：stock-price-query / stock-info-explorer",
        f"> 门禁规则：来源匹配 + 标的匹配 + 时间新鲜度({MAX_STALE_HOURS}h) + 价格区间 + 交叉验证(^DJI/^IXIC<= {CROSS_CHECK_MAX_GAP_PCT}%)",
        "",
        "## 强制使用说明",
        "- 以下数据是撰写《核心资产与市场异动》模块的唯一数值基准。",
        "- 若某资产状态不是 `OK`，本日报应立即停止生成并先修复数据源。",
        "- 每条行情数据必须附带原始时间戳与时区，不得脱离时间上下文引用。",
        "",
        "## 行情汇总",
        "",
    ]

    for r in records:
        if r["status"] != "OK":
            lines.append(f"### {r['name']} ({r['ticker']})")
            lines.append(f"- 状态：**{r['status']}（阻塞）**")
            for e in r.get("errors", []):
                lines.append(f"- 错误：{e}")
            lines.append("")
            continue

        close_str = f"{float(r['close']):,.2f}"
        change_str = "N/A"
        if isinstance(r["change"], (int, float)):
            change_val = float(r["change"])
            sign = "+" if change_val >= 0 else ""
            change_str = f"{sign}{change_val:,.2f}"

        pct_str = "N/A"
        if isinstance(r["change_percent"], (int, float)):
            pct_val = float(r["change_percent"])
            sign = "+" if pct_val >= 0 else ""
            pct_str = f"{sign}{pct_val:.2f}%"

        ts_str = r["timestamp_raw"] if r["timestamp_raw"] else r["timestamp"]

        lines.append(f"### {r['name']} ({r['ticker']})")
        lines.append(f"- 状态：**OK**")
        lines.append(f"- 收盘价：{close_str}")
        lines.append(f"- 涨跌额：{change_str}")
        lines.append(f"- 涨跌幅：{pct_str}")
        lines.append(f"- 时间戳：{ts_str} ({r['tz']})")
        lines.append(f"- 来源：{r['source']}")
        lines.append(f"- 原始文件：{r['source_file']}")
        if r.get("cross_check"):
            lines.append(f"- 交叉验证：{r['cross_check']}")
        lines.append("")

    output_path = output_dir / f"market_data_{date_str}.md"
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    output_json = output_dir / f"market_data_{date_str}.json"
    output_json.write_text(
        json.dumps(
            {
                "date": date_str,
                "validated_at": datetime.now().isoformat(),
                "ok": len(errors) == 0,
                "errors": errors,
                "records": records,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    if errors:
        print("[BLOCKED] 行情真实性门禁未通过。")
        for idx, err in enumerate(errors, start=1):
            print(f"  {idx}. {err}")
        print(f"[INFO] 门禁报告：{output_path}")
        print(f"[INFO] 结构化结果：{output_json}")
        return 1

    print(f"[OK] 所有基准资产真实性校验通过。")
    print(f"[INFO] 门禁报告：{output_path}")
    print(f"[INFO] 结构化结果：{output_json}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="验证并格式化行情数据（真实性门禁）")
    parser.add_argument("--date", required=True, help="日期格式 YYYY-MM-DD")
    parser.add_argument("--input-dir", default=str(DEFAULT_INPUT_DIR), help="原始 JSON 文件目录")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="输出 Markdown 目录")
    parser.add_argument("--no-clean", action="store_true", help="保留旧的 raw_stock_*.json（不推荐，仅调试使用）")
    args = parser.parse_args()

    return validate_and_generate(
        args.date,
        Path(args.input_dir),
        Path(args.output_dir),
        clean=not args.no_clean,
    )


if __name__ == "__main__":
    raise SystemExit(main())
