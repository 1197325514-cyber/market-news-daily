#!/usr/bin/env python3
"""
全市场新闻日报保存脚本
用于保存 Markdown 格式的日报到指定目录。
"""

import argparse
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # pragma: no cover - Python 3.8 fallback
    ZoneInfo = None


DEFAULT_TZ = "Asia/Shanghai"


def _now(tz_name: str = DEFAULT_TZ) -> datetime:
    if ZoneInfo:
        try:
            return datetime.now(ZoneInfo(tz_name))
        except Exception:
            pass
    return datetime.now()


def _resolve_report_path(
    output_dir: str,
    report_date: str,
    overwrite: bool,
) -> Path:
    base_dir = Path(output_dir)
    base_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{report_date}_全市场综合早报.md"
    target = base_dir / filename
    if overwrite or not target.exists():
        return target

    suffix = _now().strftime("%H%M%S")
    return base_dir / f"{report_date}_全市场综合早报_{suffix}.md"


def _atomic_write(path: Path, content: str) -> None:
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        delete=False,
        dir=str(path.parent),
    ) as tmp:
        tmp.write(content)
        tmp_path = Path(tmp.name)
    os.replace(tmp_path, path)


def save_markdown_report(
    content: str,
    output_dir: str = "日报",
    report_date: str = None,
    timezone: str = DEFAULT_TZ,
    overwrite: bool = True,
) -> str:
    """
    保存 Markdown 日报到文件

    Args:
        content: Markdown 格式的日报内容
        output_dir: 输出目录名称，默认为 "日报"
        report_date: 报告日期，格式 YYYY-MM-DD；不传则按当前时间生成
        timezone: 时区名称，默认 Asia/Shanghai
        overwrite: 是否覆盖同名文件

    Returns:
        保存文件的绝对路径
    """
    cleaned = content.strip()
    if not cleaned:
        raise ValueError("日报内容为空，未执行保存。")

    current = _now(timezone)
    date_text = report_date or current.strftime("%Y-%m-%d")
    target = _resolve_report_path(output_dir, date_text, overwrite)
    _atomic_write(target, cleaned + "\n")

    return str(target.resolve())


def create_report_template(
    report_date: str = None,
    timezone: str = DEFAULT_TZ,
) -> str:
    """
    创建日报模板

    Returns:
        日报模板字符串
    """
    current = _now(timezone)
    today = report_date or current.strftime("%Y-%m-%d")
    now_time = current.strftime("%H:%M")

    template = f"""# {today} 全市场综合早报

> 生成时间：{now_time}（{timezone}）
> 数据来源：ddg-search + fetch4ai

---

## 一、宏观经济与政策

### 今日暂无重大动态

---

## 二、地缘政治与国际形势

### 今日暂无重大动态

---

## 三、核心资产与市场异动

### 今日暂无重大动态

---

## 四、前沿技术动向

### 今日暂无重大动态

---

## 今日概览
- 宏观：今日暂无重大动态
- 地缘：今日暂无重大动态
- 市场：今日暂无重大动态
- 技术：今日暂无重大动态
"""
    return template


def _load_content(args: argparse.Namespace) -> str:
    if args.template:
        return create_report_template(args.date, args.timezone)

    if args.input_file:
        input_path = Path(args.input_file)
        return input_path.read_text(encoding="utf-8")

    if args.stdin:
        return sys.stdin.read()

    return create_report_template(args.date, args.timezone)


def main() -> int:
    parser = argparse.ArgumentParser(description="保存全市场综合早报 Markdown 文件")
    parser.add_argument("--input-file", help="从文件读取 Markdown 内容")
    parser.add_argument("--stdin", action="store_true", help="从标准输入读取 Markdown 内容")
    parser.add_argument("--template", action="store_true", help="忽略输入，直接保存模板")
    parser.add_argument("--output-dir", default="日报", help="输出目录，默认: 日报")
    parser.add_argument("--date", help="报告日期（YYYY-MM-DD）")
    parser.add_argument("--timezone", default=DEFAULT_TZ, help=f"时区，默认: {DEFAULT_TZ}")
    parser.add_argument("--no-overwrite", action="store_true", help="同名文件存在时自动追加时间后缀")
    args = parser.parse_args()

    try:
        content = _load_content(args)
        path = save_markdown_report(
            content=content,
            output_dir=args.output_dir,
            report_date=args.date,
            timezone=args.timezone,
            overwrite=not args.no_overwrite,
        )
    except Exception as exc:
        print(f"保存失败: {exc}", file=sys.stderr)
        return 1

    print(f"日报已保存至: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
