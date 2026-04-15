# market-news-daily

一个可直接被 OpenClaw 拉取部署的新闻抓取 Skill。

## 目录结构

- `SKILL.md`：Skill 主说明与执行规范
- `scripts/fetch_market_data.py`：行情真实性门禁脚本（校验来源/代码/时间戳/跨源一致性）
- `scripts/save_report.py`：日报保存脚本
- `evals/evals.json`：评测用例

## 本地快速测试

```bash
python3 scripts/save_report.py --template --output-dir 日报 --timezone Asia/Shanghai
```

## 行情真实性门禁（建议每次日报前执行）

在 `data/` 下保存 `raw_stock_*.json` 和（针对 `^DJI` / `^IXIC`）`raw_stock2_*.json` 后执行：

```bash
python3 scripts/fetch_market_data.py --date 2026-04-16
```

- 返回 `0`：门禁通过，可继续生成日报
- 返回非 `0`：门禁失败，必须修复数据后重跑，不能继续出报告

脚本会生成：
- `data/market_data_YYYY-MM-DD.md`
- `data/market_data_YYYY-MM-DD.json`

## 发布到 GitHub

在当前目录执行（将 `YOUR_GITHUB_ID` 替换为你的 GitHub 用户名）：

```bash
git branch -m main
git add .
git commit -m "feat: add market-news-daily skill"
gh repo create market-news-daily --public --source=. --remote=origin --push
```

如果你已经有远程仓库：

```bash
git branch -m main
git remote add origin git@github.com:YOUR_GITHUB_ID/market-news-daily.git
git add .
git commit -m "feat: add market-news-daily skill"
git push -u origin main
```

## OpenClaw 拉取地址示例

发布后你可以在 OpenClaw 里使用：

- 仓库地址：`https://github.com/YOUR_GITHUB_ID/market-news-daily`
- 原始技能文件地址：`https://raw.githubusercontent.com/YOUR_GITHUB_ID/market-news-daily/main/SKILL.md`

如果 OpenClaw 支持子目录安装，也可以直接指向该仓库根目录。
