# market-news-daily

一个可直接被 OpenClaw 拉取部署的新闻抓取 Skill。

## 目录结构

- `SKILL.md`：Skill 主说明与执行规范
- `scripts/save_report.py`：日报保存脚本
- `evals/evals.json`：评测用例

## 本地快速测试

```bash
python3 scripts/save_report.py --template --output-dir 日报 --timezone Asia/Shanghai
```

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
