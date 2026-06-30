---
name: "提交到github"
description: "将指定的策略 py 文件提交到 tomuopc/my-quant-code 仓库的指定分支和文件夹。当用户要求上传/提交/推送策略文件到 GitHub 时调用。"
---

# 提交到 GitHub

将本地策略文件提交到 GitHub 仓库 `tomuopc/my-quant-code`。

## 使用方式

**调用格式**：用户指定 3 个参数：`<py文件路径> <分支名> <目标二级文件夹>`

然后用 `python 提交到github.py` 脚本执行。

### 参数说明

| 参数 | 说明 | 示例 |
|------|------|------|
| 本地py文件路径 | 要提交的 .py 文件的路径 | `策略/突破买入/main.py` |
| 分支名 | 要提交到的 GitHub 分支 | `main` 或 `dev` |
| 目标二级文件夹 | 仓库中的两级文件夹路径 | `动量突破策略/ETF` |

### 示例

用户说：
> 把 策略/突破买入/main.py 提交到 main 分支的 动量突破策略/ETF 文件夹

AI 应执行：
```
cd <项目根目录>
python 提交到github.py "策略/突破买入/main.py" "main" "动量突破策略/ETF"
```

### 功能特性

1. **自动创建分支**：如果指定的分支不存在，会自动从默认分支创建
2. **自动识别新建/更新**：文件已存在则更新，不存在则新建
3. **中文提交信息**：提交信息包含策略文件名和目标文件夹

## 输出示例

```
文件不存在，将新建: 动量突破策略/ETF/main.py
✓ 提交成功!
  仓库: tomuopc/my-quant-code
  分支: main
  路径: 动量突破策略/ETF/main.py
  提交: a1b2c3d
```

## 依赖

安装 `requests` 库（如未安装）：
```
pip install requests
```
