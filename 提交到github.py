"""
将本地策略文件提交到 GitHub 仓库 tomuopc/my-quant-code。

用法：
    python 提交到github.py <本地py文件路径> <分支名> <目标二级文件夹>

示例：
    python 提交到github.py main.py main "动量突破策略/ETF"
"""

import sys
import os
import base64
import requests

GITHUB_TOKEN = "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"
REPO_OWNER = "tomuopc"
REPO_NAME = "my-quant-code"
API_BASE = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}"


def main():
    if len(sys.argv) < 4:
        print("用法: python 提交到github.py <本地py文件路径> <分支名> <目标二级文件夹>")
        print("示例: python 提交到github.py main.py main \"动量突破策略/ETF\"")
        sys.exit(1)

    local_file = sys.argv[1]
    branch = sys.argv[2]
    target_folder = sys.argv[3].strip("/")

    if not os.path.isfile(local_file):
        print(f"错误: 文件不存在 - {local_file}")
        sys.exit(1)

    filename = os.path.basename(local_file)
    repo_path = f"{target_folder}/{filename}" if target_folder else filename

    with open(local_file, "r", encoding="utf-8") as f:
        content = f.read()

    encoded_content = base64.b64encode(content.encode("utf-8")).decode("utf-8")

    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    # 1) 检查分支是否存在
    branch_url = f"{API_BASE}/branches/{branch}"
    r = requests.get(branch_url, headers=headers)
    if r.status_code == 404:
        # 分支不存在，从默认分支创建
        print(f"分支 '{branch}' 不存在，正在从默认分支创建...")
        repo_r = requests.get(API_BASE, headers=headers)
        if repo_r.status_code != 200:
            print(f"错误: 无法获取仓库信息 - {repo_r.status_code} {repo_r.text}")
            sys.exit(1)
        default_branch = repo_r.json().get("default_branch", "main")

        # 获取默认分支最新 commit
        ref_url = f"{API_BASE}/git/ref/heads/{default_branch}"
        ref_r = requests.get(ref_url, headers=headers)
        if ref_r.status_code != 200:
            print(f"错误: 无法获取默认分支引用 - {ref_r.status_code} {ref_r.text}")
            sys.exit(1)
        sha = ref_r.json()["object"]["sha"]

        # 创建新分支
        create_ref_url = f"{API_BASE}/git/refs"
        create_payload = {"ref": f"refs/heads/{branch}", "sha": sha}
        create_r = requests.post(create_ref_url, headers=headers, json=create_payload)
        if create_r.status_code != 201:
            print(f"错误: 分支创建失败 - {create_r.status_code} {create_r.text}")
            sys.exit(1)
        print(f"分支 '{branch}' 创建成功")
    elif r.status_code != 200:
        print(f"错误: 检查分支时出错 - {r.status_code} {r.text}")
        sys.exit(1)

    # 2) 检查目标文件是否已存在（获取 SHA）
    file_api_url = f"{API_BASE}/contents/{repo_path}?ref={branch}"
    existing_sha = None
    r = requests.get(file_api_url, headers=headers)
    if r.status_code == 200:
        existing_sha = r.json()["sha"]
        print(f"文件已存在，将更新: {repo_path}")
    elif r.status_code == 404:
        print(f"文件不存在，将新建: {repo_path}")
    else:
        print(f"警告: 检查文件时状态异常 - {r.status_code} {r.text}")
        print("将尝试直接提交...")

    # 3) 创建或更新文件
    put_url = f"{API_BASE}/contents/{repo_path}"
    payload = {
        "message": f"更新策略: {filename} -> {target_folder}",
        "content": encoded_content,
        "branch": branch,
    }
    if existing_sha:
        payload["sha"] = existing_sha

    r = requests.put(put_url, headers=headers, json=payload)
    if r.status_code in (200, 201):
        print(f"✓ 提交成功!")
        print(f"  仓库: {REPO_OWNER}/{REPO_NAME}")
        print(f"  分支: {branch}")
        print(f"  路径: {repo_path}")
        print(f"  提交: {r.json()['content']['sha'][:7]}")
    else:
        print(f"错误: 提交失败 - {r.status_code}")
        print(r.text)
        sys.exit(1)


if __name__ == "__main__":
    main()
