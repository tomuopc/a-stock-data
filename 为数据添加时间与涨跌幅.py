"""
对 stock/stock-data、fund/fund-data、index/index-data 下的所有 CSV 统一处理：
新增 day（星期几）、open-percent（开盘相对昨日收盘涨跌幅）、close-percent（收盘相对开盘涨跌幅）
"""
import os
import pandas as pd

BASE = r"f:\桌面\量化交易\全市场所有股票数据"

DIRS = [
    os.path.join(BASE, "stock", "stock-data"),
    os.path.join(BASE, "fund", "fund-data"),
    os.path.join(BASE, "index", "index-data"),
]

WEEKDAY_MAP = {
    0: "星期一", 1: "星期二", 2: "星期三", 3: "星期四",
    4: "星期五", 5: "星期六", 6: "星期天",
}


def process_csv(filepath):
    """处理单个 CSV 文件，添加 day、open-percent、close-percent 列。"""
    try:
        df = pd.read_csv(filepath, encoding="utf-8-sig")
    except Exception:
        try:
            df = pd.read_csv(filepath, encoding="gbk")
        except Exception as e:
            print(f"  跳过（无法读取）: {filepath} -> {e}")
            return

    # 检查必要列
    required = {"time", "open", "close"}
    if not required.issubset(df.columns):
        print(f"  跳过（缺少必要列）: {filepath}")
        return

    # 统一按时间升序排列（老数据在前），方便计算涨跌幅
    df["time"] = pd.to_datetime(df["time"])
    df = df.sort_values("time").reset_index(drop=True)

    # --- day（星期） ---
    df["day"] = df["time"].dt.dayofweek.map(WEEKDAY_MAP)

    # --- open-percent：今日开盘相对于昨日收盘 ---
    # shift(1) 得到前一行（即昨天）的 close
    df["open-percent"] = (df["open"] - df["close"].shift(1)) / df["close"].shift(1)
    # 第一天没有昨日数据，留空
    # df.loc[0, "open-percent"] = 0.0  # 或留 NaN，保持原样

    # --- close-percent：今日收盘相对于今日开盘 ---
    df["close-percent"] = (df["close"] - df["open"]) / df["open"]

    # 保留四位小数
    df["open-percent"] = df["open-percent"].round(4)
    df["close-percent"] = df["close-percent"].round(4)

    # 按 time 降序（最新在前），恢复原始排序
    df = df.sort_values("time", ascending=False).reset_index(drop=True)

    # 写回，用 utf-8-sig 避免中文乱码
    df.to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f"  完成: {os.path.basename(filepath)}")


def main():
    total = 0
    for d in DIRS:
        if not os.path.isdir(d):
            print(f"目录不存在，跳过: {d}")
            continue
        csv_files = [f for f in os.listdir(d) if f.endswith(".csv")]
        print(f"\n>>> {d}  ({len(csv_files)} 个文件)")
        for fname in csv_files:
            process_csv(os.path.join(d, fname))
            total += 1
    print(f"\n处理完毕，共处理 {total} 个文件。")


if __name__ == "__main__":
    main()
