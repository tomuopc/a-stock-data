"""
获取全市场所有板块指数的日 K 线数据。
沪市指数（000/001 开头）和深市指数（399 开头）均通过 pytdx 获取。

使用方式：
    直接运行 python get-index-data.py
    首次运行会自动扫描指数列表，生成 index_name.txt
    之后直接读取 index_name.txt 按列表顺序下载数据

文件名格式: {代码}{名称}.csv，保存于 index-data 目录
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path
import csv
import sys
import threading
import time
from typing import Dict, List, Optional, Sequence, Tuple

import requests as req
from pytdx.hq import TdxHq_API


ROOT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT_DIR / "index-data"
INDEX_NAME_FILE = ROOT_DIR / "index_name.txt"
END_DATE = date(2026, 6, 26)
BAR_CATEGORY = 9
BAR_PAGE_SIZE = 800
MAX_WORKERS = 6

REQUEST_SLEEP_SECONDS = 0.02
CSV_HEADERS = ["time", "open", "close", "high", "low", "vol"]
HOSTS: List[Tuple[str, int]] = [
    ("119.147.212.81", 7709),
    ("119.147.171.206", 7709),
    ("39.108.28.83", 7709),
    ("180.153.18.170", 7709),
    ("180.153.18.171", 7709),
    ("202.108.253.131", 7709),
    ("60.191.117.167", 7709),
    ("115.238.90.165", 7709),
]

thread_local = threading.local()
progress_lock = threading.Lock()


def inject_project_root() -> None:
    if str(ROOT_DIR) not in sys.path:
        sys.path.insert(0, str(ROOT_DIR))


def sanitize_filename(name: str) -> str:
    invalid_chars = str.maketrans({
        "\\": "_", "/": "_", ":": "_", "*": "#",
        "?": "_", '"': "_", "<": "_", ">": "_", "|": "_",
    })
    return name.replace(" ", "").translate(invalid_chars)


def build_output_path(code: str, name: str) -> Path:
    return OUTPUT_DIR / f"{code}{sanitize_filename(name)}.csv"


def shift_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year + years)
    except ValueError:
        return value.replace(month=2, day=28, year=value.year + years)


def build_date_segments(start_date: date, end_date: date) -> List[Tuple[date, date]]:
    segments: List[Tuple[date, date]] = []
    current_end = end_date
    while current_end >= start_date:
        current_start = shift_years(current_end, -3)
        if current_start < start_date:
            current_start = start_date
        segments.append((current_start, current_end))
        if current_start == start_date:
            break
        current_end = current_start - timedelta(days=1)
    return segments


def connect_api() -> TdxHq_API:
    api = TdxHq_API()
    last_error = None
    for host, port in HOSTS:
        try:
            if api.connect(host, port):
                return api
        except Exception as exc:
            last_error = exc
    if last_error is not None:
        raise ConnectionError(f"无法连接到任何可用的通达信行情服务器: {last_error}")
    raise ConnectionError("无法连接到任何可用的通达信行情服务器")


def get_thread_api() -> TdxHq_API:
    api = getattr(thread_local, "api", None)
    if api is not None:
        return api
    api = connect_api()
    thread_local.api = api
    return api


def close_thread_api() -> None:
    api = getattr(thread_local, "api", None)
    if api is not None:
        try:
            api.disconnect()
        finally:
            thread_local.api = None


def normalize_date(d: str) -> str:
    """统一日期为 YYYY-MM-DD 格式（补零）"""
    normalized = d.replace("/", "-")[:10]
    try:
        return datetime.strptime(normalized, "%Y-%m-%d").date().strftime("%Y-%m-%d")
    except ValueError:
        return normalized


def read_existing_rows(csv_path: Path) -> Dict[str, Dict[str, str]]:
    if not csv_path.exists():
        return {}
    rows: Dict[str, Dict[str, str]] = {}
    with csv_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            trade_date = (row.get("time") or "").strip()
            if not trade_date:
                continue
            dt_text = normalize_date(trade_date)
            rows[dt_text] = {
                "time": dt_text,
                "open": str(row.get("open", "")),
                "close": str(row.get("close", "")),
                "high": str(row.get("high", "")),
                "low": str(row.get("low", "")),
                "vol": str(row.get("vol", "")),
            }
    return rows


def fetch_all_index_bars(api: TdxHq_API, market: int, code: str) -> List[Dict[str, object]]:
    """获取指数全量日 K，返回从旧到新的有序列表。"""
    all_records: List[Dict[str, object]] = []
    start = 0
    while True:
        raw = api.get_index_bars(BAR_CATEGORY, market, code, start, BAR_PAGE_SIZE)
        if not raw:
            break
        df = api.to_df(raw)
        records = df.iloc[::-1].to_dict("records")  # 反转为从旧到新
        all_records.extend(records)
        if len(records) < BAR_PAGE_SIZE:
            break
        start += BAR_PAGE_SIZE
        time.sleep(REQUEST_SLEEP_SECONDS)
    return all_records


def filter_segment_rows(raw_bars: Sequence[Dict[str, object]], start_date: date, end_date: date) -> List[Dict[str, str]]:
    segment_rows: List[Dict[str, str]] = []
    for row in raw_bars:
        dt_raw = row.get("datetime", "")
        if dt_raw is None:
            continue
        if isinstance(dt_raw, datetime):
            trade_date = dt_raw.date()
            dt_text = trade_date.strftime("%Y-%m-%d")
        elif isinstance(dt_raw, str):
            normalized = dt_raw.replace("/", "-")
            dt_text = normalized[:10]
            trade_date = datetime.strptime(dt_text, "%Y-%m-%d").date()
            dt_text = trade_date.strftime("%Y-%m-%d")
        else:
            dt_str = str(dt_raw)
            normalized = dt_str.replace("/", "-")
            dt_text = normalized[:10]
            try:
                trade_date = datetime.strptime(dt_text, "%Y-%m-%d").date()
                dt_text = trade_date.strftime("%Y-%m-%d")
            except ValueError:
                continue
        if not (start_date <= trade_date <= end_date):
            continue
        open_val = float(row.get("open", 0))
        close_val = float(row.get("close", 0))
        high_val = float(row.get("high", 0))
        low_val = float(row.get("low", 0))
        segment_rows.append({
            "time": dt_text,
            "open": f"{open_val:.3f}",
            "close": f"{close_val:.3f}",
            "high": f"{high_val:.3f}",
            "low": f"{low_val:.3f}",
            "vol": str(row.get("vol", "")),
        })
    return segment_rows


def get_oldest_trade_date(raw_bars: Sequence[Dict[str, object]]) -> Optional[date]:
    if not raw_bars:
        return None
    dt_raw = raw_bars[-1].get("datetime", "")
    if dt_raw is None:
        return None
    if isinstance(dt_raw, datetime):
        return dt_raw.date()
    dt_str = str(dt_raw).replace("/", "-")[:10]
    if not dt_str:
        return None
    try:
        return datetime.strptime(dt_str, "%Y-%m-%d").date()
    except ValueError:
        return None


def write_rows(csv_path: Path, rows_by_date: Dict[str, Dict[str, str]]) -> None:
    ordered_dates = sorted(rows_by_date.keys(), reverse=True)
    with csv_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_HEADERS)
        writer.writeheader()
        for trade_date in ordered_dates:
            writer.writerow(rows_by_date[trade_date])


def get_shenzhen_indexes() -> list:
    """获取深圳市场 399 开头指数"""
    api = connect_api()
    try:
        total = api.get_security_count(0) or 0
        result = []
        for start in range(0, max(total, 25000), 1000):
            batch = api.get_security_list(0, start)
            if not batch:
                continue
            for item in batch:
                if item["code"].startswith("399"):
                    name = item["name"].strip()
                    result.append((item["code"], name))
        return sorted(result, key=lambda x: x[0])
    finally:
        api.disconnect()


def scan_shanghai_indexes() -> list:
    """扫描 000000~001999 用 get_index_bars 找出所有有效上海指数"""
    print("  扫描 000000~001999 查找上海指数（首次运行，约 2 分钟）...")
    api = connect_api()
    try:
        result = []
        for i in range(0, 2000):
            code = f"{i:06d}"
            raw = api.get_index_bars(9, 1, code, 0, 1)
            if raw and len(raw) > 0:
                result.append(code)
            if (i + 1) % 500 == 0:
                print(f"    进度: {i+1}/2000, 已发现 {len(result)} 只")
        return result
    finally:
        api.disconnect()


def update_index_name_file() -> int:
    """获取全市场指数列表并写入 index_name.txt"""
    print("正在获取深圳指数...")
    sz_indexes = get_shenzhen_indexes()
    print(f"  深圳指数 (399): {len(sz_indexes)} 只")

    print("正在获取上海指数...")
    sh_codes = scan_shanghai_indexes()
    print(f"  上海指数 (000/001): {len(sh_codes)} 只")

    # 用新浪API获取上海指数的中文名称
    print("  正在获取上海指数中文名称...")
    try:
        sh_names = {}
        headers = {"Referer": "https://finance.sina.com.cn"}
        for i in range(0, len(sh_codes), 500):
            batch = sh_codes[i:i+500]
            url = "https://hq.sinajs.cn/list=" + ",".join([f"sh{c}" for c in batch])
            resp = req.get(url, headers=headers, timeout=15)
            if resp.text:
                for line in resp.text.strip().split("\n"):
                    if line.startswith("var") and '"' in line:
                        parts = line.split('"')
                        if len(parts) >= 2:
                            fields = parts[1].split(",")
                            name = fields[0]
                            var_name = line.split("=")[0].strip()
                            code = var_name.replace("var hq_str_sh", "")
                            if code and name:
                                sh_names[code] = name
        print(f"  新浪API获取名称: {len(sh_names)} 只")
    except Exception:
        sh_names = {}

    lines = []
    for code, name in sz_indexes:
        lines.append(f"{code}, {name}")
    for code in sorted(sh_codes):
        name = sh_names.get(code, code)  # 用新浪名称，拿不到则用代码
        lines.append(f"{code}, {name}")

    INDEX_NAME_FILE.write_text("\n".join(lines), encoding="utf-8")
    return len(lines)


def load_indexes() -> List[Tuple[str, str, int]]:
    indexes: List[Tuple[str, str, int]] = []
    for raw_line in INDEX_NAME_FILE.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or "," not in line:
            continue
        code, name = line.split(",", 1)
        code = code.strip()
        name = name.strip()
        if code.startswith("399"):
            market = 0
        elif code.startswith(("000", "001")):
            market = 1
        else:
            continue
        indexes.append((code, name, market))
    return indexes


def process_index(code: str, name: str, market: int, _: Sequence[Tuple[date, date]]) -> Tuple[str, str]:
    csv_path = build_output_path(code, name)
    existing_rows = read_existing_rows(csv_path) if csv_path.exists() else {}

    if existing_rows:
        existing_dates = sorted(existing_rows.keys(), reverse=True)
        newest_date = datetime.strptime(existing_dates[0], "%Y-%m-%d").date()
        if newest_date >= END_DATE:
            return code, None

        api = get_thread_api()
        raw_bars = fetch_all_index_bars(api, market, code)
        if not raw_bars:
            return code, "无数据"

        new_rows = filter_segment_rows(raw_bars, newest_date + timedelta(days=1), END_DATE)
        if not new_rows:
            return code, None

        for row in new_rows:
            existing_rows[row["time"]] = row
        write_rows(csv_path, existing_rows)
        return code, f"新增{len(new_rows)}条"

    api = get_thread_api()
    raw_bars = fetch_all_index_bars(api, market, code)
    if not raw_bars:
        return code, "无数据"
    oldest_trade_date = get_oldest_trade_date(raw_bars)
    if oldest_trade_date is None:
        return code, "无有效数据"
    segments = build_date_segments(oldest_trade_date, END_DATE)
    existing_rows = read_existing_rows(csv_path)
    fetched_count = 0
    for start_date, end_date in segments:
        for row in filter_segment_rows(raw_bars, start_date, end_date):
            if row["time"] not in existing_rows:
                existing_rows[row["time"]] = row
                fetched_count += 1
    if not existing_rows:
        return code, "无有效数据"
    write_rows(csv_path, existing_rows)
    if fetched_count == 0 and csv_path.exists():
        return code, "已补齐"
    return code, f"新增{fetched_count}条"


def run() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not INDEX_NAME_FILE.exists():
        print("正在获取全市场指数列表...")
        count = update_index_name_file()
        print(f"已更新 index_name.txt，共 {count} 只指数。")

    indexes = load_indexes()
    total = len(indexes)
    skipped = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_index, code, name, market, ())
            for code, name, market in indexes
        ]
        for index, future in enumerate(futures, start=1):
            code, name = indexes[index - 1][:2]
            try:
                _, status = future.result()
            except Exception as exc:
                status = f"失败: {exc}"
            if status is None:
                skipped += 1
                continue
            with progress_lock:
                print(f"[{index}/{total}] {code} {name} - {status}")

    missing = total - skipped
    if missing:
        print(f"\n共 {total} 只指数，跳过 {skipped} 只（已完整），处理 {missing} 只。")
    else:
        print(f"\n共 {total} 只指数，全部为最新数据。")
    close_thread_api()


def main() -> None:
    inject_project_root()
    try:
        run()
    finally:
        close_thread_api()


if __name__ == "__main__":
    main()
