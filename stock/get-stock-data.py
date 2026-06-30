from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
import csv
import sys
import threading
import time
from typing import Dict, List, Optional, Sequence, Tuple

from pytdx.hq import TdxHq_API


ROOT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT_DIR / "stock-data"
STOCK_NAME_FILE = ROOT_DIR / "stock_name.txt"
END_DATE = date.today()
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
MARKET_PREFIXES: Dict[int, Tuple[str, ...]] = {
    0: ("000", "001", "002", "003", "300", "301", "302"),
    1: ("600", "601", "603", "605"),
}
INVALID_FILENAME_CHARS = str.maketrans({
    "\\": "_",
    "/": "_",
    ":": "_",
    "*": "#",
    "?": "_",
    '"': "_",
    "<": "_",
    ">": "_",
    "|": "_",
})
PAGE_SIZE = 1000
EXCLUDED_NAME_KEYWORDS = ("退", "PT")


thread_local = threading.local()
progress_lock = threading.Lock()


def inject_project_root() -> None:
    if str(ROOT_DIR) not in sys.path:
        sys.path.insert(0, str(ROOT_DIR))


def infer_market(code: str) -> Optional[int]:
    for market, prefixes in MARKET_PREFIXES.items():
        if code.startswith(prefixes):
            return market
    return None


def sanitize_filename(name: str) -> str:
    return name.replace(" ", "").translate(INVALID_FILENAME_CHARS)


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


def load_stocks() -> List[Tuple[str, str, int]]:
    stocks: List[Tuple[str, str, int]] = []

    for raw_line in STOCK_NAME_FILE.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or "," not in line:
            continue
        code, name = line.split(",", 1)
        market = infer_market(code.strip())
        if market is None:
            continue
        stocks.append((code.strip(), name.strip(), market))

    return stocks


def fetch_market_stocks(api, market):
    total = api.get_security_count(market)
    prefixes = MARKET_PREFIXES[market]
    filtered = []
    seen_codes = set()

    for start in range(0, total, PAGE_SIZE):
        batch = api.get_security_list(market, start)
        if not batch:
            continue

        for item in batch:
            code = item["code"]
            name = item["name"].strip()
            if not code.startswith(prefixes):
                continue
            if code in seen_codes:
                continue
            if any(keyword in name for keyword in EXCLUDED_NAME_KEYWORDS):
                continue
            seen_codes.add(code)
            filtered.append((code, name))

    return filtered


def update_stock_name_file():
    api = connect_api()
    try:
        result = []
        for market in (0, 1):
            result.extend(fetch_market_stocks(api, market))
        result.sort(key=lambda item: item[0])
        lines = [f"{code}, {name}" for code, name in result]
        STOCK_NAME_FILE.write_text("\n".join(lines), encoding="utf-8")
        return len(result)
    finally:
        api.disconnect()


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


def fetch_all_daily_bars(api: TdxHq_API, market: int, code: str) -> List[Dict[str, object]]:
    all_records: List[Dict[str, object]] = []
    start = 0

    while True:
        raw = api.get_security_bars(BAR_CATEGORY, market, code, start, BAR_PAGE_SIZE)
        if not raw:
            break
        df = api.to_df(raw)
        # get_security_bars 返回的是从最新到最旧的顺序，需要反转为从旧到新
        records = df.iloc[::-1].to_dict("records")
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

        # 处理 datetime 对象或字符串
        if isinstance(dt_raw, datetime):
            trade_date = dt_raw.date()
            dt_text = trade_date.strftime("%Y-%m-%d")
        elif isinstance(dt_raw, str):
            # 兼容 2026/06/09 或 2026-06-09 等格式，统一为 YYYY-MM-DD
            normalized = dt_raw.replace("/", "-")
            dt_text = normalized[:10]
            trade_date = datetime.strptime(dt_text, "%Y-%m-%d").date()
            dt_text = trade_date.strftime("%Y-%m-%d")
        else:
            # 尝试转为字符串再处理
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

        # 数值统一保留两位小数
        open_val = float(row.get("open", 0))
        close_val = float(row.get("close", 0))
        high_val = float(row.get("high", 0))
        low_val = float(row.get("low", 0))

        segment_rows.append(
            {
                "time": dt_text,
                "open": f"{open_val:.2f}",
                "close": f"{close_val:.2f}",
                "high": f"{high_val:.2f}",
                "low": f"{low_val:.2f}",
                "vol": str(row.get("vol", "")),
            }
        )

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


def process_stock(code: str, name: str, market: int, _: Sequence[Tuple[date, date]]) -> Tuple[str, str]:
    csv_path = build_output_path(code, name)
    existing_rows = read_existing_rows(csv_path) if csv_path.exists() else {}

    if existing_rows:
        # 补充模式：已有数据，只补 newest_date 到 today 之间的数据
        existing_dates = sorted(existing_rows.keys(), reverse=True)
        newest_date = datetime.strptime(existing_dates[0], "%Y-%m-%d").date()
        if newest_date >= END_DATE:
            return code, None  # 已是最新，静默跳过

        api = get_thread_api()
        raw_bars = fetch_all_daily_bars(api, market, code)
        if not raw_bars:
            return code, "无数据"

        new_rows = filter_segment_rows(raw_bars, newest_date + timedelta(days=1), END_DATE)
        if not new_rows:
            return code, None

        for row in new_rows:
            existing_rows[row["time"]] = row
        write_rows(csv_path, existing_rows)
        return code, f"新增{len(new_rows)}条"

    # 初始下载模式：CSV 不存在，全量下载
    api = get_thread_api()
    raw_bars = fetch_all_daily_bars(api, market, code)
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

    # txt存在且目录内有csv → 直接读txt，跳过网络更新
    if STOCK_NAME_FILE.exists() and any(OUTPUT_DIR.iterdir()):
        pass
    else:
        print("正在获取最新股票列表...")
        count = update_stock_name_file()
        print(f"已更新 stock_name.txt，共 {count} 只股票。")

    stocks = load_stocks()
    total = len(stocks)
    skipped = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_stock, code, name, market, ())
            for code, name, market in stocks
        ]
        for index, future in enumerate(futures, start=1):
            code, name = stocks[index - 1][:2]
            try:
                _, status = future.result()
            except Exception as exc:
                status = f"失败: {exc}"
            if status is None:
                skipped += 1
                continue
            with progress_lock:
                print(f"[{index}/{total}] {code} {name} - {status}")

    print(f"\n共 {total} 只股票，跳过 {skipped} 只（已完整），处理 {total - skipped} 只。")
    close_thread_api()


def main() -> None:
    inject_project_root()
    try:
        run()
    finally:
        close_thread_api()


if __name__ == "__main__":
    main()
