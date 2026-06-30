from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
import csv
import re
import sys
import threading
import time
from typing import Dict, List, Optional, Sequence, Tuple

import requests as req
from pytdx.hq import TdxHq_API


ROOT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT_DIR / "fund-data"
FUND_NAME_FILE = ROOT_DIR / "fund_name.txt"
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
EXCLUDED_FUND_NAME_KEYWORDS = ("认购款",)
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


thread_local = threading.local()
progress_lock = threading.Lock()


def inject_project_root() -> None:
    if str(ROOT_DIR) not in sys.path:
        sys.path.insert(0, str(ROOT_DIR))


def is_sh_fund_code(code: str) -> bool:
    return code.startswith(("501", "51", "52", "56", "58", "59"))


def is_sz_fund_code(code: str) -> bool:
    return code.startswith("159") or code.startswith("16")


def is_target_fund_code(code: str) -> bool:
    return is_sh_fund_code(code) or is_sz_fund_code(code)


def infer_fund_market(code: str) -> Optional[int]:
    if is_sh_fund_code(code):
        return 1
    if is_sz_fund_code(code):
        return 0
    return None


def should_keep_fund(code: str, name: str) -> bool:
    if not code:
        return False
    if name and any(keyword in name for keyword in EXCLUDED_FUND_NAME_KEYWORDS):
        return False
    return is_target_fund_code(code)



def sanitize_filename(name: str) -> str:
    return name.replace(" ", "").translate(INVALID_FILENAME_CHARS)


def build_output_path(code: str, name: str) -> Path:
    if name:
        return OUTPUT_DIR / f"{code}{sanitize_filename(name)}.csv"
    return OUTPUT_DIR / f"{code}.csv"


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


def load_funds() -> List[Tuple[str, str, int]]:
    funds: List[Tuple[str, str, int]] = []

    for raw_line in FUND_NAME_FILE.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or "," not in line:
            continue
        code, name = line.split(",", 1)
        code = code.strip()
        name = name.strip()
        if is_sh_fund_code(code):
            market = 1
        elif is_sz_fund_code(code):
            market = 0
        else:
            continue
        funds.append((code, name, market))

    return funds


def get_etf_lof_by_prefix() -> list:
    """通过代码前缀扫描获取 ETF/LOF"""
    api = connect_api()
    if not api:
        return []

    etf_lof = []
    seen_codes = set()

    try:
        # 上交所: 51/52/56/58/59 + 501
        total_sh = api.get_security_count(1)
        for start in range(0, total_sh, 1000):
            batch = api.get_security_list(1, start)
            if not batch:
                continue
            for item in batch:
                code = item["code"]
                name = item["name"].strip()
                if should_keep_fund(code, name) and is_sh_fund_code(code) and code not in seen_codes:
                    etf_lof.append((code, name))
                    seen_codes.add(code)
    finally:
        api.disconnect()

    return etf_lof


def get_shenzhen_funds_by_quote() -> list:
    """并行行情探测+公司信息获取深市基金代码和名称。"""
    print("  正在并行探测 159000~169999...")

    # 先用单线程快速扫描 get_security_list 中已存在的基金
    list_funds = {}
    api = connect_api()
    try:
        for start in range(6000, 8200, 100):
            batch = api.get_security_list(0, start)
            if not batch:
                continue
            for item in batch:
                if item["code"].startswith(("159", "16")):
                    list_funds[item["code"]] = item["name"].strip()
    except:
        pass
    api.disconnect()
    print(f"    get_security_list 获取: {len(list_funds)} 只")

    # 构造需要并行探测的代码列表（排除已获取的）
    need_probe = []
    for i in range(159000, 160000):
        c = f"{i:06d}"
        if c not in list_funds:
            need_probe.append(c)
    for i in range(160000, 170000):
        c = f"{i:06d}"
        if c not in list_funds:
            need_probe.append(c)

    if need_probe:
        print(f"    需并行探测 {len(need_probe)} 个代码...")
        found = _parallel_probe(need_probe, list_funds)
    else:
        found = dict(list_funds)

    result = sorted(found.items(), key=lambda x: x[0])
    return result


def _probe_one(code: str) -> tuple:
    """探测单只深市基金，返回 (code, name_or_None)"""
    api = get_thread_api()
    if not api:
        return (code, None)
    try:
        data = api.get_security_quotes([(0, code)])
        if not data:
            return (code, None)
        item = data[0]
        p = item.get("price", 0) or 0
        v = item.get("vol", 0) or 0
        if p == 0 and v == 0:
            return (code, None)
        try:
            cat = api.get_company_info_category(0, code)
            if cat and len(cat) > 0:
                first = cat[0]
                content = api.get_company_info_content(
                    0, code, first["filename"], first["start"],
                    max(first["length"], 2000)
                )
                if content:
                    text = content if not isinstance(content, bytes) else content.decode("gbk", errors="ignore")
                    m = re.search(rf"◇\s*{re.escape(code)}\s+(.+?)\s*更新日期", text)
                    if m:
                        return (code, m.group(1).strip())
        except:
            pass
        # 存在但公司信息获取不到名称，用代码做占位
        return (code, code)
    except:
        return (code, None)


def _parallel_probe(need_probe: List[str], found: Dict[str, str]) -> Dict[str, str]:
    """并行探测深市基金列表"""
    worker_count = 8
    batch_size = max(1, len(need_probe) // (worker_count * 3) + 1)
    batches = [need_probe[i:i+batch_size] for i in range(0, len(need_probe), batch_size)]

    def _probe_batch(batch_codes):
        results = []
        for c in batch_codes:
            _, name = _probe_one(c)
            if name:
                results.append((c, name))
        return results

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {executor.submit(_probe_batch, b): b for b in batches}
        for i, future in enumerate(as_completed(futures), 1):
            try:
                for c, n in future.result():
                    if c not in found:
                        found[c] = n
            except:
                pass
            if i % 5 == 0 or i == len(batches):
                print(f"      进度: {i}/{len(batches)} 批")

    return found


def sina_batch_fill_names(funds: Dict[str, str]) -> None:
    """用新浪API批量补全名称缺失的基金（name==code的项）"""
    need_fill = [c for c, n in funds.items() if n == c]
    if not need_fill:
        return

    print(f"    新浪API补全 {len(need_fill)} 只基金名称...")

    headers = {"Referer": "https://finance.sina.com.cn"}
    batch_size = 500
    batches = [need_fill[i:i+batch_size] for i in range(0, len(need_fill), batch_size)]

    filled = 0
    for batch in batches:
        url = "https://hq.sinajs.cn/list=" + ",".join([f"sz{c}" for c in batch])
        try:
            resp = req.get(url, headers=headers, timeout=15)
            if resp.text:
                for line in resp.text.strip().split("\n"):
                    if line.startswith("var") and '"' in line:
                        parts = line.split('"')
                        if len(parts) >= 2:
                            fields = parts[1].split(",")
                            name = fields[0]
                            var_name = line.split("=")[0].strip()
                            code = var_name.replace("var hq_str_sz", "")
                            if code and name and is_sz_fund_code(code):
                                funds[code] = name
                                filled += 1
        except:
            pass

    if filled:
        print(f"    新浪API补全了 {filled} 只基金名称")


def update_fund_name_file() -> int:
    """获取 ETF/LOF 列表并写入 fund_name.txt，返回数量"""
    print("正在通过代码前缀扫描 ETF/LOF...")
    etf_lof = get_etf_lof_by_prefix()
    print(f"前缀方式获取: {len(etf_lof)} 只")

    print("正在通过行情查询获取深市 ETF/LOF...")
    sz_funds = get_shenzhen_funds_by_quote()
    print(f"深市补充获取: {len(sz_funds)} 只")

    # 合并去重，沪市优先（名称更完整）
    all_funds = {}
    for code, name in etf_lof:
        all_funds[code] = name
    for code, name in sz_funds:
        if code not in all_funds:
            all_funds[code] = name

    # 对名称缺失的（name==code）用新浪API补全
    sina_batch_fill_names(all_funds)

    result = sorted(all_funds.items(), key=lambda x: x[0])
    lines = [f"{code}, {name}" for code, name in result]
    FUND_NAME_FILE.write_text("\n".join(lines), encoding="utf-8")
    return len(result)


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
                "open": f"{open_val:.3f}",
                "close": f"{close_val:.3f}",
                "high": f"{high_val:.3f}",
                "low": f"{low_val:.3f}",
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


def process_fund(code: str, name: str, market: int, _: Sequence[Tuple[date, date]]) -> Tuple[str, str]:
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
    if FUND_NAME_FILE.exists() and any(OUTPUT_DIR.iterdir()):
        pass
    else:
        print("正在获取最新 ETF/LOF 列表...")
        count = update_fund_name_file()
        print(f"已更新 fund_name.txt，共 {count} 只基金。")

    funds = load_funds()
    total = len(funds)
    skipped = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_fund, code, name, market, ())
            for code, name, market in funds
        ]
        for index, future in enumerate(futures, start=1):
            code, name = funds[index - 1][:2]
            try:
                _, status = future.result()
            except Exception as exc:
                status = f"失败: {exc}"
            if status is None:
                skipped += 1
                continue
            with progress_lock:
                print(f"[{index}/{total}] {code} {name} - {status}")

    print(f"\n共 {total} 只基金，跳过 {skipped} 只（已完整），处理 {total - skipped} 只。")
    close_thread_api()


def main() -> None:
    inject_project_root()
    try:
        run()
    finally:
        close_thread_api()


if __name__ == "__main__":
    main()
