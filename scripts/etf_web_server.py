#!/usr/bin/env python3
"""
Local web entry for the ETF three-factor report.

It keeps the generated report as the UI, while adding server-backed actions for:
  - date switching: GET /?date=YYYY-MM-DD
  - custom ETF creation: POST /api/etfs
"""

import argparse
import contextlib
import io
import json
import os
import re
import threading
import time
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import etf_v7_threefactor as report


DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
FLOW_CACHE_SECONDS = 60
FLOW_CACHE_DIR = Path(report.WORKSPACE) / "fund_flow_intraday"
FLOW_LOCK = threading.Lock()
FLOW_NO_PROXY_DOMAINS = ["push2.eastmoney.com"]
TENCENT_NO_PROXY_DOMAINS = ["web.ifzq.gtimg.cn"]
HOURLY_FLOW_CACHE = {}
HOURLY_FLOW_CACHE_SECONDS = 300


def validate_date(value):
    value = (value or "").strip()
    if not value:
        return None
    if not DATE_RE.match(value):
        raise ValueError("日期格式必须是 YYYY-MM-DD")
    datetime.strptime(value, "%Y-%m-%d")
    return value


def read_form(handler):
    length = int(handler.headers.get("Content-Length", "0") or "0")
    raw = handler.rfile.read(length).decode("utf-8", errors="replace")
    return {k: v[0] if v else "" for k, v in parse_qs(raw, keep_blank_values=True).items()}


def configure_flow_no_proxy():
    for key in ("NO_PROXY", "no_proxy"):
        current = os.environ.get(key, "")
        values = [x.strip() for x in current.split(",") if x.strip()]
        changed = False
        for domain in FLOW_NO_PROXY_DOMAINS + TENCENT_NO_PROXY_DOMAINS:
            if domain not in values:
                values.append(domain)
                changed = True
        if changed or not current:
            os.environ[key] = ",".join(values)


def flow_cache_path(date_value):
    FLOW_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return FLOW_CACHE_DIR / f"{date_value}.json"


def load_flow_cache(date_value):
    path = flow_cache_path(date_value)
    if not path.exists():
        return {"date": date_value, "snapshots": []}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or not isinstance(data.get("snapshots"), list):
            return {"date": date_value, "snapshots": []}
        data["date"] = date_value
        return data
    except Exception:
        return {"date": date_value, "snapshots": []}


def save_flow_cache(date_value, data):
    path = flow_cache_path(date_value)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def parse_eastmoney_ts(value):
    try:
        return datetime.fromtimestamp(int(value))
    except Exception:
        return datetime.now()


def fetch_sector_flow_snapshot():
    configure_flow_no_proxy()
    import requests

    params = {
        "pn": "1",
        "pz": "200",
        "po": "1",
        "np": "1",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "fltt": "2",
        "invt": "2",
        "fid0": "f62",
        "fs": "m:90 t:2",
        "stat": "1",
        "fields": "f12,f14,f2,f3,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f204,f205,f124",
        "_": int(time.time() * 1000),
    }
    response = requests.get(
        "https://push2.eastmoney.com/api/qt/clist/get",
        params=params,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=15,
    )
    response.raise_for_status()
    diff = (response.json().get("data") or {}).get("diff") or []
    rows = []
    for row in diff:
        try:
            ts = parse_eastmoney_ts(row.get("f124"))
            rows.append({
                "code": str(row.get("f12") or ""),
                "name": str(row.get("f14") or ""),
                "change_pct": float(row.get("f3") or 0),
                "main_net": float(row.get("f62") or 0),
                "main_net_pct": float(row.get("f184") or 0),
                "super_net": float(row.get("f66") or 0),
                "large_net": float(row.get("f72") or 0),
                "mid_net": float(row.get("f78") or 0),
                "small_net": float(row.get("f84") or 0),
                "leader": str(row.get("f204") or ""),
                "leader_code": str(row.get("f205") or ""),
                "source_ts": ts.isoformat(timespec="seconds"),
            })
        except Exception:
            continue
    source_dt = parse_eastmoney_ts(diff[0].get("f124")) if diff else datetime.now()
    return {
        "time": source_dt.strftime("%H:%M"),
        "timestamp": source_dt.isoformat(timespec="seconds"),
        "items": rows,
    }


def is_today(date_value):
    return date_value == datetime.now().strftime("%Y-%m-%d")


def append_flow_snapshot_if_needed(date_value, cache):
    if not is_today(date_value):
        return cache, "history_cache"
    snapshots = cache.get("snapshots", [])
    if snapshots:
        try:
            last_ts = datetime.fromisoformat(snapshots[-1]["timestamp"])
            if (datetime.now() - last_ts).total_seconds() < FLOW_CACHE_SECONDS:
                return cache, "hit"
        except Exception:
            pass

    snapshot = fetch_sector_flow_snapshot()
    if snapshot["timestamp"][:10] != date_value:
        return cache, "source_date_mismatch"
    if snapshots and snapshots[-1].get("time") == snapshot.get("time"):
        return cache, "hit"
    snapshots.append(snapshot)
    cache["snapshots"] = snapshots[-240:]
    save_flow_cache(date_value, cache)
    return cache, "fetched"


def build_flow_payload(date_value, allow_fetch=True):
    with FLOW_LOCK:
        cache = load_flow_cache(date_value)
        if allow_fetch:
            try:
                cache, status = append_flow_snapshot_if_needed(date_value, cache)
            except Exception as exc:
                status = f"fetch_error: {exc}"
        else:
            status = "history_cache" if cache.get("snapshots") else "empty_history"

    snapshots = cache.get("snapshots", [])
    latest = snapshots[-1] if snapshots else None
    latest_items = latest.get("items", []) if latest else []
    positives = sorted(latest_items, key=lambda x: x.get("main_net", 0), reverse=True)[:5]
    negatives = sorted(latest_items, key=lambda x: x.get("main_net", 0))[:5]
    selected_codes = []
    for item in positives + negatives:
        if item["code"] and item["code"] not in selected_codes:
            selected_codes.append(item["code"])

    meta = {}
    points_by_code = {code: [] for code in selected_codes}
    for snapshot in snapshots:
        by_code = {item.get("code"): item for item in snapshot.get("items", [])}
        for code in selected_codes:
            item = by_code.get(code)
            if not item:
                continue
            meta[code] = {
                "code": code,
                "name": item.get("name", code),
                "change_pct": item.get("change_pct", 0),
                "main_net_pct": item.get("main_net_pct", 0),
            }
            points_by_code[code].append({
                "time": snapshot.get("time"),
                "value": round((item.get("main_net") or 0) / 1e8, 2),
            })

    series = []
    for code in selected_codes:
        if points_by_code[code]:
            series.append({**meta.get(code, {"code": code, "name": code}), "points": points_by_code[code]})

    latest_rank = [
        {
            "code": item.get("code"),
            "name": item.get("name"),
            "change_pct": item.get("change_pct"),
            "main_net_yi": round((item.get("main_net") or 0) / 1e8, 2),
            "main_net_pct": item.get("main_net_pct"),
        }
        for item in positives + negatives
    ]
    return {
        "date": date_value,
        "updated_at": latest.get("timestamp") if latest else None,
        "series": series,
        "latest_rank": latest_rank,
        "cache_status": status,
    }


def tencent_symbol(code):
    code = str(code).strip()
    prefix = "sz" if code.startswith(("15", "16", "18")) else "sh"
    return f"{prefix}{code}"


def fetch_tencent_minute_day(code, date_value):
    configure_flow_no_proxy()
    import requests

    symbol = tencent_symbol(code)
    date_key = date_value.replace("-", "")
    headers = {"User-Agent": "Mozilla/5.0"}
    url = f"http://web.ifzq.gtimg.cn/appstock/app/day/query?code={symbol}"
    response = requests.get(url, headers=headers, timeout=12)
    response.raise_for_status()
    payload = response.json()
    container = (payload.get("data") or {}).get(symbol) or {}
    for item in container.get("data") or []:
        if str(item.get("date")) == date_key:
            return item.get("data") or [], item.get("prec")

    # When the selected analysis day is the latest session, minute/query is usually smaller and fresher.
    url = f"http://web.ifzq.gtimg.cn/appstock/app/minute/query?code={symbol}"
    response = requests.get(url, headers=headers, timeout=12)
    response.raise_for_status()
    payload = response.json()
    data_block = ((payload.get("data") or {}).get(symbol) or {}).get("data") or {}
    if str(data_block.get("date")) == date_key:
        return data_block.get("data") or [], data_block.get("prec")
    return [], None


def minute_label(raw_time):
    raw_time = str(raw_time).zfill(4)
    return f"{raw_time[:2]}:{raw_time[2:]}"


def should_keep_minute(raw_time, index, total):
    if index == 0 or index == total - 1:
        return True
    raw_time = str(raw_time).zfill(4)
    try:
        minute = int(raw_time[-2:])
    except ValueError:
        return False
    return minute in (0, 30)


def build_etf_flow_points(minute_rows, preclose):
    points = []
    previous_price = float(preclose or 0)
    previous_amount = 0.0
    cumulative = 0.0
    parsed = []
    for raw in minute_rows:
        parts = str(raw).split()
        if len(parts) < 4:
            continue
        try:
            parsed.append({
                "time": parts[0],
                "price": float(parts[1]),
                "amount": float(parts[3]),
            })
        except ValueError:
            continue
    if not parsed:
        return []
    if previous_price <= 0:
        previous_price = parsed[0]["price"]

    for idx, row in enumerate(parsed):
        amount_delta = max(0.0, row["amount"] - previous_amount)
        price_delta = row["price"] - previous_price
        if price_delta > 0:
            direction = 1
        elif price_delta < 0:
            direction = -1
        else:
            direction = 0
        net = direction * amount_delta / 1e8
        cumulative += net
        previous_price = row["price"]
        previous_amount = row["amount"]
        if should_keep_minute(row["time"], idx, len(parsed)):
            points.append({
                "time": minute_label(row["time"]),
                "value": round(cumulative, 2),
                "net": round(net, 2),
                "price": round(row["price"], 4),
            })
    return points


def build_hourly_flow_payload(date_value):
    report.refresh_custom_etfs()
    cache_key = (date_value, tuple(report.ETFS.keys()))
    now_ts = time.time()
    cached = HOURLY_FLOW_CACHE.get(cache_key)
    if cached and now_ts - cached.get("ts", 0) < HOURLY_FLOW_CACHE_SECONDS:
        return cached["payload"]

    colors = [
        "#ef4444", "#f97316", "#eab308", "#a855f7", "#38bdf8", "#14b8a6",
        "#22c55e", "#818cf8", "#f43f5e", "#06b6d4", "#84cc16", "#fb7185",
    ]
    series = []
    errors = []
    for idx, (code, info) in enumerate(report.ETFS.items()):
        try:
            rows, preclose = fetch_tencent_minute_day(code, date_value)
            points = build_etf_flow_points(rows, preclose)
        except Exception as exc:
            errors.append({"code": code, "error": str(exc)})
            points = []
        if not points:
            continue
        latest = points[-1]["value"]
        series.append({
            "code": code,
            "name": info.get("n") or code,
            "idx": info.get("idx") or "",
            "color": colors[idx % len(colors)],
            "points": points,
            "latest": latest,
        })

    values = [point["value"] for item in series for point in item["points"]]
    leaders = sorted(
        [{"code": item["code"], "name": item["name"], "color": item["color"], "value": item["latest"]} for item in series],
        key=lambda item: item["value"],
        reverse=True,
    )
    max_frames = max([len(item["points"]) for item in series], default=0)
    payload = {
        "date": date_value,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "unit": "亿",
        "source": "tencent-minute",
        "source_label": "腾讯分钟线 · 半小时采样 · 方向性成交额",
        "series": series,
        "errors": errors,
        "summary": {
            "high": round(max(values), 2) if values else 0,
            "low": round(min(values), 2) if values else 0,
            "frames": max_frames,
            "leaders": leaders,
            "etf_count": len(report.ETFS),
            "available_count": len(series),
        },
    }
    HOURLY_FLOW_CACHE[cache_key] = {"ts": now_ts, "payload": payload}
    return payload


def capture_report(date_value):
    # Keep terminal output readable in the server process; the HTML is returned to the browser.
    old_skip_backfill = os.environ.get("ETF_SKIP_BACKFILL")
    old_skip_live_shares = os.environ.get("ETF_SKIP_LIVE_SHARES")
    old_use_kline_cache = os.environ.get("ETF_USE_KLINE_CACHE")
    os.environ["ETF_SKIP_BACKFILL"] = "1"
    os.environ["ETF_SKIP_LIVE_SHARES"] = "1"
    os.environ["ETF_USE_KLINE_CACHE"] = "1"
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            return report.main(target_date=date_value)
    finally:
        if old_skip_backfill is None:
            os.environ.pop("ETF_SKIP_BACKFILL", None)
        else:
            os.environ["ETF_SKIP_BACKFILL"] = old_skip_backfill
        if old_skip_live_shares is None:
            os.environ.pop("ETF_SKIP_LIVE_SHARES", None)
        else:
            os.environ["ETF_SKIP_LIVE_SHARES"] = old_skip_live_shares
        if old_use_kline_cache is None:
            os.environ.pop("ETF_USE_KLINE_CACHE", None)
        else:
            os.environ["ETF_USE_KLINE_CACHE"] = old_use_kline_cache


class ETFHandler(BaseHTTPRequestHandler):
    server_version = "ETFThreeFactorWeb/1.0"

    def log_message(self, fmt, *args):
        print(f"[web] {self.address_string()} - {fmt % args}")

    def send_text(self, status, text, content_type="text/plain; charset=utf-8"):
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def redirect(self, path, params=None):
        location = path
        if params:
            location += "?" + urlencode({k: v for k, v in params.items() if v})
        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", location)
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/flow/hourly":
            try:
                query = parse_qs(parsed.query)
                date_value = validate_date(query.get("date", [""])[0]) or datetime.now().strftime("%Y-%m-%d")
                payload = build_hourly_flow_payload(date_value)
            except Exception as exc:
                self.send_text(HTTPStatus.INTERNAL_SERVER_ERROR, json.dumps({
                    "error": f"资金流曲线数据生成失败: {exc}"
                }, ensure_ascii=False), "application/json; charset=utf-8")
                return
            self.send_text(HTTPStatus.OK, json.dumps(payload, ensure_ascii=False, indent=2), "application/json; charset=utf-8")
            return

        if parsed.path == "/api/flow/intraday":
            try:
                query = parse_qs(parsed.query)
                date_value = validate_date(query.get("date", [""])[0]) or datetime.now().strftime("%Y-%m-%d")
                allow_fetch = query.get("realtime", ["1"])[0] not in ("0", "false", "no")
                payload = build_flow_payload(date_value, allow_fetch=allow_fetch)
            except Exception as exc:
                self.send_text(HTTPStatus.INTERNAL_SERVER_ERROR, json.dumps({
                    "error": f"资金流数据获取失败: {exc}"
                }, ensure_ascii=False), "application/json; charset=utf-8")
                return
            self.send_text(HTTPStatus.OK, json.dumps(payload, ensure_ascii=False, indent=2), "application/json; charset=utf-8")
            return

        if parsed.path == "/api/etfs":
            payload = {
                "etfs": [
                    {"code": code, "name": info["n"], "idx": info["idx"], "custom": code not in report.BUILTIN_ETF_CODES}
                    for code, info in report.ETFS.items()
                ]
            }
            self.send_text(HTTPStatus.OK, json.dumps(payload, ensure_ascii=False, indent=2), "application/json; charset=utf-8")
            return

        if parsed.path not in ("/", "/report", "/ETF三因子分析-v7.html"):
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        try:
            query = parse_qs(parsed.query)
            date_value = validate_date(query.get("date", [""])[0])
            html = capture_report(date_value)
        except Exception as exc:
            self.send_text(HTTPStatus.INTERNAL_SERVER_ERROR, f"报告生成失败: {exc}")
            return
        self.send_text(HTTPStatus.OK, html, "text/html; charset=utf-8")

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != "/api/etfs":
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        try:
            form = read_form(self)
            return_date = validate_date(form.get("return_date"))
            report.add_custom_etf(
                form.get("code", ""),
                form.get("name", ""),
                form.get("idx", "自选ETF"),
                form.get("priority", 3),
            )
        except Exception as exc:
            self.send_text(HTTPStatus.BAD_REQUEST, f"新增ETF失败: {exc}")
            return
        self.redirect("/", {"date": return_date, "refresh": "1"})


def main():
    parser = argparse.ArgumentParser(description="ETF三因子报告本地Web服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=12980)
    args = parser.parse_args()

    httpd = ThreadingHTTPServer((args.host, args.port), ETFHandler)
    url = f"http://{args.host}:{args.port}/"
    print(f"ETF三因子报告Web服务已启动: {url}")
    print("按 Ctrl+C 停止服务")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
