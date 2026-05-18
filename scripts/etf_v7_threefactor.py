#!/usr/bin/env python3
"""
ETF国家队资金监测流水线 v6.1 — 三因子模型 + 本地SQLite数据存储
量能概率 50% + 方向概率 20% + 份额概率 30%

v5 → v6 升级：
  - 新增份额因子（sprob），权重30%，捕捉一级市场申购
  - 综合概率 cp = vp*0.5 + dp*0.2 + sp*0.3 （原 vp*0.7 + dp*0.3）
  - 份额因子基于日份额变化/20日均份额
  - 支持指定分析日期（--date YYYY-MM-DD）
  - 份额因子历史回溯：load历史份额数据，用当日vs前日的变化计算

v6 → v6.1 升级（本地数据存储）：
  - 集成 etf_data_store.py 的 SQLite 数据库 (etf_history.db)
  - 实时份额数据自动写入 DB，解决 push2 API 不可回溯问题
  - 历史分析优先查本地 DB，其次查 JSON 历史文件
  - 分析结果自动存入 DB（包含概率、信号级别等）
  - 新增 --record 参数：只采集当日数据入库，不做完整分析
  - 新增 --stats 参数：查看数据库状态

v6.1 → v7 升级（数据源替换）：
  - push2.eastmoney.com 长期中断（Empty reply from server）
  - 替换为 akshare 上交所/深交所 ETF 份额接口
  - 上交所：fund_etf_scale_sse(date) - 按日期查询全市场SSE ETF份额
  - 深交所：fund_scale_daily_szse(start,end) - 按日期范围查询全市场SZSE ETF份额
  - 新增 fetch_history_shares_bulk() 批量回溯函数
  - 自动回补历史数据（JSON历史不足60天时触发）

使用方式：
  python3 etf_v6_threefactor.py                # 默认：最近交易日
  python3 etf_v6_threefactor.py --date 2026-04-30  # 指定日期
  python3 etf_v6_threefactor.py --send           # 发送邮件
  python3 etf_v6_threefactor.py --record         # 仅采集份额数据入库
  python3 etf_v6_threefactor.py --stats          # 查看DB状态
"""

import json, urllib.request, ssl, os, sys, math, argparse, smtplib, re, html as html_lib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, timedelta

# ---------- 本地数据存储模块 ----------
# 确保脚本所在目录在 sys.path 中（无论从哪个目录运行）
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)
try:
    from etf_data_store import ETFDataStore
    DATA_STORE_AVAILABLE = True
except ImportError:
    DATA_STORE_AVAILABLE = False
    print("⚠️ etf_data_store.py 未找到，本地数据存储功能不可用")

ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE

WORKSPACE = os.path.expanduser(os.environ.get("ETF_WORKSPACE", "~/.etf-skill/workspace"))
os.makedirs(WORKSPACE, exist_ok=True)
HTML_OUT = os.path.join(WORKSPACE, "ETF国家队监测-终版.html")
JSON_OUT = os.path.join(WORKSPACE, "ETF国家队监测-终版.json")
SHARES_OUT = os.path.join(WORKSPACE, "etf_shares_history.json")
THREE_FACTOR_OUT = os.path.join(WORKSPACE, "ETF三因子分析-v7.json")
THREE_FACTOR_HTML = os.path.join(WORKSPACE, "ETF三因子分析-v7.html")
CUSTOM_ETFS_PATH = os.path.join(WORKSPACE, "custom_etfs.json")
KLINE_CACHE_PATH = os.path.join(WORKSPACE, "kline_cache.json")
_KLINE_CACHE = None
EXCHANGE_NO_PROXY_DOMAINS = ["query.sse.com.cn", "www.sse.com.cn", "www.szse.cn"]

# ---------- 邮件配置（支持自定义） ----------
# 发件邮箱/收件邮箱：从环境变量读取，默认留空（用户需配置）
EMAIL_TO   = os.environ.get("ETF_EMAIL_TO",   "YOUR_EMAIL@qq.com")
EMAIL_FROM = os.environ.get("ETF_EMAIL_FROM", "YOUR_EMAIL@qq.com")
SMTP_HOST  = os.environ.get("ETF_SMTP_HOST",  "smtp.qq.com")
SMTP_PORT  = int(os.environ.get("ETF_SMTP_PORT", "465"))

ETFS = {
    "510300": {"n": "华泰柏瑞沪深300ETF", "idx": "沪深300", "p": 5},
    "510310": {"n": "易方达沪深300ETF",   "idx": "沪深300", "p": 5},
    "510330": {"n": "华夏沪深300ETF",     "idx": "沪深300", "p": 5},
    "159919": {"n": "嘉实沪深300ETF",     "idx": "沪深300", "p": 4},
    "510050": {"n": "华夏上证50ETF",      "idx": "上证50",  "p": 4},
    "510500": {"n": "华泰柏瑞中证500ETF",  "idx": "中证500",  "p": 3},
    "512100": {"n": "南方中证1000ETF",    "idx": "中证1000", "p": 3},
}
BUILTIN_ETF_CODES = set(ETFS)
ETF_CODE_RE = re.compile(r"^\d{6}$")


def normalize_etf_code(code):
    return (code or "").strip()


def normalize_etf_entry(code, entry):
    code = normalize_etf_code(code)
    if not ETF_CODE_RE.match(code):
        raise ValueError("ETF代码必须是6位数字")
    name = (entry.get("n") or entry.get("name") or "").strip()
    idx_name = (entry.get("idx") or entry.get("idx_name") or "自选ETF").strip()
    try:
        priority = int(entry.get("p", 3))
    except (TypeError, ValueError):
        priority = 3
    priority = max(1, min(priority, 5))
    if not name:
        raise ValueError("ETF名称不能为空")
    return code, {"n": name, "idx": idx_name, "p": priority}


def load_custom_etfs():
    if not os.path.exists(CUSTOM_ETFS_PATH):
        return {}
    try:
        with open(CUSTOM_ETFS_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as exc:
        print(f"⚠️ 自选ETF配置读取失败: {exc}")
        return {}

    items = raw.items() if isinstance(raw, dict) else ((x.get("code"), x) for x in raw if isinstance(x, dict))
    loaded = {}
    for code, entry in items:
        if not isinstance(entry, dict):
            continue
        try:
            norm_code, norm_entry = normalize_etf_entry(code, entry)
        except ValueError as exc:
            print(f"⚠️ 跳过自选ETF {code}: {exc}")
            continue
        if norm_code not in BUILTIN_ETF_CODES:
            loaded[norm_code] = norm_entry
    return loaded


def save_custom_etfs():
    custom = {code: info for code, info in ETFS.items() if code not in BUILTIN_ETF_CODES}
    with open(CUSTOM_ETFS_PATH, "w", encoding="utf-8") as f:
        json.dump(custom, f, ensure_ascii=False, indent=2)
    return custom


def refresh_custom_etfs():
    for code in list(ETFS):
        if code not in BUILTIN_ETF_CODES:
            del ETFS[code]
    ETFS.update(load_custom_etfs())
    return ETFS


def add_custom_etf(code, name, idx_name="自选ETF", priority=3):
    code, entry = normalize_etf_entry(code, {"n": name, "idx": idx_name, "p": priority})
    if code in BUILTIN_ETF_CODES:
        raise ValueError(f"{code} 已在默认ETF列表中")
    ETFS[code] = entry
    save_custom_etfs()
    return code, entry


refresh_custom_etfs()


def use_kline_cache():
    return os.environ.get("ETF_USE_KLINE_CACHE", "").lower() in ("1", "true", "yes")


def load_kline_cache():
    global _KLINE_CACHE
    if _KLINE_CACHE is not None:
        return _KLINE_CACHE
    if not os.path.exists(KLINE_CACHE_PATH):
        _KLINE_CACHE = {}
        return _KLINE_CACHE
    try:
        with open(KLINE_CACHE_PATH, "r", encoding="utf-8") as f:
            _KLINE_CACHE = json.load(f)
    except Exception:
        _KLINE_CACHE = {}
    return _KLINE_CACHE


def save_kline_cache():
    if _KLINE_CACHE is None:
        return
    with open(KLINE_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(_KLINE_CACHE, f, ensure_ascii=False)


def configure_exchange_no_proxy():
    for key in ("NO_PROXY", "no_proxy"):
        current = os.environ.get(key, "")
        values = [x.strip() for x in current.split(",") if x.strip()]
        changed = False
        for domain in EXCHANGE_NO_PROXY_DOMAINS:
            if domain not in values:
                values.append(domain)
                changed = True
        if changed or not current:
            os.environ[key] = ",".join(values)

PUSH2_MKT = {
    "510300": "1", "510310": "1", "510330": "1", "159919": "0",
    "510050": "1", "510500": "1", "512100": "1",
}

SPECIAL = {
    "2026-04-30": "五一前", "2026-05-06": "五一后",
}

# ============================================================
# 数据获取
# ============================================================

def fetch(code, limit=60):
    cache_key = f"{code}:{limit}"
    if use_kline_cache():
        cached = load_kline_cache().get(cache_key)
        if cached and cached.get("data"):
            return cached["data"]

    if code.startswith("sh") or code.startswith("sz"):
        pfx = code[:2]; numcode = code[2:]
    else:
        pfx = "sh" if code.startswith(("51", "56", "0")) else "sz"; numcode = code
    u = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={pfx}{numcode},day,,,{limit},qfq"
    try:
        r = urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(r, timeout=15, context=ssl_ctx) as resp:
            d = json.loads(resp.read().decode("utf-8"))
        k = d.get("data", {}).get(f"{pfx}{numcode}", {}).get("day", []) or \
            d.get("data", {}).get(f"{pfx}{numcode}", {}).get("qfqday", [])
        data = [{"date": r[0], "o": float(r[1]), "c": float(r[2]),
                 "h": float(r[3]), "l": float(r[4]), "v": float(r[5])} for r in k if len(r) >= 6 and r[0]]
        if use_kline_cache() and data:
            cache = load_kline_cache()
            cache[cache_key] = {"ts": datetime.now().isoformat(), "data": data}
            save_kline_cache()
        return data
    except:
        return []


# ============================================================
# 份额数据获取 (v7: akshare替代push2.eastmoney.com)
# ============================================================
# push2.eastmoney.com已长期中断(返回Empty reply from server)
# 替代方案:
#   上交所ETF: ak.fund_etf_scale_sse(date) — 返回指定日期全市场上交所ETF份额
#   深交所ETF: ak.fund_scale_daily_szse(start, end) — 返回日期范围内的深交所ETF份额
# 注意: 份额数据盘后更新(约19:00), 当日盘中无数据

# 缓存最近的SSE/SZSE结果, 避免重复API调用
_SSE_CACHE = {}     # {date_str: DataFrame}
_SZSE_CACHE = {}    # {date_str: {code: shares_yi}}

def _get_shares_sse(date_str):
    """获取指定日期的上交所ETF份额数据 (带缓存)"""
    if date_str in _SSE_CACHE:
        return _SSE_CACHE[date_str]
    try:
        configure_exchange_no_proxy()
        import akshare as ak
        import pandas as pd
        df = ak.fund_etf_scale_sse(date=date_str)
        if df is not None and len(df) > 0 and '基金代码' in df.columns:
            _SSE_CACHE[date_str] = df
            return df
    except Exception as e:
        # 当日数据未发布时会抛异常(empty DataFrame无预期列)
        print(f"  ⚠️ SSE份额获取失败 {date_str}: {e}")
    _SSE_CACHE[date_str] = None
    return None

def _get_shares_szse_range(start_date, end_date):
    """获取日期范围内的深交所ETF份额数据 (带缓存)"""
    cache_key = f"{start_date}_{end_date}"
    if cache_key in _SZSE_CACHE:
        return _SZSE_CACHE[cache_key]
    result = {}
    try:
        configure_exchange_no_proxy()
        import akshare as ak
        df = ak.fund_scale_daily_szse(start_date=start_date, end_date=end_date, symbol='ETF')
        if df is not None and len(df) > 0:
            for _, row in df.iterrows():
                code = str(row['基金代码'])
                try:
                    d = str(row['日期'])[:10].replace('-','')
                except:
                    d = start_date
                shares = float(row['基金份额'])
                if d not in result:
                    result[d] = {}
                result[d][code] = shares / 1e8  # 份 → 亿份
    except Exception as e:
        print(f"  ⚠️ SZSE份额获取失败 {start_date}~{end_date}: {e}")
    _SZSE_CACHE[cache_key] = result
    return result

def _get_price_from_kline(code, target_date):
    """从腾讯K线数据获取指定日期的收盘价"""
    # 复用已有的fetch()函数
    data = fetch(code, 60)
    if not data:
        return None
    for row in reversed(data):
        if row['date'] == target_date:
            return row['c']
    # 未匹配到精确日期, 返回最新收盘价
    return data[-1]['c'] if data else None

def fetch_fund_shares(code, target_date=None):
    """
    获取ETF份额数据 (v7: 使用 akshare SSE/SZSE API)
    
    参数:
        code: ETF代码 (如 '510300')
        target_date: 目标日期 YYYY-MM-DD 格式, None=最近交易日
    返回:
        {"shares_yi": 份额(亿份), "price": 收盘价, "mktval_yi": 市值(亿元)}
        或 None (数据不可用)
    """
    try:
        configure_exchange_no_proxy()
        import akshare as ak
    except ImportError:
        print(f"  ⚠️ akshare 未安装")
        return None

    if target_date:
        date_str = target_date.replace('-', '')
    else:
        # 使用最近交易日 (今天或昨天, 取决于份额是否发布)
        today = datetime.now()
        date_str = today.strftime('%Y%m%d')

    # 判断交易所
    if code.startswith('159') or code.startswith('16'):
        # 深交所ETF
        return _fetch_szse_shares(code, date_str)
    else:
        # 上交所ETF (51xxxx / 56xxxx / 58xxxx / 588xxx)
        return _fetch_sse_shares(code, date_str)

def _fetch_sse_shares(code, date_str):
    """从SSE获取上交所ETF份额"""
    # 尝试3天: 今天→昨天→前天 (份额数据可能延迟)
    from datetime import datetime, timedelta
    current_date = datetime.strptime(date_str, '%Y%m%d')

    for offset in [0, -1, -2]:
        try_date = (current_date + timedelta(days=offset)).strftime('%Y%m%d')
        df = _get_shares_sse(try_date)
        if df is None:
            continue
        try:
            row = df[df['基金代码'] == code]
            if len(row) > 0:
                shares_fen = float(row['基金份额'].values[0])
                shares_yi = round(shares_fen / 1e8, 4)
                # 获取价格
                target_display = try_date[:4] + '-' + try_date[4:6] + '-' + try_date[6:8]
                price = _get_price_from_kline(code, target_display)
                if price is None:
                    price = 0
                mktval_yi = round(shares_yi * price, 1)
                return {"shares_yi": shares_yi, "price": round(price, 3),
                        "mktval_yi": mktval_yi, "data_date": target_display}
        except (KeyError, IndexError, ValueError):
            continue

    return None

def _fetch_szse_shares(code, date_str):
    """从SZSE获取深交所ETF份额"""
    from datetime import datetime, timedelta
    current_date = datetime.strptime(date_str, '%Y%m%d')

    # SZSE批量查询最近7天
    start_date = (current_date - timedelta(days=7)).strftime('%Y%m%d')
    end_date = current_date.strftime('%Y%m%d')

    data_map = _get_shares_szse_range(start_date, end_date)

    # 从最新日期开始查找
    for offset in [0, -1, -2, -3, -4, -5, -6]:
        try_date = (current_date + timedelta(days=offset)).strftime('%Y%m%d')
        if try_date in data_map and code in data_map[try_date]:
            shares_yi = round(data_map[try_date][code], 4)
            target_display = try_date[:4] + '-' + try_date[4:6] + '-' + try_date[6:8]
            price = _get_price_from_kline(code, target_display)
            if price is None:
                price = 0
            mktval_yi = round(shares_yi * price, 1)
            return {"shares_yi": shares_yi, "price": round(price, 3),
                    "mktval_yi": mktval_yi, "data_date": target_display}

    return None

def fetch_history_shares_bulk(dates_list):
    """
    批量获取历史份额数据 (用于回溯初始化)
    
    参数:
        dates_list: 日期列表 ["2026-05-06", "2026-05-07", ...]
    返回:
        history_dict: {date: {code: {shares_yi: ...}}}  (与 load_shares_history 格式兼容)
    """
    history = {}
    if not dates_list:
        return history

    # 上交所: 逐日查询
    print(f"  📡 SSE份额: {len(dates_list)}日...")
    for d in sorted(dates_list):
        d8 = d.replace('-', '')
        df = _get_shares_sse(d8)
        if df is None:
            continue
        for code in ETFS:
            if code.startswith('159'):
                continue  # 深交所在后面处理
            try:
                row = df[df['基金代码'] == code]
                if len(row) > 0:
                    shares_yi = round(float(row['基金份额'].values[0]) / 1e8, 2)
                    if d not in history:
                        history[d] = {}
                    history[d][code] = {"shares_yi": shares_yi, "ts": d + "T19:00:00"}
            except (KeyError, IndexError, ValueError):
                pass

    # 深交所: 批量查询日期范围
    if dates_list:
        min_d = min(dates_list).replace('-', '')
        max_d = max(dates_list).replace('-', '')
        print(f"  📡 SZSE份额: {min_d}~{max_d}...")
        data_map = _get_shares_szse_range(min_d, max_d)
        for d_str, codes in data_map.items():
            d = f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:8]}"
            for code, shares_yi in codes.items():
                if code in ETFS:
                    if d not in history:
                        history[d] = {}
                    history[d][code] = {"shares_yi": round(shares_yi, 2), "ts": d + "T19:00:00"}

    return history


def load_shares_history():
    if not os.path.exists(SHARES_OUT): return {}
    try:
        with open(SHARES_OUT, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}


def save_shares_history(history):
    dates = sorted(history.keys())
    if len(dates) > 60:
        for old in dates[:-60]:
            del history[old]
    with open(SHARES_OUT, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def get_historical_share(code, target_date, history):
    """从历史记录中查找目标日期的份额数据，返回 (share_yi, prev_share_yi, delta_yi, delta_pct)"""
    if target_date in history and isinstance(history[target_date], dict) and code in history[target_date]:
        target_share = history[target_date][code].get("shares_yi")
        # 找前一日
        prev_share = None
        all_dates = sorted(history.keys())
        idx = all_dates.index(target_date) if target_date in all_dates else -1
        if idx > 0:
            for prev_d in all_dates[idx-1::-1]:
                if isinstance(history.get(prev_d, {}), dict) and code in history[prev_d]:
                    prev_share = history[prev_d][code].get("shares_yi")
                    break
        if target_share and prev_share:
            delta_yi = round(target_share - prev_share, 2)
            delta_pct = round(delta_yi / prev_share * 100, 2)
            return target_share, prev_share, delta_yi, delta_pct
        elif target_share:
            return target_share, None, None, None
    return None, None, None, None


def date_has_complete_shares(history, date):
    entries = history.get(date)
    if not isinstance(entries, dict):
        return False
    return all(
        code in entries and entries[code].get("shares_yi") is not None
        for code in ETFS
    )


def merge_shares_history(history, new_history):
    changed = False
    for date, entries in (new_history or {}).items():
        if not isinstance(entries, dict) or not entries:
            continue
        if date not in history or not isinstance(history.get(date), dict):
            history[date] = {}
        before = json.dumps(history[date], ensure_ascii=False, sort_keys=True)
        history[date].update(entries)
        after = json.dumps(history[date], ensure_ascii=False, sort_keys=True)
        changed = changed or before != after
    return changed


def get_prev_trading_date(trading_dates, target_date):
    if not target_date or not trading_dates:
        return None
    dates = sorted(trading_dates)
    if target_date in dates:
        idx = dates.index(target_date)
        return dates[idx - 1] if idx > 0 else None
    prev = [d for d in dates if d < target_date]
    return prev[-1] if prev else None


def ensure_share_cache(history, required_dates):
    missing = [d for d in required_dates if d and not date_has_complete_shares(history, d)]
    if not missing:
        print(f"  ✅ 份额缓存命中: {', '.join(required_dates)}")
        return False

    print(f"  📡 份额缓存缺失，获取: {', '.join(missing)}")
    fetched = fetch_history_shares_bulk(missing)
    if not fetched:
        print("  ⚠️ 份额接口未返回可用数据")
        return False
    changed = merge_shares_history(history, fetched)
    if changed:
        save_shares_history(history)
        print(f"  ✅ 已缓存{len(fetched)}日份额数据")
    return changed


# ============================================================
# 三因子模型核心函数
# ============================================================

def vprob(r):
    """量能概率（原权重70%→现50%）"""
    if r < 0.5: return max(0, r / 0.5 * 5)
    if r < 1.0: return 5 + (r - 0.5) / 0.5 * 12
    if r < 1.3: return 17 + (r - 1) / 0.3 * 18
    if r < 1.5: return 35 + (r - 1.3) / 0.2 * 20
    if r < 2.0: return 55 + (r - 1.5) / 0.5 * 25
    if r < 3.0: return 80 + (r - 2) / 1 * 15
    if r < 5.0: return 95 + (r - 3) / 2 * 3
    return min(100, 98 + (r - 5) / 5 * 2)


def dprob(chg, t5_etf, t5_idx, vr, idx_chg):
    """方向概率（原权重30%→现20%）"""
    rally_discount = 1.0
    if idx_chg > 2.0: rally_discount = 0.60
    elif idx_chg > 1.5: rally_discount = 0.70
    elif idx_chg > 1.0: rally_discount = 0.80
    elif idx_chg > 0.5: rally_discount = 0.90

    if chg > 0.3 and t5_idx < -1:      f1 = 95
    elif chg > 0 and t5_idx < -0.5:     f1 = 85
    elif chg > 0 and t5_idx < 0:        f1 = 70
    elif abs(chg) < 0.15 and t5_idx < -1: f1 = 80
    elif abs(chg) < 0.3 and t5_idx < -0.5: f1 = 65
    elif chg > 1 and vr > 1.5 and idx_chg > 1: f1 = 25
    elif chg > 1 and vr > 1.5:          f1 = 45
    elif chg > 0.5 and vr > 1.3 and idx_chg > 1: f1 = 35
    elif chg > 0.5 and vr > 1.3:        f1 = 50
    elif chg > 0:                       f1 = 40
    elif chg < -1.5 and vr > 2:         f1 = 8
    elif chg < -0.5 and vr > 1.5:       f1 = 15
    else:                               f1 = 25

    gap = t5_etf - t5_idx
    if gap > 3:      f2 = 95
    elif gap > 2:    f2 = 85
    elif gap > 1.2:  f2 = 75
    elif gap > 0.6:  f2 = 60
    elif gap > 0.2:  f2 = 50
    elif gap > -0.2: f2 = 40
    elif gap > -0.6: f2 = 30
    else:            f2 = 15

    if t5_idx < -4:     f3 = 95
    elif t5_idx < -3:   f3 = 90
    elif t5_idx < -2:   f3 = 80
    elif t5_idx < -1:   f3 = 70
    elif t5_idx < -0.5: f3 = 55
    elif t5_idx < 0:    f3 = 45
    elif t5_idx < 1:    f3 = 35
    elif t5_idx < 3:    f3 = 20
    else:               f3 = 10
    f4 = 35

    raw = f1 * 0.4 + f2 * 0.3 + f3 * 0.2 + f4 * 0.1
    return round(raw * rally_discount, 1)


def sprob(share_delta_pct):
    """
    份额概率（权重30%）【v6新增】
    基于日份额变化 / 20日均份额（近似用日变化比）
    
    份额变动比 → 份额概率：
      >10% → 95%  |  >5% → 80%  |  >3% → 65%  |  >1% → 45%
      0-1% → 30%  |  < -1% → 15% |  < -5% → 5%  
    """
    if share_delta_pct is None:
        return None  # 数据不可用
    ap = abs(share_delta_pct)
    if share_delta_pct > 10:   return 95
    elif share_delta_pct > 5:  return 80 + (share_delta_pct - 5) / 5 * 15
    elif share_delta_pct > 3:  return 65 + (share_delta_pct - 3) / 2 * 15
    elif share_delta_pct > 1:  return 45 + (share_delta_pct - 1) / 2 * 20
    elif share_delta_pct > 0:  return 30 + share_delta_pct / 1 * 15
    elif share_delta_pct > -1:  return 15 + (share_delta_pct + 1) / 1 * 15
    elif share_delta_pct > -5:  return 5 + (share_delta_pct + 5) / 4 * 10
    else:                       return max(0, 5 + (share_delta_pct + 5) / 5 * 5)


def analyze_all(data, idx_d, shares_map, target_date, days=35, code=None):
    """
    三因子模型分析
    shares_map: {code: {date: {shares_yi, prev_shares_yi, delta_yi, delta_pct}}}
    """
    if len(data) < 22: return []
    res = []
    aligned = align_idx(data, idx_d)
    for i in range(max(21, len(data) - days), len(data)):
        d = data[i]
        v = d["v"] / 10000
        pv = [data[j]["v"] / 10000 for j in range(i - 20, i)]
        ma = sum(pv) / 20
        if ma == 0: continue
        vr = v / ma
        pc = data[i - 1]["c"]
        chg = (d["c"] - pc) / pc * 100 if pc > 0 else 0
        t5 = i >= 6 and data[i - 5]["c"] > 0 and (d["c"] - data[i - 5]["c"]) / data[i - 5]["c"] * 100 or 0
        t5i = t5
        idchg = 0
        if i < len(aligned) and aligned[i] is not None:
            ii = aligned[i]
            vp_idx = ii > 0 and idx_d[ii - 1]["c"] > 0 and (idx_d[ii]["c"] - idx_d[ii - 1]["c"]) / idx_d[ii - 1]["c"] * 100 or 0
            idchg = round(vp_idx, 1)
            if i >= 6 and aligned[i - 5] is not None:
                j5 = aligned[i - 5]
                t5i = idx_d[j5]["c"] > 0 and (idx_d[ii]["c"] - idx_d[j5]["c"]) / idx_d[j5]["c"] * 100 or 0
        vp = vprob(vr)
        dp = dprob(chg, t5, round(t5i, 2), vr, idchg)

        # 三因子：份额概率
        sp = None
        share_delta_pct = None
        share_delta_yi = None
        if code and d["date"] in shares_map.get(code, {}):
            info = shares_map[code][d["date"]]
            share_delta_pct = info.get("delta_pct")
            share_delta_yi = info.get("delta_yi")
            sp = sprob(share_delta_pct)

        # 三因子综合概率
        if sp is not None:
            cp = round(vp * 0.5 + dp * 0.2 + sp * 0.3, 1)
        else:
            # 份额数据不可用，退化为二因子（保持70/30用于对比）
            cp = round(vp * 0.7 + dp * 0.3, 1)

        tag = SPECIAL.get(d["date"], "")
        res.append({
            "d": d["date"], "c": d["c"], "chg": round(chg, 2),
            "t5": round(t5, 2), "t5i": round(t5i, 2), "idx_chg": idchg,
            "v": round(v, 2), "vma": round(ma, 2), "vr": round(vr, 2),
            "vp": round(vp, 1), "dp": dp, "sp": sp, "cp": cp,
            "share_delta_pct": share_delta_pct, "share_delta_yi": share_delta_yi,
            "tag": tag, "has_shares": sp is not None,
        })
    return res


def align_idx(data, idx_d):
    idx_map = {}
    for j, d in enumerate(idx_d):
        idx_map[d["date"]] = j
    return [idx_map.get(d["date"]) for d in data]


# ============================================================
# HTML 报告生成（三因子版）
# ============================================================

def gen_html(all_hist, latest_map, idx_300_data, shares_data, target_date):
    esc = html_lib.escape
    dates = set()
    for hh in all_hist.values():
        for h in hh:
            dates.add(h["d"])
    dates = sorted(dates)
    primary_date = target_date if target_date in dates else (dates[-1] if dates else target_date)
    view_date = target_date or primary_date
    date_caption = f"分析日: {view_date}"
    if view_date and primary_date and view_date != primary_date:
        date_caption += f" · 表格基准: {primary_date}"
    etf_count = len(ETFS)

    primary = {}
    high_codes = []
    mid_codes = []
    for code, hist in all_hist.items():
        for h in hist:
            if h["d"] == primary_date:
                primary[code] = h
                if h["cp"] >= 70: high_codes.append(code)
                elif h["cp"] >= 50: mid_codes.append(code)
                break

    hs300_codes = [c for c in ETFS if ETFS[c]["idx"] == "沪深300"]
    hs300_alerts = sum(1 for c in hs300_codes if c in primary and primary[c]["cp"] >= 50)
    hs300_total = max(1, len(hs300_codes))
    total_high = len(high_codes)
    total_mid = len(mid_codes)

    idx_300_hist = {}
    if idx_300_data:
        for d in idx_300_data:
            idx_300_hist[d["date"]] = d
    idx_gain = 0
    if primary_date in idx_300_hist and primary_date in dates:
        pd = idx_300_hist[primary_date]
        prev_d = dates.index(primary_date) > 0 and dates[dates.index(primary_date) - 1]
        if prev_d and prev_d in idx_300_hist:
            pp = idx_300_hist[prev_d]
            idx_gain = round((pd["c"] - pp["c"]) / pp["c"] * 100, 2)

    avg_dp = 0
    dp_count = 0
    for code in primary:
        avg_dp += primary[code]["dp"]
        dp_count += 1
    avg_dp = avg_dp / dp_count if dp_count > 0 else 0

    # 份额数据汇总
    net_purchase_total = 0
    net_redempt_total = 0
    shares_available_count = 0
    for code, sd in shares_data.items():
        d = sd.get("delta_yi")
        if d is not None:
            if d > 0: net_purchase_total += d
            else: net_redempt_total += abs(d)
            shares_available_count += 1

    # 三因子模型标识
    threef_tag = "三因子: 量能50%+方向20%+份额30%"
    if shares_available_count == 0:
        threef_tag += " (份额数据不可回溯，退化为二因子70/30)"

    # 综合判断
    if total_high >= 2 and hs300_alerts >= 3:
        if idx_gain > 1.0 and avg_dp < 40:
            verdict = f"⚠️ 多ETF放量高确信({total_high}只)，但{primary_date}大盘涨{idx_gain:+.2f}%。普涨环境下放量未必国家队专属。{threef_tag}。份额端：{'+' if net_purchase_total > net_redempt_total else ''}{net_purchase_total:.1f}亿净申购/{net_redempt_total:.1f}亿净赎回。"
            vcls = "warn"
        elif idx_gain > 1.5:
            verdict = f"⚠️ 多ETF放量高确信，但{primary_date}大盘涨{idx_gain:+.2f}%。{threef_tag}。份额端：{'+' if net_purchase_total > net_redempt_total else ''}{net_purchase_total:.1f}亿净申购/{net_redempt_total:.1f}亿净赎回。"
            vcls = "warn"
        else:
            verdict = f"🔥 {total_high}只ETF高确信·{hs300_alerts}/4沪深300同步。{threef_tag}。份额端：{'+' if net_purchase_total > net_redempt_total else ''}{net_purchase_total:.1f}亿净申购/{net_redempt_total:.1f}亿净赎回。"
            vcls = "warn"
    elif total_high >= 1:
        verdict = f"⚠️ 部分ETF触发高确信（{', '.join(ETFS[c]['n'][:6] for c in high_codes)}等）。{threef_tag}。份额端：{'+' if net_purchase_total > net_redempt_total else ''}{net_purchase_total:.1f}亿净申购/{net_redempt_total:.1f}亿净赎回。"
        vcls = "warn"
    elif total_mid >= 2:
        verdict = f"📊 {total_mid}只中等信号。{threef_tag}。份额端：{'+' if net_purchase_total > net_redempt_total else ''}{net_purchase_total:.1f}亿净申购/{net_redempt_total:.1f}亿净赎回。"
        vcls = "mid"
    else:
        verdict = f"✅ {primary_date} 全市场正常。{threef_tag}。"
        if shares_available_count == 0:
            verdict += " 份额数据不可回溯"

    # 15日信号趋势
    date_score = {}
    for code, hist in all_hist.items():
        for h in hist:
            d = h["d"]
            if d not in date_score:
                date_score[d] = {"cnt": 0, "high": 0, "mid": 0, "avg": 0}
            date_score[d]["cnt"] += 1
            date_score[d]["avg"] += h["cp"]
            if h["cp"] >= 70: date_score[d]["high"] += 1
            elif h["cp"] >= 50: date_score[d]["mid"] += 1
    for d in date_score:
        date_score[d]["avg"] = round(date_score[d]["avg"] / date_score[d]["cnt"], 1)
    date_score[d]["total"] = date_score[d]["high"] * 2 + date_score[d]["mid"]

    trend_dates = sorted(date_score.keys())[-15:]
    bars = ""
    for d in trend_dates:
        ds = date_score[d]
        h = min(42, max(3, ds["avg"] * 0.55))
        if ds["high"] >= 2: cls = "bar-hi"
        elif ds["high"] + ds["mid"] >= 2: cls = "bar-md"
        elif ds["mid"] >= 2: cls = "bar-md"
        else: cls = "bar-lo"
        tag = SPECIAL.get(d, "")
        tl = f"{d} {tag}" if tag else d
        bars += f'<div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:3px"><div class="bar {cls}" style="height:{h}px" title="{tl}: {ds["high"]}高+{ds["mid"]}中 CP均{ds["avg"]:.0f}%"></div><span style="font-size:8px;color:#4a5568">{d[5:]}</span></div>'

    # ETF表格行
    rows = ""
    for code, info in ETFS.items():
        p = primary.get(code)
        sd = shares_data.get(code, {})
        cp = p["cp"] if p else 0
        if cp >= 70:
            cls = "tr-hi"; sc = "#ef4444"; si = "🔴"
        elif cp >= 50:
            cls = "tr-md"; sc = "#f59e0b"; si = "🟡"
        else:
            cls = ""; sc = "#22c55e"; si = "🟢"

        chg = p["chg"] if p else 0
        chc = "#ef4444" if chg > 0 else ("#22c55e" if chg < 0 else "#94a3b8")
        tag_html = f'<span style="font-size:8px;background:rgba(239,68,68,0.15);color:#fca5a5;padding:1px 4px;border-radius:2px;margin-left:4px">{p["tag"]}</span>' if (p and p.get("tag")) else ""

        # 份额列
        sh_html = ""
        if sd:
            sh_yi = sd.get("shares_yi", "-")
            d_yi = sd.get("delta_yi")
            d_pct = sd.get("delta_pct")
            if d_yi is not None:
                dc = "#22c55e" if d_yi > 0 else ("#ef4444" if d_yi < 0 else "#94a3b8")
                arrow = "↑" if d_yi > 0 else ("↓" if d_yi < 0 else "→")
                sh_html = f'<td style="color:#94a3b8">{sh_yi:.1f}亿</td><td style="font-weight:600;color:{dc}">{arrow}{abs(d_yi):.1f}亿({d_pct:+.2f}%)</td>'
            else:
                sh_html = f'<td style="color:#94a3b8">{sh_yi}亿</td><td style="color:#64748b">-</td>'
        else:
            sh_html = '<td style="color:#64748b">-</td><td style="color:#64748b">-</td>'

        # 份额概率列（新增）
        sp_val = p["sp"] if p and p.get("has_shares") else "-"
        sp_col = "#94a3b8"
        if isinstance(sp_val, (int, float)):
            sp_col = "#ef4444" if sp_val >= 70 else ("#f59e0b" if sp_val >= 50 else "#22c55e")
            sp_display = f'{sp_val:.0f}%'
        else:
            sp_display = "-"

        # 模型标识
        model_note = "三因子" if (p and p.get("has_shares")) else "二因子"

        safe_name = esc(info["n"])
        safe_code = esc(code)
        rows += f'''<tr class="{cls}">
  <td style="white-space:nowrap">{si} <b>{safe_name}</b></td>
  <td style="color:#64748b">{safe_code}</td>
  <td style="color:{chc}">{chg:+.2f}%</td>
  <td>{p["v"]:.0f}万</td>
  <td>{p["vma"]:.0f}万</td>
  <td style="font-weight:600;color:#cbd5e1">{p["vr"]:.2f}x</td>{sh_html}
  <td style="color:#94a3b8">{p["vp"]:.0f}%</td>
  <td style="color:#94a3b8">{p["dp"]:.0f}%</td>
  <td style="color:{sp_col}">{sp_display}</td>
  <td style="font-weight:700;font-size:13px;color:{sc};white-space:nowrap">{cp:.0f}%{tag_html}</td>
</tr>'''

    # 信号列表
    signal_dates = [(d, v) for d, v in date_score.items() if v["high"] + v["mid"] >= 3]
    signal_dates.sort(key=lambda x: x[0], reverse=True)
    sig_list = ""
    for d, v in signal_dates[:8]:
        tag = SPECIAL.get(d, "")
        dots = '<div class="sig-dots">'
        dots += '<div class="sig-dot hi"></div>' * v["high"]
        dots += '<div class="sig-dot md"></div>' * v["mid"]
        dots += '</div>'
        tag_html = f'<span class="sig-tag">{tag}</span>' if tag else ""
        cnt = f'🔥{v["high"]} 🟡{v["mid"]} CP{v["avg"]:.0f}%'
        sig_list += f'<div class="sig-row"><span class="sig-date">{d[5:]}</span>{tag_html}{dots}<span class="sig-cnt">{cnt}</span></div>'

    # 份额概览
    total_shares = sum((shares_data.get(c, {}).get("shares_yi") or 0) for c in ETFS)
    total_delta = sum((shares_data.get(c, {}).get("delta_yi") or 0) for c in ETFS)
    delta_cls = "#22c55e" if total_delta > 0 else ("#ef4444" if total_delta < 0 else "#94a3b8")
    delta_arrow = "↑" if total_delta > 0 else ("↓" if total_delta < 0 else "→")

    # 模型切换说明
    model_desc = "三因子模型: 量能P×50% + 方向P×20% + 份额P×30%"
    if shares_available_count == 0:
        model_desc += " (份额数据不可回溯，当前退化为二因子70/30)"

    idx_options = ""
    for opt in ["沪深300", "上证50", "中证500", "中证1000", "科创50", "创业板指", "自选ETF"]:
        idx_options += f'<option value="{esc(opt)}">{esc(opt)}</option>'

    return f'''<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="UTF-8"><meta name="viewport" content="width=1440,initial-scale=1">
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
html{{display:flex;justify-content:center;align-items:center;min-height:100vh;background:#0a0f1a}}
body{{width:1440px;height:810px;overflow:hidden;font-family:-apple-system,"SF Pro Display","PingFang SC","Microsoft YaHei",sans-serif;background:#111c2e;color:#dfe6ef;display:flex;flex-direction:column;border-radius:12px;box-shadow:0 0 80px rgba(56,189,248,0.04)}}
body::before{{content:'';position:absolute;inset:0;background-image:radial-gradient(rgba(148,163,184,0.03) 1px,transparent 1px);background-size:28px 28px;pointer-events:none;z-index:0}}
.hdr{{padding:8px 24px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid rgba(56,189,248,0.12);flex-shrink:0;position:relative;z-index:1;background:rgba(17,28,46,0.7);backdrop-filter:blur(8px)}}
.hdr-left{{display:flex;align-items:baseline;gap:14px}}
.hdr h1{{font-size:18px;font-weight:700;color:#f1f5f9;letter-spacing:-0.3px}}
.hdr .sub{{font-size:12px;color:#8896ab}}
.hdr .meta{{font-size:11px;color:#7a8ba0;text-align:right;line-height:1.6}}
.hdr .meta .dot{{display:inline-block;width:6px;height:6px;border-radius:50%;background:#22c55e;margin-right:5px;box-shadow:0 0 6px rgba(34,197,94,0.5)}}
.banner{{margin:6px 22px 0;padding:6px 16px;border-radius:8px;font-size:12px;line-height:1.45;flex-shrink:0;position:relative;z-index:1;display:flex;align-items:flex-start;gap:8px}}
.banner.warn{{background:rgba(239,68,68,0.06);border:1px solid rgba(239,68,68,0.18);color:#fca5a5}}
.banner.mid{{background:rgba(245,158,11,0.05);border:1px solid rgba(245,158,11,0.15);color:#fcd34d}}
.banner.ok{{background:rgba(34,197,94,0.04);border:1px solid rgba(34,197,94,0.12);color:#86efac}}
.banner .ico{{font-size:18px;flex-shrink:0;margin-top:1px}}
.banner b{{color:#e8edf5}}
.controls{{margin:6px 22px 0;display:grid;grid-template-columns:520px 1fr;gap:10px;position:relative;z-index:1;flex-shrink:0}}
.ctrl-form{{min-width:0;display:flex;align-items:center;gap:8px;background:rgba(24,36,56,0.46);border:1px solid rgba(56,189,248,0.1);border-radius:8px;padding:8px 10px}}
.ctrl-form.compact{{justify-content:flex-start}}
.ctrl-form label{{font-size:11px;color:#7a8ba0;font-weight:600;white-space:nowrap}}
.ctrl-form input,.ctrl-form select{{height:28px;border:1px solid rgba(148,163,184,0.22);background:#0f1726;color:#dfe6ef;border-radius:6px;padding:0 9px;font:inherit;font-size:12px;outline:none;min-width:0}}
.ctrl-form input:focus,.ctrl-form select:focus{{border-color:#38bdf8;box-shadow:0 0 0 2px rgba(56,189,248,0.12)}}
.ctrl-form.compact input[type="date"]{{width:142px;flex:0 0 142px}}
.ctrl-form input[name="code"]{{width:92px}}
.ctrl-form input[name="name"]{{width:190px}}
.ctrl-form select{{width:118px}}
.ctrl-form button{{height:28px;border:0;border-radius:6px;background:#38bdf8;color:#06111f;font-weight:700;font-size:12px;padding:0 14px;cursor:pointer;white-space:nowrap}}
.ctrl-form button.secondary{{background:#818cf8;color:#f8fafc}}
.ctrl-form button.flow-btn{{background:#f59e0b;color:#111827}}
.ctrl-hint{{font-size:11px;color:#64748b;white-space:nowrap;margin-left:auto}}
.stats{{display:flex;gap:10px;padding:6px 22px 4px;flex-shrink:0;position:relative;z-index:1}}
.stat{{flex:1;background:rgba(24,36,56,0.5);border:1px solid rgba(56,189,248,0.1);border-radius:8px;padding:10px 14px;display:flex;align-items:center;gap:10px}}
.stat .vi{{font-size:26px;font-weight:900;line-height:1}}
.stat .tx{{font-size:11px;color:#8896ab;line-height:1.3}}
.stat .tx span{{display:block;font-size:12px;color:#dfe6ef;font-weight:600}}
.main{{display:flex;flex:1;padding:8px 22px 6px;gap:14px;overflow:hidden;position:relative;z-index:1;align-items:stretch}}
.tbl-wrap{{flex:1;min-width:0;overflow:hidden;background:rgba(20,32,50,0.4);border-radius:8px;border:1px solid rgba(56,189,248,0.08);display:flex;flex-direction:column}}
.tbl-wrap table{{width:100%;border-collapse:collapse;font-size:12px;table-layout:fixed}}
.tbl-wrap thead th{{text-align:left;padding:5px 6px;font-weight:600;color:#7a8ba0;font-size:10px;text-transform:uppercase;letter-spacing:0.5px;border-bottom:1px solid rgba(56,189,248,0.1);white-space:nowrap;background:rgba(18,28,44,0.4)}}
.tbl-wrap td{{padding:12px 6px;border-bottom:1px solid rgba(20,30,50,0.5);color:#b0bdd0;transition:background 0.15s;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.tbl-wrap tbody tr:hover td{{background:rgba(56,189,248,0.03)}}
.tbl-wrap tr.tr-hi td{{background:rgba(239,68,68,0.05)}}
.tbl-wrap tr.tr-hi:hover td{{background:rgba(239,68,68,0.08)}}
.tbl-wrap tr.tr-md td{{background:rgba(245,158,11,0.03)}}
.tbl-wrap tr.tr-md:hover td{{background:rgba(245,158,11,0.06)}}
.tbl-wrap .tnote{{font-size:10px;color:#64748b;padding:6px 10px;border-top:1px solid rgba(56,189,248,0.08);margin-top:auto}}
.rp{{width:380px;display:flex;flex-direction:column;gap:10px;overflow:hidden;flex-shrink:0;align-self:stretch}}
.rp .card{{background:rgba(22,34,52,0.45);border:1px solid rgba(56,189,248,0.08);border-radius:8px;overflow:hidden}}
.rp .card:last-child{{flex:1;display:flex;flex-direction:column}}
.rp .card .ttl{{font-size:11px;font-weight:600;color:#8b9bb5;padding:8px 10px 6px;display:flex;align-items:center;gap:6px}}
.rp .card .ttl::before{{content:'';width:3px;height:12px;background:linear-gradient(180deg,#38bdf8,#818cf8);border-radius:2px}}
.flow-modal{{position:fixed;inset:0;background:rgba(4,10,18,0.74);display:none;align-items:center;justify-content:center;z-index:50;padding:28px}}
.flow-modal.open{{display:flex}}
.flow-panel{{width:min(1060px,96vw);height:min(650px,90vh);background:#101b2d;border:1px solid rgba(56,189,248,0.18);border-radius:8px;box-shadow:0 24px 80px rgba(0,0,0,0.35);display:flex;flex-direction:column;overflow:hidden}}
.flow-panel-hd{{height:54px;display:flex;align-items:center;justify-content:space-between;padding:0 18px;border-bottom:1px solid rgba(56,189,248,0.12)}}
.flow-title{{font-size:16px;font-weight:800;color:#f8fafc}}
.flow-meta{{font-size:11px;color:#64748b;margin-top:3px}}
.flow-tools{{display:flex;align-items:center;gap:8px}}
.flow-tools button{{height:30px;border:0;border-radius:6px;background:#38bdf8;color:#07111f;font-weight:700;font-size:12px;padding:0 12px;cursor:pointer}}
.flow-tools button.alt{{background:#24344f;color:#cbd5e1;border:1px solid rgba(148,163,184,0.16)}}
.flow-panel-bd{{flex:1;display:grid;grid-template-columns:1fr 260px;gap:14px;padding:14px;min-height:0}}
.flow-visual{{min-width:0;min-height:0;display:flex;flex-direction:column;gap:8px}}
.flow-market{{min-height:42px;display:grid;grid-template-columns:repeat(3,1fr);gap:8px}}
.flow-index{{display:flex;align-items:center;justify-content:space-between;gap:8px;border:1px solid rgba(56,189,248,0.1);border-radius:8px;background:rgba(8,14,26,0.34);padding:7px 10px;min-width:0}}
.flow-index .nm{{font-size:12px;font-weight:800;color:#dbeafe;white-space:nowrap}}
.flow-index .pct{{font-size:14px;font-weight:900;font-variant-numeric:tabular-nums;white-space:nowrap}}
.flow-index .tm{{font-size:10px;color:#64748b;margin-top:2px;text-align:right;white-space:nowrap}}
.flow-stage{{position:relative;flex:1;min-width:0;min-height:0;border:1px solid rgba(56,189,248,0.1);border-radius:8px;background:rgba(8,14,26,0.5);overflow:hidden}}
.flow-stage svg{{display:block;width:100%;height:100%}}
.flow-empty{{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;text-align:center;color:#64748b;font-size:13px;line-height:1.6;padding:40px}}
.flow-axis{{stroke:rgba(148,163,184,0.34);stroke-width:1}}
.flow-grid{{stroke:rgba(148,163,184,0.08);stroke-width:1}}
.flow-line{{filter:drop-shadow(0 0 4px rgba(56,189,248,0.08))}}
.flow-label{{font-size:11px;font-weight:700;paint-order:stroke;stroke:#101b2d;stroke-width:4px;stroke-linejoin:round}}
.flow-side{{min-width:0;border:1px solid rgba(56,189,248,0.1);border-radius:8px;background:rgba(8,14,26,0.35);padding:10px;overflow:auto}}
.flow-side h3{{font-size:12px;color:#8b9bb5;margin-bottom:8px}}
.flow-metric{{padding:10px;border-radius:7px;background:rgba(20,30,50,0.48);border:1px solid rgba(148,163,184,0.08);margin-bottom:8px}}
.flow-metric .k{{font-size:10px;color:#64748b;margin-bottom:4px}}
.flow-metric .v{{font-size:18px;font-weight:900;font-variant-numeric:tabular-nums;color:#e2e8f0}}
.flow-metric .s{{font-size:11px;color:#8b9bb5;margin-top:3px}}
.flow-rank-row{{display:flex;align-items:center;justify-content:space-between;gap:8px;padding:7px 6px;border-radius:6px;background:rgba(20,30,50,0.46);margin-bottom:6px;font-size:12px}}
.flow-rank-row .nm{{color:#dbeafe;font-weight:700;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.flow-rank-row .val{{font-variant-numeric:tabular-nums;font-weight:800;white-space:nowrap}}
.flow-rank-left{{display:flex;align-items:center;gap:7px;min-width:0}}
.flow-swatch{{width:9px;height:9px;border-radius:50%;flex-shrink:0;box-shadow:0 0 10px currentColor}}
.flow-progress{{height:30px;display:flex;align-items:center;gap:10px;padding:0 18px 14px;color:#64748b;font-size:11px}}
.flow-progress input{{flex:1;accent-color:#38bdf8}}
.rp .trend{{display:flex;align-items:flex-end;gap:2px;height:44px;padding:4px 10px 8px}}
.rp .trend .bar{{flex:1;border-radius:2px 2px 0 0;min-width:4px}}
.rp .trend .bar-hi{{background:linear-gradient(180deg,#ef4444aa,#ef444444)}}
.rp .trend .bar-md{{background:linear-gradient(180deg,#f59e0baa,#f59e0b44)}}
.rp .trend .bar-lo{{background:linear-gradient(180deg,#334155,#1a2234)}}
.rp .sig{{padding:6px 12px 8px;display:flex;flex-direction:column;gap:5px;flex:1;overflow-y:auto}}
.rp .sig-row{{display:flex;align-items:center;gap:8px;padding:5px 8px;border-radius:6px;background:rgba(20,30,50,0.4)}}
.rp .sig-date{{font-size:12px;font-weight:600;color:#dfe6ef;min-width:60px}}
.rp .sig-tag{{font-size:9px;background:rgba(56,189,248,0.1);color:#7dd3fc;padding:1px 6px;border-radius:3px}}
.rp .sig-dots{{display:flex;gap:3px;flex:1}}
.rp .sig-dot{{width:7px;height:7px;border-radius:50%}}
.rp .sig-dot.hi{{background:#ef4444;box-shadow:0 0 4px rgba(239,68,68,0.4)}}
.rp .sig-dot.md{{background:#f59e0b;box-shadow:0 0 4px rgba(245,158,11,0.3)}}
.rp .sig-cnt{{font-size:11px;color:#8896ab;white-space:nowrap}}
.ftr{{padding:8px 24px;font-size:11px;color:#64748b;border-top:1px solid rgba(56,189,248,0.1);flex-shrink:0;display:flex;justify-content:center;align-items:center;gap:6px;position:relative;z-index:1}}
.ftr span{{color:#7a8ba0}}
</style></head><body>

<div class="hdr">
  <div class="hdr-left">
    <h1>ETF三因子监测报告</h1>
    <span class="sub">{esc(model_desc)}</span>
  </div>
  <div class="meta">
    <div><span class="dot"></span>{esc(date_caption)}</div>
    <div>{datetime.now().strftime("%Y-%m-%d %H:%M")} · v7</div>
  </div>
</div>

<div class="controls">
  <form class="ctrl-form compact" method="get" action="/">
    <label for="date">日期查询</label>
    <input id="date" type="date" name="date" value="{esc(view_date)}">
    <button type="submit">查询</button>
    <button type="button" id="flowReplayBtn" class="flow-btn">资金流回放</button>
    <span class="ctrl-hint">{etf_count}只ETF</span>
  </form>
  <form class="ctrl-form" method="post" action="/api/etfs">
    <label for="code">新增ETF</label>
    <input id="code" name="code" inputmode="numeric" pattern="\\d{{6}}" maxlength="6" placeholder="代码" required>
    <input name="name" placeholder="ETF名称" required>
    <select name="idx">{idx_options}</select>
    <input type="hidden" name="return_date" value="{esc(view_date)}">
    <button class="secondary" type="submit">新增并刷新</button>
  </form>
</div>

<div class="banner {'warn' if total_high>=2 else 'mid' if total_high>=1 or total_mid>=2 else 'ok'}">
  <span class="ico">{'🔥' if total_high>=2 else '⚠️' if total_high>=1 or total_mid>=2 else '✅'}</span>
  <div>📋 <b>综合判断：</b>{esc(verdict)}</div>
</div>

<div class="stats">
  <div class="stat">
    <div class="vi" style="color:{'#ef4444' if total_high>0 else '#f59e0b' if total_mid>0 else '#22c55e'}">{total_high}</div>
    <div class="tx"><span>高确信</span>🔴 触发警报</div>
  </div>
  <div class="stat">
    <div class="vi" style="color:{'#f59e0b' if total_mid>0 else '#4a5568'}">{total_mid}</div>
    <div class="tx"><span>中等关注</span>🟡 需跟踪</div>
  </div>
  <div class="stat">
    <div class="vi" style="color:{'#ef4444' if hs300_alerts>=3 else '#f59e0b' if hs300_alerts>=2 else '#22c55e'}">{hs300_alerts}/{hs300_total}</div>
    <div class="tx"><span>沪深300</span>一致性</div>
  </div>
  <div class="stat">
    <div class="vi" style="color:{delta_cls}">{delta_arrow}{abs(total_delta):.1f}</div>
    <div class="tx"><span>份额日变</span>亿份 · 净申赎</div>
  </div>
  <div class="stat">
    <div class="vi" style="color:#818cf8">{shares_available_count}/{etf_count}</div>
    <div class="tx"><span>份额覆盖</span>三因子完整度</div>
  </div>
</div>

<div class="main">
  <div class="tbl-wrap">
    <table>
    <thead><tr>
      <th style="width:20%">ETF名称</th>
      <th style="width:7%">代码</th>
      <th style="width:6%">涨跌</th>
      <th style="width:8%">成交量</th>
      <th style="width:8%">20日均</th>
      <th style="width:6%">倍量</th>
      <th style="width:7%">份额</th>
      <th style="width:8%">份额日变</th>
      <th style="width:6%">量能P</th>
      <th style="width:6%">方向P</th>
      <th style="width:6%">份额P</th>
      <th style="width:6%">综合</th>
    </tr></thead>
    <tbody>{rows}
    </tbody>
    </table>
    <div class="tnote">
      ⚡ {esc(model_desc)} · 含普涨折扣 · 份额数据来源上交所/深交所akshare · 份额P=份额日变化/前日份额
    </div>
  </div>

  <div class="rp">
    <div class="card">
      <div class="ttl">📈 15日信号趋势（综合概率）</div>
      <div class="trend">{bars}</div>
    </div>
    <div class="card">
      <div class="ttl">📅 30日同步信号</div>
      <div class="sig">{sig_list}</div>
    </div>
  </div>
</div>

<div id="flowModal" class="flow-modal" aria-hidden="true">
  <div class="flow-panel" role="dialog" aria-modal="true" aria-labelledby="flowModalTitle">
    <div class="flow-panel-hd">
      <div>
        <div id="flowModalTitle" class="flow-title">资金流曲线回放</div>
        <div id="flowModalMeta" class="flow-meta">真实数据 · ETF分钟线 · 半小时采样 · 方向性成交额 · {esc(view_date)}</div>
      </div>
      <div class="flow-tools">
        <button id="flowPlayBtn" type="button">播放</button>
        <button id="flowReloadBtn" class="alt" type="button">重置</button>
        <button id="flowCloseBtn" class="alt" type="button">关闭</button>
      </div>
    </div>
    <div class="flow-panel-bd">
      <div class="flow-visual">
        <div id="flowMarketBar" class="flow-market"></div>
        <div id="flowStage" class="flow-stage">
          <div class="flow-empty">点击“播放”查看资金流历史走势</div>
        </div>
      </div>
      <div id="flowRank" class="flow-side">
        <h3>当前排行</h3>
      </div>
    </div>
    <div class="flow-progress">
      <span id="flowFrameText">0/0</span>
      <input id="flowFrameRange" type="range" min="0" max="0" value="0">
      <span id="flowCacheText">未载入</span>
    </div>
  </div>
</div>

<div class="ftr">
  <span>ETF国家队资金监测 · 三因子模型 v7 · 腾讯财经API + 上交所/深交所akshare</span>
</div>

<script>
(function() {{
  const flowDate = "{esc(view_date)}";
  const openBtn = document.getElementById("flowReplayBtn");
  const modal = document.getElementById("flowModal");
  const closeBtn = document.getElementById("flowCloseBtn");
  const playBtn = document.getElementById("flowPlayBtn");
  const reloadBtn = document.getElementById("flowReloadBtn");
  const marketBar = document.getElementById("flowMarketBar");
  const stage = document.getElementById("flowStage");
  const rank = document.getElementById("flowRank");
  const meta = document.getElementById("flowModalMeta");
  const frameText = document.getElementById("flowFrameText");
  const frameRange = document.getElementById("flowFrameRange");
  const cacheText = document.getElementById("flowCacheText");
  let flowPayload = null;
  let frame = 0;
  let framePosition = 0;
  let timer = null;
  const frameDuration = 620;

  function fmtYi(value) {{
    const sign = value > 0 ? "+" : "";
    return sign + value.toFixed(1) + "亿";
  }}

  function htmlEscape(value) {{
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }}

  function renderEmpty(text) {{
    stage.innerHTML = '<div class="flow-empty">' + text + '</div>';
    rank.innerHTML = '<h3>当前排行</h3>';
  }}

  function pctText(value) {{
    if (value === null || value === undefined || Number.isNaN(Number(value))) return "--";
    const num = Number(value);
    return (num > 0 ? "+" : "") + num.toFixed(2) + "%";
  }}

  function pctColor(value) {{
    const num = Number(value);
    if (!Number.isFinite(num)) return "#64748b";
    return num >= 0 ? "#ef4444" : "#10b981";
  }}

  function renderMarket(payload, visibleFrame) {{
    const items = (payload && payload.market_indices) || [];
    if (!items.length) {{
      marketBar.innerHTML = '<div class="flow-index"><span class="nm">上证</span><span class="pct" style="color:#64748b">--</span></div><div class="flow-index"><span class="nm">深证</span><span class="pct" style="color:#64748b">--</span></div><div class="flow-index"><span class="nm">创业板</span><span class="pct" style="color:#64748b">--</span></div>';
      return;
    }}
    marketBar.innerHTML = items.map(item => {{
      const point = pointAt(item, visibleFrame);
      const pct = point ? point.pct : item.pct;
      const color = pctColor(pct);
      const sub = point && point.time ? point.time : (item.time || "暂无");
      return '<div class="flow-index"><span class="nm">' + htmlEscape(item.name) + '</span><div><div class="pct" style="color:' + color + '">' + pctText(pct) + '</div><div class="tm">' + htmlEscape(sub) + '</div></div></div>';
    }}).join("");
  }}

  function seriesPoint(item, frameIndex) {{
    const points = item.points || [];
    if (!points.length) return null;
    return points[Math.min(frameIndex, points.length - 1)];
  }}

  function valueOf(point, key, fallback) {{
    const value = Number(point && point[key]);
    return Number.isFinite(value) ? value : fallback;
  }}

  function lerp(a, b, t) {{
    return a + (b - a) * t;
  }}

  function easeInOut(t) {{
    return t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2;
  }}

  function pointAt(item, frameValue) {{
    const points = item.points || [];
    if (!points.length) return null;
    const maxIdx = points.length - 1;
    const clamped = Math.max(0, Math.min(maxIdx, Number(frameValue) || 0));
    const leftIdx = Math.floor(clamped);
    const rightIdx = Math.min(maxIdx, leftIdx + 1);
    const t = clamped - leftIdx;
    const left = points[leftIdx];
    const right = points[rightIdx];
    if (rightIdx === leftIdx || t <= 0) {{
      return {{
        time: left.time,
        value: valueOf(left, "value", 0),
        net: valueOf(left, "net", 0),
        price: valueOf(left, "price", 0),
        pct: valueOf(left, "pct", null),
        amount_yi: valueOf(left, "amount_yi", 0),
      }};
    }}
    return {{
      time: t < 0.5 ? left.time : right.time,
      value: lerp(valueOf(left, "value", 0), valueOf(right, "value", 0), t),
      net: lerp(valueOf(left, "net", 0), valueOf(right, "net", 0), t),
      price: lerp(valueOf(left, "price", 0), valueOf(right, "price", 0), t),
      pct: lerp(valueOf(left, "pct", 0), valueOf(right, "pct", 0), t),
      amount_yi: lerp(valueOf(left, "amount_yi", 0), valueOf(right, "amount_yi", 0), t),
    }};
  }}

  function renderInfo(payload, visibleFrame) {{
    const rows = (payload.series || [])
      .map(item => {{
        const point = pointAt(item, visibleFrame);
        return point ? {{
          name: item.name,
          color: item.color || "#38bdf8",
          value: Number(point.value || 0),
          net: Number(point.net || 0),
        }} : null;
      }})
      .filter(Boolean)
      .sort((a, b) => b.value - a.value);
    const time = currentTime(payload, visibleFrame);
    let html = '<h3>当前排行 · ' + htmlEscape(time || "--:--") + '</h3>';
    rows.forEach(row => {{
      const color = row.color;
      html += '<div class="flow-rank-row">';
      html += '<div class="flow-rank-left"><span class="flow-swatch" style="color:' + color + ';background:' + color + '"></span><span class="nm">' + htmlEscape(row.name) + '</span></div>';
      html += '<span class="val" style="color:' + color + '">' + fmtYi(row.value) + '</span>';
      html += '</div>';
    }});
    rank.innerHTML = html;
  }}

  function maxFrame(payload) {{
    const lengths = (payload.series || []).map(item => (item.points || []).length);
    return Math.max(0, ...lengths) - 1;
  }}

  function currentTime(payload, visibleFrame) {{
    const first = (payload.series || [])[0];
    const points = (first && first.points) || [];
    if (!points.length) return "";
    const frameValue = Math.max(0, Math.min(points.length - 1, Number(visibleFrame) || 0));
    const leftIdx = Math.floor(frameValue);
    const rightIdx = Math.min(points.length - 1, leftIdx + 1);
    const left = points[leftIdx];
    const right = points[rightIdx];
    if (rightIdx > leftIdx && frameValue - leftIdx > 0.02) {{
      return left.time + " → " + right.time;
    }}
    return left.time;
  }}

  function curvePath(points) {{
    if (!points.length) return "";
    if (points.length === 1) return "M" + points[0].x.toFixed(1) + " " + points[0].y.toFixed(1);
    let d = "M" + points[0].x.toFixed(1) + " " + points[0].y.toFixed(1);
    for (let i = 1; i < points.length; i++) {{
      const prev = points[i - 1];
      const cur = points[i];
      const midX = (prev.x + cur.x) / 2;
      const midY = (prev.y + cur.y) / 2;
      d += " Q" + prev.x.toFixed(1) + " " + prev.y.toFixed(1) + " " + midX.toFixed(1) + " " + midY.toFixed(1);
    }}
    const last = points[points.length - 1];
    d += " T" + last.x.toFixed(1) + " " + last.y.toFixed(1);
    return d;
  }}

  function renderFlow(payload, visibleFrame) {{
    const series = payload.series || [];
    const total = maxFrame(payload);
    if (!series.length || total < 0) {{
      renderMarket(payload, 0);
      renderEmpty(payload.message || "暂无该日期的ETF分钟资金流数据");
      frameText.textContent = "0/0";
      frameRange.max = 0;
      frameRange.value = 0;
      cacheText.textContent = payload.source || "无数据";
      return;
    }}
    const frameValue = Math.max(0, Math.min(total, Number(visibleFrame) || 0));
    renderMarket(payload, frameValue);
    const frameFloor = Math.floor(frameValue);
    const frameFrac = frameValue - frameFloor;
    framePosition = frameValue;
    frame = Math.max(0, Math.min(total, Math.floor(frameValue)));

    const width = Math.max(620, stage.clientWidth || 760);
    const height = Math.max(420, stage.clientHeight || 470);
    const pad = {{ left: 54, right: 126, top: 26, bottom: 40 }};
    const plotW = width - pad.left - pad.right;
    const plotH = height - pad.top - pad.bottom;
    const allValues = series.flatMap(item => (item.points || []).map(point => Number(point.value || 0)));
    const maxV = Math.max(1, ...allValues);
    const minV = Math.min(-1, ...allValues);
    const span = Math.max(1, maxV - minV);

    function xCenter(i) {{
      return pad.left + (total <= 0 ? 0 : (i / total) * plotW);
    }}
    function yAt(v) {{
      return pad.top + (maxV - v) / span * plotH;
    }}

    const zeroY = yAt(0);
    let svg = '<svg viewBox="0 0 ' + width + ' ' + height + '" aria-label="资金流曲线回放">';
    svg += '<line class="flow-grid" x1="' + pad.left + '" y1="' + pad.top + '" x2="' + (pad.left + plotW) + '" y2="' + pad.top + '"></line>';
    svg += '<line class="flow-grid" x1="' + pad.left + '" y1="' + (pad.top + plotH) + '" x2="' + (pad.left + plotW) + '" y2="' + (pad.top + plotH) + '"></line>';
    svg += '<line class="flow-axis" x1="' + pad.left + '" y1="' + zeroY.toFixed(1) + '" x2="' + (pad.left + plotW) + '" y2="' + zeroY.toFixed(1) + '"></line>';
    svg += '<text x="' + (pad.left - 46) + '" y="' + (pad.top + 9) + '" fill="#64748b" font-size="10">' + fmtYi(maxV) + '</text>';
    svg += '<text x="' + (pad.left - 46) + '" y="' + (pad.top + plotH - 3) + '" fill="#64748b" font-size="10">' + fmtYi(minV) + '</text>';

    const tickSource = (series[0] && series[0].points) || [];
    tickSource.forEach((point, i) => {{
      const x = xCenter(i);
      svg += '<text x="' + x.toFixed(1) + '" y="' + (height - 14) + '" fill="#64748b" font-size="10" text-anchor="middle">' + htmlEscape(point.time) + '</text>';
    }});

    const renderedSeries = series.map(item => {{
      const rawPoints = item.points || [];
      const points = rawPoints
        .slice(0, frameFloor + 1)
        .map((point, i) => ({{ x: xCenter(i), y: yAt(Number(point.value || 0)), raw: point }}));
      if (frameFrac > 0 && frameFloor + 1 < rawPoints.length) {{
        const smoothPoint = pointAt(item, frameValue);
        points.push({{ x: xCenter(frameValue), y: yAt(Number(smoothPoint.value || 0)), raw: smoothPoint }});
      }}
      if (!points.length) return null;
      const color = item.color || "#38bdf8";
      const last = points[points.length - 1];
      return {{ item, points, color, last, d: curvePath(points) }};
    }}).filter(Boolean);

    renderedSeries.forEach(row => {{
      svg += '<path class="flow-line" pathLength="1" d="' + row.d + '" fill="none" stroke="' + row.color + '" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round" opacity="0.94"></path>';
      svg += '<circle class="flow-dot" cx="' + row.last.x.toFixed(1) + '" cy="' + row.last.y.toFixed(1) + '" r="3.2" fill="' + row.color + '"></circle>';
    }});

    const labels = renderedSeries
      .map(row => ({{ name: row.item.name, color: row.color, x: row.last.x, y: row.last.y, value: Number(row.last.raw.value || 0) }}))
      .sort((a, b) => a.y - b.y);
    labels.forEach((label, idx) => {{
      const minY = idx === 0 ? pad.top + 8 : labels[idx - 1].labelY + 14;
      label.labelY = Math.min(pad.top + plotH - 4, Math.max(label.y + 4, minY));
    }});
    labels.forEach(label => {{
      svg += '<text class="flow-label" x="' + (label.x + 8).toFixed(1) + '" y="' + label.labelY.toFixed(1) + '" fill="' + label.color + '">' + htmlEscape(label.name) + ' ' + fmtYi(label.value) + '</text>';
    }});
    svg += '</svg>';
    stage.innerHTML = svg;
    const displayFrame = Math.max(0, Math.min(total, Math.floor(frameValue + 0.001)));
    const time = currentTime(payload, frameValue);
    frameText.textContent = (displayFrame + 1) + "/" + (total + 1) + " · " + time;
    frameRange.max = total;
    frameRange.value = displayFrame;
    cacheText.textContent = (payload.source_label || payload.source || "真实数据") + " · " + (payload.unit || "亿");
    renderInfo(payload, visibleFrame);
  }}

  function stopPlayback() {{
    if (timer) {{
      window.cancelAnimationFrame(timer);
      timer = null;
    }}
    playBtn.textContent = "播放";
  }}

  function animateSegment(fromFrame, toFrame, startTime) {{
    const duration = Math.max(90, (toFrame - fromFrame) * frameDuration);
    function step(now) {{
      const rawProgress = Math.min(1, (now - startTime) / duration);
      const pos = lerp(fromFrame, toFrame, easeInOut(rawProgress));
      renderFlow(flowPayload, pos);
      if (rawProgress < 1) {{
        timer = window.requestAnimationFrame(step);
        return;
      }}
      framePosition = toFrame;
      frame = Math.floor(toFrame);
      if (toFrame >= maxFrame(flowPayload)) {{
        stopPlayback();
        return;
      }}
      const nextFrame = Math.min(maxFrame(flowPayload), Math.floor(toFrame) + 1);
      timer = window.requestAnimationFrame(nextStart => animateSegment(toFrame, nextFrame, nextStart));
    }}
    timer = window.requestAnimationFrame(step);
  }}

  function startPlayback() {{
    if (!flowPayload) return;
    const total = maxFrame(flowPayload);
    if (total <= 0) {{
      frame = 0;
      framePosition = 0;
      renderFlow(flowPayload, frame);
      return;
    }}
    stopPlayback();
    playBtn.textContent = "暂停";
    if (framePosition >= total) {{
      frame = 0;
      framePosition = 0;
      renderFlow(flowPayload, framePosition);
    }}
    const nextFrame = Math.min(total, Math.floor(framePosition) + 1);
    timer = window.requestAnimationFrame(startTime => animateSegment(framePosition, nextFrame, startTime));
  }}

  async function loadFlow() {{
    try {{
      cacheText.textContent = "载入中";
      const res = await fetch("/api/flow/hourly?date=" + encodeURIComponent(flowDate), {{ cache: "no-store" }});
      if (!res.ok) throw new Error("HTTP " + res.status);
      flowPayload = await res.json();
      frame = maxFrame(flowPayload);
      framePosition = frame;
      const summary = flowPayload.summary || {{}};
      const countText = (summary.available_count || (flowPayload.series || []).length) + "/" + (summary.etf_count || (flowPayload.series || []).length) + "只ETF";
      meta.textContent = "真实数据 · ETF分钟线 · 半小时采样 · 方向性成交额 · " + countText + " · " + flowPayload.date;
      renderFlow(flowPayload, frame);
    }} catch (err) {{
      renderEmpty("资金流历史数据暂不可用");
      cacheText.textContent = "获取失败";
    }}
  }}

  openBtn.addEventListener("click", () => {{
    modal.classList.add("open");
    modal.setAttribute("aria-hidden", "false");
    loadFlow();
  }});
  closeBtn.addEventListener("click", () => {{
    stopPlayback();
    modal.classList.remove("open");
    modal.setAttribute("aria-hidden", "true");
  }});
  modal.addEventListener("click", event => {{
    if (event.target === modal) closeBtn.click();
  }});
  reloadBtn.addEventListener("click", () => {{
    stopPlayback();
    frame = 0;
    framePosition = 0;
    if (flowPayload) renderFlow(flowPayload, frame);
    else loadFlow();
  }});
  playBtn.addEventListener("click", () => {{
    if (timer) stopPlayback();
    else startPlayback();
  }});
  frameRange.addEventListener("input", event => {{
    stopPlayback();
    frame = Number(event.target.value || 0);
    framePosition = frame;
    if (flowPayload) renderFlow(flowPayload, frame);
  }});
}})();
</script>

</body></html>'''


# ============================================================
# 邮件发送
# ============================================================

def send_email(html_path, json_path, target_date):
    password = os.environ.get("QQMAIL_AUTH_CODE") or os.environ.get("SMTP_PASS")
    if not password:
        print("⚠️ 未设置 QQMAIL_AUTH_CODE 或 SMTP_PASS 环境变量，跳过邮件发送")
        return False

    msg = MIMEMultipart()
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg["Subject"] = f"ETF三因子分析报告 - {target_date} - v7"

    body = f"📊 ETF三因子监测报告（v7）\n\n分析日期: {target_date}\n模型: 量能50% + 方向20% + 份额30%\n\n报告详见附件。\n\n---\n此邮件由ETF三因子监测系统v7自动发送"
    msg.attach(MIMEText(body, "plain", "utf-8"))

    for fpath, fname in [(html_path, f"ETF三因子-{target_date}.html"),
                           (json_path, f"ETF三因子-{target_date}.json")]:
        if os.path.exists(fpath):
            with open(fpath, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition", f"attachment; filename={fname}")
                msg.attach(part)

    try:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=15) as server:
            server.login(EMAIL_FROM, password)
            server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
        print(f"✅ 邮件已发送至 {EMAIL_TO}")
        return True
    except Exception as e:
        print(f"❌ 邮件发送失败: {e}")
        return False


# ============================================================
# 主程序
# ============================================================

def record_shares_only():
    """仅采集当日份额数据到本地DB（不跑完整分析）"""
    if not DATA_STORE_AVAILABLE:
        print("❌ etf_data_store 不可用")
        return
    store = ETFDataStore()
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"📊 采集 {today} 的ETF份额数据...")
    for code, info in ETFS.items():
        print(f"  📊 {code} {info['n']}...", end=" ")
        sh_data = fetch_fund_shares(code)
        if sh_data:
            store.upsert_record(today, code, {
                "date": today, "code": code,
                "name": info["n"], "idx_name": info["idx"],
                "shares_yi": sh_data.get("shares_yi"),
            })
            print(f"✅ {sh_data['shares_yi']:.1f}亿份")
        else:
            print("❌ 获取失败")
    stats = store.get_stats()
    print(f"\n📊 数据库状态: {stats['total_records']}条记录, {stats['total_dates']}个交易日")
    print(f"   日期范围: {stats['date_range'][0]} ~ {stats['date_range'][1]}")
    print(f"   含份额: {stats['records_with_shares']}条")


def main(target_date=None, do_send=False, record_only=False):
    refresh_custom_etfs()
    # 初始化 DB
    store = ETFDataStore() if DATA_STORE_AVAILABLE else None

    if record_only:
        record_shares_only()
        return

    print("=" * 70)
    print("🛡️ ETF国家队资金监测 v7 — 三因子模型 + 本地DB")
    print(f"   量能50% + 方向20% + 份额30%")
    if store:
        db_stats = store.get_stats()
        print(f"   📦 本地DB: {db_stats['total_records']}条 / {db_stats['total_dates']}日")
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)

    # 1. 获取沪深300指数数据
    print("\n📊 Step 1: 获取沪深300指数数据...")
    idx_300 = fetch("sh000300", 60)
    if idx_300:
        print(f"  ✅ {len(idx_300)}条  {idx_300[-1]['date']}~{idx_300[0]['date']}")
    trading_dates = sorted([d["date"] for d in idx_300]) if idx_300 else []
    analysis_date = target_date if target_date else (trading_dates[-1] if trading_dates else None)
    if target_date and target_date not in trading_dates:
        fallback_date = get_prev_trading_date(trading_dates, target_date) or (trading_dates[-1] if trading_dates else target_date)
        print(f"  ⚠️ {target_date} 不是当前K线范围内交易日，使用 {fallback_date} 分析")
        analysis_date = fallback_date

    # 2. 加载历史份额数据（优先从本地DB/JSON，其次akshare回溯）
    print("\n📊 Step 2: 加载历史份额数据...")
    shares_history = load_shares_history()
    print(f"  📦 JSON历史: {len(shares_history)}日")
    
    # 清理空日期 (push2失败残留)
    empty_dates = [d for d, v in shares_history.items() if isinstance(v, dict) and len(v) == 0]
    for d in empty_dates:
        del shares_history[d]
    if empty_dates:
        print(f"  🧹 清理{len(empty_dates)}个空日期: {empty_dates}")

    prev_share_date = get_prev_trading_date(trading_dates, analysis_date)
    required_share_dates = [d for d in [prev_share_date, analysis_date] if d]
    if required_share_dates:
        ensure_share_cache(shares_history, required_share_dates)

    # 如果历史数据不足60天, 用akshare回溯补充
    need_dates = 60 - len(shares_history)
    skip_backfill = os.environ.get("ETF_SKIP_BACKFILL", "").lower() in ("1", "true", "yes")
    skip_live_shares = os.environ.get("ETF_SKIP_LIVE_SHARES", "").lower() in ("1", "true", "yes")
    if need_dates > 10 and not skip_backfill:
        print(f"  📡 JSON历史不足({len(shares_history)}日), 需补充{need_dates}日...")
        # 获取过去N个交易日的日期列表
        all_dates = set()
        if idx_300:
            for d in idx_300:
                all_dates.add(d['date'])
        # 也从每只ETF的K线获取日期
        for code in ETFS:
            try:
                kdata = fetch(code, 60)
                for d in kdata:
                    all_dates.add(d['date'])
            except:
                pass
        # 筛选需要补充的日期
        dates_to_fetch = sorted([d for d in all_dates if d not in shares_history])
        # 限制回溯数量，避免超长等待(每次最多回溯20天)
        MAX_BACKFILL = 20
        if len(dates_to_fetch) > MAX_BACKFILL:
            print(f"  📡 仅回溯最近{MAX_BACKFILL}日(共需{len(dates_to_fetch)}日, 后续增量采集)")
            dates_to_fetch = dates_to_fetch[-MAX_BACKFILL:]
        if dates_to_fetch:
            print(f"  📡 从akshare回溯{len(dates_to_fetch)}日份额数据...")
            bulk_history = fetch_history_shares_bulk(dates_to_fetch)
            if bulk_history:
                new_count = 0
                for d, entries in bulk_history.items():
                    if d not in shares_history:
                        shares_history[d] = {}
                    shares_history[d].update(entries)
                    new_count += 1
                save_shares_history(shares_history)
                print(f"  ✅ 补充了{new_count}日份额数据")
    elif need_dates > 10 and skip_backfill:
        print(f"  ⏩ Web模式跳过批量历史份额回补({need_dates}日)，保持页面查询响应")
    
    # 获取实时份额并写入本地DB
    if store and not target_date and not skip_live_shares:
        # 实时运行：采集份额数据
        today_str = analysis_date or datetime.now().strftime("%Y-%m-%d")
        print(f"  📡 确认 {today_str} 份额数据...")
        for code, info in ETFS.items():
            if code in shares_history.get(today_str, {}):
                print(f"    ✅ {code} {info['n'][:12]}: 缓存命中")
                continue
            sh_data = fetch_fund_shares(code, today_str)
            if sh_data:
                data_date = sh_data.get("data_date") or today_str
                store.upsert_record(today_str, code, {
                    "date": today_str, "code": code,
                    "name": info["n"], "idx_name": info["idx"],
                    "shares_yi": sh_data.get("shares_yi"),
                })
                # 也记录到 JSON（保持兼容）
                if data_date not in shares_history:
                    shares_history[data_date] = {}
                shares_history[data_date][code] = {"shares_yi": sh_data["shares_yi"], "ts": datetime.now().isoformat()}
                print(f"    ✅ {code} {info['n'][:12]}: {sh_data['shares_yi']:.1f}亿份 ({data_date})")
            else:
                print(f"    ⚠️ {code} {info['n'][:12]}: 今日份额数据暂未发布")
        save_shares_history(shares_history)
    elif store and not target_date and skip_live_shares:
        print("  ⏩ Web模式跳过实时份额采集，使用本地已有份额数据")
    elif store and target_date and not skip_live_shares:
        # 指定日期：尝试从akshare获取该日的份额数据
        fetch_dates = [d for d in required_share_dates if not date_has_complete_shares(shares_history, d)]
        bulk = fetch_history_shares_bulk(fetch_dates)
        if bulk:
            merge_shares_history(shares_history, bulk)
            save_shares_history(shares_history)
            print(f"  ✅ 已从akshare获取{', '.join(sorted(bulk))}的份额数据")
        else:
            db_shares = store.get_range(start_date=analysis_date, end_date=analysis_date)
            if db_shares:
                print(f"  📦 本地DB有{analysis_date}的份额记录 ({len(db_shares)}条)")
            else:
                print(f"  ⚠️ 无{analysis_date}份额数据，退化为二因子")
    elif store and target_date and skip_live_shares:
        db_shares = store.get_range(start_date=analysis_date, end_date=analysis_date)
        if db_shares:
            print(f"  📦 Web模式使用本地DB中{analysis_date}的份额记录 ({len(db_shares)}条)")
        else:
            print(f"  ⏩ Web模式跳过{analysis_date}份额接口查询，退化为二因子")
    print(f"  📊 累计历史: {len(shares_history)}日")

    # 3. 构建份额映射
    shares_map = {}
    for code in ETFS:
        shares_map[code] = {}
        for date, entries in shares_history.items():
            if isinstance(entries, dict) and code in entries:
                target_sh, prev_sh, delta_yi, delta_pct = get_historical_share(code, date, shares_history)
                shares_map[code][date] = {
                    "shares_yi": entries[code].get("shares_yi"),
                    "delta_yi": delta_yi,
                    "delta_pct": delta_pct,
                }

    # 4. 获取ETF行情 + 三因子分析
    print("\n📊 Step 3: 获取ETF行情 + 三因子分析...")
    if analysis_date:
        print(f"  🎯 目标分析日期: {analysis_date}")

    all_hist = {}
    latest_map = {}
    target_shares_data = {}

    for code, info in ETFS.items():
        print(f"\n  📊 {code} {info['n']} ({info['idx']})")
        data = fetch(code, 60)
        if not data:
            print("    ❌ 数据获取失败")
            continue
        if len(data) < 22:
            print(f"    ⚠️ 仅{len(data)}条，不足22条")
            continue

        hist = analyze_all(data, idx_300, shares_map, analysis_date or "", 35, code=code)
        if not hist:
            print("    ⚠️ 分析失败")
            continue

        all_hist[code] = hist

        # 找到目标日期的分析结果
        target_hist = None
        if analysis_date:
            for h in hist:
                if h["d"] == analysis_date:
                    target_hist = h
                    break
        if not target_hist:
            target_hist = hist[-1]

        l = target_hist

        # 获取份额数据（目标日期）
        sh_on_target = shares_map.get(code, {}).get(analysis_date or l["d"], {})
        target_shares_data[code] = sh_on_target

        latest_map[code] = {
            "d": l["d"], "c": l["c"], "chg": l["chg"], "cp": l["cp"],
            "vr": l["vr"], "vp": l["vp"], "dp": l["dp"], "sp": l["sp"],
            "v": l["v"], "vma": l["vma"],
            "shares_yi": sh_on_target.get("shares_yi"),
            "delta_yi": sh_on_target.get("delta_yi"),
            "delta_pct": sh_on_target.get("delta_pct"),
        }

        sp_str = f"份额P:{l['sp']:.0f}%" if l.get("has_shares") else "份额P:N/A"
        s = "🔥" if l["cp"] >= 70 else ("⚠️" if l["cp"] >= 50 else "○")
        model_flag = "三因子" if l.get("has_shares") else "二因子"
        t = f"[{l['tag']}]" if l.get("tag") else ""
        print(f"    {s} {l['d']} {t} | {l['chg']:+.2f}% | {l['v']:.0f}万({l['vr']:.2f}x) | 量能P:{l['vp']:.0f}% 方向P:{l['dp']:.0f}% {sp_str} → CP:{l['cp']:.0f}% [{model_flag}]")

    # 5. 重要信号回溯 (生成 actual_date)
    print("\n" + "=" * 70)
    print("📋 30日重要信号回溯（三因子模型）")
    print("=" * 70)
    date_sig = {}
    for code, hist in all_hist.items():
        for h in hist:
            d = h["d"]
            if d not in date_sig:
                date_sig[d] = {"total": 0, "high": 0, "mid": 0, "codes": []}
            date_sig[d]["total"] += 1
            if h["cp"] >= 70: date_sig[d]["high"] += 1; date_sig[d]["codes"].append(f"{code}({h['cp']:.0f}%)")
            elif h["cp"] >= 50: date_sig[d]["mid"] += 1
    sigs = [(d, v) for d, v in date_sig.items() if v["high"] >= 2 or v["high"] + v["mid"] >= 4]
    sigs.sort(key=lambda x: x[0], reverse=True)
    if sigs:
        for d, v in sigs[:10]:
            t = SPECIAL.get(d, "")
            ts = f" [{t}]" if t else ""
            print(f"  📅 {d}{ts}: {v['high']}🔴+{v['mid']}🟡 → {', '.join(v['codes'][:5])}")
    else:
        print("  ℹ️ 无多ETF同步信号")

    actual_date = analysis_date if analysis_date else list(date_sig.keys())[-1]

    # 计算当日沪深300涨跌（用于DB记录）
    idx_gain = 0
    if idx_300 and len(idx_300) >= 2:
        idx_today = None
        for d in idx_300:
            if d["date"] == actual_date:
                idx_today = d
                break
        if idx_today:
            prev_idx = None
            for d in idx_300:
                if d["date"] < actual_date:
                    prev_idx = d
                    break
            if prev_idx and prev_idx.get("c"):
                idx_gain = round((idx_today["c"] - prev_idx["c"]) / prev_idx["c"] * 100, 2)

    # 6. 记录分析结果到本地DB
    if store:
        print(f"\n💾 Step 6: 保存分析结果到本地DB...")
        etf_results_for_db = {}
        for code, hist in all_hist.items():
            for h in hist:
                if h["d"] == actual_date:
                    sh = target_shares_data.get(code, {})
                    etf_results_for_db[code] = {
                        "name": ETFS[code]["n"], "idx_name": ETFS[code]["idx"],
                        "c": h["c"], "chg": h["chg"],
                        "v": h["v"], "vma": h["vma"], "vr": h["vr"],
                        "vp": h["vp"], "dp": h["dp"], "sp": h["sp"], "cp": h["cp"],
                        "shares_yi": sh.get("shares_yi"),
                        "delta_yi": sh.get("delta_yi"),
                        "delta_pct": sh.get("delta_pct"),
                    }
                    break
        cnt = store.record_from_v6_result(actual_date, etf_results_for_db, idx_gain)
        print(f"  ✅ 已记录 {cnt}条数据到本地DB")
        db_stats = store.get_stats()
        print(f"  📦 DB总量: {db_stats['total_records']}条 / {db_stats['total_dates']}日 / 含份额{db_stats['records_with_shares']}条")
    else:
        print(f"\n💾 本地DB不可用，跳过数据持久化")

    # 7. 生成HTML (原step 6)
    print(f"\n🎨 Step 7: 生成三因子HTML报告 (分析日: {actual_date})...")
    html = gen_html(all_hist, latest_map, idx_300, target_shares_data, target_date or actual_date or "")
    with open(THREE_FACTOR_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  ✅ {THREE_FACTOR_HTML} ({len(html)} bytes)")

    # 8. 保存JSON (原step 7)
    with open(THREE_FACTOR_OUT, "w", encoding="utf-8") as f:
        json.dump({
            "run_time": datetime.now().isoformat(),
            "model": "三因子: 量能50%+方向20%+份额30%",
            "target_date": actual_date,
            "signal_dates": [(d, v["high"], v["mid"], v["codes"][:4]) for d, v in sigs[:10]],
            "latest": latest_map,
            "shares_data": target_shares_data,
        }, f, ensure_ascii=False, indent=2)
    print(f"  ✅ {THREE_FACTOR_OUT}")

    # 9. 发送邮件 (原step 8)
    if do_send:
        print(f"\n📧 Step 8: 发送邮件到 {EMAIL_TO}...")
        send_email(THREE_FACTOR_HTML, THREE_FACTOR_OUT, actual_date)
    else:
        print(f"\n📧 跳过邮件发送 (使用 --send 启用)")

    return html


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETF三因子监测 v6.1")
    parser.add_argument("--date", type=str, default=None,
                        help="分析日期 (YYYY-MM-DD)，默认最近交易日")
    parser.add_argument("--send", action="store_true",
                        help="发送邮件")
    parser.add_argument("--record", action="store_true",
                        help="仅采集当日份额数据入库，不做完整分析")
    parser.add_argument("--stats", action="store_true",
                        help="查看本地DB状态，不做分析")
    args = parser.parse_args()

    if args.stats:
        if not DATA_STORE_AVAILABLE:
            print("❌ etf_data_store.py 不可用")
            sys.exit(1)
        store = ETFDataStore()
        stats = store.get_stats()
        print("=" * 60)
        print("📊 ETF本地数据库状态")
        print("=" * 60)
        print(f"  数据库路径: {store.db_path}")
        print(f"  总记录数:   {stats['total_records']}")
        print(f"  覆盖交易日: {stats['total_dates']}")
        print(f"  日期范围:   {stats['date_range'][0]} ~ {stats['date_range'][1]}")
        print(f"  含份额记录: {stats['records_with_shares']}/{stats['total_records']}")
        print(f"\n  最近5个交易日:")
        for d, cnt in stats["recent_dates"]:
            print(f"    {d}: {cnt}只ETF")
        if stats["total_records"] == 0:
            print("\n  💡 提示: 运行 --record 采集今日数据入库")
        sys.exit(0)

    main(args.date, args.send, args.record)
