import argparse
import ctypes
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

import redis
import ujson as json

from utils.terminal import dan_tran_cua_so
from utils.spread_pivot_provider import (
    dong_bo_spread_pivot_tu_api,
    lay_chu_ky_cap_nhat_spread_pivot,
)
from utils.trading_logic import check_tin_hieu_arbitrage, lay_spread_pivot


CONFIG_FILE = "config.json"
VALID_TRADE_MODES = {"copy_diff"}


def safe_upper(value):
    return str(value or "").strip().upper()


def as_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def load_config():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def find_pair(config, pair_id):
    return next((cap for cap in config.get("danh_sach_cap", []) if cap.get("id") == pair_id), None)


def normalize_mode(cap):
    mode = str(cap.get("trade_mode", "hedge")).strip().lower()
    if mode not in VALID_TRADE_MODES:
        raise ValueError(f"master_copy_diff chi phuc vu trade_mode='copy_diff', nhan: {mode}")
    return mode


def resolve_execution(cap):
    """
    Phan giai block execution cho copy_diff.
    Khac voi single: san execution KHONG CAN trung base/diff, va role luon la COPY_DIFF.
    """
    mode = normalize_mode(cap)

    execution = cap.get("execution") or {}
    exec_exchange = safe_upper(execution.get("exchange"))
    exec_symbol = str(execution.get("symbol")).strip()
    if not exec_exchange or not exec_symbol:
        raise ValueError("trade_mode='copy_diff' bat buoc co execution.exchange va execution.symbol")

    # copy_diff: role luon la COPY_DIFF, logic vao lenh theo chieu cua Diff
    default_volume = cap.get("volume_diff", 0.01)

    return {
        "role": "COPY_DIFF",
        "exchange": exec_exchange,
        "symbol": exec_symbol,
        "volume": as_float(execution.get("volume", default_volume), 0.01),
        "order_key": f"QUEUE:ORDER:{exec_exchange}",
        "position_key": f"POSITION:{exec_exchange}:{exec_symbol}",
        "equity_key": f"ACCOUNT:{exec_exchange}:EQUITY",
    }


def infer_loai_lenh_from_side(side):
    """
    Copy_diff luon theo chieu Diff:
    - TH1: BUY Diff -> BUY san 3
    - TH2: SELL Diff -> SELL san 3
    """
    side = safe_upper(side)
    if side == "BUY":
        return "TH1"
    if side == "SELL":
        return "TH2"
    return "UNKNOWN"


def infer_open_action(tin_hieu):
    """Copy_diff luon lay lenh tu chieu Diff."""
    return tin_hieu.get("lenh_diff")


def opposite_action(action):
    action = safe_upper(action)
    if action == "BUY":
        return "SELL"
    if action == "SELL":
        return "BUY"
    return "UNKNOWN"


def kiem_tra_gio_giao_dich(trading_hours, current_time_str):
    if not trading_hours:
        return True
    for khung_gio in trading_hours:
        start, end = khung_gio.split("-")
        if start <= end:
            if start <= current_time_str <= end:
                return True
        else:
            if current_time_str >= start or current_time_str <= end:
                return True
    return False


def kiem_tra_gio_cam(blackout_hours, current_time_str):
    if not blackout_hours:
        return False
    for khung_gio in blackout_hours:
        start, end = khung_gio.split("-")
        if start <= end:
            if start <= current_time_str <= end:
                return True
        else:
            if current_time_str >= start or current_time_str <= end:
                return True
    return False


def make_context(cap, execution, order_data, close_data):
    return {
        "trade_mode": "copy_diff",
        "is_single_cut": True,
        "pair_token": order_data["id_lenh"],
        "pair_id": cap["id"],
        "execution_exchange": execution["exchange"],
        "execution_symbol": execution["symbol"],
        "execution_role": execution["role"],
        "execution_ticket": order_data.get("ticket"),
        "execution_side": order_data.get("action", "UNKNOWN"),
        "chenh_vao": order_data.get("chenh_lech_vao", 0),
        "chenh_vao_raw": order_data.get("chenh_lech_vao_raw", order_data.get("chenh_lech_vao", 0)),
        "mode_vao": order_data.get("tinh_chat_vao", "UNKNOWN"),
        "chenh_dong": close_data.get("chenh_dong", 0),
        "chenh_dong_raw": close_data.get("chenh_dong_raw", close_data.get("chenh_dong", 0)),
        "entry_spread_pivot": order_data.get("entry_spread_pivot", 0.0),
        "close_spread_pivot": close_data.get("close_spread_pivot", lay_spread_pivot(cap)),
        "mode_dong": close_data.get("mode_dong", "UNKNOWN"),
        "action_type": close_data.get("action_type", "COPY_DIFF_CLOSE"),
        "conf_dev_entry": order_data.get("conf_dev_entry", cap.get("deviation_entry", 0)),
        "conf_dev_close": close_data.get("conf_dev_close", cap.get("deviation_close", 0)),
        "conf_stable_time": cap.get("stable_time", 0),
        "entry_stable_time": order_data.get("entry_stable_time", cap.get("stable_time", 0)),
        "tick_hz_base_in": order_data.get("tick_hz_base_in", 0),
        "tick_hz_diff_in": order_data.get("tick_hz_diff_in", 0),
        "tick_hz_base_out": close_data.get("tick_hz_base_out", 0),
        "tick_hz_diff_out": close_data.get("tick_hz_diff_out", 0),
    }


def lay_huong_tu_lich_su(lich_su_lenh):
    for order_data in lich_su_lenh:
        loai_lenh = order_data.get("loai_lenh")
        if loai_lenh in ("TH1", "TH2"):
            return loai_lenh
    return None


def should_skip_by_filter(filter_mode, action, start_price, current_price):
    if filter_mode == "none" or start_price <= 0:
        return False, ""

    diff_price = current_price - start_price
    if diff_price > 0:
        if filter_mode == "thuan" and action == "BUY":
            return True, "[THUAN] Gia tang, tranh BUY duoi gia."
        if filter_mode == "nguoc" and action == "SELL":
            return True, "[NGUOC] Gia tang, tranh SELL."
    elif diff_price < 0:
        if filter_mode == "thuan" and action == "SELL":
            return True, "[THUAN] Gia giam, tranh SELL duoi gia."
        if filter_mode == "nguoc" and action == "BUY":
            return True, "[NGUOC] Gia giam, tranh BUY."

    return False, ""


parser = argparse.ArgumentParser()
parser.add_argument("--pair_id", required=True)
args = parser.parse_args()

try:
    kernel32 = ctypes.windll.kernel32
    kernel32.SetConsoleMode(kernel32.GetStdHandle(-10), 128)
except Exception:
    pass

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(log_dir, f"log_master_copy_diff_{args.pair_id}.txt")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[RotatingFileHandler(log_filename, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")],
)

config = load_config()
cap_hien_tai = find_pair(config, args.pair_id)
if not cap_hien_tai:
    print(f"LOI: Khong tim thay pair_id {args.pair_id} trong {CONFIG_FILE}")
    raise SystemExit(1)

try:
    execution = resolve_execution(cap_hien_tai)
except Exception as exc:
    print(f"LOI config copy_diff: {exc}")
    logging.error("LOI config copy_diff: %s", exc)
    raise SystemExit(1)

vps_name = config.get("vps_name", "LOCAL")
master_name = f"[{vps_name} | COPY_DIFF | {args.pair_id}]"
try:
    ctypes.windll.kernel32.SetConsoleTitleW(f"MASTER COPY_DIFF {master_name}")
except Exception:
    pass
dan_tran_cua_so(4)

redis_conf = config["redis"]
r = redis.Redis(
    host=redis_conf["host"],
    port=redis_conf["port"],
    db=redis_conf["db"],
    decode_responses=True,
    socket_timeout=2.0,
    socket_connect_timeout=2.0,
)

key_base = f"TICK:{safe_upper(cap_hien_tai['base_exchange'])}:{cap_hien_tai['base_symbol']}"
key_diff = f"TICK:{safe_upper(cap_hien_tai['diff_exchange'])}:{cap_hien_tai['diff_symbol']}"
# 🆕 Subscribe tick cua san thu 3 (Execution) de kiem tra mang va gia
key_exec = f"TICK:{execution['exchange']}:{execution['symbol']}"
key_state = f"STATE:COPY_DIFF_MASTER:{args.pair_id}"
QUEUE_ORDER_RESULT = f"QUEUE:ORDER_RESULT:{args.pair_id}"
QUEUE_TELEGRAM = "TELEGRAM_QUEUE"
SHUTDOWN_KEY = "SIGNAL:SHUTDOWN"

dev_entry = cap_hien_tai["deviation_entry"]
dev_close = cap_hien_tai["deviation_close"]
spread_pivot = lay_spread_pivot(cap_hien_tai)
spread_pivot_source = "config"
spread_pivot_detail = ""
stable_time_sec = cap_hien_tai.get("stable_time", 0) / 1000.0
cooldown_sec = cap_hien_tai["cooldown_second"]
cooldown_close_sec = cap_hien_tai.get("cooldown_close_second", 2)
max_orders = cap_hien_tai["max_orders"]
hold_time_sec = cap_hien_tai.get("hold_time", 180)
alert_equity = cap_hien_tai.get("alert_equity", 0)
stable_mode = cap_hien_tai.get("stable_mode", "freeze")
max_tick_delay = cap_hien_tai.get("max_tick_delay", 5.0)
filter_entry = cap_hien_tai.get("filter_entry", "nguoc")
filter_close = cap_hien_tai.get("filter_close", "none")
max_tick_hz_base = cap_hien_tai.get("max_tick_hz_base", 0)
max_tick_hz_diff = cap_hien_tai.get("max_tick_hz_diff", 0)
spread_pivot_refresh_second = lay_chu_ky_cap_nhat_spread_pivot(cap_hien_tai)
last_spread_pivot_refresh = 0.0

saved_state_raw = r.get(key_state)
if saved_state_raw:
    try:
        saved_state = json.loads(saved_state_raw)
    except Exception:
        saved_state = {}
    huong_dang_danh = saved_state.get("huong_dang_danh")
    lich_su_lenh = saved_state.get("lich_su_lenh", [])
    thoi_diem_vao_lenh_cuoi = saved_state.get("thoi_diem_vao_lenh_cuoi", 0)
    thoi_diem_vua_ra_lenh_dong = saved_state.get("thoi_diem_vua_ra_lenh_dong", 0)
    print(f"[COPY_DIFF] Khoi phuc so lenh: {len(lich_su_lenh)} lenh.")
else:
    huong_dang_danh = None
    lich_su_lenh = []
    thoi_diem_vao_lenh_cuoi = 0
    thoi_diem_vua_ra_lenh_dong = 0
    print("[COPY_DIFF] Bat dau voi so lenh trong.")

huong_tu_so = lay_huong_tu_lich_su(lich_su_lenh)
if huong_tu_so:
    huong_dang_danh = huong_tu_so
elif not lich_su_lenh:
    huong_dang_danh = None


def luu_tri_nho():
    state = {
        "huong_dang_danh": huong_dang_danh,
        "lich_su_lenh": lich_su_lenh,
        "thoi_diem_vao_lenh_cuoi": thoi_diem_vao_lenh_cuoi,
        "thoi_diem_vua_ra_lenh_dong": thoi_diem_vua_ra_lenh_dong,
        "execution": execution,
    }
    r.set(key_state, json.dumps(state))


def reload_runtime_config(new_cap):
    global cap_hien_tai, execution
    global dev_entry, dev_close, spread_pivot, stable_time_sec, cooldown_sec
    global cooldown_close_sec, max_orders, hold_time_sec, alert_equity, stable_mode
    global max_tick_delay, filter_entry, filter_close, max_tick_hz_base, max_tick_hz_diff
    global spread_pivot_refresh_second

    new_execution = resolve_execution(new_cap)
    old_exec_key = (execution["exchange"], execution["symbol"])
    new_exec_key = (new_execution["exchange"], new_execution["symbol"])
    if old_exec_key != new_exec_key and len(lich_su_lenh) > 0:
        print("[HOT RELOAD] Dang co lenh copy_diff, khong doi execution broker/symbol.")
        new_execution = execution

    cap_hien_tai = new_cap
    execution = new_execution
    dev_entry = cap_hien_tai["deviation_entry"]
    dev_close = cap_hien_tai["deviation_close"]
    spread_pivot = lay_spread_pivot(cap_hien_tai)
    stable_time_sec = cap_hien_tai.get("stable_time", 0) / 1000.0
    cooldown_sec = cap_hien_tai["cooldown_second"]
    cooldown_close_sec = cap_hien_tai.get("cooldown_close_second", 2)
    max_orders = cap_hien_tai["max_orders"]
    hold_time_sec = cap_hien_tai.get("hold_time", 180)
    alert_equity = cap_hien_tai.get("alert_equity", 0)
    stable_mode = cap_hien_tai.get("stable_mode", "freeze")
    max_tick_delay = cap_hien_tai.get("max_tick_delay", 5.0)
    filter_entry = cap_hien_tai.get("filter_entry", "nguoc")
    filter_close = cap_hien_tai.get("filter_close", "none")
    max_tick_hz_base = cap_hien_tai.get("max_tick_hz_base", 0)
    max_tick_hz_diff = cap_hien_tai.get("max_tick_hz_diff", 0)
    spread_pivot_refresh_second = lay_chu_ky_cap_nhat_spread_pivot(cap_hien_tai)


def refresh_runtime_spread_pivot(reason, force_log=False):
    global spread_pivot, spread_pivot_source, spread_pivot_detail
    global spread_pivot_refresh_second, last_spread_pivot_refresh

    new_spread_pivot, new_source, new_detail = dong_bo_spread_pivot_tu_api(
        cap_hien_tai
    )
    spread_pivot_refresh_second = lay_chu_ky_cap_nhat_spread_pivot(cap_hien_tai)
    last_spread_pivot_refresh = time.time()

    changed = (
        abs(new_spread_pivot - spread_pivot) > 1e-9
        or new_source != spread_pivot_source
        or new_detail != spread_pivot_detail
    )
    spread_pivot = new_spread_pivot
    spread_pivot_source = new_source
    spread_pivot_detail = new_detail
    cap_hien_tai["spread_pivot"] = spread_pivot

    if force_log or changed:
        message = (
            f"[PIVOT AUTO] {reason}: pivot {spread_pivot:+.2f} "
            f"| source={spread_pivot_source}"
        )
        if spread_pivot_detail:
            message += f" | {spread_pivot_detail}"
        print(message)
        logging.info(message)


def gui_lenh_dong(order_data, close_data, comment):
    context_data = make_context(cap_hien_tai, execution, order_data, close_data)
    r.lpush(
        execution["order_key"],
        json.dumps({
            "action": "CLOSE_BY_TICKET",
            "ticket": order_data["ticket"],
            "comment": comment,
            "role": execution["role"],
            "context": context_data,
        }),
    )


last_config_modified = 0
last_config_check_time = 0
last_time_update = 0
current_utc_time_str = "00:00"

last_tick_base_raw = ""
last_tick_diff_raw = ""
last_tick_exec_raw = ""
last_pos_raw = ""
last_base_msc = 0
last_diff_msc = 0
last_exec_msc = 0
local_nhan_base = time.time()
local_nhan_diff = time.time()
local_nhan_exec = time.time()
thoi_diem_nhan_tick_cuoi = 0
da_xu_ly_lenh_cho_tick_nay = False
tick_base = {"connected": False, "time_msc": 0}
tick_diff = {"connected": False, "time_msc": 0}
tick_exec = {"connected": False, "time_msc": 0}
list_pos_execution = []
pending_jobs = {}

thoi_diem_bat_dau_lech_vao = 0
thoi_diem_bat_dau_lech_dong = 0
gia_base_luc_bat_dau_lech = 0.0
gia_base_luc_bat_dau_lech_dong = 0.0
thoi_diem_spam_cuoi = 0

startup_time = time.time()
STARTUP_GRACE_SECOND = cap_hien_tai.get("startup_grace_second", 15)

print(
    f"[COPY_DIFF] Master san sang: execution={execution['exchange']}:{execution['symbol']} "
    f"role={execution['role']} volume={execution['volume']}"
)
logging.info("=== START MASTER COPY_DIFF %s ===", args.pair_id)
refresh_runtime_spread_pivot("startup", force_log=True)

try:
    while True:
        try:
            time.sleep(0.001)
            now_sec = time.time()

            if now_sec - last_time_update >= 1.0:
                current_utc_time_str = datetime.now(timezone.utc).strftime("%H:%M")
                last_time_update = now_sec

            if now_sec - last_config_check_time >= 1.0:
                last_config_check_time = now_sec
                if r.get(SHUTDOWN_KEY):
                    print("[SHUTDOWN] Master copy_diff nhan tin hieu tat.")
                    logging.info("[SHUTDOWN] Redis shutdown signal.")
                    break

                current_modified = os.path.getmtime(CONFIG_FILE)
                if current_modified != last_config_modified:
                    time.sleep(0.05)
                    try:
                        config = load_config()
                        new_cap = find_pair(config, args.pair_id)
                        if new_cap:
                            new_mode = str(new_cap.get("trade_mode", "")).strip().lower()
                            if new_mode != "copy_diff":
                                msg_mode = (
                                    f"[MODE] {args.pair_id} doi sang trade_mode={new_mode}. "
                                    "Hay tat/bat launcher de chuyen master."
                                )
                                print(msg_mode)
                                logging.error(msg_mode)
                                break
                            reload_runtime_config(new_cap)
                            refresh_runtime_spread_pivot("hot-reload")
                            last_config_modified = current_modified
                            print(
                                f"[HOT RELOAD COPY_DIFF] {dev_entry}|{dev_close}, pivot {spread_pivot:+.2f}, "
                                f"{stable_mode}, {stable_time_sec * 1000:.0f}ms, "
                                f"execution {execution['exchange']}:{execution['symbol']}"
                            )
                            logging.info(
                                "[HOT RELOAD COPY_DIFF] entry=%s close=%s pivot=%s execution=%s:%s",
                                dev_entry,
                                dev_close,
                                spread_pivot,
                                execution["exchange"],
                                execution["symbol"],
                            )
                    except Exception as exc:
                        print(f"[HOT RELOAD COPY_DIFF] Loi config, giu cau hinh cu: {exc}")
                        logging.error("[HOT RELOAD COPY_DIFF] Loi config: %s", exc)

                if (
                    spread_pivot_refresh_second > 0
                    and now_sec - last_spread_pivot_refresh >= spread_pivot_refresh_second
                ):
                    refresh_runtime_spread_pivot("interval")

            trong_gio_cam = kiem_tra_gio_cam(cap_hien_tai.get("force_close_hours", []), current_utc_time_str)

            # 🆕 Doc ca 3 nguon tick: base, diff, VA san thu 3 (exec)
            pos_raw, tick_base_raw, tick_diff_raw, tick_exec_raw, eq_raw = r.mget(
                execution["position_key"], key_base, key_diff, key_exec, execution["equity_key"]
            )
            if pos_raw is None or tick_base_raw is None or tick_diff_raw is None:
                continue
            # San thu 3 chua co tick thi cho tiep
            if tick_exec_raw is None:
                continue

            if pos_raw != last_pos_raw:
                try:
                    list_pos_execution = json.loads(pos_raw) if pos_raw else []
                    if isinstance(list_pos_execution, int):
                        list_pos_execution = []
                except Exception:
                    list_pos_execution = []
                last_pos_raw = pos_raw

            equity_execution = as_float(eq_raw, 999999.0)
            trong_thoi_gian_bao_ve = (now_sec - thoi_diem_vao_lenh_cuoi < 5.0) or (
                now_sec - thoi_diem_vua_ra_lenh_dong < 5.0
            )
            trong_thoi_gian_khoi_dong = (now_sec - startup_time) < STARTUP_GRACE_SECOND

            while True:
                msg_raw = r.rpop(QUEUE_ORDER_RESULT)
                if not msg_raw:
                    break

                try:
                    result_data = json.loads(msg_raw)
                    job_id = result_data.get("job_id")
                    role = result_data.get("role")
                    ticket = result_data.get("ticket")
                    if not job_id or not ticket or role != execution["role"]:
                        continue

                    draft = pending_jobs.pop(job_id, {})
                    action = draft.get("action", result_data.get("execution_action", "UNKNOWN"))
                    loai_lenh = draft.get("loai_lenh") or infer_loai_lenh_from_side(action)
                    order_data = {
                        "id_lenh": f"COPYDIFF_{job_id}",
                        "ticket": ticket,
                        "role": execution["role"],
                        "action": action,
                        "loai_lenh": loai_lenh,
                        "time_entry": time.time(),
                        "chenh_lech_vao": result_data.get("chenh_vao", draft.get("chenh_vao", 0)),
                        "chenh_lech_vao_raw": result_data.get(
                            "chenh_vao_raw", draft.get("chenh_vao_raw", draft.get("chenh_vao", 0))
                        ),
                        "tinh_chat_vao": result_data.get("tinh_chat_vao", draft.get("tinh_chat_vao", "UNKNOWN")),
                        "entry_spread_pivot": result_data.get(
                            "entry_spread_pivot", draft.get("entry_spread_pivot", spread_pivot)
                        ),
                        "conf_dev_entry": result_data.get("conf_dev_entry", draft.get("conf_dev_entry", dev_entry)),
                        "entry_stable_time": result_data.get(
                            "entry_stable_time", draft.get("entry_stable_time", cap_hien_tai.get("stable_time", 0))
                        ),
                        "tick_hz_base_in": result_data.get("tick_hz_base_in", draft.get("tick_hz_base_in", 0)),
                        "tick_hz_diff_in": result_data.get("tick_hz_diff_in", draft.get("tick_hz_diff_in", 0)),
                    }
                    lich_su_lenh.append(order_data)
                    huong_dang_danh = loai_lenh
                    luu_tri_nho()
                    print(f"[COPY_DIFF] Da ghi so ticket #{ticket} {action} ({loai_lenh}) tu job {job_id}.")
                except Exception as exc:
                    print(f"[COPY_DIFF] Loi doc ORDER_RESULT: {exc}")

            expired_jobs = [jid for jid, data in pending_jobs.items() if now_sec - data.get("time", now_sec) > 60]
            for jid in expired_jobs:
                print(f"[COPY_DIFF] Xoa job mo lenh qua 60s khong co ticket: {jid}")
                del pending_jobs[jid]
            if not lich_su_lenh and not pending_jobs:
                huong_dang_danh = None

            position_by_ticket = {p.get("ticket"): p for p in list_pos_execution if isinstance(p, dict)}
            tickets_on_exchange = set(position_by_ticket.keys())
            tracked_tickets = {o.get("ticket") for o in lich_su_lenh}

            if not trong_thoi_gian_bao_ve:
                lenh_con_song = []
                for order_data in lich_su_lenh:
                    if order_data.get("ticket") in tickets_on_exchange:
                        lenh_con_song.append(order_data)
                        continue

                    if trong_thoi_gian_khoi_dong:
                        lenh_con_song.append(order_data)
                        continue

                    close_data = {
                        "chenh_dong": 0,
                        "chenh_dong_raw": 0,
                        "close_spread_pivot": spread_pivot,
                        "mode_dong": "[STOPOUT]",
                        "action_type": "FORCE_CLOSE",
                        "conf_dev_close": dev_close,
                        "tick_hz_base_out": tick_base.get("tick_hz", 0),
                        "tick_hz_diff_out": tick_diff.get("tick_hz", 0),
                    }
                    context_data = make_context(cap_hien_tai, execution, order_data, close_data)
                    r.lpush(
                        execution["order_key"],
                        json.dumps({
                            "action": "FETCH_HISTORY_ONLY",
                            "ticket": order_data["ticket"],
                            "role": execution["role"],
                            "context": context_data,
                        }),
                    )
                    print(f"[COPY_DIFF] Ticket #{order_data['ticket']} bien mat. Lay lich su va ghi so.")
                    thoi_diem_vua_ra_lenh_dong = time.time()

                if len(lenh_con_song) != len(lich_su_lenh):
                    lich_su_lenh = lenh_con_song
                    if not lich_su_lenh:
                        huong_dang_danh = None
                    luu_tri_nho()

                untracked_positions = [p for p in list_pos_execution if p.get("ticket") not in tracked_tickets]
                if untracked_positions and not trong_thoi_gian_khoi_dong:
                    for pos in sorted(untracked_positions, key=lambda x: x.get("time_msc", 0)):
                        side = pos.get("side", "UNKNOWN")
                        loai_lenh_pos = infer_loai_lenh_from_side(side)
                        if loai_lenh_pos == "UNKNOWN":
                            order_data = {
                                "id_lenh": f"ORPHAN_UNKNOWN_{pos['ticket']}",
                                "ticket": pos["ticket"],
                                "role": execution["role"],
                                "action": side,
                                "loai_lenh": "UNKNOWN",
                                "time_entry": time.time(),
                                "chenh_lech_vao": 0,
                                "chenh_lech_vao_raw": 0,
                                "tinh_chat_vao": "[UNKNOWN]",
                                "entry_spread_pivot": 0.0,
                                "conf_dev_entry": 0,
                                "entry_stable_time": 0,
                                "tick_hz_base_in": 0,
                                "tick_hz_diff_in": 0,
                            }
                            close_data = {
                                "chenh_dong": 0,
                                "chenh_dong_raw": 0,
                                "close_spread_pivot": spread_pivot,
                                "mode_dong": "[ORPHAN_UNKNOWN_CUT]",
                                "action_type": "COPY_DIFF_CLOSE",
                                "conf_dev_close": dev_close,
                                "tick_hz_base_out": tick_base.get("tick_hz", 0),
                                "tick_hz_diff_out": tick_diff.get("tick_hz", 0),
                            }
                            gui_lenh_dong(order_data, close_data, "ORPHAN_UNKNOWN")
                            print(f"[COPY_DIFF] Cat lenh khong ro huong #{pos['ticket']} bang ticket.")
                            thoi_diem_vua_ra_lenh_dong = time.time()
                            continue

                        if huong_dang_danh and loai_lenh_pos != "UNKNOWN" and loai_lenh_pos != huong_dang_danh:
                            order_data = {
                                "id_lenh": f"ORPHAN_COPYDIFF_{pos['ticket']}",
                                "ticket": pos["ticket"],
                                "role": execution["role"],
                                "action": side,
                                "loai_lenh": loai_lenh_pos,
                                "time_entry": time.time(),
                                "chenh_lech_vao": 0,
                                "chenh_lech_vao_raw": 0,
                                "tinh_chat_vao": "[UNKNOWN]",
                                "entry_spread_pivot": 0.0,
                                "conf_dev_entry": 0,
                                "entry_stable_time": 0,
                                "tick_hz_base_in": 0,
                                "tick_hz_diff_in": 0,
                            }
                            close_data = {
                                "chenh_dong": 0,
                                "chenh_dong_raw": 0,
                                "close_spread_pivot": spread_pivot,
                                "mode_dong": "[ORPHAN_CUT]",
                                "action_type": "COPY_DIFF_CLOSE",
                                "conf_dev_close": dev_close,
                                "tick_hz_base_out": tick_base.get("tick_hz", 0),
                                "tick_hz_diff_out": tick_diff.get("tick_hz", 0),
                            }
                            gui_lenh_dong(order_data, close_data, "ORPHAN_COPYDIFF")
                            print(f"[COPY_DIFF] Cat lenh la nguoc huong #{pos['ticket']} {side}.")
                            thoi_diem_vua_ra_lenh_dong = time.time()
                        else:
                            adopted = {
                                "id_lenh": f"ADOPT_COPYDIFF_{pos['ticket']}",
                                "ticket": pos["ticket"],
                                "role": execution["role"],
                                "action": side,
                                "loai_lenh": loai_lenh_pos,
                                "time_entry": time.time(),
                                "chenh_lech_vao": 0,
                                "chenh_lech_vao_raw": 0,
                                "tinh_chat_vao": "[ADOPTED]",
                                "entry_spread_pivot": spread_pivot,
                                "conf_dev_entry": dev_entry,
                                "entry_stable_time": cap_hien_tai.get("stable_time", 0),
                                "tick_hz_base_in": 0,
                                "tick_hz_diff_in": 0,
                            }
                            lich_su_lenh.append(adopted)
                            if huong_dang_danh is None and loai_lenh_pos in ("TH1", "TH2"):
                                huong_dang_danh = loai_lenh_pos
                            print(f"[COPY_DIFF] Adopt ticket #{pos['ticket']} {side} ({loai_lenh_pos}) vao so lenh.")
                    luu_tri_nho()

            co_tick_moi = False
            base_co_bien_dong = False
            diff_co_bien_dong = False
            exec_co_bien_dong = False

            if tick_base_raw != last_tick_base_raw:
                tick_base = json.loads(tick_base_raw)
                last_tick_base_raw = tick_base_raw
                base_co_bien_dong = True

            if tick_diff_raw != last_tick_diff_raw:
                tick_diff = json.loads(tick_diff_raw)
                last_tick_diff_raw = tick_diff_raw
                diff_co_bien_dong = True

            # 🆕 Parse tick san thu 3
            if tick_exec_raw != last_tick_exec_raw:
                tick_exec = json.loads(tick_exec_raw)
                last_tick_exec_raw = tick_exec_raw
                exec_co_bien_dong = True

            if base_co_bien_dong and tick_base.get("time_msc", 0) > last_base_msc:
                last_base_msc = tick_base["time_msc"]
                local_nhan_base = time.time()
                co_tick_moi = True

            if diff_co_bien_dong and tick_diff.get("time_msc", 0) > last_diff_msc:
                last_diff_msc = tick_diff["time_msc"]
                local_nhan_diff = time.time()
                co_tick_moi = True

            if exec_co_bien_dong and tick_exec.get("time_msc", 0) > last_exec_msc:
                last_exec_msc = tick_exec["time_msc"]
                local_nhan_exec = time.time()

            if co_tick_moi:
                thoi_diem_nhan_tick_cuoi = time.time()
                da_xu_ly_lenh_cho_tick_nay = False

            hz_base = tick_base.get("tick_hz", 0)
            hz_diff = tick_diff.get("tick_hz", 0)
            tick_hz_vuot_nguong = (
                (max_tick_hz_base > 0 and hz_base > max_tick_hz_base)
                or (max_tick_hz_diff > 0 and hz_diff > max_tick_hz_diff)
            )

            if not tick_base.get("connected", False) or not tick_diff.get("connected", False):
                print("[COPY_DIFF] Mat mang/tick source (Base/Diff), khoa lenh.", end="\r")
                thoi_diem_nhan_tick_cuoi = time.time()
                da_xu_ly_lenh_cho_tick_nay = True
                continue

            # 🆕 Kiem tra ket noi san thu 3
            if not tick_exec.get("connected", False):
                print("[COPY_DIFF] Mat mang san thu 3 (Execution), khoa lenh.", end="\r")
                thoi_diem_nhan_tick_cuoi = time.time()
                da_xu_ly_lenh_cho_tick_nay = True
                continue

            now = time.time()
            if now - local_nhan_base > max_tick_delay or now - local_nhan_diff > max_tick_delay:
                print(f"[COPY_DIFF] Tick Base/Diff tre qua {max_tick_delay}s, khoa lenh.", end="\r")
                da_xu_ly_lenh_cho_tick_nay = True
                continue

            # 🆕 Kiem tra do tre tick san thu 3
            if now - local_nhan_exec > max_tick_delay:
                print(f"[COPY_DIFF] Tick san thu 3 tre qua {max_tick_delay}s, khoa lenh.", end="\r")
                da_xu_ly_lenh_cho_tick_nay = True
                continue

            if trong_gio_cam:
                if lich_su_lenh:
                    print(f"[COPY_DIFF] Gio cam: xa {len(lich_su_lenh)} lenh copy_diff.")
                    for order_data in lich_su_lenh[:]:
                        close_data = {
                            "chenh_dong": 0,
                            "chenh_dong_raw": 0,
                            "close_spread_pivot": spread_pivot,
                            "mode_dong": "[BLACKOUT_CUT]",
                            "action_type": "BLACKOUT_CLOSE",
                            "conf_dev_close": dev_close,
                            "tick_hz_base_out": tick_base.get("tick_hz", 0),
                            "tick_hz_diff_out": tick_diff.get("tick_hz", 0),
                        }
                        gui_lenh_dong(order_data, close_data, "BLACKOUT")
                    lich_su_lenh.clear()
                    huong_dang_danh = None
                    thoi_diem_vua_ra_lenh_dong = time.time()
                    da_xu_ly_lenh_cho_tick_nay = True
                    luu_tri_nho()
                    if time.time() - thoi_diem_spam_cuoi > 60:
                        r.lpush(QUEUE_TELEGRAM, f"<b>{master_name} - BLACKOUT</b>\nDa xa toan bo lenh copy_diff.")
                        thoi_diem_spam_cuoi = time.time()
                continue

            huong_tu_so = lay_huong_tu_lich_su(lich_su_lenh)
            if huong_tu_so and huong_dang_danh not in ("TH1", "TH2"):
                huong_dang_danh = huong_tu_so

            tin_hieu = check_tin_hieu_arbitrage(tick_base, tick_diff, cap_hien_tai, huong_dang_danh)
            hanh_dong = tin_hieu["hanh_dong"]

            if hanh_dong == "VAO_LENH":
                if thoi_diem_bat_dau_lech_vao == 0:
                    thoi_diem_bat_dau_lech_vao = time.time()
                    gia_base_luc_bat_dau_lech = tick_base["bid"]
                thoi_diem_bat_dau_lech_dong = 0
                gia_base_luc_bat_dau_lech_dong = 0.0
            elif hanh_dong == "DONG_LENH":
                if thoi_diem_bat_dau_lech_dong == 0:
                    thoi_diem_bat_dau_lech_dong = time.time()
                    gia_base_luc_bat_dau_lech_dong = tick_base["bid"]
                thoi_diem_bat_dau_lech_vao = 0
                gia_base_luc_bat_dau_lech = 0.0
            else:
                thoi_diem_bat_dau_lech_vao = 0
                thoi_diem_bat_dau_lech_dong = 0
                gia_base_luc_bat_dau_lech = 0.0
                gia_base_luc_bat_dau_lech_dong = 0.0

            if not co_tick_moi:
                if not lich_su_lenh and (hanh_dong != "VAO_LENH" or da_xu_ly_lenh_cho_tick_nay):
                    continue

            if hanh_dong == "DONG_LENH" and lich_su_lenh:
                if tick_hz_vuot_nguong:
                    print(f"[COPY_DIFF] Tick Hz vuot nguong, khoa chot. {hz_base}|{hz_diff}", end="\r")
                    continue
                if time.time() - thoi_diem_vua_ra_lenh_dong < cooldown_close_sec:
                    continue

                lenh_du_tuoi = [o for o in lich_su_lenh if time.time() - o.get("time_entry", 0) >= hold_time_sec]
                if not lenh_du_tuoi:
                    continue

                dk_thoi_gian = (
                    time.time() - thoi_diem_bat_dau_lech_dong >= stable_time_sec
                    if stable_mode == "continuous"
                    else time.time() - thoi_diem_nhan_tick_cuoi >= stable_time_sec
                )
                if not dk_thoi_gian or da_xu_ly_lenh_cho_tick_nay:
                    continue

                order_data = lenh_du_tuoi[0]
                close_action = opposite_action(order_data.get("action"))

                mode_dong = "[F]"
                if stable_mode == "continuous" and time.time() - thoi_diem_nhan_tick_cuoi < stable_time_sec:
                    mode_dong = "[C]"

                close_data = {
                    "chenh_dong": tin_hieu.get("chenh_lech", 0),
                    "chenh_dong_raw": tin_hieu.get("chenh_lech_raw", tin_hieu.get("chenh_lech", 0)),
                    "close_spread_pivot": tin_hieu.get("spread_pivot", spread_pivot),
                    "mode_dong": mode_dong,
                    "action_type": tin_hieu.get("loai_dong", "COPY_DIFF_CLOSE"),
                    "conf_dev_close": dev_close,
                    "tick_hz_base_out": tick_base.get("tick_hz", 0),
                    "tick_hz_diff_out": tick_diff.get("tick_hz", 0),
                }
                gui_lenh_dong(order_data, close_data, cap_hien_tai.get("comment_close", ""))
                lich_su_lenh.remove(order_data)
                thoi_diem_vua_ra_lenh_dong = time.time()
                da_xu_ly_lenh_cho_tick_nay = True
                if not lich_su_lenh:
                    thoi_diem_vao_lenh_cuoi = 0
                    huong_dang_danh = None
                luu_tri_nho()
                print(
                    f"[COPY_DIFF] Dong ticket #{order_data['ticket']} {close_action}, "
                    f"dev {close_data['chenh_dong']:.2f}."
                )

            elif hanh_dong == "VAO_LENH":
                if tick_hz_vuot_nguong:
                    print(f"[COPY_DIFF] Tick Hz vuot nguong, khoa vao. {hz_base}|{hz_diff}", end="\r")
                    continue
                if equity_execution < alert_equity:
                    print(
                        f"[COPY_DIFF] LOW EQUITY {execution['exchange']} {equity_execution:.2f} < {alert_equity}",
                        end="\r",
                    )
                    continue
                if not kiem_tra_gio_giao_dich(cap_hien_tai.get("trading_hours", []), current_utc_time_str):
                    continue

                loai_lenh_moi = tin_hieu["loai_lenh"]
                so_lenh_hien_tai = len(lich_su_lenh) + len(pending_jobs)
                dang_cooldown = time.time() - thoi_diem_vao_lenh_cuoi < cooldown_sec
                if (
                    so_lenh_hien_tai >= max_orders
                    or dang_cooldown
                    or (huong_dang_danh is not None and huong_dang_danh != loai_lenh_moi)
                ):
                    continue

                dk_thoi_gian = (
                    time.time() - thoi_diem_bat_dau_lech_vao >= stable_time_sec
                    if stable_mode == "continuous"
                    else time.time() - thoi_diem_nhan_tick_cuoi >= stable_time_sec
                )
                if not dk_thoi_gian or da_xu_ly_lenh_cho_tick_nay:
                    continue

                # 🆕 Copy_diff: luon lay lenh theo chieu Diff
                action = infer_open_action(tin_hieu)

                mode_vao = "[F]"
                if stable_mode == "continuous" and time.time() - thoi_diem_nhan_tick_cuoi < stable_time_sec:
                    mode_vao = "[C]"

                job_id = f"J_{str(uuid.uuid4()).split('-')[0].upper()}"
                context_vao = {
                    "trade_mode": "copy_diff",
                    "job_id": job_id,
                    "pair_id": args.pair_id,
                    "execution_exchange": execution["exchange"],
                    "execution_symbol": execution["symbol"],
                    "execution_role": execution["role"],
                    "execution_action": action,
                    "chenh_vao": tin_hieu["chenh_lech"],
                    "chenh_vao_raw": tin_hieu.get("chenh_lech_raw", tin_hieu["chenh_lech"]),
                    "entry_spread_pivot": tin_hieu.get("spread_pivot", spread_pivot),
                    "tinh_chat_vao": mode_vao,
                    "conf_dev_entry": dev_entry,
                    "conf_dev_close": dev_close,
                    "conf_stable_time": cap_hien_tai.get("stable_time", 0),
                    "entry_stable_time": cap_hien_tai.get("stable_time", 0),
                    "tick_hz_base_in": tick_base.get("tick_hz", 0),
                    "tick_hz_diff_in": tick_diff.get("tick_hz", 0),
                }
                pending_jobs[job_id] = {
                    "time": time.time(),
                    "action": action,
                    "loai_lenh": loai_lenh_moi,
                    "chenh_vao": tin_hieu["chenh_lech"],
                    "chenh_vao_raw": tin_hieu.get("chenh_lech_raw", tin_hieu["chenh_lech"]),
                    "entry_spread_pivot": tin_hieu.get("spread_pivot", spread_pivot),
                    "tinh_chat_vao": mode_vao,
                    "conf_dev_entry": dev_entry,
                    "entry_stable_time": cap_hien_tai.get("stable_time", 0),
                    "tick_hz_base_in": tick_base.get("tick_hz", 0),
                    "tick_hz_diff_in": tick_diff.get("tick_hz", 0),
                }
                r.lpush(
                    execution["order_key"],
                    json.dumps({
                        "action": action,
                        "volume": execution["volume"],
                        "comment": cap_hien_tai.get("comment_entry", ""),
                        "role": execution["role"],
                        "context": context_vao,
                    }),
                )
                huong_dang_danh = loai_lenh_moi
                thoi_diem_vao_lenh_cuoi = time.time()
                da_xu_ly_lenh_cho_tick_nay = True
                luu_tri_nho()
                print(
                    f"[COPY_DIFF] Vao {loai_lenh_moi} {action} {execution['exchange']}:{execution['symbol']} "
                    f"dev {tin_hieu['chenh_lech']:.2f} job {job_id}."
                )
                logging.info(
                    "[COPY_DIFF ENTRY] %s %s dev=%s raw=%s job=%s",
                    loai_lenh_moi,
                    action,
                    tin_hieu["chenh_lech"],
                    tin_hieu.get("chenh_lech_raw", tin_hieu["chenh_lech"]),
                    job_id,
                )

        except Exception as exc:
            print(f"\n[COPY_DIFF CRITICAL] {exc}")
            logging.error("[COPY_DIFF CRITICAL]", exc_info=True)
            thoi_diem_nhan_tick_cuoi = time.time()
            da_xu_ly_lenh_cho_tick_nay = True
            time.sleep(0.5)
            continue

except KeyboardInterrupt:
    print(f"\n[MASTER COPY_DIFF {args.pair_id}] Tat an toan.")
    logging.info("=== STOP MASTER COPY_DIFF ===")

print(f"[MASTER COPY_DIFF {args.pair_id}] Da thoat.")
