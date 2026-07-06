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
VALID_TRADE_MODES = {"copy_multi"}


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
        raise ValueError(f"master_copy_multi chi phuc vu trade_mode='copy_multi', nhan: {mode}")
    return mode


def resolve_executions(cap):
    mode = normalize_mode(cap)
    executions = cap.get("executions") or []
    if not isinstance(executions, list) or len(executions) == 0:
        raise ValueError("trade_mode='copy_multi' bat buoc co mang 'executions'")
    
    parsed = []
    for exec_cfg in executions:
        exec_exchange = safe_upper(exec_cfg.get("exchange"))
        exec_symbol = str(exec_cfg.get("symbol")).strip()
        copy_side = safe_upper(exec_cfg.get("copy_side", "DIFF"))
        if not exec_exchange or not exec_symbol:
            continue
        
        default_volume = cap.get(f"volume_{copy_side.lower()}", 0.01)
        parsed.append({
            "role": f"COPY_{copy_side}",
            "copy_side": copy_side,
            "exchange": exec_exchange,
            "symbol": exec_symbol,
            "volume": as_float(exec_cfg.get("volume", default_volume), 0.01),
            "order_key": f"QUEUE:ORDER:{exec_exchange}",
            "position_key": f"POSITION:{exec_exchange}:{exec_symbol}",
            "equity_key": f"ACCOUNT:{exec_exchange}:EQUITY",
        })
    if len(parsed) == 0:
        raise ValueError("Khong co execution hop le nao trong mang executions")
    return parsed


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
        "pair_token": order_data.get("id_lenh", "UNKNOWN"),
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
        "action_type": close_data.get("action_type", "COPY_MULTI_CLOSE"),
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
log_filename = os.path.join(log_dir, f"log_master_copy_multi_{args.pair_id}.txt")
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
    executions = resolve_executions(cap_hien_tai)
except Exception as exc:
    print(f"LOI config copy_diff: {exc}")
    logging.error("LOI config copy_multi: %s", exc)
    raise SystemExit(1)

vps_name = config.get("vps_name", "LOCAL")
master_name = f"[{vps_name} | MULTI | {args.pair_id}]"
try:
    ctypes.windll.kernel32.SetConsoleTitleW(f"MASTER MULTI {master_name}")
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
# key_exec removed
key_state = f"STATE:COPY_MULTI_MASTER:{args.pair_id}"
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
else:
    saved_state = {}

exec_states = {}
for ex in executions:
    ex_id = f"{ex['exchange']}:{ex['symbol']}"
    if ex_id in saved_state:
        exec_states[ex_id] = saved_state[ex_id]
        print(f"[COPY_MULTI] Khoi phuc so lenh cho {ex_id}: {len(exec_states[ex_id]['lich_su_lenh'])} lenh.")
    else:
        exec_states[ex_id] = {
            "huong_dang_danh": None,
            "lich_su_lenh": [],
            "thoi_diem_vao_lenh_cuoi": 0,
            "thoi_diem_vua_ra_lenh_dong": 0
        }
        print(f"[COPY_MULTI] Bat dau voi so lenh trong cho {ex_id}.")

def luu_tri_nho():
    r.set(key_state, json.dumps(exec_states))

def get_state(ex_id):
    return exec_states[ex_id]

def update_state(ex_id, key, value):
    exec_states[ex_id][key] = value




def reload_runtime_config(new_cap):
    global cap_hien_tai, executions
    global dev_entry, dev_close, spread_pivot, stable_time_sec, cooldown_sec
    global cooldown_close_sec, max_orders, hold_time_sec, alert_equity, stable_mode
    global max_tick_delay, filter_entry, filter_close, max_tick_hz_base, max_tick_hz_diff
    global spread_pivot_refresh_second

    new_executions = resolve_executions(new_cap)
    
    # We allow adding/removing executions dynamically, but for simplicity we just update it
    cap_hien_tai = new_cap
    executions = new_executions
    
    # Initialize state for any new executions
    for ex in executions:
        ex_id = f"{ex['exchange']}:{ex['symbol']}"
        if ex_id not in exec_states:
            exec_states[ex_id] = {
                "huong_dang_danh": None,
                "lich_su_lenh": [],
                "thoi_diem_vao_lenh_cuoi": 0,
                "thoi_diem_vua_ra_lenh_dong": 0
            }

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

for ex in executions:
    print(
        f"[COPY_MULTI] Master san sang: execution={ex['exchange']}:{ex['symbol']} "
        f"role={ex['role']} volume={ex['volume']}"
    )
logging.info("=== START MASTER COPY_MULTI %s ===", args.pair_id)
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
                    print("[SHUTDOWN] Master copy_multi nhan tin hieu tat.")
                    logging.info("[SHUTDOWN] Redis shutdown signal.")
                    break

                current_modified = os.path.getmtime(CONFIG_FILE)
                if current_modified != last_config_modified:
                    time.sleep(0.05)
                    try:
                        config = load_config()
                        new_cap = find_pair(config, args.pair_id)
                        if new_cap:
                            reload_runtime_config(new_cap)
                            print("[HOT RELOAD] Da cap nhat config thanh cong.")
                    except Exception as e:
                        print(f"[HOT RELOAD] Loi config: {e}")
                    last_config_modified = os.path.getmtime(CONFIG_FILE)

                refresh_runtime_spread_pivot("Chu ky config")

            # Xay dung danh sach key can lay tu Redis
            keys_to_get = [key_base, key_diff]
            for ex in executions:
                keys_to_get.append(f"TICK:{ex['exchange']}:{ex['symbol']}")
            # 🆕 Them position keys de doc position thuc tren san
            for ex in executions:
                keys_to_get.append(ex["position_key"])
            
            raw_data = r.mget(keys_to_get)
            last_tick_base_raw = raw_data[0]
            last_tick_diff_raw = raw_data[1]
            n_exec = len(executions)
            exec_ticks_raw = raw_data[2:2 + n_exec]
            exec_pos_raw = raw_data[2 + n_exec:]

            if last_tick_base_raw:
                try:
                    tb = json.loads(last_tick_base_raw)
                    if "time_msc" in tb and tb["time_msc"] > last_base_msc:
                        last_base_msc = tb["time_msc"]
                        local_nhan_base = now_sec
                        tick_base = tb
                        thoi_diem_nhan_tick_cuoi = now_sec
                except Exception:
                    pass

            if last_tick_diff_raw:
                try:
                    td = json.loads(last_tick_diff_raw)
                    if "time_msc" in td and td["time_msc"] > last_diff_msc:
                        last_diff_msc = td["time_msc"]
                        local_nhan_diff = now_sec
                        tick_diff = td
                        thoi_diem_nhan_tick_cuoi = now_sec
                except Exception:
                    pass

            # Update exec ticks status
            exec_connected = {}
            for i, ex in enumerate(executions):
                ex_id = f"{ex['exchange']}:{ex['symbol']}"
                exec_connected[ex_id] = False
                if exec_ticks_raw[i]:
                    try:
                        te = json.loads(exec_ticks_raw[i])
                        exec_connected[ex_id] = te.get("connected", False)
                    except Exception:
                        pass

            if not tick_base.get("connected") or not tick_diff.get("connected"):
                time.sleep(0.01)
                continue

            delay_base = now_sec - local_nhan_base
            delay_diff = now_sec - local_nhan_diff

            if max_tick_delay > 0:
                if delay_base > max_tick_delay or delay_diff > max_tick_delay:
                    time.sleep(0.01)
                    continue

            # Xu ly bien lai (Receipts) tu Worker
            da_xu_ly_lenh = False
            while True:
                raw_result = r.rpop(QUEUE_ORDER_RESULT)
                if not raw_result:
                    break
                try:
                    res = json.loads(raw_result)
                    context = res.get("context", {})
                    # 🆕 Đọc action_type từ top-level trước (worker report), fallback context
                    action_type = res.get("action_type") or context.get("action_type")
                    ex_id = f"{context.get('execution_exchange')}:{context.get('execution_symbol')}"
                    
                    if ex_id not in exec_states:
                        continue # Khong thuoc san dang chay, bo qua
                        
                    st = exec_states[ex_id]

                    if action_type == "COPY_DIFF_OPEN" or action_type == "COPY_BASE_OPEN":
                        if res.get("ticket"):
                            st["lich_su_lenh"].append({
                                "id_lenh": context.get("job_id", f"MULTI_{res.get('ticket')}"),
                                "ticket": res.get("ticket"),
                                "loai_lenh": context.get("loai_lenh", "UNKNOWN"),
                                "action": context.get("execution_side", context.get("execution_action", "UNKNOWN")),
                                "chenh_lech_vao": context.get("chenh_vao", context.get("chenh_lech_vao", 0)),
                                "chenh_lech_vao_raw": context.get("chenh_vao_raw", context.get("chenh_lech_vao_raw", 0)),
                                "tinh_chat_vao": context.get("tinh_chat_vao", context.get("mode_vao", "UNKNOWN")),
                                "entry_spread_pivot": context.get("entry_spread_pivot", 0.0),
                                "conf_dev_entry": context.get("conf_dev_entry", 0),
                                "entry_stable_time": context.get("entry_stable_time", 0),
                                "tick_hz_base_in": context.get("tick_hz_base_in", 0),
                                "tick_hz_diff_in": context.get("tick_hz_diff_in", 0),
                                "time_entry": time.time(),
                            })
                            st["huong_dang_danh"] = context.get("loai_lenh")
                            st["thoi_diem_vao_lenh_cuoi"] = now_sec
                            luu_tri_nho()
                            print(f"[VAO LENH OK {ex_id}] -> Dang om: {len(st['lich_su_lenh'])} lenh. Huong: {st['huong_dang_danh']}")

                    elif action_type in ("COPY_DIFF_CLOSE", "COPY_BASE_CLOSE", "CLOSE", "COPY_MULTI_CLOSE"):
                        if res.get("ticket"):
                            ticket_dong = res.get("ticket")
                            st["lich_su_lenh"] = [x for x in st["lich_su_lenh"] if str(x.get("ticket")) != str(ticket_dong)]
                            if not st["lich_su_lenh"]:
                                st["huong_dang_danh"] = None
                            st["thoi_diem_vua_ra_lenh_dong"] = now_sec
                            luu_tri_nho()
                            print(f"[DONG LENH OK {ex_id}] -> Con lai: {len(st['lich_su_lenh'])} lenh. Huong: {st['huong_dang_danh']}")

                    # 🆕 Xử lý CLOSE_FAILED: reset pending_close nhưng GIỮ lệnh trong sổ
                    elif action_type == "CLOSE_FAILED":
                        ticket_fail = res.get("ticket")
                        for o in st["lich_su_lenh"]:
                            if str(o.get("ticket")) == str(ticket_fail):
                                o["pending_close"] = False
                        # Cooldown dài hơn (30s) để tránh spam khi market đóng
                        st["thoi_diem_vua_ra_lenh_dong"] = now_sec + 27
                        luu_tri_nho()
                        print(f"[CLOSE_FAILED {ex_id}] Ticket #{ticket_fail} - {res.get('comment', '')} ({res.get('retcode', '')}). Se thu lai sau 30s.")

                    da_xu_ly_lenh = True
                except Exception as e:
                    print(f"Loi xu ly ket qua vao/ra: {e}")

            if da_xu_ly_lenh:
                continue

            # 🆕 FORCE CLOSE HOURS: xa toan bo lenh khi trong gio cam
            trong_gio_force_close = kiem_tra_gio_cam(cap_hien_tai.get("force_close_hours", []), current_utc_time_str)
            if trong_gio_force_close:
                co_lenh_can_xa = False
                for ex in executions:
                    ex_id = f"{ex['exchange']}:{ex['symbol']}"
                    st = exec_states[ex_id]
                    if st["lich_su_lenh"]:
                        co_lenh_can_xa = True
                        for order_data in st["lich_su_lenh"][:]:
                            if order_data.get("pending_close"):
                                continue
                            close_data = {
                                "chenh_dong": 0, "chenh_dong_raw": 0,
                                "close_spread_pivot": spread_pivot,
                                "mode_dong": "[BLACKOUT_CUT]",
                                "action_type": "BLACKOUT_CLOSE",
                            }
                            context_data = make_context(cap_hien_tai, ex, order_data, close_data)
                            r.lpush(
                                ex["order_key"],
                                json.dumps({
                                    "action": "CLOSE_BY_TICKET",
                                    "ticket": order_data["ticket"],
                                    "comment": "FORCE_CLOSE_HOURS",
                                    "role": ex["role"],
                                    "context": context_data,
                                })
                            )
                            order_data["pending_close"] = True
                            print(f"[FORCE_CLOSE {ex_id}] Xa lenh #{order_data['ticket']}")
                        st["thoi_diem_vua_ra_lenh_dong"] = now_sec
                if co_lenh_can_xa:
                    luu_tri_nho()
                    if now_sec - thoi_diem_spam_cuoi > 60:
                        try:
                            r.lpush(QUEUE_TELEGRAM, f"<b>{master_name} - FORCE CLOSE</b>\nXa toan bo lenh trong gio cam.")
                        except Exception:
                            pass
                        thoi_diem_spam_cuoi = now_sec
                continue

            # 🆕 ORPHAN ADOPTION + STOPOUT DETECTION (giong master_single)
            trong_thoi_gian_bao_ve = (now_sec - startup_time < STARTUP_GRACE_SECOND)
            for i, ex in enumerate(executions):
                ex_id = f"{ex['exchange']}:{ex['symbol']}"
                st = exec_states[ex_id]
                
                # Parse position tu Redis
                list_pos_exec = []
                if exec_pos_raw[i]:
                    try:
                        parsed_pos = json.loads(exec_pos_raw[i])
                        if isinstance(parsed_pos, list):
                            list_pos_exec = parsed_pos
                    except Exception:
                        pass
                
                tickets_on_exchange = {p.get("ticket") for p in list_pos_exec if isinstance(p, dict)}
                tracked_tickets = {o.get("ticket") for o in st["lich_su_lenh"]}
                
                if not trong_thoi_gian_bao_ve:
                    # Phat hien lenh bien mat (stopout / dong tay)
                    lenh_con_song = []
                    for order_data in st["lich_su_lenh"]:
                        if order_data.get("ticket") in tickets_on_exchange:
                            lenh_con_song.append(order_data)
                        elif order_data.get("pending_close"):
                            lenh_con_song.append(order_data)  # Cho bien lai tu worker
                        else:
                            # Lenh bien mat, ghi so
                            close_data = {
                                "chenh_dong": 0, "chenh_dong_raw": 0,
                                "close_spread_pivot": spread_pivot,
                                "mode_dong": "[STOPOUT]",
                                "action_type": "FORCE_CLOSE",
                            }
                            context_data = make_context(cap_hien_tai, ex, order_data, close_data)
                            r.lpush(
                                ex["order_key"],
                                json.dumps({
                                    "action": "FETCH_HISTORY_ONLY",
                                    "ticket": order_data["ticket"],
                                    "role": ex["role"],
                                    "context": context_data,
                                }),
                            )
                            print(f"[STOPOUT {ex_id}] Ticket #{order_data['ticket']} bien mat. Lay lich su.")
                    
                    if len(lenh_con_song) != len(st["lich_su_lenh"]):
                        st["lich_su_lenh"] = lenh_con_song
                        if not st["lich_su_lenh"]:
                            st["huong_dang_danh"] = None
                        luu_tri_nho()
                    
                    # Adopt orphan: lenh tren san nhung master khong biet
                    untracked = [p for p in list_pos_exec if isinstance(p, dict) and p.get("ticket") not in tracked_tickets]
                    for pos in untracked:
                        side = safe_upper(pos.get("side", "UNKNOWN"))
                        loai_lenh_pos = infer_loai_lenh_from_side(side)
                        adopted = {
                            "id_lenh": f"ADOPT_MULTI_{pos['ticket']}",
                            "ticket": pos["ticket"],
                            "action": side,
                            "loai_lenh": loai_lenh_pos,
                            "time_entry": time.time(),
                            "chenh_lech_vao": 0,
                            "chenh_lech_vao_raw": 0,
                            "tinh_chat_vao": "[ADOPTED]",
                            "entry_spread_pivot": spread_pivot,
                            "conf_dev_entry": dev_entry,
                            "entry_stable_time": stable_time_sec,
                            "tick_hz_base_in": 0,
                            "tick_hz_diff_in": 0,
                        }
                        st["lich_su_lenh"].append(adopted)
                        if st["huong_dang_danh"] is None and loai_lenh_pos in ("TH1", "TH2"):
                            st["huong_dang_danh"] = loai_lenh_pos
                        print(f"[ADOPT {ex_id}] Adopt ticket #{pos['ticket']} {side} ({loai_lenh_pos}) vao so lenh.")
                    if untracked:
                        luu_tri_nho()

            # Kiem tra gio giao dich
            if not kiem_tra_gio_giao_dich(cap_hien_tai.get("trading_hours"), current_utc_time_str):
                continue
            if kiem_tra_gio_cam(cap_hien_tai.get("blackout_hours"), current_utc_time_str):
                continue
            if now_sec - startup_time < STARTUP_GRACE_SECOND:
                continue
                
            # Lay gia hien tai cua base, diff
            ask_base = tick_base["ask"]
            bid_base = tick_base["bid"]
            ask_diff = tick_diff["ask"]
            bid_diff = tick_diff["bid"]
            
            # Tinh toan tin hieu arbitrage
            # Xac dinh huong dang danh chung (cac execution cung base/diff)
            huong_chung = None
            for ex in executions:
                ex_id_tmp = f"{ex['exchange']}:{ex['symbol']}"
                h = exec_states[ex_id_tmp]["huong_dang_danh"]
                if h is not None:
                    huong_chung = h
                    break
            
            tin_hieu = check_tin_hieu_arbitrage(tick_base, tick_diff, cap_hien_tai, huong_chung)
            hanh_dong = tin_hieu["hanh_dong"]
            
            # Tracking stable_time (giong master_single)
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
            
            # Tinh san cac chenh lech de dong lenh (Dong lenh tinh rieng cho tung execution)
            chenh_th1_raw = bid_base - ask_diff
            chenh_th2_raw = bid_diff - ask_base
            chenh_th1 = chenh_th1_raw - spread_pivot
            chenh_th2 = chenh_th2_raw + spread_pivot
            
            # QUAN TRONG NHAST: LOOP QUA TAT CA CAC SAN COPY DE XU LY
            for ex in executions:
                ex_id = f"{ex['exchange']}:{ex['symbol']}"
                st = exec_states[ex_id]
                
                # Check connected
                if not exec_connected[ex_id]:
                    continue
                    
                copy_side = ex["copy_side"] # 'DIFF' or 'BASE'
                role = ex["role"] # 'COPY_DIFF' or 'COPY_BASE'
                
                # --- XU LY DONG LENH (CLOSE) ---
                if len(st["lich_su_lenh"]) > 0:
                    if now_sec - st["thoi_diem_vua_ra_lenh_dong"] < cooldown_close_sec:
                        continue
                    
                    huong = st["huong_dang_danh"]
                    dong_ly_thuyet = False
                    if huong == "TH1" and chenh_th2 >= dev_close:
                        dong_ly_thuyet = True
                        tin_hieu_dong = {"chenh_lech": chenh_th2, "chenh_lech_raw": chenh_th2_raw, "spread_pivot": spread_pivot, "loai_dong": "TH1"}
                    elif huong == "TH2" and chenh_th1 >= dev_close:
                        dong_ly_thuyet = True
                        tin_hieu_dong = {"chenh_lech": chenh_th1, "chenh_lech_raw": chenh_th1_raw, "spread_pivot": spread_pivot, "loai_dong": "TH2"}
                    
                    for order_data in st["lich_su_lenh"]:
                        if order_data.get("pending_close"):
                            continue
                        
                        # Tinh thoi gian lenh da song (giong master_single)
                        thoi_gian_song = time.time() - order_data.get("time_entry", 0)
                        
                        # Stable time check cho dong lenh
                        dk_stable_dong = (
                            time.time() - thoi_diem_bat_dau_lech_dong >= stable_time_sec
                            if stable_mode == "continuous"
                            else time.time() - thoi_diem_nhan_tick_cuoi >= stable_time_sec
                        ) if thoi_diem_bat_dau_lech_dong > 0 else False
                        
                        # Dong theo deviation - CHI KHI da giu lenh du hold_time VA stable_time
                        if dong_ly_thuyet and dk_stable_dong and (hold_time_sec <= 0 or thoi_gian_song >= hold_time_sec):
                            comment = f"[DONG {huong}] Chenh <= Pivot | dev_close={dev_close}"
                            context_data = make_context(cap_hien_tai, ex, order_data, tin_hieu_dong)
                            r.lpush(
                                ex["order_key"],
                                json.dumps({
                                    "action": "CLOSE_BY_TICKET",
                                    "ticket": order_data["ticket"],
                                    "comment": comment,
                                    "role": role,
                                    "context": context_data,
                                })
                            )
                            order_data["pending_close"] = True
                            st["thoi_diem_vua_ra_lenh_dong"] = now_sec
                            print(f"[COPY_MULTI DONG {ex_id}] -> GUi lenh dong {order_data['ticket']} (song {thoi_gian_song:.0f}s)")
                            luu_tri_nho()
                            break  # Chi dong 1 lenh moi lan, lenh tiep cho vong lap sau (cooldown)
                                
                # --- XU LY VAO LENH (OPEN) ---
                if len(st["lich_su_lenh"]) == 0 and st["huong_dang_danh"] is not None:
                    if now_sec - st["thoi_diem_vao_lenh_cuoi"] > 10:
                        st["huong_dang_danh"] = None # Unlock if timeout and no order
                
                # Block vao lenh neu con lenh dang cho dong
                co_lenh_pending_close = any(o.get("pending_close") for o in st["lich_su_lenh"])
                if co_lenh_pending_close:
                    continue
                        
                co_tin_hieu = hanh_dong == "VAO_LENH"
                if co_tin_hieu and len(st["lich_su_lenh"]) < max_orders:
                    if now_sec - st["thoi_diem_vao_lenh_cuoi"] < cooldown_sec:
                        continue
                    if now_sec - st["thoi_diem_vua_ra_lenh_dong"] < cooldown_close_sec:
                        continue
                    
                    # Stable time check cho vao lenh (giong master_single)
                    dk_stable_vao = (
                        time.time() - thoi_diem_bat_dau_lech_vao >= stable_time_sec
                        if stable_mode == "continuous"
                        else time.time() - thoi_diem_nhan_tick_cuoi >= stable_time_sec
                    ) if thoi_diem_bat_dau_lech_vao > 0 else False
                    if not dk_stable_vao:
                        continue
                        
                    # Target side
                    target_action = tin_hieu['lenh_diff'] if copy_side == "DIFF" else tin_hieu['lenh_base']
                    if not target_action:
                        continue
                        
                    # Filter
                    current_price = tick_base["ask"] if target_action == "BUY" else tick_base["bid"] # Approximation
                    skip, reason = should_skip_by_filter(filter_entry, target_action, gia_base_luc_bat_dau_lech, current_price)
                    if skip:
                        if now_sec - thoi_diem_spam_cuoi > 3:
                            print(f"[FILTER] {ex_id} {reason}")
                            thoi_diem_spam_cuoi = now_sec
                        continue
                        
                    # Equity check
                    equity_raw = r.get(ex["equity_key"])
                    equity_execution = as_float(equity_raw, 0)
                    if alert_equity > 0 and equity_execution > 0 and equity_execution < alert_equity:
                        if now_sec - thoi_diem_spam_cuoi > 10:
                            msg = f"[LOW EQUITY {ex_id}] Equity {equity_execution} < {alert_equity}. Tam ngung vao lenh!"
                            print(msg)
                            logging.warning(msg)
                            thoi_diem_spam_cuoi = now_sec
                        continue
                        
                    # Execute
                    loai_lenh = "TH1" if tin_hieu["lenh_diff"] == "BUY" else "TH2"
                    if copy_side == "BASE":
                        loai_lenh = "TH1" if tin_hieu["lenh_base"] == "SELL" else "TH2"
                        
                    if st["huong_dang_danh"] is not None and st["huong_dang_danh"] != loai_lenh:
                        continue
                    
                    st["thoi_diem_vao_lenh_cuoi"] = now_sec # Block spam
                    st["huong_dang_danh"] = loai_lenh # Tentative lock
                    
                    # Tao context
                    id_lenh = f"O_{str(uuid.uuid4()).split('-')[0].upper()}"
                    order_data = {
                        "id_lenh": id_lenh,
                        "action": target_action,
                        "chenh_lech_vao": tin_hieu["chenh_lech"],
                        "chenh_lech_vao_raw": tin_hieu["chenh_lech_raw"],
                        "tinh_chat_vao": tin_hieu.get("mode", "N/A"),
                        "entry_spread_pivot": tin_hieu["spread_pivot"],
                        "conf_dev_entry": dev_entry,
                        "entry_stable_time": stable_time_sec,
                        "tick_hz_base_in": tin_hieu.get("tick_hz_base", 0),
                        "tick_hz_diff_in": tin_hieu.get("tick_hz_diff", 0)
                    }
                    context = make_context(cap_hien_tai, ex, order_data, {})
                    context["action_type"] = f"{role}_OPEN"
                    context["loai_lenh"] = loai_lenh
                    context["job_id"] = id_lenh
                    
                    comment = f"[COPY_MULTI {copy_side}] Dev={tin_hieu['chenh_lech']:+.2f}"
                    
                    r.lpush(
                        ex["order_key"],
                        json.dumps({
                            "action": target_action,
                            "volume": ex["volume"],
                            "comment": comment,
                            "role": role,
                            "context": context,
                        })
                    )
                    print(f"🚀 [COPY_MULTI {ex_id}] -> Ban lenh {target_action} {ex['volume']}")
                    logging.info(f"[COPY_MULTI {ex_id}] -> Ban lenh {target_action} | dev={tin_hieu['chenh_lech']:+.2f}")

        except Exception as e:
            print(f"Loi dong lap multi: {e}")
            logging.error(f"Loi dong lap multi: {e}", exc_info=True)
            time.sleep(0.1)
except KeyboardInterrupt:
    print("[EXIT] Nguoi dung dung chuong trinh.")
    logging.info("Keyboard interrupt.")
