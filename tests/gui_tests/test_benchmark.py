"""
BENCHMARK: So sanh toc do mt5.order_send() vs DOM GUI click
Tach thanh:
  - T_local: Thoi gian thuc hien lenh cuc bo (click/send)
  - T_roundtrip: Thoi gian tu luc gui lenh den khi lenh xuat hien trong positions
  - T_total: Tong thoi gian (T_local + cho position xac nhan)

Chay tren HFM demo account.
"""
import sys
import os
import time
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
os.environ["PYTHONIOENCODING"] = "utf-8"

import MetaTrader5 as mt5
from utils.gui_trader import DomTrader, tim_dom_window, resize_dom, _tim_tat_ca_controls
import ctypes
import ctypes.wintypes

user32 = ctypes.windll.user32
EnumWindowsProc = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.wintypes.HWND, ctypes.wintypes.LPARAM)

def get_window_text(hwnd):
    length = user32.GetWindowTextLengthW(hwnd)
    if length == 0:
        return ""
    buf = ctypes.create_unicode_buffer(length + 1)
    user32.GetWindowTextW(hwnd, buf, length + 1)
    return buf.value

def get_class_name(hwnd):
    buf = ctypes.create_unicode_buffer(256)
    user32.GetClassNameW(hwnd, buf, 256)
    return buf.value

def header(text):
    print("\n" + "=" * 75)
    print(f"  {text}")
    print("=" * 75)

# ==========================================
# CONFIG
# ==========================================
import json
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

HFM_PATH = config["brokers"]["HFM"]["path"]
SYMBOL = "XAUUSD"
VOLUME = 0.01
NUM_ROUNDS = 3  # So vong test

# ==========================================
# KHOI TAO MT5 (HFM)
# ==========================================
header("KHOI TAO MT5 + DOM")

print(f"  Dang ket noi MT5 HFM tai: {HFM_PATH}")
if not mt5.initialize(path=HFM_PATH, portable=True, timeout=60000):
    print(f"  LOI: {mt5.last_error()}")
    sys.exit(1)

mt5.symbol_select(SYMBOL, True)
info = mt5.symbol_info(SYMBOL)

# Xac dinh filling mode
SYMBOL_FILLING_IOC = 2
SYMBOL_FILLING_FOK = 1
filling_mode = mt5.ORDER_FILLING_IOC
if info.filling_mode & SYMBOL_FILLING_IOC:
    filling_mode = mt5.ORDER_FILLING_IOC
elif info.filling_mode & SYMBOL_FILLING_FOK:
    filling_mode = mt5.ORDER_FILLING_FOK
else:
    filling_mode = mt5.ORDER_FILLING_RETURN

acc = mt5.account_info()
print(f"  Account: {acc.login} | Balance: {acc.balance} | Server: {acc.server}")
print(f"  Symbol: {SYMBOL} | Filling: {filling_mode}")

# KHOI TAO DOM
dom_hfm = None
dom_list = []
def enum_dom(hwnd, lParam):
    if user32.IsWindowVisible(hwnd):
        cls = get_class_name(hwnd)
        if "MiniFrame" in cls and "Afx:" in cls:
            title = get_window_text(hwnd)
            if "Gold Spot" in title:
                dom_list.append(hwnd)
    return True
user32.EnumWindows(EnumWindowsProc(enum_dom), 0)

trader = None
if dom_list:
    dom_hwnd = dom_list[0]
    resize_dom(dom_hwnd, 520, 500)
    time.sleep(0.2)
    trader = DomTrader(SYMBOL, bot_name="[HFM]")
    trader.dom_hwnd = dom_hwnd
    trader.controls = _tim_tat_ca_controls(dom_hwnd)
    if trader.controls:
        trader._da_khoi_tao = True
        print(f"  DOM HFM: OK (HWND=0x{dom_hwnd:08X})")
    else:
        print(f"  DOM HFM: LOI - khong tim du controls")
        trader = None
else:
    print("  DOM HFM: KHONG TIM THAY! Chi test API.")

# ==========================================
# HELPER: Dem so position hien tai
# ==========================================
def dem_positions():
    pos = mt5.positions_get(symbol=SYMBOL)
    return len(pos) if pos else 0

def cho_position_tang(so_cu, timeout_ms=5000):
    """Cho den khi so position tang len, tra ve thoi gian (ms)."""
    start = time.perf_counter()
    deadline = start + timeout_ms / 1000.0
    while time.perf_counter() < deadline:
        # Refresh symbol data truoc khi check
        mt5.symbol_info(SYMBOL)
        current = dem_positions()
        if current > so_cu:
            return (time.perf_counter() - start) * 1000
        time.sleep(0.005)  # Poll moi 5ms
    return -1  # Timeout

def cho_position_giam(so_cu, timeout_ms=5000):
    """Cho den khi so position giam xuong."""
    start = time.perf_counter()
    deadline = start + timeout_ms / 1000.0
    while time.perf_counter() < deadline:
        mt5.symbol_info(SYMBOL)
        current = dem_positions()
        if current < so_cu:
            return (time.perf_counter() - start) * 1000
        time.sleep(0.005)
    return -1

# ==========================================
# BENCHMARK 1: mt5.order_send() - MO LENH
# ==========================================
header(f"BENCHMARK: mt5.order_send() BUY ({NUM_ROUNDS} vong)")

api_results_open = []
api_results_close = []

for i in range(NUM_ROUNDS):
    tick = mt5.symbol_info_tick(SYMBOL)
    pos_truoc = dem_positions()

    # --- MO LENH ---
    request_buy = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": SYMBOL,
        "volume": VOLUME,
        "type": mt5.ORDER_TYPE_BUY,
        "price": tick.ask,
        "deviation": 20,
        "magic": 0,
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": filling_mode,
    }

    t_start = time.perf_counter()
    result = mt5.order_send(request_buy)
    t_api = (time.perf_counter() - t_start) * 1000  # ms

    if result.retcode == mt5.TRADE_RETCODE_DONE:
        # Cho position xuat hien (de do roundtrip)
        t_confirm = cho_position_tang(pos_truoc)
        api_results_open.append({
            "round": i + 1,
            "t_api_call": t_api,
            "t_position_confirm": t_confirm,
            "ticket": result.order,
        })
        print(f"  Vong {i+1}: order_send={t_api:.2f}ms | position_confirm={t_confirm:.2f}ms | ticket={result.order}")
    else:
        print(f"  Vong {i+1}: LOI {result.retcode} - {result.comment}")
        api_results_open.append({"round": i + 1, "t_api_call": t_api, "t_position_confirm": -1, "ticket": 0})

    time.sleep(1.0)  # Nghi giua cac vong

# --- DONG LENH API ---
header(f"BENCHMARK: mt5.order_send() CLOSE ({NUM_ROUNDS} vong)")

positions = mt5.positions_get(symbol=SYMBOL)
if positions:
    for i, pos in enumerate(positions[:NUM_ROUNDS]):
        tick = mt5.symbol_info_tick(SYMBOL)
        pos_truoc = dem_positions()
        close_type = mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
        price = tick.bid if close_type == mt5.ORDER_TYPE_SELL else tick.ask

        request_close = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": SYMBOL,
            "volume": pos.volume,
            "type": close_type,
            "position": pos.ticket,
            "price": price,
            "deviation": 20,
            "magic": 0,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": filling_mode,
        }

        t_start = time.perf_counter()
        result = mt5.order_send(request_close)
        t_api = (time.perf_counter() - t_start) * 1000

        if result.retcode == mt5.TRADE_RETCODE_DONE:
            t_confirm = cho_position_giam(pos_truoc)
            api_results_close.append({"round": i + 1, "t_api_call": t_api, "t_position_confirm": t_confirm})
            print(f"  Vong {i+1}: order_send={t_api:.2f}ms | position_confirm={t_confirm:.2f}ms")
        else:
            print(f"  Vong {i+1}: LOI {result.retcode}")
            api_results_close.append({"round": i + 1, "t_api_call": t_api, "t_position_confirm": -1})
        time.sleep(1.0)

# ==========================================
# BENCHMARK 2: DOM GUI click - MO LENH
# ==========================================
if trader:
    header(f"BENCHMARK: DOM GUI click BUY ({NUM_ROUNDS} vong)")

    gui_results_open = []
    gui_results_close = []

    # Dam bao khong con position nao tu test truoc
    print(f"  Positions hien tai truoc DOM test: {dem_positions()}")

    trader.dat_volume(str(VOLUME))
    time.sleep(0.3)
    print(f"  Volume da set: '{trader.lay_volume_hien_tai()}'")

    for i in range(NUM_ROUNDS):
        pos_truoc = dem_positions()
        print(f"  Vong {i+1}: positions truoc={pos_truoc}, dang click...")

        # Do thoi gian click (local)
        t_start = time.perf_counter()
        ok = trader.buy()
        t_click = (time.perf_counter() - t_start) * 1000

        if ok:
            # Do thoi gian cho position xuat hien (roundtrip server)
            t_confirm = cho_position_tang(pos_truoc, timeout_ms=5000)
            pos_sau = dem_positions()
            gui_results_open.append({"round": i + 1, "t_click": t_click, "t_position_confirm": t_confirm})
            if t_confirm >= 0:
                print(f"  Vong {i+1}: click={t_click:.2f}ms | server_roundtrip={t_confirm:.2f}ms | total={t_click + t_confirm:.2f}ms | pos={pos_sau}")
            else:
                print(f"  Vong {i+1}: click={t_click:.2f}ms | TIMEOUT (pos={pos_sau})")
        else:
            print(f"  Vong {i+1}: CLICK THAT BAI")
            gui_results_open.append({"round": i + 1, "t_click": t_click, "t_position_confirm": -1})

        time.sleep(1.0)

    # --- DONG LENH GUI ---
    header(f"BENCHMARK: DOM GUI click CLOSE ({NUM_ROUNDS} vong)")

    for i in range(NUM_ROUNDS):
        pos_truoc = dem_positions()
        if pos_truoc == 0:
            print(f"  Vong {i+1}: Khong con lenh de dong!")
            break

        t_start = time.perf_counter()
        ok = trader.close_position()
        t_click = (time.perf_counter() - t_start) * 1000

        if ok:
            t_confirm = cho_position_giam(pos_truoc, timeout_ms=5000)
            pos_sau = dem_positions()
            gui_results_close.append({"round": i + 1, "t_click": t_click, "t_position_confirm": t_confirm})
            if t_confirm >= 0:
                print(f"  Vong {i+1}: click={t_click:.2f}ms | server_roundtrip={t_confirm:.2f}ms | total={t_click + t_confirm:.2f}ms | pos={pos_sau}")
            else:
                print(f"  Vong {i+1}: click={t_click:.2f}ms | TIMEOUT (pos={pos_sau})")
        else:
            print(f"  Vong {i+1}: CLICK THAT BAI")
            gui_results_close.append({"round": i + 1, "t_click": t_click, "t_position_confirm": -1})

        time.sleep(1.0)

# ==========================================
# TONG KET
# ==========================================
header("TONG KET BENCHMARK")

def avg(lst, key):
    vals = [x[key] for x in lst if x[key] >= 0]
    return sum(vals) / len(vals) if vals else -1

print(f"\n  {'Phuong phap':<30s} | {'T_local (ms)':>14s} | {'T_confirm (ms)':>15s}")
print(f"  {'-'*30}-+-{'-'*14}-+-{'-'*15}")

# API open
api_open_local = avg(api_results_open, "t_api_call")
api_open_confirm = avg(api_results_open, "t_position_confirm")
print(f"  {'API order_send() MO':<30s} | {api_open_local:>14.2f} | {api_open_confirm:>15.2f}")

# API close
api_close_local = avg(api_results_close, "t_api_call")
api_close_confirm = avg(api_results_close, "t_position_confirm")
print(f"  {'API order_send() DONG':<30s} | {api_close_local:>14.2f} | {api_close_confirm:>15.2f}")

if trader:
    # GUI open
    gui_open_local = avg(gui_results_open, "t_click")
    gui_open_confirm = avg(gui_results_open, "t_position_confirm")
    print(f"  {'DOM GUI click MO':<30s} | {gui_open_local:>14.2f} | {gui_open_confirm:>15.2f}")

    # GUI close
    gui_close_local = avg(gui_results_close, "t_click")
    gui_close_confirm = avg(gui_results_close, "t_position_confirm")
    print(f"  {'DOM GUI click DONG':<30s} | {gui_close_local:>14.2f} | {gui_close_confirm:>15.2f}")

    print(f"\n  GHI CHU:")
    print(f"    T_local  = Thoi gian thuc thi lenh cuc bo (API call / GUI click)")
    print(f"    T_confirm = Thoi gian tu SAU khi local xong den khi position xuat hien/mat")
    print(f"               (roundtrip toi server san)")
    print(f"\n  LUU Y: Dang test tren may nha, VPS se nhanh hon do gan server san hon.")

print("\n" + "=" * 75)

mt5.shutdown()
