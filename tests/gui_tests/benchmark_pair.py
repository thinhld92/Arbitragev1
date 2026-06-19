"""
BENCHMARK THEO CAP (TICKMILL & HFM) - 10 VONG
"""
import sys
import os
import time
import threading
import json
import ctypes
import ctypes.wintypes

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "src"))

import MetaTrader5 as mt5
from utils.gui_trader import DomTrader, _tim_tat_ca_controls, resize_dom

def header(text):
    print("\n" + "=" * 75)
    print(f"  {text}")
    print("=" * 75)

user32 = ctypes.windll.user32
EnumWindowsProc = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.wintypes.HWND, ctypes.wintypes.LPARAM)

def get_window_text(hwnd):
    length = user32.GetWindowTextLengthW(hwnd)
    if length == 0: return ""
    buf = ctypes.create_unicode_buffer(length + 1)
    user32.GetWindowTextW(hwnd, buf, length + 1)
    return buf.value

def get_class_name(hwnd):
    buf = ctypes.create_unicode_buffer(256)
    user32.GetClassNameW(hwnd, buf, 256)
    return buf.value

SYMBOL = "XAUUSD"
VOLUME = 0.01
NUM_ROUNDS = 10

with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

PATH_TICKMILL = config["brokers"]["TICKMILL"]["path"]
PATH_HFM = config["brokers"]["HFM"]["path"]

header(f"KHOI TAO MT5 & DOM CHO CAP TICKMILL - HFM ({NUM_ROUNDS} VONG)")

print(f"  [TICKMILL] Dang ket noi MT5...")
if not mt5.initialize(path=PATH_TICKMILL, portable=True, timeout=60000):
    print(f"  [TICKMILL] LOI: {mt5.last_error()}")
    sys.exit(1)

mt5.symbol_select(SYMBOL, True)
acc = mt5.account_info()
print(f"  [TICKMILL] Account: {acc.login} | Balance: {acc.balance}")

dom_list_tm = []
def enum_dom_tm(hwnd, lParam):
    if user32.IsWindowVisible(hwnd):
        cls = get_class_name(hwnd)
        if "MiniFrame" in cls and "Afx:" in cls:
            title = get_window_text(hwnd)
            if "Gold" in title or "XAU" in title:
                if len(dom_list_tm) == 0:
                    dom_list_tm.append(hwnd)
    return True
user32.EnumWindows(EnumWindowsProc(enum_dom_tm), 0)

trader_tm = None
if dom_list_tm:
    dom_hwnd = dom_list_tm[0]
    resize_dom(dom_hwnd, 520, 500)
    time.sleep(0.2)
    trader_tm = DomTrader(SYMBOL, bot_name="[TICKMILL]")
    trader_tm.dom_hwnd = dom_hwnd
    trader_tm.controls = _tim_tat_ca_controls(dom_hwnd)
    if trader_tm.controls:
        trader_tm._da_khoi_tao = True
        print(f"  [TICKMILL] DOM OK (HWND=0x{dom_hwnd:08X})")
else:
    print("  [TICKMILL] DOM KHONG TIM THAY! Benchmark se loi.")



print(f"\n  [HFM] Tim DOM")
dom_list_hfm = []
def enum_dom_hfm(hwnd, lParam):
    if user32.IsWindowVisible(hwnd):
        cls = get_class_name(hwnd)
        if "MiniFrame" in cls and "Afx:" in cls:
            title = get_window_text(hwnd)
            if "Gold" in title or "XAU" in title:
                if hwnd not in dom_list_tm and len(dom_list_hfm) == 0:
                    dom_list_hfm.append(hwnd)
    return True
user32.EnumWindows(EnumWindowsProc(enum_dom_hfm), 0)

trader_hfm = None
if dom_list_hfm:
    dom_hwnd = dom_list_hfm[0]
    resize_dom(dom_hwnd, 520, 500)
    time.sleep(0.2)
    trader_hfm = DomTrader(SYMBOL, bot_name="[HFM]")
    trader_hfm.dom_hwnd = dom_hwnd
    trader_hfm.controls = _tim_tat_ca_controls(dom_hwnd)
    if trader_hfm.controls:
        trader_hfm._da_khoi_tao = True
        print(f"  [HFM] DOM OK (HWND=0x{dom_hwnd:08X})")
else:
    print("  [HFM] DOM KHONG TIM THAY!")

if not trader_tm or not trader_hfm:
    print("\n  CAN MO CA 2 CUA SO DOM TREN TICKMILL VA HFM DE TEST!")
    sys.exit(1)

def thuc_thi_dom(trader, action, results, index):
    t_start = time.perf_counter()
    if action == "BUY":
        trader.buy()
    else:
        trader.close_position()
    t_click = (time.perf_counter() - t_start) * 1000
    results[index] = t_click

header(f"BAT DAU BENCHMARK 10 VONG (CLICK DONG THOI 2 DOM)")
print(f"  Vui long theo doi Terminal MT5 de xem lenh vao/ra.")

for i in range(NUM_ROUNDS):
    print(f"\n  --- VONG {i+1} ---")
    
    results_open = [0, 0]
    t1 = threading.Thread(target=thuc_thi_dom, args=(trader_tm, "BUY", results_open, 0))
    t2 = threading.Thread(target=thuc_thi_dom, args=(trader_hfm, "BUY", results_open, 1))
    
    t1.start(); t2.start()
    t1.join(); t2.join()
    
    print(f"  [BUY] TICKMILL click: {results_open[0]:.2f}ms | HFM click: {results_open[1]:.2f}ms")
    
    time.sleep(3.0) 
    
    results_close = [0, 0]
    t1 = threading.Thread(target=thuc_thi_dom, args=(trader_tm, "CLOSE", results_close, 0))
    t2 = threading.Thread(target=thuc_thi_dom, args=(trader_hfm, "CLOSE", results_close, 1))
    
    t1.start(); t2.start()
    t1.join(); t2.join()
    
    print(f"  [CLOSE] TICKMILL click: {results_close[0]:.2f}ms | HFM click: {results_close[1]:.2f}ms")
    time.sleep(2.0)

print("\n  DA HOAN THANH 10 VONG TEST DOM DONG THOI!")
mt5.shutdown()
