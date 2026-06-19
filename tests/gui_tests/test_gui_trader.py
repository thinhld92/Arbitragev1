"""
TEST TOAN DIEN GUI TRADER - 2 SAN CUNG LUC
Phan biet san bang title DOM: "Gold Spot" = HFM, "Gold vs US Dollar" = Tickmill
"""
import sys
import os
import time
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
os.environ["PYTHONIOENCODING"] = "utf-8"

import ctypes
import ctypes.wintypes
from utils.gui_trader import DomTrader, resize_dom, _tim_tat_ca_controls

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

def sep():
    print("-" * 70)

def header(text):
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70)

# ==========================================
# TIM VA KHOI TAO 2 DOM
# ==========================================
header("KHOI TAO DOM TRADER CHO 2 SAN")

dom_list = []
def enum_dom(hwnd, lParam):
    if user32.IsWindowVisible(hwnd):
        cls = get_class_name(hwnd)
        # Chi tim MiniFrame (DOM cua MT5), loai bo browser/app khac
        if "MiniFrame" in cls and "Afx:" in cls:
            title = get_window_text(hwnd)
            if "XAUUSD" in title.upper() or "GOLD" in title.upper():
                # Phan biet bang title: HFM dung "Gold Spot", Tickmill dung "Gold vs US Dollar"
                if "Gold Spot" in title:
                    broker = "HFM"
                elif "Gold vs US Dollar" in title:
                    broker = "TICKMILL"
                else:
                    broker = "UNKNOWN"
                dom_list.append({"hwnd": hwnd, "title": title, "broker": broker})
    return True
user32.EnumWindows(EnumWindowsProc(enum_dom), 0)

print(f"\nTim thay {len(dom_list)} DOM windows:")
for d in dom_list:
    print(f"  [{d['broker']}] '{d['title']}' (HWND=0x{d['hwnd']:08X})")

traders = {}
for d in dom_list:
    t = DomTrader("XAUUSD", bot_name=f"[{d['broker']}]")
    t.dom_hwnd = d["hwnd"]
    resize_dom(d["hwnd"])
    time.sleep(0.2)
    t.controls = _tim_tat_ca_controls(d["hwnd"])
    if t.controls:
        t._da_khoi_tao = True
        traders[d["broker"]] = t
        print(f"  [{d['broker']}] OK! Buy=0x{t.controls['buy']:08X} Sell=0x{t.controls['sell']:08X} Close=0x{t.controls['close']:08X}")
    else:
        print(f"  [{d['broker']}] LOI: Thieu controls!")

hfm = traders.get("HFM")
tick = traders.get("TICKMILL")

if not hfm or not tick:
    print(f"\nCHI TIM THAY: {list(traders.keys())}. Can ca HFM + TICKMILL!")
    print("Mo DOM tren ca 2 san roi chay lai.")
    sys.exit(1)

# ==========================================
# TEST 1: DAT VOLUME
# ==========================================
header("TEST 1: DAT VOLUME 0.01 CHO CA 2 BEN")
for name, t in traders.items():
    t.dat_volume("0.01")
    print(f"  [{name}] Volume = '{t.lay_volume_hien_tai()}'")
print("  OK!")

# ==========================================
# TEST 2: MO LENH TH2 (BUY HFM + SELL TICKMILL)
# ==========================================
header("TEST 2: MO LENH TH2 (BUY HFM + SELL TICKMILL)")

results = {}

def _run(name, fn):
    start = time.perf_counter()
    ok = fn()
    ms = (time.perf_counter() - start) * 1000
    results[name] = {"ok": ok, "ms": ms}

t1 = threading.Thread(target=_run, args=("HFM_BUY", hfm.buy))
t2 = threading.Thread(target=_run, args=("TICK_SELL", tick.sell))

overall = time.perf_counter()
t1.start(); t2.start()
t1.join(); t2.join()
overall_ms = (time.perf_counter() - overall) * 1000

for k, v in results.items():
    print(f"  {k}: {'OK' if v['ok'] else 'FAIL'} ({v['ms']:.1f} ms)")
print(f"  Tong (song song): {overall_ms:.1f} ms")

# ==========================================
# TEST 3: DONG TH2
# ==========================================
header("TEST 3: DONG LENH TH2 (sau 3 giay)")
print("  Cho 3s...")
time.sleep(3)

results.clear()
t3 = threading.Thread(target=_run, args=("HFM_CLOSE", hfm.close_position))
t4 = threading.Thread(target=_run, args=("TICK_CLOSE", tick.close_position))

overall = time.perf_counter()
t3.start(); t4.start()
t3.join(); t4.join()
overall_ms2 = (time.perf_counter() - overall) * 1000

for k, v in results.items():
    print(f"  {k}: {'OK' if v['ok'] else 'FAIL'} ({v['ms']:.1f} ms)")
print(f"  Tong (song song): {overall_ms2:.1f} ms")

# ==========================================
# TEST 4: MO LENH TH1 (SELL HFM + BUY TICKMILL)
# ==========================================
header("TEST 4: MO LENH TH1 (SELL HFM + BUY TICKMILL)")

results.clear()
t5 = threading.Thread(target=_run, args=("HFM_SELL", hfm.sell))
t6 = threading.Thread(target=_run, args=("TICK_BUY", tick.buy))

overall = time.perf_counter()
t5.start(); t6.start()
t5.join(); t6.join()
overall_ms3 = (time.perf_counter() - overall) * 1000

for k, v in results.items():
    print(f"  {k}: {'OK' if v['ok'] else 'FAIL'} ({v['ms']:.1f} ms)")
print(f"  Tong (song song): {overall_ms3:.1f} ms")

# ==========================================
# TEST 5: DONG TH1
# ==========================================
header("TEST 5: DONG LENH TH1 (sau 3 giay)")
print("  Cho 3s...")
time.sleep(3)

results.clear()
t7 = threading.Thread(target=_run, args=("HFM_CLOSE", hfm.close_position))
t8 = threading.Thread(target=_run, args=("TICK_CLOSE", tick.close_position))

overall = time.perf_counter()
t7.start(); t8.start()
t7.join(); t8.join()
overall_ms4 = (time.perf_counter() - overall) * 1000

for k, v in results.items():
    print(f"  {k}: {'OK' if v['ok'] else 'FAIL'} ({v['ms']:.1f} ms)")
print(f"  Tong (song song): {overall_ms4:.1f} ms")

# ==========================================
# TONG KET
# ==========================================
header("TONG KET TOC DO")
print(f"  Mo TH2  (BUY HFM + SELL TICK):  {overall_ms:.1f} ms")
print(f"  Dong TH2:                        {overall_ms2:.1f} ms")
print(f"  Mo TH1  (SELL HFM + BUY TICK):   {overall_ms3:.1f} ms")
print(f"  Dong TH1:                         {overall_ms4:.1f} ms")
print(f"\n  >>> Kiem tra MT5 HFM + Tickmill xem 4 lenh da vao/dong chua! <<<")
print("=" * 70)
