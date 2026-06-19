"""
TEST CAC PHUONG PHAP CLICK KHAC NHAU tren DOM HFM
Tim ra cach nao THUC SU tao duoc order.

Phuong phap:
  A. BM_CLICK (SendMessage)
  B. WM_LBUTTONDOWN + WM_LBUTTONUP (SendMessage) 
  C. WM_LBUTTONDOWN + WM_LBUTTONUP (PostMessage)
  D. SetFocus + BM_CLICK
  E. SetForegroundWindow + WM_LBUTTONDOWN/UP

KHONG dung mt5.initialize() de tranh conflict.
Kiem tra ket qua bang mat thuong tren MT5.
"""
import sys
import os
import time
import ctypes
import ctypes.wintypes

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
os.environ["PYTHONIOENCODING"] = "utf-8"

from utils.gui_trader import (
    DomTrader, tim_dom_window, resize_dom, _tim_tat_ca_controls,
    _get_edit_text, _set_edit_text, CTRL_ID_BUY, CTRL_ID_SELL, CTRL_ID_CLOSE
)

user32 = ctypes.windll.user32
EnumWindowsProc = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.wintypes.HWND, ctypes.wintypes.LPARAM)

# Win32 constants
WM_LBUTTONDOWN = 0x0201
WM_LBUTTONUP = 0x0202
WM_COMMAND = 0x0111
BM_CLICK = 0x00F5
BN_CLICKED = 0
MK_LBUTTON = 0x0001
SW_RESTORE = 9
SW_SHOW = 5

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
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70)

def MAKELPARAM(lo, hi):
    return (hi << 16) | (lo & 0xFFFF)

def MAKEWPARAM(lo, hi):
    return (hi << 16) | (lo & 0xFFFF)

# ==========================================
# TIM DOM HFM
# ==========================================
header("TIM DOM HFM")

dom_hwnd = None
def enum_dom(hwnd, lParam):
    global dom_hwnd
    if user32.IsWindowVisible(hwnd):
        cls = get_class_name(hwnd)
        if "MiniFrame" in cls and "Afx:" in cls:
            title = get_window_text(hwnd)
            if "Gold Spot" in title:
                dom_hwnd = hwnd
    return True
user32.EnumWindows(EnumWindowsProc(enum_dom), 0)

if not dom_hwnd:
    print("  KHONG TIM THAY DOM HFM!")
    sys.exit(1)

resize_dom(dom_hwnd, 520, 500)
time.sleep(0.3)
controls = _tim_tat_ca_controls(dom_hwnd)
if not controls:
    print("  KHONG TIM DU CONTROLS!")
    sys.exit(1)

btn_buy = controls["buy"]
btn_sell = controls["sell"]
btn_close = controls["close"]
edt_vol = controls["volume"]

print(f"  DOM HFM:  0x{dom_hwnd:08X}")
print(f"  Buy:      0x{btn_buy:08X}")
print(f"  Sell:     0x{btn_sell:08X}")
print(f"  Close:    0x{btn_close:08X}")
print(f"  Volume:   0x{edt_vol:08X}")

# Check button styles
for name, hwnd in [("Buy", btn_buy), ("Sell", btn_sell), ("Close", btn_close)]:
    style = user32.GetWindowLongW(hwnd, -16)
    enabled = user32.IsWindowEnabled(hwnd)
    visible = user32.IsWindowVisible(hwnd)
    # Tim parent
    parent = user32.GetParent(hwnd)
    parent_cls = get_class_name(parent)
    print(f"  {name}: Style=0x{style:08X} Enabled={enabled} Visible={visible} Parent=[{parent_cls[:30]}]")

# Set volume
_set_edit_text(edt_vol, "0.01")
time.sleep(0.1)
print(f"  Volume set: '{_get_edit_text(edt_vol)}'")

# ==========================================
# HELPER: Lay rect cua button (relative to parent)
# ==========================================
def get_button_center_in_parent(btn_hwnd):
    """Tra ve toa do trung tam cua button RELATIVE TO PARENT."""
    btn_rect = ctypes.wintypes.RECT()
    user32.GetWindowRect(btn_hwnd, ctypes.byref(btn_rect))
    
    parent = user32.GetParent(btn_hwnd)
    parent_rect = ctypes.wintypes.RECT()
    user32.GetWindowRect(parent, ctypes.byref(parent_rect))
    
    # Relative to parent
    cx = (btn_rect.left + btn_rect.right) // 2 - parent_rect.left
    cy = (btn_rect.top + btn_rect.bottom) // 2 - parent_rect.top
    return cx, cy

# ==========================================
# METHOD A: BM_CLICK (SendMessage)
# ==========================================
header("METHOD A: BM_CLICK (SendMessage) -> BUY")
print("  Gui BM_CLICK toi nut Buy...")
t = time.perf_counter()
user32.SendMessageW(btn_buy, BM_CLICK, 0, 0)
ms = (time.perf_counter() - t) * 1000
print(f"  Thoi gian: {ms:.2f} ms")
print("  >>> Kiem tra MT5 HFM xem co lenh moi khong (doi 3s)...")
time.sleep(3)

input("  Nhan ENTER de tiep tuc (ghi lai ket qua: co/khong co lenh moi)...")

# ==========================================
# METHOD B: WM_LBUTTONDOWN + WM_LBUTTONUP (SendMessage to button)
# ==========================================
header("METHOD B: WM_LBUTTONDOWN/UP (SendMessage to button) -> BUY")
cx, cy = get_button_center_in_parent(btn_buy)
# Gui truc tiep toi button, toa do (5,5) - center of button
lParam = MAKELPARAM(5, 5)
print(f"  Gui WM_LBUTTONDOWN + UP toi nut Buy tai (5,5)...")
t = time.perf_counter()
user32.SendMessageW(btn_buy, WM_LBUTTONDOWN, MK_LBUTTON, lParam)
time.sleep(0.01)
user32.SendMessageW(btn_buy, WM_LBUTTONUP, 0, lParam)
ms = (time.perf_counter() - t) * 1000
print(f"  Thoi gian: {ms:.2f} ms")
print("  >>> Kiem tra MT5 HFM (doi 3s)...")
time.sleep(3)

input("  Nhan ENTER de tiep tuc...")

# ==========================================
# METHOD C: WM_LBUTTONDOWN/UP (SendMessage to PARENT at button coords)
# ==========================================
header("METHOD C: WM_LBUTTONDOWN/UP (SendMessage to PARENT) -> BUY")
parent = user32.GetParent(btn_buy)
cx, cy = get_button_center_in_parent(btn_buy)
lParam = MAKELPARAM(cx, cy)
print(f"  Gui WM_LBUTTONDOWN + UP toi PARENT tai ({cx},{cy})...")
t = time.perf_counter()
user32.SendMessageW(parent, WM_LBUTTONDOWN, MK_LBUTTON, lParam)
time.sleep(0.01)
user32.SendMessageW(parent, WM_LBUTTONUP, 0, lParam)
ms = (time.perf_counter() - t) * 1000
print(f"  Thoi gian: {ms:.2f} ms")
print("  >>> Kiem tra MT5 HFM (doi 3s)...")
time.sleep(3)

input("  Nhan ENTER de tiep tuc...")

# ==========================================
# METHOD D: SetFocus + BM_CLICK
# ==========================================
header("METHOD D: SetForeground + SetFocus + BM_CLICK -> BUY")
print("  SetForegroundWindow + SetFocus + BM_CLICK...")
user32.SetForegroundWindow(dom_hwnd)
time.sleep(0.05)
user32.SetFocus(btn_buy)
time.sleep(0.05)
t = time.perf_counter()
user32.SendMessageW(btn_buy, BM_CLICK, 0, 0)
ms = (time.perf_counter() - t) * 1000
print(f"  Thoi gian: {ms:.2f} ms")
print("  >>> Kiem tra MT5 HFM (doi 3s)...")
time.sleep(3)

input("  Nhan ENTER de tiep tuc...")

# ==========================================
# METHOD E: WM_COMMAND (gui thong bao BN_CLICKED toi parent dialog)
# ==========================================
header("METHOD E: WM_COMMAND BN_CLICKED -> BUY")
parent_dialog = user32.GetParent(btn_buy)
ctrl_id = user32.GetDlgCtrlID(btn_buy)
wParam = MAKEWPARAM(ctrl_id, BN_CLICKED)
lParam = btn_buy  # LPARAM = handle of control
print(f"  Gui WM_COMMAND(ctrl_id={ctrl_id}, BN_CLICKED) toi parent 0x{parent_dialog:08X}...")
t = time.perf_counter()
user32.SendMessageW(parent_dialog, WM_COMMAND, wParam, lParam)
ms = (time.perf_counter() - t) * 1000
print(f"  Thoi gian: {ms:.2f} ms")
print("  >>> Kiem tra MT5 HFM (doi 3s)...")
time.sleep(3)

input("  Nhan ENTER de tiep tuc...")

# ==========================================
# METHOD F: PostMessage WM_LBUTTONDOWN/UP to button
# ==========================================
header("METHOD F: PostMessage WM_LBUTTONDOWN/UP -> BUY")
lParam = MAKELPARAM(5, 5)
print("  PostMessage WM_LBUTTONDOWN + UP toi nut Buy...")
t = time.perf_counter()
user32.PostMessageW(btn_buy, WM_LBUTTONDOWN, MK_LBUTTON, lParam)
time.sleep(0.01)
user32.PostMessageW(btn_buy, WM_LBUTTONUP, 0, lParam)
ms = (time.perf_counter() - t) * 1000
print(f"  Thoi gian: {ms:.2f} ms")
print("  >>> Kiem tra MT5 HFM (doi 3s)...")
time.sleep(3)

input("  Nhan ENTER de ket thuc...")

header("HOAN TAT - GHI LAI METHOD NAO HOAT DONG!")
