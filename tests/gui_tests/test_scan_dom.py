"""
Scan chi tiet cua so DOM da tim thay (HWND = 0x00101412)
"""
import ctypes
import ctypes.wintypes
import os

os.environ["PYTHONIOENCODING"] = "utf-8"

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

def is_visible(hwnd):
    return bool(user32.IsWindowVisible(hwnd))

def get_window_rect(hwnd):
    rect = ctypes.wintypes.RECT()
    user32.GetWindowRect(hwnd, ctypes.byref(rect))
    return (rect.left, rect.top, rect.right, rect.bottom)

# Tim DOM tren CA HAI san
print("=" * 90)
print("TIM TAT CA CUA SO DOM (MiniFrame)")
print("=" * 90)

dom_windows = []
def enum_cb(hwnd, lParam):
    if is_visible(hwnd):
        title = get_window_text(hwnd)
        cls = get_class_name(hwnd)
        if title and ("MiniFrame" in cls or "gold" in title.lower() or "xauusd" in title.lower()):
            # Loai bo cua so chinh MT5
            if "Demo Account" not in title and "Hedge" not in title:
                rect = get_window_rect(hwnd)
                dom_windows.append({"hwnd": hwnd, "title": title, "class": cls, "rect": rect})
    return True
user32.EnumWindows(EnumWindowsProc(enum_cb), 0)

if not dom_windows:
    print("Khong tim thay DOM nao! Dai ca mo DOM tren HFM va/hoac Tickmill roi chay lai.")
    
    # Fallback: tim tat ca MiniFrame
    def enum_mini(hwnd, lParam):
        if is_visible(hwnd):
            cls = get_class_name(hwnd)
            if "MiniFrame" in cls:
                title = get_window_text(hwnd)
                rect = get_window_rect(hwnd)
                dom_windows.append({"hwnd": hwnd, "title": title, "class": cls, "rect": rect})
        return True
    user32.EnumWindows(EnumWindowsProc(enum_mini), 0)
    
    if dom_windows:
        print(f"Tim thay {len(dom_windows)} MiniFrame windows:")
    else:
        print("Khong co MiniFrame nao. Thoat.")
        exit(1)

for d in dom_windows:
    print(f"\n  HWND: 0x{d['hwnd']:08X}")
    print(f"  Title: '{d['title']}'")
    print(f"  Class: {d['class'][:60]}")
    print(f"  Rect: {d['rect']}")

# ============================================
# SCAN CHI TIET TUNG DOM
# ============================================
for dom in dom_windows:
    dom_hwnd = dom['hwnd']
    print("\n" + "=" * 90)
    print(f"SCAN CHI TIET: '{dom['title']}' (0x{dom_hwnd:08X})")
    print("=" * 90)
    
    def scan_recursive(parent_hwnd, depth=0, max_depth=5):
        children = []
        def enum_child(hwnd, lParam):
            children.append(hwnd)
            return True
        user32.EnumChildWindows(parent_hwnd, EnumWindowsProc(enum_child), 0)
        
        direct = [h for h in children if user32.GetParent(h) == parent_hwnd]
        
        for hwnd in direct:
            title = get_window_text(hwnd)
            cls = get_class_name(hwnd)
            rect = get_window_rect(hwnd)
            visible = is_visible(hwnd)
            ctrl_id = user32.GetDlgCtrlID(hwnd)
            style = user32.GetWindowLongW(hwnd, -16)
            w = rect[2] - rect[0]
            h = rect[3] - rect[1]
            
            indent = "  " + "  " * depth
            vis = "V" if visible else "H"
            title_disp = title[:35] if title else ""
            
            # Highlight cac control dac biet
            highlight = ""
            cls_lower = cls.lower()
            title_lower = title.lower()
            if "button" in cls_lower:
                highlight = " <<<< BUTTON!"
            if any(kw in title_lower for kw in ["sell", "buy", "close"]):
                highlight = " <<<< SELL/BUY/CLOSE!!!"
            if "edit" in cls_lower:
                highlight = " <<<< EDIT (volume?)"
            if "updown" in cls_lower or "spin" in cls_lower:
                highlight = " <<<< SPINNER"
            
            print(f"{indent}[{vis}] 0x{hwnd:08X} [{cls[:40]:<40s}] "
                  f"{w:4d}x{h:3d} ID={ctrl_id:5d} Style=0x{style:08X} "
                  f"'{title_disp}'{highlight}")
            
            if depth < max_depth:
                scan_recursive(hwnd, depth + 1, max_depth)
    
    scan_recursive(dom_hwnd)

print("\n" + "=" * 90)
print("HOAN TAT! Kiem tra xem co tim thay BUTTON, EDIT controls khong.")
print("=" * 90)
