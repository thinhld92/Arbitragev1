"""
Script quét cửa sổ MT5 (HFM) để tìm tất cả child windows / controls.
Mục đích: Xem MT5 có expose handle cho nút BUY/SELL One-Click không.
"""
import ctypes
import ctypes.wintypes
import sys

# ============================================
# 1. TÌM CỬA SỔ MT5 (HFM)
# ============================================
user32 = ctypes.windll.user32
kernel32 = ctypes.windll.kernel32

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

def get_window_rect(hwnd):
    rect = ctypes.wintypes.RECT()
    user32.GetWindowRect(hwnd, ctypes.byref(rect))
    return (rect.left, rect.top, rect.right, rect.bottom)

def is_visible(hwnd):
    return bool(user32.IsWindowVisible(hwnd))

def is_enabled(hwnd):
    return bool(user32.IsWindowEnabled(hwnd))

# Tìm tất cả cửa sổ top-level có chứa "HFMarkets" hoặc "HF Markets"
print("=" * 80)
print("🔍 BƯỚC 1: TÌM CỬA SỔ MT5 (HFM)")
print("=" * 80)

mt5_windows = []

def enum_windows_callback(hwnd, lParam):
    title = get_window_text(hwnd)
    if title and ("HFMarkets" in title or "HF Markets" in title or "Neotech" in title):
        cls = get_class_name(hwnd)
        rect = get_window_rect(hwnd)
        visible = is_visible(hwnd)
        mt5_windows.append({
            "hwnd": hwnd,
            "title": title,
            "class": cls,
            "rect": rect,
            "visible": visible
        })
    return True

user32.EnumWindows(EnumWindowsProc(enum_windows_callback), 0)

if not mt5_windows:
    print("❌ Không tìm thấy cửa sổ MT5 HFM nào!")
    print("   Thử tìm tất cả cửa sổ có chứa 'terminal' hoặc 'MT5'...")
    
    def enum_all_callback(hwnd, lParam):
        title = get_window_text(hwnd)
        if title and is_visible(hwnd) and ("terminal" in title.lower() or "mt5" in title.lower() or "metatrader" in title.lower()):
            cls = get_class_name(hwnd)
            mt5_windows.append({
                "hwnd": hwnd,
                "title": title,
                "class": cls,
                "rect": get_window_rect(hwnd),
                "visible": True
            })
        return True
    
    user32.EnumWindows(EnumWindowsProc(enum_all_callback), 0)

for w in mt5_windows:
    print(f"\n  📌 HWND: {w['hwnd']} (0x{w['hwnd']:08X})")
    print(f"     Title: {w['title'][:100]}")
    print(f"     Class: {w['class']}")
    print(f"     Rect: {w['rect']}")
    print(f"     Visible: {w['visible']}")

if not mt5_windows:
    print("❌ Vẫn không tìm thấy. Thoát.")
    sys.exit(1)

# ============================================
# 2. QUÉT TẤT CẢ CHILD WINDOWS
# ============================================
print("\n" + "=" * 80)
print("🔍 BƯỚC 2: QUÉT TẤT CẢ CHILD WINDOWS CỦA MT5")
print("=" * 80)

# Lấy cửa sổ chính (visible, có title dài nhất)
main_window = max(mt5_windows, key=lambda w: len(w['title']))
main_hwnd = main_window['hwnd']
print(f"\n🎯 Cửa sổ chính: HWND={main_hwnd} (0x{main_hwnd:08X})")
print(f"   Title: {main_window['title'][:100]}")

child_windows = []

def enum_child_callback(hwnd, lParam):
    title = get_window_text(hwnd)
    cls = get_class_name(hwnd)
    rect = get_window_rect(hwnd)
    visible = is_visible(hwnd)
    enabled = is_enabled(hwnd)
    
    # Lấy style
    style = user32.GetWindowLongW(hwnd, -16)  # GWL_STYLE
    ex_style = user32.GetWindowLongW(hwnd, -20)  # GWL_EXSTYLE
    
    # Control ID
    ctrl_id = user32.GetDlgCtrlID(hwnd)
    
    child_windows.append({
        "hwnd": hwnd,
        "title": title,
        "class": cls,
        "rect": rect,
        "visible": visible,
        "enabled": enabled,
        "style": style,
        "ex_style": ex_style,
        "ctrl_id": ctrl_id,
        "width": rect[2] - rect[0],
        "height": rect[3] - rect[1],
    })
    return True

user32.EnumChildWindows(main_hwnd, EnumWindowsProc(enum_child_callback), 0)

print(f"\n📊 Tổng số child windows: {len(child_windows)}")

# Phân loại theo class name
class_groups = {}
for cw in child_windows:
    cls = cw['class']
    if cls not in class_groups:
        class_groups[cls] = []
    class_groups[cls].append(cw)

print("\n📋 PHÂN LOẠI THEO CLASS NAME:")
print("-" * 60)
for cls, items in sorted(class_groups.items(), key=lambda x: -len(x[1])):
    print(f"  [{cls}] → {len(items)} controls")
    # Hiển thị chi tiết 5 controls đầu tiên
    for i, item in enumerate(items[:5]):
        title_display = item['title'][:50] if item['title'] else "(no title)"
        vis = "👁️" if item['visible'] else "🚫"
        print(f"    {vis} HWND=0x{item['hwnd']:08X} | ID={item['ctrl_id']:5d} | "
              f"Size={item['width']}x{item['height']} | Title: {title_display}")
    if len(items) > 5:
        print(f"    ... và {len(items) - 5} controls khác")

# ============================================
# 3. TÌM KIẾM CỤ THỂ CÁC NÚT BUY/SELL/CLOSE
# ============================================
print("\n" + "=" * 80)
print("🔍 BƯỚC 3: TÌM NÚT BUY / SELL / CLOSE / ONE-CLICK")
print("=" * 80)

keywords = ["buy", "sell", "close", "order", "trade", "one", "click", "deal"]
found_buttons = []

for cw in child_windows:
    title_lower = cw['title'].lower()
    cls_lower = cw['class'].lower()
    
    # Tìm theo title
    if any(kw in title_lower for kw in keywords):
        found_buttons.append(cw)
    # Tìm Button controls
    elif "button" in cls_lower:
        found_buttons.append(cw)

if found_buttons:
    print(f"\n✅ Tìm thấy {len(found_buttons)} controls liên quan:")
    for btn in found_buttons:
        vis = "👁️" if btn['visible'] else "🚫"
        ena = "✅" if btn['enabled'] else "❌"
        print(f"  {vis}{ena} HWND=0x{btn['hwnd']:08X} | Class=[{btn['class']}] | "
              f"ID={btn['ctrl_id']} | Size={btn['width']}x{btn['height']} | "
              f"Style=0x{btn['style']:08X} | Title: '{btn['title']}'")
else:
    print("\n⚠️ Không tìm thấy nút BUY/SELL/Button nào!")
    print("   → MT5 có thể dùng custom rendered UI (DirectX/GDI)")

# ============================================
# 4. QUÉT SÂU HƠN - TÌM TẤT CẢ VISIBLE CONTROLS CÓ KÍCH THƯỚC HỢP LÝ
# ============================================
print("\n" + "=" * 80)
print("🔍 BƯỚC 4: TẤT CẢ VISIBLE CONTROLS (kích thước > 20x10)")
print("=" * 80)

visible_controls = [
    cw for cw in child_windows 
    if cw['visible'] and cw['width'] > 20 and cw['height'] > 10
]

print(f"\n📊 Có {len(visible_controls)} visible controls có kích thước > 20x10:")
for vc in sorted(visible_controls, key=lambda x: (x['rect'][1], x['rect'][0])):
    title_display = vc['title'][:40] if vc['title'] else "(no title)"
    print(f"  HWND=0x{vc['hwnd']:08X} | Class=[{vc['class']:<30s}] | "
          f"Pos=({vc['rect'][0]:4d},{vc['rect'][1]:4d}) | "
          f"Size={vc['width']:4d}x{vc['height']:4d} | "
          f"ID={vc['ctrl_id']:5d} | '{title_display}'")

# ============================================
# 5. THỬ TÌM BẰNG CÁCH ĐỆ QUY SÂU HƠN
# ============================================
print("\n" + "=" * 80)
print("🔍 BƯỚC 5: QUÉT ĐỆ QUY SÂU (tìm nested child)")
print("=" * 80)

def scan_recursive(parent_hwnd, depth=0, max_depth=5):
    results = []
    if depth > max_depth:
        return results
    
    children = []
    def enum_cb(hwnd, lParam):
        children.append(hwnd)
        return True
    
    user32.EnumChildWindows(parent_hwnd, EnumWindowsProc(enum_cb), 0)
    
    # Chỉ lấy direct children (loại bỏ nested)
    direct_children = []
    for hwnd in children:
        parent = user32.GetParent(hwnd)
        if parent == parent_hwnd:
            direct_children.append(hwnd)
    
    for hwnd in direct_children:
        title = get_window_text(hwnd)
        cls = get_class_name(hwnd)
        rect = get_window_rect(hwnd)
        visible = is_visible(hwnd)
        w = rect[2] - rect[0]
        h = rect[3] - rect[1]
        
        indent = "  " * (depth + 1)
        vis = "👁️" if visible else "🚫"
        title_display = title[:35] if title else ""
        
        # Chỉ hiện nếu visible hoặc depth <= 2
        if visible or depth <= 1:
            print(f"{indent}{vis} [{cls}] Size={w}x{h} ID={user32.GetDlgCtrlID(hwnd)} '{title_display}'")
        
        results.append({"hwnd": hwnd, "class": cls, "title": title, "visible": visible})
        
        # Đệ quy vào children
        sub = scan_recursive(hwnd, depth + 1, max_depth)
        results.extend(sub)
    
    return results

all_nested = scan_recursive(main_hwnd, depth=0, max_depth=3)
print(f"\n📊 Tổng nested controls: {len(all_nested)}")

print("\n" + "=" * 80)
print("✅ QUÉT HOÀN TẤT!")
print("=" * 80)
