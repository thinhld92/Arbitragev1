import ctypes
import ctypes.wintypes

user32 = ctypes.windll.user32
EnumWindowsProc = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.wintypes.HWND, ctypes.wintypes.LPARAM)

titles = []

def cb(hwnd, lParam):
    buf = ctypes.create_unicode_buffer(256)
    user32.GetWindowTextW(hwnd, buf, 256)
    cbuf = ctypes.create_unicode_buffer(256)
    user32.GetClassNameW(hwnd, cbuf, 256)
    if user32.IsWindowVisible(hwnd) and 'MiniFrame' in cbuf.value:
        titles.append(buf.value)
    return True

user32.EnumWindows(EnumWindowsProc(cb), 0)
print("DOM windows:")
for t in titles:
    print("-", t)
