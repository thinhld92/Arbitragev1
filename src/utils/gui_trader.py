"""
gui_trader.py — Module gia lap bam tay qua Depth of Market (DOM) tren MT5.

Su dung Win32 API de tuong tac truc tiep voi cac control Windows native
(Button, Edit) cua cua so DOM, thay vi dung mt5.order_send().

Control ID chuan MT5 (giong nhau tren moi san):
    - Buy button:    ID = 10408
    - Sell button:   ID = 10409
    - Close button:  ID = 10410
    - Volume edit:   ID = 10190
"""
import ctypes
import ctypes.wintypes
import time
import random
import threading

user32 = ctypes.windll.user32

# ==========================================
# HANG SO WIN32
# ==========================================
WM_LBUTTONDOWN = 0x0201
WM_LBUTTONUP = 0x0202
WM_SETTEXT = 0x000C
WM_GETTEXT = 0x000D
WM_GETTEXTLENGTH = 0x000E
BM_CLICK = 0x00F5
MK_LBUTTON = 0x0001
GWL_STYLE = -16

# Control ID chuan MT5 DOM
CTRL_ID_BUY = 10408
CTRL_ID_SELL = 10409
CTRL_ID_CLOSE = 10410
CTRL_ID_VOLUME = 10190

# Kich thuoc chuan de DOM hien du 3 nut
DOM_WIDTH = 520
DOM_HEIGHT = 500

# Callback type cho EnumWindows / EnumChildWindows
EnumWindowsProc = ctypes.WINFUNCTYPE(
    ctypes.c_bool, ctypes.wintypes.HWND, ctypes.wintypes.LPARAM
)

# Lock de dam bao chi 1 thread thao tac GUI tai 1 thoi diem
_gui_lock = threading.Lock()


def _get_window_text(hwnd):
    length = user32.GetWindowTextLengthW(hwnd)
    if length == 0:
        return ""
    buf = ctypes.create_unicode_buffer(length + 1)
    user32.GetWindowTextW(hwnd, buf, length + 1)
    return buf.value


def _get_class_name(hwnd):
    buf = ctypes.create_unicode_buffer(256)
    user32.GetClassNameW(hwnd, buf, 256)
    return buf.value


def _is_visible(hwnd):
    return bool(user32.IsWindowVisible(hwnd))


def _random_delay(min_ms=5, max_ms=30):
    """Delay ngau nhien nho de giong nguoi that."""
    time.sleep(random.randint(min_ms, max_ms) / 1000.0)


def _get_window_pid(hwnd):
    """Lay PID cua process so huu cua so nay."""
    pid = ctypes.wintypes.DWORD()
    user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
    return pid.value


def _tach_symbol_tu_title(title):
    """
    Tach ten symbol tu tieu de cua so DOM tren MT5.
    
    Cac dang tieu de thuong gap:
      - "XAUUSD, Gold Spot"          -> XAUUSD
      - "XAUUSD, Gold vs US Dollar"  -> XAUUSD
      - "BTCUSD"                     -> BTCUSD
      - "Depth of Market - XAUUSD"   -> XAUUSD  (MT5 ban cu)
      - "DOM XAUUSD"                 -> XAUUSD  (MT5 ban cu)
      
    Returns:
        Ten symbol (UPPER) hoac chuoi rong neu khong tach duoc.
    """
    t = title.strip()
    if not t:
        return ""
    
    t_upper = t.upper()
    
    # Dang 1: "Depth of Market - XAUUSD..."
    for prefix in ["DEPTH OF MARKET - ", "DEPTH OF MARKET -", "DOM "]:
        if t_upper.startswith(prefix):
            t_upper = t_upper[len(prefix):].strip()
            break
    
    # Cat phan mo ta sau dau phay: "XAUUSD, Gold Spot" -> "XAUUSD"
    symbol_part = t_upper.split(',')[0].strip()
    
    # Cat phan mo ta sau khoang trang dau tien (neu con): "XAUUSD Gold" -> "XAUUSD"
    symbol_part = symbol_part.split()[0].strip() if symbol_part else ""
    
    return symbol_part


def tim_dom_window(symbol, mt5_pid=None):
    """
    Tim cua so Depth of Market (DOM) dang mo cho symbol chi dinh.
    
    Args:
        symbol: Ma giao dich (VD: "XAUUSD")
        mt5_pid: (Optional) PID cua tien trinh MT5. Neu truyen vao, chi tim
                 DOM thuoc dung tien trinh nay (chong trung khi chay nhieu san).
        
    Returns:
        HWND cua DOM window, hoac None neu khong tim thay.
    """
    symbol_upper = symbol.upper()
    result = []
    all_miniframes = []  # Luu tat ca MiniFrame de debug

    def enum_cb(hwnd, lParam):
        if _is_visible(hwnd):
            cls = _get_class_name(hwnd)
            if "MiniFrame" in cls:
                title = _get_window_text(hwnd)
                extracted = _tach_symbol_tu_title(title)
                win_pid = _get_window_pid(hwnd)
                all_miniframes.append((hwnd, title, extracted, win_pid))
                
                # Kiem tra symbol khop
                if extracted == symbol_upper:
                    # Neu co PID thi phai khop PID
                    if mt5_pid is None or win_pid == mt5_pid:
                        result.append(hwnd)
        return True

    user32.EnumWindows(EnumWindowsProc(enum_cb), 0)
    
    # Debug log khi khong tim thay
    if not result and all_miniframes:
        pid_info = f" (PID filter={mt5_pid})" if mt5_pid else ""
        print(f"[GUI DEBUG] Tim thay {len(all_miniframes)} cua so MiniFrame, nhung KHONG co cai nao khop '{symbol_upper}'{pid_info}:")
        for hwnd, title, extracted, win_pid in all_miniframes:
            print(f"  - 0x{hwnd:08X} PID={win_pid} Title='{title}' -> Symbol='{extracted}'")
    elif not result:
        print(f"[GUI DEBUG] Khong tim thay bat ky cua so MiniFrame nao tren man hinh!")
    
    return result[0] if result else None


def resize_dom(dom_hwnd, width=DOM_WIDTH, height=DOM_HEIGHT):
    """
    Ep kich thuoc DOM du lon de hien 3 nut Sell/Close/Buy.
    Giu nguyen vi tri hien tai.
    """
    rect = ctypes.wintypes.RECT()
    user32.GetWindowRect(dom_hwnd, ctypes.byref(rect))
    user32.MoveWindow(dom_hwnd, rect.left, rect.top, width, height, True)


# ==========================================
# TIM CONTROL TRONG DOM
# ==========================================
def _tim_control_by_id(parent_hwnd, ctrl_id):
    """
    Tim child control theo Control ID (de quy xuong cac lop con).
    
    Returns:
        HWND cua control, hoac None.
    """
    found = []

    def enum_cb(hwnd, lParam):
        cid = user32.GetDlgCtrlID(hwnd)
        if cid == ctrl_id:
            cls = _get_class_name(hwnd)
            # Chi lay Button hoac Edit, tranh nham cac control khac cung ID
            if "Button" in cls or "Edit" in cls:
                found.append(hwnd)
                return False  # Dung tim khi thay
        return True

    user32.EnumChildWindows(parent_hwnd, EnumWindowsProc(enum_cb), 0)
    return found[0] if found else None


def _tim_tat_ca_controls(dom_hwnd):
    """
    Tim va cache tat ca controls can thiet trong DOM.
    
    Returns:
        Dict {"buy": hwnd, "sell": hwnd, "close": hwnd, "volume": hwnd}
        hoac None neu thieu control.
    """
    buy = _tim_control_by_id(dom_hwnd, CTRL_ID_BUY)
    sell = _tim_control_by_id(dom_hwnd, CTRL_ID_SELL)
    close = _tim_control_by_id(dom_hwnd, CTRL_ID_CLOSE)
    volume = _tim_control_by_id(dom_hwnd, CTRL_ID_VOLUME)

    if not all([buy, sell, close, volume]):
        return None

    return {
        "buy": buy,
        "sell": sell,
        "close": close,
        "volume": volume,
    }


# ==========================================
# THAO TAC VOI CONTROL
# ==========================================
def _click_button(hwnd_button):
    """
    Gui lenh click vao 1 Button control bang Win32 SendMessage.
    Khong can di chuot, khong phu thuoc toa do.
    """
    # BM_CLICK gui WM_LBUTTONDOWN + WM_LBUTTONUP noi bo
    user32.SendMessageW(hwnd_button, BM_CLICK, 0, 0)


def _set_edit_text(hwnd_edit, text):
    """
    Ghi text vao Edit control bang cach GIA LAP GO PHIM tung ky tu (WM_CHAR).
    Cach nay la bullet-proof nhat doi voi cac phan mem li lom nhu MT5.
    """
    # 1. Chon toan bo text hien co (EM_SETSEL)
    EM_SETSEL = 0x00B1
    user32.SendMessageW(hwnd_edit, EM_SETSEL, 0, -1)
    
    # 2. Bam nut Backspace de xoa (WM_CHAR + VK_BACK)
    WM_CHAR = 0x0102
    VK_BACK = 0x08
    user32.SendMessageW(hwnd_edit, WM_CHAR, VK_BACK, 0)
    
    # 3. Go tung ky tu vao o
    for char in text:
        user32.SendMessageW(hwnd_edit, WM_CHAR, ord(char), 0)
        time.sleep(0.005) # Delay sieu nho cho MT5 kip nhan ky tu


def _get_edit_text(hwnd_edit):
    """
    Doc noi dung cua 1 Edit control.
    """
    length = user32.SendMessageW(hwnd_edit, WM_GETTEXTLENGTH, 0, 0)
    if length == 0:
        return ""
    buf = ctypes.create_unicode_buffer(length + 1)
    user32.SendMessageW(hwnd_edit, WM_GETTEXT, length + 1, buf)
    return buf.value


# ==========================================
# CLASS CHINH: DomTrader
# ==========================================
class DomTrader:
    """
    Quan ly tuong tac voi 1 cua so DOM tren MT5.
    
    Usage:
        trader = DomTrader("XAUUSD")
        trader.khoi_tao()           # Tim DOM, resize, cache controls
        trader.dat_volume("0.01")   # Fill volume
        trader.buy()                # Click Buy
        trader.sell()               # Click Sell
        trader.close_position()     # Click Close (FIFO - cu nhat truoc)
    """

    def __init__(self, symbol, bot_name="[GUI]", mt5_pid=None):
        self.symbol = symbol.upper()
        self.bot_name = bot_name
        self.mt5_pid = mt5_pid
        self.dom_hwnd = None
        self.controls = None
        self._da_khoi_tao = False

    def khoi_tao(self):
        """
        Tim DOM window, resize, va cache controls.
        Goi 1 lan khi worker khoi dong.
        
        Returns:
            True neu thanh cong, False neu that bai.
        """
        self.dom_hwnd = tim_dom_window(self.symbol, self.mt5_pid)
        if not self.dom_hwnd:
            print(f"{self.bot_name} [GUI] Khong tim thay DOM cho {self.symbol} (PID={self.mt5_pid})!")
            self._da_khoi_tao = False
            return False

        # Ep kich thuoc du hien 3 nut
        resize_dom(self.dom_hwnd)
        # Cho DOM render lai
        time.sleep(0.1)

        self.controls = _tim_tat_ca_controls(self.dom_hwnd)
        if not self.controls:
            print(f"{self.bot_name} [GUI] Tim thay DOM nhung khong tim thay du controls!")
            self._da_khoi_tao = False
            return False

        self._da_khoi_tao = True
        print(
            f"{self.bot_name} [GUI] Da khoi tao DOM thanh cong! "
            f"Buy=0x{self.controls['buy']:08X} "
            f"Sell=0x{self.controls['sell']:08X} "
            f"Close=0x{self.controls['close']:08X} "
            f"Volume=0x{self.controls['volume']:08X}"
        )
        return True

    def kiem_tra_dom_con_song(self):
        """
        Kiem tra DOM window con ton tai va visible khong.
        Neu mat thi thu khoi tao lai.
        
        Returns:
            True neu DOM san sang, False neu khong.
        """
        if not self.dom_hwnd or not user32.IsWindow(self.dom_hwnd):
            print(f"{self.bot_name} [GUI] DOM da bi dong! Dang tim lai...")
            return self.khoi_tao()

        if not _is_visible(self.dom_hwnd):
            print(f"{self.bot_name} [GUI] DOM bi an! Dang hien lai...")
            user32.ShowWindow(self.dom_hwnd, 5)  # SW_SHOW
            time.sleep(0.05)

        return self._da_khoi_tao

    def dat_volume(self, volume_str):
        """
        Thay doi volume trong DOM.
        
        Args:
            volume_str: Volume dang string (VD: "0.01", "0.1")
        """
        if not self._da_khoi_tao:
            return False

        with _gui_lock:
            # 💡 Thuat toan detect Regional Settings: 
            # Doc thu so Volume dang co san tren man hinh MT5
            current_vol = _get_edit_text(self.controls["volume"])
            volume_str = str(volume_str)
            
            # Neu Windows dang dung dau phay (,) cho so thap phan
            if "," in current_vol:
                volume_str = volume_str.replace(".", ",")
            
            _set_edit_text(self.controls["volume"], volume_str)

        return True

    def lay_volume_hien_tai(self):
        """Doc volume hien tai trong DOM."""
        if not self._da_khoi_tao:
            return ""
        return _get_edit_text(self.controls["volume"])

    def buy(self):
        """
        Click nut Buy tren DOM.
        
        Returns:
            True neu click thanh cong (khong dam bao lenh khop).
        """
        if not self.kiem_tra_dom_con_song():
            return False

        with _gui_lock:
            _random_delay(1, 2)
            _click_button(self.controls["buy"])

        print(f"{self.bot_name} [GUI] Da click BUY tren DOM!")
        return True

    def sell(self):
        """
        Click nut Sell tren DOM.
        
        Returns:
            True neu click thanh cong.
        """
        if not self.kiem_tra_dom_con_song():
            return False

        with _gui_lock:
            _random_delay(1, 2)
            _click_button(self.controls["sell"])

        print(f"{self.bot_name} [GUI] Da click SELL tren DOM!")
        return True

    def close_position(self):
        """
        Click nut Close tren DOM (dong lenh cu nhat - FIFO).
        
        Returns:
            True neu click thanh cong.
        """
        if not self.kiem_tra_dom_con_song():
            return False

        with _gui_lock:
            _random_delay(1, 2)
            _click_button(self.controls["close"])

        print(f"{self.bot_name} [GUI] Da click CLOSE tren DOM!")
        return True

    def thuc_thi_lenh(self, action, volume=None):
        """
        Thuc thi lenh theo action string (tuong thich voi worker.py).
        
        Args:
            action: "BUY", "SELL", hoac "CLOSE"
            volume: Volume (optional, chi set neu khac volume hien tai)
            
        Returns:
            True neu click thanh cong.
        """
        if volume is not None:
            vol_str = str(volume)
            current_vol = self.lay_volume_hien_tai()
            if current_vol != vol_str:
                self.dat_volume(vol_str)
                _random_delay(1, 3)

        action_upper = action.upper()
        if action_upper == "BUY":
            return self.buy()
        elif action_upper == "SELL":
            return self.sell()
        elif action_upper == "CLOSE":
            return self.close_position()
        else:
            print(f"{self.bot_name} [GUI] Action khong ho tro: {action}")
            return False
