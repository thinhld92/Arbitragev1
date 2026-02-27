import ctypes
import json

def dan_tran_cua_so(vi_tri_hang):
    """
    Ép vị trí cửa sổ Terminal xếp chồng từ trên xuống dưới.
    Đọc kích thước từ file config.json ở thư mục gốc.
    vi_tri_hang: 1 (Telegram), 2 (Base), 3 (Diff), 4 (Master)
    """
    hwnd = ctypes.windll.kernel32.GetConsoleWindow()
    if not hwnd:
        return

    # Khai báo kích thước mặc định
    chieu_rong = 1000  
    chieu_cao = 250    
    toa_do_x = 10      
    offset_y = 0

    # Đọc siêu ngắn gọn y hệt Worker (Vì CWD đang ở thư mục gốc)
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
            if 'terminal_ui' in config:
                ui_cfg = config['terminal_ui']
                chieu_rong = ui_cfg.get('width', chieu_rong)
                chieu_cao = ui_cfg.get('height', chieu_cao)
                toa_do_x = ui_cfg.get('offset_x', toa_do_x)
                offset_y = ui_cfg.get('offset_y', offset_y)
    except Exception as e:
        print(f"⚠️ Lỗi đọc giao diện từ config: {e}. Đang dùng kích thước mặc định.")

    # Tính toán và Ép khung Windows
    toa_do_y = offset_y + (vi_tri_hang - 1) * chieu_cao 
    ctypes.windll.user32.MoveWindow(hwnd, toa_do_x, toa_do_y, chieu_rong, chieu_cao, True)