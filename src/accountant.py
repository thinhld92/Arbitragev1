import redis
import ujson as json
import time
import csv
import os
from datetime import datetime, timezone
import ctypes

os.system("title KẾ TOÁN TRƯỞNG (ACCOUNTANT)")
ctypes.windll.kernel32.SetConsoleTitleW("KẾ TOÁN TRƯỞNG")

# Điều chỉnh kích thước và vị trí Terminal (Rộng 400, Cao 700)
# Tọa độ X=1020, Y=0 (Đặt cạnh lề phải của các cửa sổ Worker/Master)
hwnd = ctypes.windll.kernel32.GetConsoleWindow()
if hwnd:
    ctypes.windll.user32.MoveWindow(hwnd, 600, 0, 320, 750, True)

with open('config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

r = redis.Redis(
    host=config['redis']['host'], 
    port=config['redis']['port'], 
    db=config['redis']['db'], 
    decode_responses=True,
    health_check_interval=30,   # ⚡ Tự động Ping để giữ kết nối
    socket_timeout=5.0,         # ⚡ Quá 5s rớt mạng tự văng lỗi để lặp lại
    socket_connect_timeout=5.0
)
history_dir = "history"
os.makedirs(history_dir, exist_ok=True)

# Trí nhớ tạm thời để chờ ghép Biên lai của 2 sàn
pending_receipts = {}
last_cleanup_time = time.time()

# BIẾN LƯU TRỮ TỔNG KẾT TRONG NGÀY
daily_stats = {}
current_day_str = None

# Bố cục Header giống Excel
HEADER_TABLE = f"{'TIME':<8} │ {'PROFIT':>8} │ {'TOTAL':>8} │ {'VOL':>5}"
SEPARATOR_LINE = "─" * 38

print("Kế Toán Trưởng Sẵn sàng ghi sổ...")
print(HEADER_TABLE)
print(SEPARATOR_LINE)

print_counter = 0

while True:
    try:
        # 🛡️ POT-2 FIX: Dọn rác pending_receipts quá 5 phút (tránh memory leak)
        now = time.time()
        if now - last_cleanup_time > 60:
            expired_tokens = [token for token, data in pending_receipts.items() 
                            if isinstance(data, dict) and any(
                                isinstance(v, dict) and now - v.get('_received_at', now) > 300 
                                for v in data.values()
                            )]
            for token in expired_tokens:
                print(f"Dọn rác: Xóa biên lai mồ côi {token} (quá 5 phút)")
                del pending_receipts[token]
            last_cleanup_time = now
        
        data_raw = r.brpop("QUEUE:ACCOUNTANT", timeout=1)
        if data_raw:
            bien_lai = json.loads(data_raw[1])
            ctx = bien_lai.get("context", {})
            pair_token = ctx.get("pair_token")
            role = bien_lai.get("role")
            
            if not pair_token or not role: continue
            
            # Lưu tạm vào khay (kèm timestamp)
            bien_lai['_received_at'] = time.time()
            if pair_token not in pending_receipts:
                pending_receipts[pair_token] = {}
            pending_receipts[pair_token][role] = bien_lai
            
            # KHI NHẬN ĐƯỢC BIÊN LAI, KIỂM TRA XEM CÓ PHẢI LÀ ÁN TRẢM ĐƠN KHÔNG
            is_single = ctx.get("is_single_cut", False)

            if is_single or ("BASE" in pending_receipts[pair_token] and "DIFF" in pending_receipts[pair_token]):
                vps_name = config.get("vps_name", "trade_data")
                today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
                
                # CHECK SANG NGÀY MỚI THÌ RESET TỔNG
                if today_str != current_day_str:
                    print(f"Ngày mới {today_str}. Reset bộ đếm...")
                    daily_stats = {}
                    current_day_str = today_str
                    print(SEPARATOR_LINE)
                    print(HEADER_TABLE)
                    print(SEPARATOR_LINE)
                    print_counter = 0

                csv_file = os.path.join(history_dir, f"{vps_name}_{ctx['pair_id']}_{today_str}.csv")
                file_exists = os.path.isfile(csv_file)
                
                try:
                    # TÍNH TOÁN LỜI LỖ CHO GIAO DỊCH HIỆN TẠI
                    if is_single:
                        single_data = pending_receipts[pair_token][role]
                        b_ticket = single_data['ticket'] if role == "BASE" else "N/A"
                        d_ticket = single_data['ticket'] if role == "DIFF" else "N/A"
                        b_prof = single_data['profit'] if role == "BASE" else 0.0
                        d_prof = single_data['profit'] if role == "DIFF" else 0.0
                        b_fee = single_data['fee'] if role == "BASE" else 0.0
                        d_fee = single_data['fee'] if role == "DIFF" else 0.0
                        b_op = single_data['open_price'] if role == "BASE" else 0.0
                        b_cp = single_data['close_price'] if role == "BASE" else 0.0
                        d_op = single_data['open_price'] if role == "DIFF" else 0.0
                        d_cp = single_data['close_price'] if role == "DIFF" else 0.0
                        vol = single_data['volume']
                        net_profit = b_prof + d_prof + b_fee + d_fee
                        total_fee = b_fee + d_fee
                    else:
                        base = pending_receipts[pair_token]["BASE"]
                        diff = pending_receipts[pair_token]["DIFF"]
                        b_ticket, d_ticket = base['ticket'], diff['ticket']
                        b_prof, d_prof = base['profit'], diff['profit']
                        b_fee, d_fee = base['fee'], diff['fee']
                        b_op, b_cp = base['open_price'], base['close_price']
                        d_op, d_cp = diff['open_price'], diff['close_price']
                        vol = base['volume']
                        net_profit = b_prof + d_prof + b_fee + d_fee
                        total_fee = b_fee + d_fee

                    # CẬP NHẬT TỔNG LŨY KẾ TRONG NGÀY
                    pair_id = ctx['pair_id']
                    if pair_id not in daily_stats:
                        daily_stats[pair_id] = {'total_volume': 0.0, 'total_net_profit': 0.0}
                        # Phục hồi tổng lũy kế nếu file ngày hôm nay đã tồn tại (Phòng trường hợp bot bị restart)
                        if file_exists:
                            try:
                                with open(csv_file, 'r', encoding='utf-8') as f_read:
                                    rows = list(csv.reader(f_read))
                                    if len(rows) > 1:
                                        last_row = rows[-1]
                                        # Cột 20: Total_Day_Volume, Cột 21: Total_Day_Profit
                                        daily_stats[pair_id]['total_volume'] = float(last_row[20])
                                        daily_stats[pair_id]['total_net_profit'] = float(last_row[21])
                            except Exception:
                                pass
                    
                    daily_stats[pair_id]['total_volume'] += vol
                    daily_stats[pair_id]['total_net_profit'] += net_profit
                    
                    current_total_vol = daily_stats[pair_id]['total_volume']
                    current_total_profit = daily_stats[pair_id]['total_net_profit']

                    with open(csv_file, mode='a', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        if not file_exists:
                            writer.writerow([
                                'Time_Closed', 'Pair_ID', 'Action', 'Volume', 
                                'Base_Ticket', 'Diff_Ticket', 'Entry_Mode', 'Entry_Dev', 
                                'Close_Mode', 'Close_Dev', 'Base_Open', 'Base_Close', 
                                'Diff_Open', 'Diff_Close', 
                                'Base_Profit', 'Diff_Profit', 'Base_Fee', 'Diff_Fee', 
                                'Total_Fee', 'Net_Profit', 'Total_Day_Volume', 'Total_Day_Profit',
                                'Base_Tick_Hz_In', 'Diff_Tick_Hz_In', 'Base_Tick_Hz_Out', 'Diff_Tick_Hz_Out', 
                                'Conf_Dev_Entry', 'Conf_Dev_Close', 'Conf_Stable_Time'
                            ])
                        
                        pair_config = next((p for p in config.get('danh_sach_cap', []) if p['id'] == ctx['pair_id']), {})
                        c_dev_entry = pair_config.get('deviation_entry', 0)
                        c_dev_close = pair_config.get('deviation_close', 0)
                        c_stable_time = pair_config.get('stable_time', 0)
                        
                        # IN RA EXCEL
                        writer.writerow([
                            datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                            ctx['pair_id'], ctx['action_type'], vol,
                            b_ticket, d_ticket,
                            ctx['mode_vao'], f"{ctx['chenh_vao']:.2f}",
                            ctx['mode_dong'], f"{ctx['chenh_dong']:.2f}",
                            b_op, b_cp, d_op, d_cp,
                            f"{b_prof:.2f}", f"{d_prof:.2f}", 
                            f"{b_fee:.2f}", f"{d_fee:.2f}",       
                            f"{total_fee:.2f}", f"{net_profit:.2f}",
                            f"{current_total_vol:.2f}", f"{current_total_profit:.2f}",
                            ctx.get('tick_hz_base_in', 0), ctx.get('tick_hz_diff_in', 0),
                            ctx.get('tick_hz_base_out', 0), ctx.get('tick_hz_diff_out', 0),
                            c_dev_entry, c_dev_close, c_stable_time
                        ])
                    
                    time_str = datetime.now(timezone.utc).strftime('%H:%M:%S')
                    print(f"{time_str:<8} │ {net_profit:>7.2f}$ │ {current_total_profit:>7.2f}$ │ {current_total_vol:>5.2f}")
                    
                    print_counter += 1
                    # In nhắc lại Header sau mỗi 15 lệnh được chốt
                    if print_counter % 30 == 0:
                        print(SEPARATOR_LINE)
                        print(HEADER_TABLE)
                        print(SEPARATOR_LINE)

                    del pending_receipts[pair_token] # Ghi xong xóa trí nhớ
                    
                except PermissionError:
                    print(f"{datetime.now().strftime('%H:%M:%S')} ERROR: KHÔNG THỂ GHI FILE. ĐẠI CA ĐANG MỞ EXCEL PHẢI KHÔNG? ĐÓNG FILE ĐI RỒI BOT GHI TIẾP!")
                    # 🛡️ POT-6 FIX: Đẩy LẠI TẤT CẢ biên lai của cặp này vào queue
                    for saved_role, saved_data in pending_receipts[pair_token].items():
                        r.lpush("QUEUE:ACCOUNTANT", json.dumps(saved_data))
                    del pending_receipts[pair_token]  # Xóa khỏi pending để tránh trùng
                    time.sleep(2)
                
    except Exception as e:
        print(f"Kế toán vấp ngã: {e}")
        time.sleep(1)