import redis
import ujson as json
import time
import csv
import os
from datetime import datetime
import ctypes

os.system("title 👓 KẾ TOÁN TRƯỞNG (ACCOUNTANT)")
ctypes.windll.kernel32.SetConsoleTitleW("👓 KẾ TOÁN TRƯỞNG")

with open('config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

r = redis.Redis(host=config['redis']['host'], port=config['redis']['port'], db=config['redis']['db'], decode_responses=True)

history_dir = "history"
os.makedirs(history_dir, exist_ok=True)

# Trí nhớ tạm thời để chờ ghép Biên lai của 2 sàn
pending_receipts = {}

print("👓 Kế Toán Trưởng đã vào vị trí. Sẵn sàng ghi sổ...")

while True:
    try:
        data_raw = r.brpop("QUEUE:ACCOUNTANT", timeout=1)
        if data_raw:
            bien_lai = json.loads(data_raw[1])
            ctx = bien_lai.get("context", {})
            pair_token = ctx.get("pair_token")
            role = bien_lai.get("role")
            
            if not pair_token or not role: continue
            
            # Lưu tạm vào khay
            if pair_token not in pending_receipts:
                pending_receipts[pair_token] = {}
            pending_receipts[pair_token][role] = bien_lai
            
            # KHI NHẬN ĐƯỢC BIÊN LAI, KIỂM TRA XEM CÓ PHẢI LÀ ÁN TRẢM ĐƠN KHÔNG
            is_single = ctx.get("is_single_cut", False)

            if is_single or ("BASE" in pending_receipts[pair_token] and "DIFF" in pending_receipts[pair_token]):
                vps_name = config.get("vps_name", "trade_data")
                csv_file = os.path.join(history_dir, f"{vps_name}_{ctx['pair_id']}.csv")
                file_exists = os.path.isfile(csv_file)
                
                try:
                    with open(csv_file, mode='a', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        if not file_exists:
                            writer.writerow([
                                'Time_Closed', 'Pair_ID', 'Action', 'Volume', 
                                'Base_Ticket', 'Diff_Ticket', 'Entry_Mode', 'Entry_Dev', 
                                'Close_Mode', 'Close_Dev', 'Base_Open', 'Base_Close', 
                                'Diff_Open', 'Diff_Close', 
                                'Base_Profit', 'Diff_Profit', 'Base_Fee', 'Diff_Fee', 
                                'Total_Fee', 'Net_Profit',
                                'Base_Tick_Hz_In', 'Diff_Tick_Hz_In', 'Base_Tick_Hz_Out', 'Diff_Tick_Hz_Out', 
                                'Conf_Dev_Entry', 'Conf_Dev_Close', 'Conf_Stable_Time'
                            ])
                        
                        # Bốc cấu hình trực tiếp từ List 'danh_sach_cap' trong config.json
                        pair_config = next((p for p in config.get('danh_sach_cap', []) if p['id'] == ctx['pair_id']), {})
                        c_dev_entry = pair_config.get('deviation_entry', 0)
                        c_dev_close = pair_config.get('deviation_close', 0)
                        c_stable_time = pair_config.get('stable_time', 0)
                        
                        if is_single:
                            # XỬ LÝ LỆNH MỒ CÔI (Chỉ có 1 chân)
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
                            # XỬ LÝ LỆNH CẶP BÌNH THƯỜNG (Có đủ 2 chân)
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
                        
                        # IN RA EXCEL
                        writer.writerow([
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            ctx['pair_id'], ctx['action_type'], vol,
                            b_ticket, d_ticket,
                            ctx['mode_vao'], f"{ctx['chenh_vao']:.2f}",
                            ctx['mode_dong'], f"{ctx['chenh_dong']:.2f}",
                            b_op, b_cp, d_op, d_cp,
                            f"{b_prof:.2f}", f"{d_prof:.2f}", 
                            f"{b_fee:.2f}", f"{d_fee:.2f}",       
                            f"{total_fee:.2f}", f"{net_profit:.2f}",
                            ctx.get('tick_hz_base_in', 0), ctx.get('tick_hz_diff_in', 0),
                            ctx.get('tick_hz_base_out', 0), ctx.get('tick_hz_diff_out', 0),
                            c_dev_entry, c_dev_close, c_stable_time
                        ])
                    
                    print(f"✅ Đã ghi sổ {'MỒ CÔI' if is_single else 'CẶP'}: {pair_token} | Lời/Lỗ: {net_profit:.2f}$")
                    del pending_receipts[pair_token] # Ghi xong xóa trí nhớ
                    
                except PermissionError:
                    print(f"⚠️ {datetime.now().strftime('%H:%M:%S')} ERROR: KHÔNG THỂ GHI FILE. ĐẠI CA ĐANG MỞ EXCEL PHẢI KHÔNG? ĐÓNG FILE ĐI RỒI BOT GHI TIẾP! 😡")
                    r.lpush("QUEUE:ACCOUNTANT", json.dumps(bien_lai)) # Đẩy lại vào hàng đợi chờ ghi sau
                    time.sleep(2)
                
    except Exception as e:
        print(f"⚠️ Kế toán vấp ngã: {e}")
        time.sleep(1)