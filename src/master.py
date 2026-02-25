import os
import redis
import json
import time
import argparse
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

# Import Quân Sư
from utils.trading_logic import check_tin_hieu_arbitrage 

CONFIG_FILE = 'config.json'
last_config_modified = 0  

# ==========================================
# 1. KHỞI TẠO & ĐỌC CONFIG
# ==========================================
parser = argparse.ArgumentParser()
parser.add_argument("--pair_id", required=True)
args = parser.parse_args()

os.system(f"title 🧠 MASTER BRAIN - {args.pair_id}")

log_filename = f"log_master_{args.pair_id}.txt"

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        RotatingFileHandler(log_filename, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
    ]
)
logging.info(f"=== KHỞI ĐỘNG MASTER BRAIN {args.pair_id} ===")

with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
    config = json.load(f)

redis_conf = config['redis']
r = redis.Redis(host=redis_conf['host'], port=redis_conf['port'], db=redis_conf['db'], decode_responses=True)

cap_hien_tai = next((cap for cap in config['danh_sach_cap'] if cap['id'] == args.pair_id), None)
if cap_hien_tai is None:
    print(f"❌ LỖI: Không tìm thấy ID {args.pair_id} trong {CONFIG_FILE}!")
    quit()

key_base = f"TICK:{cap_hien_tai['base_exchange']}:{cap_hien_tai['base_symbol']}"
key_diff = f"TICK:{cap_hien_tai['diff_exchange']}:{cap_hien_tai['diff_symbol']}"
key_state = f"STATE:MASTER:{args.pair_id}" 

dev_entry = cap_hien_tai['deviation_entry']
dev_close = cap_hien_tai['deviation_close']
stable_time_sec = cap_hien_tai['stable_time'] / 1000.0  
cooldown_sec = cap_hien_tai['cooldown_second']
max_orders = cap_hien_tai['max_orders']

# ==========================================
# 2. KHÔI PHỤC TRÍ NHỚ TỪ REDIS
# ==========================================
saved_state_raw = r.get(key_state)
if saved_state_raw:
    saved_state = json.loads(saved_state_raw)
    huong_dang_danh = saved_state.get("huong_dang_danh")
    lich_su_vao_lenh = saved_state.get("lich_su_vao_lenh", [])
    thoi_diem_vao_lenh_cuoi = saved_state.get("thoi_diem_vao_lenh_cuoi", 0)
    print(f"🧠 Đã khôi phục trí nhớ: Đang đánh {huong_dang_danh}, gồng {len(lich_su_vao_lenh)} cặp lệnh.")
    logging.info(f"RESTORE STATE: {huong_dang_danh} - Lịch sử: {len(lich_su_vao_lenh)} lệnh")
else:
    huong_dang_danh = None
    lich_su_vao_lenh = []
    thoi_diem_vao_lenh_cuoi = 0
    print("🧠 Bắt đầu với trí nhớ trống rỗng.")

def luu_tri_nho():
    state = {
        "huong_dang_danh": huong_dang_danh,
        "lich_su_vao_lenh": lich_su_vao_lenh,
        "thoi_diem_vao_lenh_cuoi": thoi_diem_vao_lenh_cuoi
    }
    r.set(key_state, json.dumps(state))

def kiem_tra_gio_giao_dich(trading_hours):
    if not trading_hours:
        return True 
        
    current_time = datetime.now().strftime("%H:%M")
    
    for khung_gio in trading_hours:
        start, end = khung_gio.split('-')
        if start <= end:
            if start <= current_time <= end:
                return True
        else: 
            if current_time >= start or current_time <= end:
                return True
                
    return False

# CÁC BIẾN STATE (Đã thay thế last_latest_msc bằng 2 biến theo dõi độc lập)
last_base_msc = 0
last_diff_msc = 0

thoi_diem_bat_dau_lech = 0
dang_trong_chu_ky_debounce = False
loai_lenh_dang_cho = None
thoi_diem_phat_hien_lech_chan = 0 
thoi_gian_cho_phep_delay = 5.0  
da_gui_canh_bao_lech_chan = False

print(f"🚀 MASTER {args.pair_id} SẴN SÀNG CHIẾN ĐẤU (REAL-TIME CORE)!")
print("Bấm Ctrl + C để dừng Master an toàn!\n")

# ==========================================
# 3. VÒNG LẶP SUY NGHĨ CỦA MASTER
# ==========================================
try:
    while True:
        time.sleep(0.001) 

        pos_base_raw = r.get(f"POSITION:{cap_hien_tai['base_exchange']}:{cap_hien_tai['base_symbol']}")
        pos_diff_raw = r.get(f"POSITION:{cap_hien_tai['diff_exchange']}:{cap_hien_tai['diff_symbol']}")
        
        if pos_base_raw is None or pos_diff_raw is None:
            continue
            
        pos_base = int(pos_base_raw)
        pos_diff = int(pos_diff_raw)
        so_lenh_thuc_te = max(pos_base, pos_diff)

        if so_lenh_thuc_te == 0:
            if (time.time() - thoi_diem_vao_lenh_cuoi) > 5.0:
                if huong_dang_danh is not None or len(lich_su_vao_lenh) > 0:
                    huong_dang_danh = None
                    lich_su_vao_lenh.clear()
                    luu_tri_nho() 
                    print("🧹 Cập nhật: Đã làm mới trí nhớ (Không còn lệnh nào trên sàn).")

        if pos_base != pos_diff:
            if thoi_diem_phat_hien_lech_chan == 0:
                thoi_diem_phat_hien_lech_chan = time.time()
                da_gui_canh_bao_lech_chan = False
                
            elif (time.time() - thoi_diem_phat_hien_lech_chan) > thoi_gian_cho_phep_delay:
                if not da_gui_canh_bao_lech_chan:
                    canh_bao = f"🚨 [BÁO ĐỘNG ĐỎ] LỆCH CHÂN!!! {cap_hien_tai['base_exchange']}: {pos_base} lệnh | {cap_hien_tai['diff_exchange']}: {pos_diff} lệnh."
                    print(canh_bao)
                    logging.warning(canh_bao)
                    
                    msg_canh_bao = f"🚨 <b>BÁO ĐỘNG LỆCH CHÂN</b> 🚨\nCặp: {args.pair_id}\nLệnh Base: {pos_base}\nLệnh Diff: {pos_diff}\n⚠️ Hãy khẩn trương kiểm tra sự cố!"
                    r.lpush("TELEGRAM_QUEUE", msg_canh_bao)
                    da_gui_canh_bao_lech_chan = True
                
                dang_trong_chu_ky_debounce = False
                continue 
        else:
            thoi_diem_phat_hien_lech_chan = 0
            da_gui_canh_bao_lech_chan = False 

        current_modified = os.path.getmtime(CONFIG_FILE)
        if current_modified != last_config_modified:
            try:
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                cap_hien_tai = next((cap for cap in config['danh_sach_cap'] if cap['id'] == args.pair_id), None)
                if cap_hien_tai:
                    dev_entry = cap_hien_tai['deviation_entry']
                    dev_close = cap_hien_tai['deviation_close']
                    max_orders = cap_hien_tai['max_orders']
                    cooldown_sec = cap_hien_tai['cooldown_second']
                    hold_time_sec = cap_hien_tai.get('hold_time', 180)
                    stable_time_sec = cap_hien_tai['stable_time'] / 1000.0
                last_config_modified = current_modified
                
                msg_reload = f"🔄 [HOT RELOAD] Cập nhật: Lệch {dev_entry}|{dev_close}|{stable_time_sec*1000}ms, Max {max_orders}, Cooldown {cooldown_sec}s, Hold {hold_time_sec}s"
                print(msg_reload)
                logging.info(msg_reload)
            except Exception as e:
                pass

        tick_base_raw = r.get(key_base)
        tick_diff_raw = r.get(key_diff)
        
        if not (tick_base_raw and tick_diff_raw):
            continue
            
        tick_base = json.loads(tick_base_raw)
        tick_diff = json.loads(tick_diff_raw)
        
        # ==========================================
        # GUARD: CHECK TICK MỚI ĐỘC LẬP TỪNG SÀN
        # ==========================================
        current_base_msc = tick_base['time_msc']
        current_diff_msc = tick_diff['time_msc']
        
        co_tick_moi = False
        if current_base_msc > last_base_msc or current_diff_msc > last_diff_msc:
            last_base_msc = current_base_msc
            last_diff_msc = current_diff_msc
            co_tick_moi = True
            
        if not co_tick_moi:
            if so_lenh_thuc_te == 0 and not dang_trong_chu_ky_debounce:
                continue

        if (time.time() - thoi_diem_vao_lenh_cuoi) > 5.0:
            if len(lich_su_vao_lenh) > so_lenh_thuc_te:
                if so_lenh_thuc_te > 0:
                    lich_su_vao_lenh = lich_su_vao_lenh[-so_lenh_thuc_te:]
                else:
                    lich_su_vao_lenh.clear()
                luu_tri_nho()

        # ==========================================
        # GỌI QUÂN SƯ TÍNH TOÁN
        # ==========================================
        tin_hieu = check_tin_hieu_arbitrage(tick_base, tick_diff, cap_hien_tai, huong_dang_danh) 
        
        hanh_dong = tin_hieu["hanh_dong"]
        hold_time_sec = cap_hien_tai.get('hold_time', 180)

        # --------------------------------------------------
        # TRƯỜNG HỢP 1: CÓ TÍN HIỆU ĐÓNG LỆNH CHỐT LỜI
        # --------------------------------------------------
        if hanh_dong == "DONG_LENH" and so_lenh_thuc_te > 0:
            lenh_du_tuoi = [t for t in lich_su_vao_lenh if (time.time() - t) >= hold_time_sec]
            so_luong_can_dong = len(lenh_du_tuoi)
            
            if so_luong_can_dong > 0:
                chenh_lech_close = tin_hieu.get("chenh_lech", 0)
                msg_chot_loi = f"💰 CHỐT LỜI {so_luong_can_dong} LỆNH! (Đã giữ > {hold_time_sec}s). Lệch: {chenh_lech_close:.2f}."
                print(msg_chot_loi)
                logging.info(msg_chot_loi)
                
                chi_thi_dong = {"action": "CLOSE_OLDEST", "count": so_luong_can_dong}
                r.lpush(f"QUEUE:ORDER:{cap_hien_tai['base_exchange']}", json.dumps(chi_thi_dong))
                r.lpush(f"QUEUE:ORDER:{cap_hien_tai['diff_exchange']}", json.dumps(chi_thi_dong))
                
                lich_su_vao_lenh = [t for t in lich_su_vao_lenh if t not in lenh_du_tuoi]
                dang_trong_chu_ky_debounce = False
                loai_lenh_dang_cho = None
                luu_tri_nho()
            else:
                pass 

        # --------------------------------------------------
        # TRƯỜNG HỢP 2: CÓ TÍN HIỆU VÀO LỆNH (MỞ MỚI)
        # --------------------------------------------------
        elif hanh_dong == "VAO_LENH":
            khung_gio_cho_phep = cap_hien_tai.get('trading_hours', [])
            if not kiem_tra_gio_giao_dich(khung_gio_cho_phep):
                if dang_trong_chu_ky_debounce:
                    print("⏰ Đã ra ngoài giờ giao dịch. Hủy ngắm bắn!")
                    dang_trong_chu_ky_debounce = False
                    loai_lenh_dang_cho = None
                continue 

            chenh_lech = tin_hieu["chenh_lech"]
            loai_lenh_moi = tin_hieu["loai_lenh"] 

            if so_lenh_thuc_te >= max_orders or (time.time() - thoi_diem_vao_lenh_cuoi) < cooldown_sec or (huong_dang_danh is not None and huong_dang_danh != loai_lenh_moi):
                if dang_trong_chu_ky_debounce:
                    dang_trong_chu_ky_debounce = False
                    loai_lenh_dang_cho = None
                pass 
            
            else:
                if not dang_trong_chu_ky_debounce:
                    dang_trong_chu_ky_debounce = True
                    thoi_diem_bat_dau_lech = time.time()
                    loai_lenh_dang_cho = loai_lenh_moi
                    print(f"⏱️ Phát hiện kèo lệch {chenh_lech:.2f} điểm ({loai_lenh_moi})! Đang đếm ngược...")
                else:
                    if loai_lenh_moi != loai_lenh_dang_cho:
                        print(f"🔄 Kèo lật mặt từ {loai_lenh_dang_cho} sang {loai_lenh_moi}! Reset đồng hồ.")
                        thoi_diem_bat_dau_lech = time.time()
                        loai_lenh_dang_cho = loai_lenh_moi
                        continue 
                    
                    thoi_gian_debounce = time.time() - thoi_diem_bat_dau_lech
                    
                    if thoi_gian_debounce >= stable_time_sec:
                        msg_vao_lenh = f"🔥 KÈO ĐÃ CHÍN! Lệch {chenh_lech:.2f}. BÓP CÒ MỞ LỆNH {loai_lenh_moi}!!! (Time chờ: {thoi_gian_debounce*1000:.0f}ms)"
                        print(msg_vao_lenh)
                        logging.info(msg_vao_lenh)

                        huong_dang_danh = loai_lenh_moi
                        lot_size = cap_hien_tai.get('lot_size', 0.1)
                        
                        chi_thi_base = {"action": tin_hieu["lenh_base"], "volume": lot_size}
                        r.lpush(f"QUEUE:ORDER:{cap_hien_tai['base_exchange']}", json.dumps(chi_thi_base))
                        
                        chi_thi_diff = {"action": tin_hieu["lenh_diff"], "volume": lot_size}
                        r.lpush(f"QUEUE:ORDER:{cap_hien_tai['diff_exchange']}", json.dumps(chi_thi_diff))
                        
                        lich_su_vao_lenh.append(time.time())
                        thoi_diem_vao_lenh_cuoi = time.time() 
                        
                        dang_trong_chu_ky_debounce = False
                        loai_lenh_dang_cho = None 
                        luu_tri_nho()

        # --------------------------------------------------
        # TRƯỜNG HỢP 3: CHỜ ĐỢI HOẶC MẤT TÍN HIỆU
        # --------------------------------------------------
        else:
            if dang_trong_chu_ky_debounce:
                thoi_gian_debounce = time.time() - thoi_diem_bat_dau_lech
                msg_lo = f"❌ Kèo lỏ (Giá sập). Time chờ {thoi_gian_debounce*1000:.0f}ms / {stable_time_sec*1000:.0f}ms. Hủy tín hiệu!"
                print(msg_lo)
                # logging.info(msg_lo)
                dang_trong_chu_ky_debounce = False
                loai_lenh_dang_cho = None

except KeyboardInterrupt:
    print(f"\n🛑 [MASTER {args.pair_id}] Đã tắt an toàn!")
    logging.info(f"=== TẮT MASTER {args.pair_id} ===")