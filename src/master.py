import os
import redis
import ujson as json
import time
import argparse
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
import ctypes

# Import Quân Sư
from utils.trading_logic import check_tin_hieu_arbitrage 
from utils.terminal import dan_tran_cua_so

CONFIG_FILE = 'config.json'
last_config_modified = 0  

try:
    kernel32 = ctypes.windll.kernel32
    kernel32.SetConsoleMode(kernel32.GetStdHandle(-10), 128)
except Exception:
    pass

# ==========================================
# 1. KHỞI TẠO & ĐỌC CONFIG
# ==========================================
parser = argparse.ArgumentParser()
parser.add_argument("--pair_id", required=True)
args = parser.parse_args()

os.system(f"title 🧠 MASTER BRAIN - {args.pair_id}")
dan_tran_cua_so(4)

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(log_dir, f"log_master_{args.pair_id}.txt")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[RotatingFileHandler(log_filename, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')])

logging.info(f"=== KHỞI ĐỘNG MASTER BRAIN {args.pair_id} ===")

with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
    config = json.load(f)

redis_conf = config['redis']
r = redis.Redis(
    host=redis_conf['host'], 
    port=redis_conf['port'], 
    db=redis_conf['db'], 
    decode_responses=True,
    socket_timeout=2.0,         # ⚡ Chặn đứng hình khi gọi dữ liệu
    socket_connect_timeout=2.0  # ⚡ Chặn đứng hình khi kết nối
)

cap_hien_tai = next((cap for cap in config['danh_sach_cap'] if cap['id'] == args.pair_id), None)
if cap_hien_tai is None:
    print(f"❌ LỖI: Không tìm thấy ID {args.pair_id} trong {CONFIG_FILE}!")
    quit()

key_base = f"TICK:{cap_hien_tai['base_exchange'].upper()}:{cap_hien_tai['base_symbol'].upper()}"
key_diff = f"TICK:{cap_hien_tai['diff_exchange'].upper()}:{cap_hien_tai['diff_symbol'].upper()}"
key_state = f"STATE:MASTER:{args.pair_id}" 

dev_entry = cap_hien_tai['deviation_entry']
dev_close = cap_hien_tai['deviation_close']
stable_time_sec = cap_hien_tai['stable_time'] / 1000.0  
cooldown_close_sec = cap_hien_tai.get('cooldown_close_second', 2)
cooldown_sec = cap_hien_tai['cooldown_second']
max_orders = cap_hien_tai['max_orders']
hold_time_sec = cap_hien_tai.get('hold_time', 180)
alert_equity = cap_hien_tai.get('alert_equity', 0)
stable_mode = cap_hien_tai.get('stable_mode', 'freeze')
max_orphan_count = cap_hien_tai.get('max_orphan_count', 3) 
orphan_cooldown_second = cap_hien_tai.get('orphan_cooldown_second', 1800) 

# ==========================================
# 2. KHÔI PHỤC TRÍ NHỚ (SỔ CÁI) TỪ REDIS
# ==========================================
saved_state_raw = r.get(key_state)
if saved_state_raw:
    saved_state = json.loads(saved_state_raw)
    huong_dang_danh = saved_state.get("huong_dang_danh")
    lich_su_vao_lenh = saved_state.get("lich_su_vao_lenh", [])
    
    if len(lich_su_vao_lenh) > 0 and not isinstance(lich_su_vao_lenh[0], dict):
        print("🧹 Phát hiện Sổ Cái phiên bản cũ, tiến hành dọn dẹp để nâng cấp Sổ Kép!")
        lich_su_vao_lenh = []
        huong_dang_danh = None
        
    thoi_diem_vao_lenh_cuoi = saved_state.get("thoi_diem_vao_lenh_cuoi", 0)
    print(f"🧠 Đã khôi phục Sổ Cái: Gồng {len(lich_su_vao_lenh)} cặp lệnh đã ghép đôi.")
else:
    huong_dang_danh = None
    lich_su_vao_lenh = []
    thoi_diem_vao_lenh_cuoi = 0
    print("🧠 Bắt đầu với Sổ Cái trống rỗng.")

def luu_tri_nho():
    state = {
        "huong_dang_danh": huong_dang_danh,
        "lich_su_vao_lenh": lich_su_vao_lenh,
        "thoi_diem_vao_lenh_cuoi": thoi_diem_vao_lenh_cuoi
    }
    r.set(key_state, json.dumps(state))

def kiem_tra_gio_giao_dich(trading_hours, current_time_str):
    if not trading_hours: return True 
    for khung_gio in trading_hours:
        start, end = khung_gio.split('-')
        if start <= end:
            if start <= current_time_str <= end: return True
        else: 
            if current_time_str >= start or current_time_str <= end: return True
    return False

def kiem_tra_gio_cam(blackout_hours, current_time_str):
    if not blackout_hours: return False 
    for khung_gio in blackout_hours:
        start, end = khung_gio.split('-')
        if start <= end:
            if start <= current_time_str <= end: return True
        else: # Vắt qua đêm
            if current_time_str >= start or current_time_str <= end: return True
    return False

# ==========================================
# CÁC BIẾN QUẢN LÝ JS DEBOUNCE, WATCHDOG & CACHE
# ==========================================
last_base_msc = 0
last_diff_msc = 0
thoi_diem_nhan_tick_cuoi = 0
da_xu_ly_vao_lenh_cho_tick_nay = False
thoi_diem_vua_ra_lenh_dong = 0  

last_tick_base_raw = ""
last_tick_diff_raw = ""
last_pos_base_raw = ""
last_pos_diff_raw = ""
list_pos_base = []
list_pos_diff = []
tick_base = {"connected": False, "time_msc": 0} 
tick_diff = {"connected": False, "time_msc": 0}

local_nhan_base = time.time()
local_nhan_diff = time.time()

# --- Chống Spam Telegram ---
thoi_diem_spam_tram_cuoi = 0

print(f"🚀 MASTER {args.pair_id} SẴN SÀNG CHIẾN ĐẤU (SELF-HEALING + BLACKOUT GUILLOTINE)!")

# --- Cache Đồng Hồ ---
last_time_update = 0
current_utc_time_str = "00:00"

# --- Cầu Dao Chống Mồ Côi ---
dem_so_lan_mo_coi_lien_tiep = 0
thoi_diem_mo_khoa_cau_dao = 0

# --- Đồng Hồ Đếm Ngược Chênh Lệch Liên Tục ---
thoi_diem_bat_dau_lech_vao = 0
thoi_diem_bat_dau_lech_dong = 0

# ==========================================
# 3. VÒNG LẶP SUY NGHĨ CỦA MASTER
# ==========================================
try:
    while True:
        # 🛡️ ÁO GIÁP KEVLAR BỌC BÊN TRONG VÒNG LẶP
        try:
            time.sleep(0.001)
            now_sec = time.time() # time.time() cực kỳ nhẹ, không tốn CPU
            
            # ⚡ ĐỒNG HỒ CACHE: Cứ đúng 1 giây mới format chuỗi giờ UTC 1 lần
            if now_sec - last_time_update >= 1.0:
                current_utc_time_str = datetime.now(timezone.utc).strftime("%H:%M")
                last_time_update = now_sec

            # ========================================================
            # 👑 1. HOT RELOAD & CHECK GIỜ
            # ========================================================
            current_modified = os.path.getmtime(CONFIG_FILE)
            if current_modified != last_config_modified:
                time.sleep(0.05)
                try:
                    with open(CONFIG_FILE, 'r', encoding='utf-8') as f: config = json.load(f)
                    cap_hien_tai = next((cap for cap in config['danh_sach_cap'] if cap['id'] == args.pair_id), None)
                    if cap_hien_tai:
                        dev_entry = cap_hien_tai['deviation_entry']
                        dev_close = cap_hien_tai['deviation_close']
                        max_orders = cap_hien_tai['max_orders']
                        cooldown_sec = cap_hien_tai['cooldown_second']
                        cooldown_close_sec = cap_hien_tai.get('cooldown_close_second', 2)
                        hold_time_sec = cap_hien_tai.get('hold_time', 180)
                        stable_time_sec = cap_hien_tai['stable_time'] / 1000.0
                        max_tick_delay = cap_hien_tai.get('max_tick_delay', 5.0)
                        stable_mode = cap_hien_tai.get('stable_mode', 'freeze')
                        alert_equity = cap_hien_tai.get('alert_equity', 0)
                        max_orphan_count = cap_hien_tai.get('max_orphan_count', 3)          
                        orphan_cooldown_second = cap_hien_tai.get('orphan_cooldown_second', 1800) 
                    last_config_modified = current_modified
                    
                    vol_b = cap_hien_tai.get('volume_base', 0.01)
                    vol_d = cap_hien_tai.get('volume_diff', 0.01)
                    msg_reload = (
                        f"🔄 [HOT RELOAD] ĐÃ CẬP NHẬT THÔNG SỐ MỚI:\n"
                        f"   ├─ Chiến thuật : {stable_mode} {dev_entry}|{dev_close} | {stable_time_sec*1000:.0f}ms | Hold {hold_time_sec}s\n"
                        f"   ├─ Quản lý vốn : Cảnh báo EQ < {alert_equity}$ | Vol {vol_b}|{vol_d}\n"
                        f"   └─ Cầu dao     : Khóa {orphan_cooldown_second}s nếu mồ côi {max_orphan_count} lần"
                    )
                    print(msg_reload)

                    # 2. Chuỗi dành cho File Log (Nén lại thành 1 dòng duy nhất, không xuống dòng)
                    msg_reload_log = f"[HOT RELOAD] Lệch {dev_entry}|{dev_close}, Băng {stable_time_sec*1000:.0f}ms, Hold {hold_time_sec}s, EQ<{alert_equity}$, Cầu dao {max_orphan_count}x/{orphan_cooldown_second}s"
                    logging.info(msg_reload_log)
                except Exception as e:
                    pass

            # ⚡ Kiểm tra xem có đang bị vướng vào "Giờ Tử Thần" không?
            trong_gio_cam = kiem_tra_gio_cam(cap_hien_tai.get('force_close_hours', []), current_utc_time_str)

            # ========================================================
            # 🛡️ 2. CHECK WORKER ALIVE & DỊCH JSON SỔ SÁCH 
            # ========================================================
            key_pos_base = f"POSITION:{cap_hien_tai['base_exchange'].upper()}:{cap_hien_tai['base_symbol'].upper()}"
            key_pos_diff = f"POSITION:{cap_hien_tai['diff_exchange'].upper()}:{cap_hien_tai['diff_symbol'].upper()}"
            key_equity_base = f"ACCOUNT:{cap_hien_tai['base_exchange'].upper()}:EQUITY"
            key_equity_diff = f"ACCOUNT:{cap_hien_tai['diff_exchange'].upper()}:EQUITY"
            
            pos_base_raw, pos_diff_raw, tick_base_raw, tick_diff_raw, eq_base_raw, eq_diff_raw = r.mget(
                key_pos_base, key_pos_diff, key_base, key_diff, key_equity_base, key_equity_diff
            )
            
            if pos_base_raw is None or pos_diff_raw is None or tick_base_raw is None or tick_diff_raw is None:
                continue
                
            if pos_base_raw != last_pos_base_raw:
                try:
                    list_pos_base = json.loads(pos_base_raw) if pos_base_raw else []
                    if isinstance(list_pos_base, int): list_pos_base = [] 
                except Exception:
                    list_pos_base = []
                last_pos_base_raw = pos_base_raw 
                
            if pos_diff_raw != last_pos_diff_raw:
                try:
                    list_pos_diff = json.loads(pos_diff_raw) if pos_diff_raw else []
                    if isinstance(list_pos_diff, int): list_pos_diff = []
                except Exception:
                    list_pos_diff = []
                last_pos_diff_raw = pos_diff_raw

            so_lenh_base = len(list_pos_base)
            so_lenh_diff = len(list_pos_diff)

            equity_base = float(eq_base_raw) if eq_base_raw is not None else 999999.0
            equity_diff = float(eq_diff_raw) if eq_diff_raw is not None else 999999.0

            thoi_gian_tu_lan_vao_cuoi = time.time() - thoi_diem_vao_lenh_cuoi
            thoi_gian_tu_lan_dong_cuoi = time.time() - thoi_diem_vua_ra_lenh_dong
            trong_thoi_gian_bao_ve = (thoi_gian_tu_lan_vao_cuoi < 5.0) or (thoi_gian_tu_lan_dong_cuoi < 5.0)

            # XÓA TRÍ NHỚ AN TOÀN KHI THỊ TRƯỜNG SẠCH BÓNG LỆNH
            if so_lenh_base == 0 and so_lenh_diff == 0:
                if not trong_thoi_gian_bao_ve and (huong_dang_danh is not None or len(lich_su_vao_lenh) > 0):
                    huong_dang_danh = None
                    lich_su_vao_lenh.clear()
                    da_xu_ly_vao_lenh_cho_tick_nay = False 
                    luu_tri_nho() 
                    print("🧹 Đã dọn sạch Sổ Cái (2 sàn đều không còn lệnh).")

            # ========================================================
            # 🔗 3. ÔNG TƠ BÀ NGUYỆT: GHÉP CẶP TICKET THÔNG MINH
            # ========================================================
            base_tickets_on_exchange = [p['ticket'] for p in list_pos_base]
            diff_tickets_on_exchange = [p['ticket'] for p in list_pos_diff]

            paired_base_tickets = [p['base_ticket'] for p in lich_su_vao_lenh]
            paired_diff_tickets = [p['diff_ticket'] for p in lich_su_vao_lenh]

            unpaired_base = [p for p in list_pos_base if p['ticket'] not in paired_base_tickets]
            unpaired_diff = [p for p in list_pos_diff if p['ticket'] not in paired_diff_tickets]

            so_luong_co_the_ghep = min(len(unpaired_base), len(unpaired_diff))
            
            # ⚡ CẤM GHÉP CẶP LÚC TRỄ MẠNG HOẶC ĐANG TRONG GIỜ TỬ THẦN
            if so_luong_co_the_ghep > 0 and not trong_thoi_gian_bao_ve and not trong_gio_cam:
                unpaired_base.sort(key=lambda x: x['time_msc']) 
                unpaired_diff.sort(key=lambda x: x['time_msc'])
                
                for i in range(so_luong_co_the_ghep):
                    b = unpaired_base[i]
                    d = unpaired_diff[i]
                    lich_su_vao_lenh.append({
                        "id_cap": f"PAIR_{b['ticket']}_{d['ticket']}",
                        "base_ticket": b['ticket'],
                        "diff_ticket": d['ticket'],
                        "time_match": time.time()
                    })
                luu_tri_nho()
                # Khớp được lệnh ngon lành thì reset bộ đếm mồ côi về 0
                dem_so_lan_mo_coi_lien_tiep = 0
                print(f"💞 [SỔ CÁI] Ghép cặp thành công cho {so_luong_co_the_ghep} cặp lệnh mới!")
                
                # Cập nhật lại danh sách FA sau khi đã ghép
                unpaired_base = unpaired_base[so_luong_co_the_ghep:]
                unpaired_diff = unpaired_diff[so_luong_co_the_ghep:]

            # ========================================================
            # 🔪 4. BAO THANH THIÊN: XỬ TRẢM LỆNH TỰ ĐỘNG (SELF-HEALING)
            # ========================================================
            if not trong_thoi_gian_bao_ve:
                cac_cap_con_song = []
                co_lenh_bi_tram = False
                
                # --- TỘI 1: KHUYẾT 1 CHÂN (Do StopOut hoặc cắt tay) ---
                for cap in lich_su_vao_lenh:
                    base_alive = cap['base_ticket'] in base_tickets_on_exchange
                    diff_alive = cap['diff_ticket'] in diff_tickets_on_exchange
                    
                    if base_alive and diff_alive:
                        cac_cap_con_song.append(cap) # Sống đủ cặp
                    elif base_alive and not diff_alive:
                        msg = f"🚨 [Lệnh FA] Cặp {cap['id_cap']} khuyết Diff. Trảm Base #{cap['base_ticket']}!"
                        print(msg)
                        r.lpush(f"QUEUE:ORDER:{cap_hien_tai['base_exchange'].upper()}", json.dumps({"action": "CLOSE_BY_TICKET", "ticket": cap['base_ticket']}))
                        co_lenh_bi_tram = True
                        if time.time() - thoi_diem_spam_tram_cuoi > 60:
                            r.lpush("TELEGRAM_QUEUE", f"🔪 <b>AUTO-CUT (STOPOUT)</b>\n{msg}")
                    elif not base_alive and diff_alive:
                        msg = f"🚨 [Lệnh FA] Cặp {cap['id_cap']} khuyết Base. Trảm Diff #{cap['diff_ticket']}!"
                        print(msg)
                        r.lpush(f"QUEUE:ORDER:{cap_hien_tai['diff_exchange'].upper()}", json.dumps({"action": "CLOSE_BY_TICKET", "ticket": cap['diff_ticket']}))
                        co_lenh_bi_tram = True
                        if time.time() - thoi_diem_spam_tram_cuoi > 60:
                            r.lpush("TELEGRAM_QUEUE", f"🔪 <b>AUTO-CUT (STOPOUT)</b>\n{msg}")
                        
                if len(cac_cap_con_song) != len(lich_su_vao_lenh):
                    lich_su_vao_lenh = cac_cap_con_song # Xóa sổ vĩnh viễn cặp khuyết
                    luu_tri_nho()

                # --- TỘI 2: LỆNH LẠ MẶT (Lỗi vào xịt 1 bên, dư lệnh mồ côi, rớt cắt) ---
                if len(unpaired_base) > 0 or len(unpaired_diff) > 0:
                    # Tăng biến đếm mồ côi
                    dem_so_lan_mo_coi_lien_tiep += 1
                    
                    # NẾU ĐẠT LIMIT (Ví dụ 3 lần liên tiếp) -> SẬP CẦU DAO
                    if dem_so_lan_mo_coi_lien_tiep >= max_orphan_count:
                        thoi_diem_mo_khoa_cau_dao = time.time() + orphan_cooldown_second
                        msg_cau_dao = f"🔌 <b>[CẦU DAO] ĐÃ SẬP! KHÓA NÒNG {orphan_cooldown_second} GIÂY!</b>\nPhát hiện sàn bị lỗi Illiquidity (Mồ côi liên tục 3 lần). Tạm dừng chờ thanh khoản ổn hơn!"
                        print(f"🆘 {msg_cau_dao}")
                        r.lpush("TELEGRAM_QUEUE", msg_cau_dao)
                        dem_so_lan_mo_coi_lien_tiep = 0 # Reset đếm lại

                    for ub in unpaired_base:
                        msg = f"🚨 [MỒ CÔI {args.pair_id}] Lệnh lạ mặt Base #{ub['ticket']}! Trảm!"
                        print(msg)
                        r.lpush(f"QUEUE:ORDER:{cap_hien_tai['base_exchange'].upper()}", json.dumps({"action": "CLOSE_BY_TICKET", "ticket": ub['ticket']}))
                        co_lenh_bi_tram = True
                        if time.time() - thoi_diem_spam_tram_cuoi > 60:
                            r.lpush("TELEGRAM_QUEUE", f"🔪 <b>AUTO-CUT (ORPHAN)</b>\n{msg}")
                            
                    for ud in unpaired_diff:
                        msg = f"🚨 [MỒ CÔI {args.pair_id}] Lệnh lạ mặt Diff #{ud['ticket']}! Trảm!"
                        print(msg)
                        r.lpush(f"QUEUE:ORDER:{cap_hien_tai['diff_exchange'].upper()}", json.dumps({"action": "CLOSE_BY_TICKET", "ticket": ud['ticket']}))
                        co_lenh_bi_tram = True
                        if time.time() - thoi_diem_spam_tram_cuoi > 60:
                            r.lpush("TELEGRAM_QUEUE", f"🔪 <b>AUTO-CUT (ORPHAN)</b>\n{msg}")

                # Khởi động lại khiên bảo vệ nếu có vung đao
                if co_lenh_bi_tram:
                    thoi_diem_vua_ra_lenh_dong = time.time()
                    if time.time() - thoi_diem_spam_tram_cuoi > 60:
                        thoi_diem_spam_tram_cuoi = time.time()

            # ========================================================
            # 🚀 5. TỐI ƯU JSON CACHE & CHECK MSC ĐỘC LẬP
            # ========================================================
            co_tick_moi = False
            base_co_bien_dong = False
            diff_co_bien_dong = False

            if tick_base_raw != last_tick_base_raw:
                tick_base = json.loads(tick_base_raw)
                last_tick_base_raw = tick_base_raw
                base_co_bien_dong = True

            if tick_diff_raw != last_tick_diff_raw:
                tick_diff = json.loads(tick_diff_raw)
                last_tick_diff_raw = tick_diff_raw
                diff_co_bien_dong = True

            if base_co_bien_dong:
                if tick_base['time_msc'] > last_base_msc:
                    last_base_msc = tick_base['time_msc']
                    local_nhan_base = time.time() 
                    co_tick_moi = True

            if diff_co_bien_dong:
                if tick_diff['time_msc'] > last_diff_msc:
                    last_diff_msc = tick_diff['time_msc']
                    local_nhan_diff = time.time() 
                    co_tick_moi = True

            if co_tick_moi:
                thoi_diem_nhan_tick_cuoi = time.time() 
                da_xu_ly_vao_lenh_cho_tick_nay = False

            # ==========================================
            # 🛡️ 6. CHỐT CHẶN MẠNG & TICK THIU
            # ==========================================
            if not tick_base.get("connected", False) or not tick_diff.get("connected", False):
                print(f"⚠️ [MẤT MẠNG] -> KHÓA NÒNG!      ", end='\r')
                thoi_diem_nhan_tick_cuoi = time.time()
                da_xu_ly_vao_lenh_cho_tick_nay = True 
                continue

            now = time.time()
            tre_base = now - local_nhan_base
            tre_diff = now - local_nhan_diff
            
            if tre_base > max_tick_delay or tre_diff > max_tick_delay:
                print(f"⚠️ [ĐÓNG BĂNG] Quá {max_tick_delay}s k có giá mới! -> Hoãn!       ", end='\r')
                da_xu_ly_vao_lenh_cho_tick_nay = True 
                continue

            # ==========================================
            # ⛔ 6.5. MÁY CHÉM GIỜ CẤM (ĐÓNG PHIÊN/GIÃN SPREAD)
            # ==========================================
            if trong_gio_cam:
                if len(lich_su_vao_lenh) > 0:
                    print(f"🛑 [GIỜ CẤM] Vào khung giờ tử thần! XẢ TOÀN BỘ {len(lich_su_vao_lenh)} CẶP LỆNH!")
                    for cap in lich_su_vao_lenh:
                        r.lpush(f"QUEUE:ORDER:{cap_hien_tai['base_exchange'].upper()}", json.dumps({"action": "CLOSE_BY_TICKET", "ticket": cap['base_ticket'], "comment": "BLACKOUT_CUT"}))
                        r.lpush(f"QUEUE:ORDER:{cap_hien_tai['diff_exchange'].upper()}", json.dumps({"action": "CLOSE_BY_TICKET", "ticket": cap['diff_ticket'], "comment": "BLACKOUT_CUT"}))
                    
                    lich_su_vao_lenh.clear()
                    thoi_diem_vua_ra_lenh_dong = time.time()
                    da_xu_ly_vao_lenh_cho_tick_nay = True
                    luu_tri_nho()
                    
                    # Báo Telegram ngay lập tức khi xả
                    if time.time() - thoi_diem_spam_tram_cuoi > 60:
                        r.lpush("TELEGRAM_QUEUE", f"🛑 <b>GIỜ CẤM GIAO DỊCH</b>\nĐã kích hoạt máy chém, xả toàn bộ lệnh {args.pair_id} để né dãn Spread!")
                        thoi_diem_spam_tram_cuoi = time.time()
                
                # Ngăn không cho chạy xuống phần Quân sư tính toán vào lệnh mới
                continue

            # ==========================================
            # 🧠 7. GỌI QUÂN SƯ TÍNH TOÁN
            # ==========================================
            tin_hieu = check_tin_hieu_arbitrage(tick_base, tick_diff, cap_hien_tai, huong_dang_danh) 
            hanh_dong = tin_hieu["hanh_dong"]

            # ⏱️ LOGIC ĐỒNG HỒ CÁT (Tích lũy chênh lệch)
            if hanh_dong == "VAO_LENH":
                if thoi_diem_bat_dau_lech_vao == 0: thoi_diem_bat_dau_lech_vao = time.time()
                thoi_diem_bat_dau_lech_dong = 0 # Hủy đếm chốt lời
            elif hanh_dong == "DONG_LENH":
                if thoi_diem_bat_dau_lech_dong == 0: thoi_diem_bat_dau_lech_dong = time.time()
                thoi_diem_bat_dau_lech_vao = 0  # Hủy đếm vào lệnh
            else:
                # Giá lọt ra ngoài vùng lệch -> Reset toàn bộ đồng hồ về 0
                thoi_diem_bat_dau_lech_vao = 0
                thoi_diem_bat_dau_lech_dong = 0

            if not co_tick_moi:
                if len(lich_su_vao_lenh) == 0 and (hanh_dong != "VAO_LENH" or da_xu_ly_vao_lenh_cho_tick_nay):
                    continue

            # --------------------------------------------------
            # TRƯỜNG HỢP A: CÓ TÍN HIỆU ĐÓNG LỆNH CHỐT LỜI
            # --------------------------------------------------
            if hanh_dong == "DONG_LENH" and len(lich_su_vao_lenh) > 0:
                if (time.time() - thoi_diem_vua_ra_lenh_dong) >= cooldown_close_sec:
                    cap_du_tuoi = [cap for cap in lich_su_vao_lenh if (time.time() - cap['time_match']) >= hold_time_sec]
                    
                    if len(cap_du_tuoi) > 0:
                        # 🔄 CHỌN CHẾ ĐỘ BĂNG GIÁ
                        dk_thoi_gian = False
                        if stable_mode == 'continuous':
                            dk_thoi_gian = (time.time() - thoi_diem_bat_dau_lech_dong) >= stable_time_sec
                        else: # Mặc định là 'freeze'
                            dk_thoi_gian = (time.time() - thoi_diem_nhan_tick_cuoi) >= stable_time_sec

                        if dk_thoi_gian:
                            if not da_xu_ly_vao_lenh_cho_tick_nay: 
                                loai_dong = tin_hieu.get("loai_dong", "UNKNOWN") 
                                cap_bi_dong = cap_du_tuoi[0] 
                                chenh_lech_close = tin_hieu.get("chenh_lech", 0)
                                
                                msg_chot = f"💰 GIÁ BĂNG {stable_time_sec*1000:.0f}ms! TỈA LỜI CẶP {cap_bi_dong['id_cap']}. Lệch: {chenh_lech_close:.2f}."
                                print(msg_chot)
                                logging.info(msg_chot)
                                
                                close_comment = f"{cap_hien_tai.get('comment_close', '')} {loai_dong}".strip()
                                
                                r.lpush(f"QUEUE:ORDER:{cap_hien_tai['base_exchange'].upper()}", json.dumps({"action": "CLOSE_BY_TICKET", "ticket": cap_bi_dong['base_ticket'], "comment": close_comment}))
                                r.lpush(f"QUEUE:ORDER:{cap_hien_tai['diff_exchange'].upper()}", json.dumps({"action": "CLOSE_BY_TICKET", "ticket": cap_bi_dong['diff_ticket'], "comment": close_comment}))
                                                                                    
                                lich_su_vao_lenh.remove(cap_bi_dong)
                                thoi_diem_vua_ra_lenh_dong = time.time() 
                                da_xu_ly_vao_lenh_cho_tick_nay = True 
                                luu_tri_nho()

            # --------------------------------------------------
            # TRƯỜNG HỢP B: CÓ TÍN HIỆU VÀO LỆNH 
            # --------------------------------------------------
            elif hanh_dong == "VAO_LENH":
                # 🔌 CHECK CẦU DAO
                if time.time() < thoi_diem_mo_khoa_cau_dao:
                    thoi_gian_con_lai = int(thoi_diem_mo_khoa_cau_dao - time.time())
                    print(f"🔌 [CẦU DAO] Đang tạm khóa do thiếu hụt thanh khoản. Mở lại sau: {thoi_gian_con_lai}s   ", end='\r')
                    continue

                if equity_base < alert_equity or equity_diff < alert_equity:
                    print(f"🛑 [LOW EQUITY] KHÓA MỞ LỆNH MỚI! Base {equity_base:.2f}$ | Diff {equity_diff:.2f}$   ", end='\r')
                    continue 
                
                if not kiem_tra_gio_giao_dich(cap_hien_tai.get('trading_hours', []), current_utc_time_str): continue

                loai_lenh_moi = tin_hieu["loai_lenh"] 
                dang_dao_chieu_lien_thanh = (time.time() - thoi_diem_vua_ra_lenh_dong) < 2.0
                
                so_lenh_hien_tai = 0 if dang_dao_chieu_lien_thanh else len(lich_su_vao_lenh)
                dang_cooldown = (time.time() - thoi_diem_vao_lenh_cuoi) < cooldown_sec

                if so_lenh_hien_tai >= max_orders or (dang_cooldown and not dang_dao_chieu_lien_thanh) or (huong_dang_danh is not None and huong_dang_danh != loai_lenh_moi):
                    pass 
                else:
                    # 🔄 CHỌN CHẾ ĐỘ BĂNG GIÁ
                    dk_thoi_gian = False
                    if stable_mode == 'continuous':
                        dk_thoi_gian = (time.time() - thoi_diem_bat_dau_lech_vao) >= stable_time_sec
                    else: # Mặc định là 'freeze'
                        dk_thoi_gian = (time.time() - thoi_diem_nhan_tick_cuoi) >= stable_time_sec

                    if dk_thoi_gian:
                        if not da_xu_ly_vao_lenh_cho_tick_nay:
                            msg_vao = f"⚡ BÓP CÒ {loai_lenh_moi}! Lệch {tin_hieu['chenh_lech']:.2f}!!!"
                            print(msg_vao)
                            logging.info(msg_vao)

                            huong_dang_danh = loai_lenh_moi
                            order_comment = cap_hien_tai.get('comment_entry', '')
                            
                            r.lpush(f"QUEUE:ORDER:{cap_hien_tai['base_exchange'].upper()}", json.dumps({"action": tin_hieu["lenh_base"], "volume": cap_hien_tai.get('volume_base', 0.01), "comment": order_comment}))
                            r.lpush(f"QUEUE:ORDER:{cap_hien_tai['diff_exchange'].upper()}", json.dumps({"action": tin_hieu["lenh_diff"], "volume": cap_hien_tai.get('volume_diff', 0.01), "comment": order_comment}))
                            
                            thoi_diem_vao_lenh_cuoi = time.time() 
                            da_xu_ly_vao_lenh_cho_tick_nay = True 
                            luu_tri_nho()
                            
        # 🛑 NẾU CÓ BẤT KỲ LỖI NÀO XẢY RA TRONG QUÁ TRÌNH TÍNH TOÁN
        except Exception as e:
            msg_loi = f"🔥 [CRITICAL ERROR] Master bị vấp: {e}"
            print(f"\n{msg_loi}")
            logging.error(msg_loi, exc_info=True) # Ghi thẳng vào log kèm dòng code bị lỗi
            
            # Khởi động lại khiên bảo vệ để an toàn
            thoi_diem_nhan_tick_cuoi = time.time()
            da_xu_ly_vao_lenh_cho_tick_nay = True
            
            # Nghỉ ngơi 0.5s cho hệ thống hoàn hồn rồi chạy tiếp vòng lặp mới!
            time.sleep(0.5) 
            continue

except KeyboardInterrupt:
    print(f"\n🛑 [MASTER {args.pair_id}] Đã tắt an toàn!")
    logging.info(f"=== TẮT MASTER ===")