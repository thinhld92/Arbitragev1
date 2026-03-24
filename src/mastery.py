import os
import redis
import ujson as json
import time
import argparse
import logging
import uuid
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

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(log_dir, f"log_master_{args.pair_id}.txt")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[RotatingFileHandler(log_filename, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')])

logging.info(f"=== KHỞI ĐỘNG MASTER BRAIN {args.pair_id} ===")

with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
    config = json.load(f)

# 🌟 LẤY TÊN VPS VÀ TẠO BIỂN TÊN CHO MASTER
vps_name = config.get('vps_name', 'LOCAL')
master_name = f"[{vps_name} | {args.pair_id}]"

# 👑 DÙNG WINDOWS API ĐỂ ĐỔI TÊN TERMINAL CHUẨN 100%
ctypes.windll.kernel32.SetConsoleTitleW(f"🧠 MASTER {master_name}")
dan_tran_cua_so(4)

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
key_pos_base = f"POSITION:{cap_hien_tai['base_exchange'].upper()}:{cap_hien_tai['base_symbol'].upper()}"
key_pos_diff = f"POSITION:{cap_hien_tai['diff_exchange'].upper()}:{cap_hien_tai['diff_symbol'].upper()}"
key_equity_base = f"ACCOUNT:{cap_hien_tai['base_exchange'].upper()}:EQUITY"
key_equity_diff = f"ACCOUNT:{cap_hien_tai['diff_exchange'].upper()}:EQUITY"
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
max_tick_delay = cap_hien_tai.get('max_tick_delay', 5.0)

# 👉 LẤY BỘ LỌC TỪ CONFIG
filter_entry = cap_hien_tai.get('filter_entry', 'nguoc') 
filter_close = cap_hien_tai.get('filter_close', 'none')

max_orphan_count = cap_hien_tai.get('max_orphan_count', 3) 
orphan_cooldown_second = cap_hien_tai.get('orphan_cooldown_second', 1800) 

# 🛡️ CẦU DAO TẦN SUẤT TICK
max_tick_hz_base = cap_hien_tai.get('max_tick_hz_base', 0)
max_tick_hz_diff = cap_hien_tai.get('max_tick_hz_diff', 0)

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
    # 👉 THÊM: Khôi phục "Ký ức lúc VÀO" của lệnh gần nhất
    last_entry_info = saved_state.get("last_entry_info", {"chenh_lech": 0, "tinh_chat": "UNKNOWN"}) 
    print(f"🧠 Đã khôi phục Sổ Cái: Gồng {len(lich_su_vao_lenh)} cặp lệnh đã ghép đôi.")
else:
    huong_dang_danh = None
    lich_su_vao_lenh = []
    thoi_diem_vao_lenh_cuoi = 0
    # 👉 THÊM: Tạo mới Ký ức
    last_entry_info = {"chenh_lech": 0, "tinh_chat": "UNKNOWN"}
    print("🧠 Bắt đầu với Sổ Cái trống rỗng.")

def luu_tri_nho():
    state = {
        "huong_dang_danh": huong_dang_danh,
        "lich_su_vao_lenh": lich_su_vao_lenh,
        "thoi_diem_vao_lenh_cuoi": thoi_diem_vao_lenh_cuoi,
        # 👉 THÊM: Lưu Ký ức này xuống đĩa cứng (Redis) để khởi động lại không bị mất
        "last_entry_info": last_entry_info 
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

# --- Khay Hứng Két Quả JOB_ID (UUID) ---
pending_jobs = {} # Format: {"JOB_XYZ": {"base_ticket": 123, "diff_ticket": None, "time": 12345678, "chenh_vao": 0, "tinh_chat_vao": ...}}
QUEUE_ORDER_RESULT = f"QUEUE:ORDER_RESULT:{args.pair_id}"

# --- Cầu Dao Chống Mồ Côi ---
dem_so_lan_mo_coi_lien_tiep = 0
thoi_diem_mo_khoa_cau_dao = 0

# --- Đồng Hồ Đếm Ngược Chênh Lệch Liên Tục ---
thoi_diem_bat_dau_lech_vao = 0
thoi_diem_bat_dau_lech_dong = 0
gia_base_luc_bat_dau_lech = 0.0       # 👉 BIẾN LƯU GIÁ ĐỂ ĐO TREND LÚC VÀO
gia_base_luc_bat_dau_lech_dong = 0.0  # 👉 BIẾN LƯU GIÁ ĐỂ ĐO TREND LÚC ĐÓNG

# ==========================================
# 3. VÒNG LẶP SUY NGHĨ CỦA MASTER
# ==========================================
last_config_check_time = 0
SHUTDOWN_KEY = "SIGNAL:SHUTDOWN"
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
            if now_sec - last_config_check_time >= 1.0:
                last_config_check_time = now_sec
                current_modified = os.path.getmtime(CONFIG_FILE)
                
                # 🛑 CHECK TÍN HIỆU TẮT BOT AN TOÀN
                if r.get(SHUTDOWN_KEY):
                    print(f"\n🛑 [SHUTDOWN] Nhận tín hiệu tắt bot an toàn! Ngưng hoạt động ngay...")
                    logging.info("[SHUTDOWN] Nhận tín hiệu SHUTDOWN từ Redis. Thoát an toàn.")
                    break
            else:
                current_modified = last_config_modified
                
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
                        
                        # 👉 HOT RELOAD LỌC
                        filter_entry = cap_hien_tai.get('filter_entry', 'nguoc')
                        filter_close = cap_hien_tai.get('filter_close', 'none')
                        
                        alert_equity = cap_hien_tai.get('alert_equity', 0)
                        max_orphan_count = cap_hien_tai.get('max_orphan_count', 3)          
                        orphan_cooldown_second = cap_hien_tai.get('orphan_cooldown_second', 1800) 
                        max_tick_hz_base = cap_hien_tai.get('max_tick_hz_base', 0)
                        max_tick_hz_diff = cap_hien_tai.get('max_tick_hz_diff', 0)
                    last_config_modified = current_modified
                    
                    vol_b = cap_hien_tai.get('volume_base', 0.01)
                    vol_d = cap_hien_tai.get('volume_diff', 0.01)
                    msg_reload = (
                        f"🔄 [HOT RELOAD] ĐÃ CẬP NHẬT THÔNG SỐ MỚI:\n"
                        f"   ├─ Chiến thuật : {stable_mode} {dev_entry}|{dev_close} | {stable_time_sec*1000:.0f}ms | Hold {hold_time_sec}s\n"
                        f"   ├─ Bộ lọc (In/Out): {filter_entry.upper()} / {filter_close.upper()}\n"
                        f"   ├─ Quản lý vốn : Cảnh báo EQ < {alert_equity}$ | Vol {vol_b}|{vol_d}\n"
                        f"   ├─ Cầu dao    : Khóa {orphan_cooldown_second}s nếu mồ côi {max_orphan_count} lần\n"
                        f"   └─ Tick Hz Max : Base {max_tick_hz_base} | Diff {max_tick_hz_diff} (0=tắt)"
                    )
                    print(msg_reload)

                    # 2. Chuỗi dành cho File Log (Nén lại thành 1 dòng duy nhất, không xuống dòng)
                    msg_reload_log = f"[HOT RELOAD] Lệch {dev_entry}|{dev_close}, Lọc In:{filter_entry}/Out:{filter_close}, Băng {stable_time_sec*1000:.0f}ms, Hold {hold_time_sec}s, EQ<{alert_equity}$, Cầu dao {max_orphan_count}x/{orphan_cooldown_second}s"
                    logging.info(msg_reload_log)
                except Exception as e:
                    pass

            # ⚡ Kiểm tra xem có đang bị vướng vào "Giờ Tử Thần" không?
            trong_gio_cam = kiem_tra_gio_cam(cap_hien_tai.get('force_close_hours', []), current_utc_time_str)

            # ========================================================
            # 🛡️ 2. CHECK WORKER ALIVE & DỊCH JSON SỔ SÁCH 
            # ========================================================
            
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

            thoi_gian_tu_lan_vao_cuoi = now_sec - thoi_diem_vao_lenh_cuoi
            thoi_gian_tu_lan_dong_cuoi = now_sec - thoi_diem_vua_ra_lenh_dong
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
            # 🔗 2.5. NHẬN KẾT QUẢ GIAO VIỆC (JOB_ID) TỪ WORKER
            # ========================================================
            # Rút toàn bộ thư trong hòm báo cáo
            while True:
                msg_raw = r.rpop(QUEUE_ORDER_RESULT)
                if not msg_raw: break
                
                try:
                    result_data = json.loads(msg_raw)
                    job_id = result_data.get("job_id")
                    role = result_data.get("role")
                    ticket = result_data.get("ticket")
                    
                    if job_id and role and ticket:
                        print(f"📮 [THƯ KÝ] Nhận hóa đơn Ticket #{ticket} từ {role} cho Job {job_id}")
                        if job_id not in pending_jobs:
                            # Khởi tạo bản nháp nếu đây là thư đến đầu tiên của Job này
                            pending_jobs[job_id] = {
                                "base_ticket": None, "diff_ticket": None, 
                                "time": time.time(), 
                                "chenh_vao": result_data.get("chenh_vao", 0),
                                "tinh_chat_vao": result_data.get("tinh_chat_vao", "UNKNOWN"),
                                # Kế toán
                                "tick_hz_base_in": result_data.get("tick_hz_base_in", 0),
                                "tick_hz_diff_in": result_data.get("tick_hz_diff_in", 0)
                            }
                            
                        # Điền số báo danh vào bản nháp
                        if role == "BASE": pending_jobs[job_id]["base_ticket"] = ticket
                        elif role == "DIFF": pending_jobs[job_id]["diff_ticket"] = ticket
                        
                        # 👉 KIỂM TRA ĐIỀN ĐỦ 2 CHỖ TRỐNG CHƯA?
                        if pending_jobs[job_id]["base_ticket"] is not None and pending_jobs[job_id]["diff_ticket"] is not None:
                            b_ticket = pending_jobs[job_id]["base_ticket"]
                            d_ticket = pending_jobs[job_id]["diff_ticket"]
                            
                            lich_su_vao_lenh.append({
                                "id_cap": f"PAIR_{job_id}", # Dùng luôn Job ID siêu duy nhất
                                "base_ticket": b_ticket,
                                "diff_ticket": d_ticket,
                                "time_match": time.time(),
                                "chenh_lech_vao": pending_jobs[job_id]["chenh_vao"],
                                "tinh_chat_vao": pending_jobs[job_id]["tinh_chat_vao"],
                                # Kế toán
                                "tick_hz_base_in": pending_jobs[job_id].get("tick_hz_base_in", 0),
                                "tick_hz_diff_in": pending_jobs[job_id].get("tick_hz_diff_in", 0)
                            })
                            luu_tri_nho()
                            dem_so_lan_mo_coi_lien_tiep = 0
                            print(f"💞 [GHÉP UUID] Ghép thành công 100% cặp lệnh {b_ticket} 💍 {d_ticket} (Job: {job_id})!")
                            del pending_jobs[job_id] # Xóa nháp
                except Exception as e:
                    print(f"⚠️ Lỗi đọc thư ký: {e}")

            # Dọn dẹp rác (Pending Jobs quá 60s mà vẫn mồ côi 1 bên)
            now_sec = time.time()
            expired_jobs = [jid for jid, jdata in pending_jobs.items() if now_sec - jdata["time"] > 60]
            for jid in expired_jobs:
                print(f"🗑️ [DỌN RÁC] Xóa Job {jid} do kẹt hóa đơn quá 60s!")
                del pending_jobs[jid]

            # ========================================================
            # 🔗 3. ÔNG TƠ BÀ NGUYỆT: GHÉP CẶP DỰ PHÒNG (FALLBACK TIME_MSC)
            # ========================================================
            # Tối ưu: Dùng set để tăng tốc độ truy xuất in (O(1) so với O(N))
            base_tickets_on_exchange = {p['ticket'] for p in list_pos_base}
            diff_tickets_on_exchange = {p['ticket'] for p in list_pos_diff}

            paired_base_tickets = {p['base_ticket'] for p in lich_su_vao_lenh}
            paired_diff_tickets = {p['diff_ticket'] for p in lich_su_vao_lenh}

            unpaired_base = [p for p in list_pos_base if p['ticket'] not in paired_base_tickets]
            unpaired_diff = [p for p in list_pos_diff if p['ticket'] not in paired_diff_tickets]

            so_luong_co_the_ghep = min(len(unpaired_base), len(unpaired_diff))
            
            # ⚡ CẤM GHÉP CẶP LÚC TRỄ MẠNG HOẶC ĐANG TRONG GIỜ TỬ THẦN
            if so_luong_co_the_ghep > 0 and not trong_thoi_gian_bao_ve and not trong_gio_cam:
                # 👉 Dùng .get() để an toàn nếu cấu trúc position dictionary thiếu key time
                unpaired_base.sort(key=lambda x: x.get('time_msc', x.get('time_update_msc', 0))) 
                unpaired_diff.sort(key=lambda x: x.get('time_msc', x.get('time_update_msc', 0)))
                
                for i in range(so_luong_co_the_ghep):
                    b = unpaired_base[i]
                    d = unpaired_diff[i]
                    lich_su_vao_lenh.append({
                        "id_cap": f"PAIR_{b['ticket']}_{d['ticket']}",
                        "base_ticket": b['ticket'],
                        "diff_ticket": d['ticket'],
                        "time_match": time.time(),
                        # 👉 THÊM: Lột tờ giấy nhớ dán vào Sổ cái
                        "chenh_lech_vao": last_entry_info.get("chenh_lech", 0),
                        "tinh_chat_vao": last_entry_info.get("tinh_chat", "UNKNOWN")
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
                        msg = f"🚨 [STOPOUT] Cặp {cap['id_cap']} mất Diff. Trảm nốt Base #{cap['base_ticket']}!"
                        print(msg)
                        
                        # 👉 GÓI KÝ ỨC DÀNH CHO CẢ CẶP (ĐỂ KẾ TOÁN ĐOÀN TỤ SỔ SÁCH)
                        context_data = {
                            "pair_token": cap['id_cap'], # Giữ nguyên ID cặp gốc để Kế toán ghép
                            "pair_id": args.pair_id,
                            "chenh_vao": cap.get('chenh_lech_vao', 0),
                            "mode_vao": cap.get('tinh_chat_vao', 'UNKNOWN'),
                            "chenh_dong": 0, 
                            "mode_dong": "[STOPOUT]", # Ghi sổ là chết do Stopout/Mất 1 chân
                            "action_type": "FORCE_CLOSE",
                            "tick_hz_base_in": cap.get("tick_hz_base_in", 0),
                            "tick_hz_diff_in": cap.get("tick_hz_diff_in", 0),
                            "tick_hz_base_out": tick_base.get("tick_hz", 0),
                            "tick_hz_diff_out": tick_diff.get("tick_hz", 0)
                        }
                        
                        pipe = r.pipeline()
                        # 1. Sai Worker BASE đi chém cái chân CÒN SỐNG
                        pipe.lpush(f"QUEUE:ORDER:{cap_hien_tai['base_exchange'].upper()}", json.dumps({
                            "action": "CLOSE_BY_TICKET", "ticket": cap['base_ticket'], "comment": "FA_CUT", "role": "BASE", "context": context_data
                        }))
                        
                        # 2. Sai Worker DIFF đi truy xuất lịch sử cái chân ĐÃ CHẾT
                        pipe.lpush(f"QUEUE:ORDER:{cap_hien_tai['diff_exchange'].upper()}", json.dumps({
                            "action": "FETCH_HISTORY_ONLY", "ticket": cap['diff_ticket'], "role": "DIFF", "context": context_data
                        }))
                        pipe.execute()
                        
                        co_lenh_bi_tram = True
                        
                        # 🛡️ BỌC THÉP CHỐNG SPAM TELEGRAM
                        if config.get('telegram', {}).get('enable', False):
                            if time.time() - thoi_diem_spam_tram_cuoi > 60:
                                r.lpush("TELEGRAM_QUEUE", f"🔪 <b>{master_name} - STOPOUT DETECTED</b>\n{msg}")
                                thoi_diem_spam_tram_cuoi = time.time() # Reset đồng hồ
                    elif not base_alive and diff_alive:
                        msg = f"🚨 [STOPOUT] Cặp {cap['id_cap']} chết Base. Trảm nốt Diff #{cap['diff_ticket']}!"
                        print(msg)
                        
                        # 👉 GÓI KÝ ỨC DÀNH CHO CẢ CẶP (KHÔNG DÙNG is_single_cut NỮA)
                        context_data = {
                            "pair_token": cap['id_cap'], # Giữ nguyên ID cặp gốc để Kế toán ghép
                            "pair_id": args.pair_id,
                            "chenh_vao": cap.get('chenh_lech_vao', 0),
                            "mode_vao": cap.get('tinh_chat_vao', 'UNKNOWN'),
                            "chenh_dong": 0, 
                            "mode_dong": "[STOPOUT]", # Ghi sổ là chết do Stopout
                            "action_type": "FORCE_CLOSE",
                            "tick_hz_base_in": cap.get("tick_hz_base_in", 0),
                            "tick_hz_diff_in": cap.get("tick_hz_diff_in", 0),
                            "tick_hz_base_out": tick_base.get("tick_hz", 0),
                            "tick_hz_diff_out": tick_diff.get("tick_hz", 0)
                        }
                        
                        pipe = r.pipeline()
                        # 1. Sai Worker DIFF đi chém cái chân còn sống
                        pipe.lpush(f"QUEUE:ORDER:{cap_hien_tai['diff_exchange'].upper()}", json.dumps({
                            "action": "CLOSE_BY_TICKET", "ticket": cap['diff_ticket'], "comment": "FA_CUT", "role": "DIFF", "context": context_data
                        }))
                        
                        # 2. Sai Worker BASE đi nhặt xác cái chân đã chết Stopout
                        pipe.lpush(f"QUEUE:ORDER:{cap_hien_tai['base_exchange'].upper()}", json.dumps({
                            "action": "FETCH_HISTORY_ONLY", "ticket": cap['base_ticket'], "role": "BASE", "context": context_data
                        }))
                        pipe.execute()
                        
                        co_lenh_bi_tram = True
                        if config.get('telegram', {}).get('enable', False) and (time.time() - thoi_diem_spam_tram_cuoi > 60):
                            r.lpush("TELEGRAM_QUEUE", f"🔪 <b>{master_name} - STOPOUT DETECTED</b>\n{msg}")
                            thoi_diem_spam_tram_cuoi = time.time()
                        
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
                        msg_cau_dao = f"🔌 <b>{master_name} - [CẦU DAO] ĐÃ SẬP! KHÓA NÒNG {orphan_cooldown_second} GIÂY!</b>\nPhát hiện sàn bị lỗi Illiquidity (Mồ côi liên tục 3 lần). Tạm dừng chờ thanh khoản ổn hơn!"
                        print(f"🆘 {msg_cau_dao}")
                        r.lpush("TELEGRAM_QUEUE", msg_cau_dao)
                        dem_so_lan_mo_coi_lien_tiep = 0 # Reset đếm lại

                    for ub in unpaired_base:
                        msg = f"🚨 [MỒ CÔI {args.pair_id}] Lệnh lạ mặt Base #{ub['ticket']}! Trảm!"
                        print(msg)
                        # 👉 TẠO GIẤY CHỨNG TỬ CHO LỆNH MỒ CÔI
                        context_data = {
                            "pair_token": f"ORPHAN_{ub['ticket']}",
                            "pair_id": args.pair_id,
                            "chenh_vao": 0,
                            "mode_vao": "[UNKNOWN]",
                            "chenh_dong": 0,
                            "mode_dong": "[ORPHAN_CUT]",
                            "action_type": "SINGLE_CLOSE",
                            "is_single_cut": True,
                            "tick_hz_base_in": 0,
                            "tick_hz_diff_in": 0,
                            "tick_hz_base_out": tick_base.get("tick_hz", 0),
                            "tick_hz_diff_out": tick_diff.get("tick_hz", 0)
                        }
                        r.lpush(f"QUEUE:ORDER:{cap_hien_tai['base_exchange'].upper()}", json.dumps({
                            "action": "CLOSE_BY_TICKET", "ticket": ub['ticket'], "comment": "ORPHAN_CUT", "role": "BASE", "context": context_data
                        }))
                        co_lenh_bi_tram = True
                        if time.time() - thoi_diem_spam_tram_cuoi > 60:
                            r.lpush("TELEGRAM_QUEUE", f"🔪 <b>{master_name} - AUTO-CUT (ORPHAN)</b>\n{msg}")
                            thoi_diem_spam_tram_cuoi = time.time()
                            
                    for ud in unpaired_diff:
                        msg = f"🚨 [MỒ CÔI {args.pair_id}] Lệnh lạ mặt Diff #{ud['ticket']}! Trảm!"
                        print(msg)
                        # 👉 TẠO GIẤY CHỨNG TỬ CHO LỆNH MỒ CÔI BÊN DIFF
                        context_data = {
                            "pair_token": f"ORPHAN_{ud['ticket']}",
                            "pair_id": args.pair_id,
                            "chenh_vao": 0,
                            "mode_vao": "[UNKNOWN]",
                            "chenh_dong": 0,
                            "mode_dong": "[ORPHAN_CUT]",
                            "action_type": "SINGLE_CLOSE",
                            "is_single_cut": True,
                            "tick_hz_base_in": 0,
                            "tick_hz_diff_in": 0,
                            "tick_hz_base_out": tick_base.get("tick_hz", 0),
                            "tick_hz_diff_out": tick_diff.get("tick_hz", 0)
                        }
                        r.lpush(f"QUEUE:ORDER:{cap_hien_tai['diff_exchange'].upper()}", json.dumps({
                            "action": "CLOSE_BY_TICKET", "ticket": ud['ticket'], "comment": "ORPHAN_CUT", "role": "DIFF", "context": context_data
                        }))
                        co_lenh_bi_tram = True
                        if time.time() - thoi_diem_spam_tram_cuoi > 60:
                            r.lpush("TELEGRAM_QUEUE", f"🔪 <b>{master_name} - AUTO-CUT (ORPHAN)</b>\n{msg}")
                            thoi_diem_spam_tram_cuoi = time.time()

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

            # 🛡️ CẦU DAO TẦN SUẤT TICK - Kiểm tra mật độ tick có vượt ngưỡng không
            hz_base = tick_base.get("tick_hz", 0)
            hz_diff = tick_diff.get("tick_hz", 0)
            tick_hz_vuot_nguong = (
                (max_tick_hz_base > 0 and hz_base > max_tick_hz_base) or
                (max_tick_hz_diff > 0 and hz_diff > max_tick_hz_diff)
            )

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
                    
                    pipe = r.pipeline()
                    # Dùng [:] để lặp qua bản sao của list, tránh lỗi khi đang lặp mà lại xóa phần tử
                    for cap in lich_su_vao_lenh[:]:
                        # 👉 TẠO KÝ ỨC DÀNH RIÊNG CHO CÚ CHÉM GIỜ CẤM ĐỂ KẾ TOÁN GHI SỔ
                        context_data = {
                            "pair_token": cap['id_cap'], # Giữ nguyên ID cặp gốc để Kế toán ghép đôi
                            "pair_id": args.pair_id,
                            "chenh_vao": cap.get('chenh_lech_vao', 0),
                            "mode_vao": cap.get('tinh_chat_vao', 'UNKNOWN'),
                            "chenh_dong": 0, # Giờ cấm thì chém bất chấp lệch giá
                            "mode_dong": "[BLACKOUT_CUT]", # Đánh dấu sẹo trên Excel là do Giờ cấm
                            "action_type": "BLACKOUT_CLOSE",
                            "tick_hz_base_in": cap.get("tick_hz_base_in", 0),
                            "tick_hz_diff_in": cap.get("tick_hz_diff_in", 0),
                            "tick_hz_base_out": tick_base.get("tick_hz", 0),
                            "tick_hz_diff_out": tick_diff.get("tick_hz", 0)
                        }

                        # Sai 2 thằng Worker xách đao đi chém kèm theo tờ giấy chứng tử
                        pipe.lpush(f"QUEUE:ORDER:{cap_hien_tai['base_exchange'].upper()}", json.dumps({
                            "action": "CLOSE_BY_TICKET", "ticket": cap['base_ticket'], "comment": "BLACKOUT", "role": "BASE", "context": context_data
                        }))
                        pipe.lpush(f"QUEUE:ORDER:{cap_hien_tai['diff_exchange'].upper()}", json.dumps({
                            "action": "CLOSE_BY_TICKET", "ticket": cap['diff_ticket'], "comment": "BLACKOUT", "role": "DIFF", "context": context_data
                        }))
                    pipe.execute()
                    
                    lich_su_vao_lenh.clear()
                    thoi_diem_vua_ra_lenh_dong = time.time()
                    da_xu_ly_vao_lenh_cho_tick_nay = True
                    luu_tri_nho()
                    
                    # Báo Telegram ngay lập tức khi xả
                    if time.time() - thoi_diem_spam_tram_cuoi > 60:
                        r.lpush("TELEGRAM_QUEUE", f"🛑 <b>{master_name} - GIỜ CẤM GIAO DỊCH</b>\nĐã kích hoạt máy chém, xả toàn bộ lệnh {args.pair_id} để né dãn Spread!")
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
                if thoi_diem_bat_dau_lech_vao == 0: 
                    thoi_diem_bat_dau_lech_vao = time.time()
                    gia_base_luc_bat_dau_lech = tick_base['bid'] # LƯU GIÁ ENTRY
                thoi_diem_bat_dau_lech_dong = 0 # Hủy đếm chốt lời
                gia_base_luc_bat_dau_lech_dong = 0.0 # Reset
                
            elif hanh_dong == "DONG_LENH":
                if thoi_diem_bat_dau_lech_dong == 0: 
                    thoi_diem_bat_dau_lech_dong = time.time()
                    gia_base_luc_bat_dau_lech_dong = tick_base['bid'] # LƯU GIÁ CLOSE
                thoi_diem_bat_dau_lech_vao = 0  # Hủy đếm vào lệnh
                gia_base_luc_bat_dau_lech = 0.0 # Reset
                
            else:
                # Giá lọt ra ngoài vùng lệch -> Reset toàn bộ đồng hồ về 0
                thoi_diem_bat_dau_lech_vao = 0
                thoi_diem_bat_dau_lech_dong = 0
                gia_base_luc_bat_dau_lech = 0.0 
                gia_base_luc_bat_dau_lech_dong = 0.0

            if not co_tick_moi:
                if len(lich_su_vao_lenh) == 0 and (hanh_dong != "VAO_LENH" or da_xu_ly_vao_lenh_cho_tick_nay):
                    continue

            # --------------------------------------------------
            # TRƯỜNG HỢP A: CÓ TÍN HIỆU ĐÓNG LỆNH CHỐT LỜI
            # --------------------------------------------------
            if hanh_dong == "DONG_LENH" and len(lich_su_vao_lenh) > 0:
                # 🛡️ CẦU DAO TICK HZ: Chặn đóng lệnh chốt lời khi tick quá dày
                if tick_hz_vuot_nguong:
                    print(f"⚡ [TICK HZ] Khóa chốt lời! Base {hz_base} | Diff {hz_diff} t/p (Max: {max_tick_hz_base}|{max_tick_hz_diff})   ", end='\r')
                elif (time.time() - thoi_diem_vua_ra_lenh_dong) >= cooldown_close_sec:
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
                                
                                # ==================================================
                                # 🛡️ BỘ LỌC ĐÓNG LỆNH (THUẬN / NGƯỢC / NONE)
                                # ==================================================
                                bo_qua_dong_lenh = False
                                ly_do_dong = ""
                                
                                # Chỉ kích hoạt lọc nếu đã lưu giá mốc VÀ chế độ khác 'none'
                                if gia_base_luc_bat_dau_lech_dong > 0 and filter_close != 'none':
                                    chenh_lech_gia_hien_tai = tick_base['bid'] - gia_base_luc_bat_dau_lech_dong
                                    lenh_dong_cua_thang_cham = "BUY" if huong_dang_danh == "TH2" else "SELL"
                                    
                                    if chenh_lech_gia_hien_tai > 0: # 🚀 GIÁ BASE ĐANG VỌT TĂNG
                                        if filter_close == 'thuan' and lenh_dong_cua_thang_cham == "BUY": 
                                            bo_qua_dong_lenh = True
                                            ly_do_dong = "[THUẬN] Giá TĂNG. Tránh chốt lệnh DIFF BUY đu đỉnh!"
                                        elif filter_close == 'nguoc' and lenh_dong_cua_thang_cham == "SELL":
                                            bo_qua_dong_lenh = True
                                            ly_do_dong = "[NGƯỢC] Giá TĂNG. Tránh chốt lệnh DIFF SELL!"
                                            
                                    elif chenh_lech_gia_hien_tai < 0: # 📉 GIÁ BASE ĐANG ĐỔ SẬP
                                        if filter_close == 'thuan' and lenh_dong_cua_thang_cham == "SELL":  
                                            bo_qua_dong_lenh = True
                                            ly_do_dong = "[THUẬN] Giá GIẢM. Tránh chốt lệnh DIFF SELL bán đáy!"
                                        elif filter_close == 'nguoc' and lenh_dong_cua_thang_cham == "BUY":
                                            bo_qua_dong_lenh = True
                                            ly_do_dong = "[NGƯỢC] Giá GIẢM. Tránh chốt lệnh DIFF BUY!"

                                if bo_qua_dong_lenh:
                                    print(f"🛡️ Hủy chốt lời! {ly_do_dong} (Quán tính: {chenh_lech_gia_hien_tai:.2f})")
                                    # Hủy đếm ngược, ép hệ thống chờ giá ổn định lại mới cho đóng
                                    thoi_diem_bat_dau_lech_dong = 0
                                    gia_base_luc_bat_dau_lech_dong = 0.0 
                                    continue
                                # ==================================================

                                loai_dong = tin_hieu.get("loai_dong", "UNKNOWN") 
                                cap_bi_dong = cap_du_tuoi[0]
                                chenh_lech_close = tin_hieu.get("chenh_lech", 0)
                                
                                # 👉 PHÂN TÍCH CHIẾN THUẬT LÚC ĐÓNG
                                loai_tinh_chat_dong = "[F]" 
                                if stable_mode == 'continuous' and (time.time() - thoi_diem_nhan_tick_cuoi) < stable_time_sec:
                                    loai_tinh_chat_dong = "[C]"

                                msg_chot = f"💰 GIÁ BĂNG {stable_time_sec*1000:.0f}ms! TỈA LỜI CẶP {cap_bi_dong['id_cap']} {loai_tinh_chat_dong}. Lệch: {chenh_lech_close:.2f}."
                                print(msg_chot)
                                logging.info(msg_chot)
                                
                                close_comment = f"{cap_hien_tai.get('comment_close', '')} {loai_dong}".strip()
                                
                                # 👉 ĐÓNG GÓI KÝ ỨC (CONTEXT) GỬI CHO WORKER
                                context_data = {
                                    "pair_token": cap_bi_dong['id_cap'],
                                    "pair_id": args.pair_id,
                                    "chenh_vao": cap_bi_dong.get('chenh_lech_vao', 0),
                                    "mode_vao": cap_bi_dong.get('tinh_chat_vao', 'UNKNOWN'),
                                    "chenh_dong": chenh_lech_close,
                                    "mode_dong": loai_tinh_chat_dong,
                                    "action_type": loai_dong,
                                    # Kẹp thêm thông số Accountant
                                    "tick_hz_base_in": cap_bi_dong.get("tick_hz_base_in", 0),
                                    "tick_hz_diff_in": cap_bi_dong.get("tick_hz_diff_in", 0),
                                    "tick_hz_base_out": tick_base.get("tick_hz", 0),
                                    "tick_hz_diff_out": tick_diff.get("tick_hz", 0)
                                }

                                pipe = r.pipeline()
                                # Gửi lệnh KÈM THEO CONTEXT và ROLE (Để Kế toán phân biệt Base/Diff)
                                pipe.lpush(f"QUEUE:ORDER:{cap_hien_tai['base_exchange'].upper()}", json.dumps({
                                    "action": "CLOSE_BY_TICKET", "ticket": cap_bi_dong['base_ticket'], "comment": close_comment, "role": "BASE", "context": context_data
                                }))
                                pipe.lpush(f"QUEUE:ORDER:{cap_hien_tai['diff_exchange'].upper()}", json.dumps({
                                    "action": "CLOSE_BY_TICKET", "ticket": cap_bi_dong['diff_ticket'], "comment": close_comment, "role": "DIFF", "context": context_data
                                }))
                                pipe.execute()
                                                                                                                                                                                                
                                lich_su_vao_lenh.remove(cap_bi_dong)
                                thoi_diem_vua_ra_lenh_dong = time.time() 
                                da_xu_ly_vao_lenh_cho_tick_nay = True

                                # 👉 BÍ KÍP ĐẢO CHIỀU TỨC THÌ (VÉ VIP 1 LẦN)
                                if len(lich_su_vao_lenh) == 0:
                                    thoi_diem_vao_lenh_cuoi = 0  # Xóa sạch Cooldown cũ
                                    huong_dang_danh = None       # Xóa luôn hướng cũ để được đánh ngược chiều
                                    print("🎁 Vừa chốt sạch lệnh! Reset Cooldown, sẵn sàng bắt sóng đảo chiều!")

                                luu_tri_nho()

            # --------------------------------------------------
            # TRƯỜNG HỢP B: CÓ TÍN HIỆU VÀO LỆNH 
            # --------------------------------------------------
            elif hanh_dong == "VAO_LENH":
                # 🛡️ CẦU DAO TICK HZ: Chặn vào lệnh khi tick quá dày
                if tick_hz_vuot_nguong:
                    print(f"⚡ [TICK HZ] Khóa vào lệnh! Base {hz_base} | Diff {hz_diff} t/p (Max: {max_tick_hz_base}|{max_tick_hz_diff})   ", end='\r')
                    continue

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
                
                # 👉 ĐẾM LỆNH VÀ KIỂM TRA COOLDOWN CHUẨN MỰC
                so_lenh_hien_tai = len(lich_su_vao_lenh)
                dang_cooldown = (time.time() - thoi_diem_vao_lenh_cuoi) < cooldown_sec

                if so_lenh_hien_tai >= max_orders or dang_cooldown or (huong_dang_danh is not None and huong_dang_danh != loai_lenh_moi):
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
                            
                            # ==================================================
                            # 🛡️ BỘ LỌC VÀO LỆNH (THUẬN / NGƯỢC / NONE)
                            # ==================================================
                            bo_qua_lenh_nay = False
                            ly_do = ""
                            
                            # Chỉ kích hoạt lọc nếu đã lưu giá mốc VÀ chế độ khác 'none'
                            if gia_base_luc_bat_dau_lech > 0 and filter_entry != 'none':
                                chenh_lech_gia_hien_tai = tick_base['bid'] - gia_base_luc_bat_dau_lech
                                lenh_cua_thang_cham = tin_hieu["lenh_diff"]
                                
                                if chenh_lech_gia_hien_tai > 0: # 🚀 GIÁ BASE ĐANG VỌT TĂNG
                                    if filter_entry == 'thuan' and lenh_cua_thang_cham == "BUY": 
                                        bo_qua_lenh_nay = True
                                        ly_do = "[THUẬN] Giá TĂNG. Tránh vào lệnh DIFF BUY đu đỉnh!"
                                    elif filter_entry == 'nguoc' and lenh_cua_thang_cham == "SELL":
                                        bo_qua_lenh_nay = True
                                        ly_do = "[NGƯỢC] Giá TĂNG. Tránh vào lệnh DIFF SELL!"
                                        
                                elif chenh_lech_gia_hien_tai < 0: # 📉 GIÁ BASE ĐANG ĐỔ SẬP
                                    if filter_entry == 'thuan' and lenh_cua_thang_cham == "SELL":  
                                        bo_qua_lenh_nay = True
                                        ly_do = "[THUẬN] Giá GIẢM. Tránh vào lệnh DIFF SELL bán đáy!"
                                    elif filter_entry == 'nguoc' and lenh_cua_thang_cham == "BUY":
                                        bo_qua_lenh_nay = True
                                        ly_do = "[NGƯỢC] Giá GIẢM. Tránh vào lệnh DIFF BUY!"

                            if bo_qua_lenh_nay:
                                print(f"🛡️ Hủy bóp cò {loai_lenh_moi}! {ly_do} (Quán tính: {chenh_lech_gia_hien_tai:.2f})")
                                # Hủy đếm ngược, ép hệ thống tìm lại điểm cân bằng an toàn
                                thoi_diem_bat_dau_lech_vao = 0
                                gia_base_luc_bat_dau_lech = 0.0 
                                continue
                            # ==================================================
                            
                            # 👉 PHÂN TÍCH CHIẾN THUẬT VÀ LƯU VÀO GIẤY NHỚ
                            loai_tinh_chat = "[F]"
                            if stable_mode == 'continuous' and (time.time() - thoi_diem_nhan_tick_cuoi) < stable_time_sec:
                                loai_tinh_chat = "[C]"
                                
                            last_entry_info = {"chenh_lech": tin_hieu['chenh_lech'], "tinh_chat": loai_tinh_chat}
                            
                            msg_vao = f"⚡ BÓP CÒ {loai_lenh_moi} {loai_tinh_chat}! Lệch {tin_hieu['chenh_lech']:.2f}!!!"
                            print(msg_vao)
                            logging.info(msg_vao)

                            huong_dang_danh = loai_lenh_moi
                            order_comment = cap_hien_tai.get('comment_entry', '')
                            
                            # 👉 TẠO TOKEN GIAO VIỆC SIÊU MẠNH (JOB ID)
                            job_uuid = str(uuid.uuid4()).split('-')[0].upper() # Lấy 8 ký tự VD: 8A4D9B2
                            job_id = f"J_{job_uuid}"
                            
                            pipe = r.pipeline()
                            
                            # Gửi lệnh KÈM THEO JOB_ID để YÊU CẦU WORKER REPORT BACK
                            context_vao = {
                                "job_id": job_id, "pair_id": args.pair_id,
                                "chenh_vao": tin_hieu['chenh_lech'], "tinh_chat_vao": loai_tinh_chat,
                                # Kẹp thêm thông số Accountant
                                "tick_hz_base_in": tick_base.get("tick_hz", 0),
                                "tick_hz_diff_in": tick_diff.get("tick_hz", 0)
                            }
                            
                            pipe.lpush(f"QUEUE:ORDER:{cap_hien_tai['base_exchange'].upper()}", json.dumps({
                                "action": tin_hieu["lenh_base"], "volume": cap_hien_tai.get('volume_base', 0.01), "comment": order_comment,
                                "role": "BASE", "context": context_vao
                            }))
                            pipe.lpush(f"QUEUE:ORDER:{cap_hien_tai['diff_exchange'].upper()}", json.dumps({
                                "action": tin_hieu["lenh_diff"], "volume": cap_hien_tai.get('volume_diff', 0.01), "comment": order_comment,
                                "role": "DIFF", "context": context_vao
                            }))
                            pipe.execute()
                            
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

print(f"👋 [MASTER {args.pair_id}] Đã thoát hoàn toàn.")