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
from utils.terminal import dan_tran_cua_so

CONFIG_FILE = 'config.json'
last_config_modified = 0  

# ==========================================
# 1. KHỞI TẠO & ĐỌC CONFIG
# ==========================================
parser = argparse.ArgumentParser()
parser.add_argument("--pair_id", required=True)
args = parser.parse_args()

os.system(f"title 🧠 MASTER BRAIN - {args.pair_id}")
dan_tran_cua_so(4)

# --- TỰ ĐỘNG TẠO FOLDER LOGS NẾU CHƯA CÓ ---
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

log_filename = os.path.join(log_dir, f"log_master_{args.pair_id}.txt")

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

key_base = f"TICK:{cap_hien_tai['base_exchange'].upper()}:{cap_hien_tai['base_symbol'].upper()}"
key_diff = f"TICK:{cap_hien_tai['diff_exchange'].upper()}:{cap_hien_tai['diff_symbol'].upper()}"
key_state = f"STATE:MASTER:{args.pair_id}" 

dev_entry = cap_hien_tai['deviation_entry']
dev_close = cap_hien_tai['deviation_close']
stable_time_sec = cap_hien_tai['stable_time'] / 1000.0  
cooldown_close_sec = cap_hien_tai.get('cooldown_close_second', 2)
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

# ==========================================
# CÁC BIẾN QUẢN LÝ JS DEBOUNCE, WATCHDOG & CACHE
# ==========================================
last_base_msc = 0
last_diff_msc = 0

thoi_diem_nhan_tick_cuoi = 0
da_xu_ly_vao_lenh_cho_tick_nay = False
thoi_diem_vua_ra_lenh_dong = 0  

thoi_diem_phat_hien_lech_chan = 0 
thoi_gian_cho_phep_delay = 5.0  
thoi_diem_gui_canh_bao_cuoi = 0 
chu_ky_nhac_nho_lech_chan = 10.0  

# Bộ nhớ đệm JSON Cache
last_tick_base_raw = ""
last_tick_diff_raw = ""
tick_base = None
tick_diff = None

# Đồng hồ Watchdog báo động đứt mạng 3s
local_nhan_base = time.time()
local_nhan_diff = time.time()

print(f"🚀 MASTER {args.pair_id} SẴN SÀNG CHIẾN ĐẤU (DEBOUNCE + PIPELINING)!")
print("Bấm Ctrl + C để dừng Master an toàn!\n")

# ==========================================
# 3. VÒNG LẶP SUY NGHĨ CỦA MASTER
# ==========================================
try:
    while True:
        # Nhường CPU một xíu cho máy đỡ hú
        time.sleep(0.001)

        # ========================================================
        # 👑 1. HOT RELOAD 
        # ========================================================
        current_modified = os.path.getmtime(CONFIG_FILE)
        if current_modified != last_config_modified:
            time.sleep(0.05)
            try:
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    config = json.load(f)
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
                last_config_modified = current_modified
                
                vol_b = cap_hien_tai.get('volume_base', 0.01) 
                vol_d = cap_hien_tai.get('volume_diff', 0.01) 
                
                msg_reload = f"🔄 [HOT RELOAD] Lệch {dev_entry}|{dev_close}|{stable_time_sec*1000}ms, Max {max_orders}, Cooldown {cooldown_sec}s, Hold {hold_time_sec}s, Vol: {vol_b}/{vol_d}"
                print(msg_reload)
                logging.info(msg_reload)
            except Exception as e:
                print(f"⚠️ [LỖI ĐỌC CONFIG] Đã xảy ra lỗi khi Hot Reload: {e}")

        # ========================================================
        # 🛡️ 2. CHECK WORKER ALIVE (MGET kéo 4 Keys một lúc)
        # ========================================================
        key_pos_base = f"POSITION:{cap_hien_tai['base_exchange'].upper()}:{cap_hien_tai['base_symbol'].upper()}"
        key_pos_diff = f"POSITION:{cap_hien_tai['diff_exchange'].upper()}:{cap_hien_tai['diff_symbol'].upper()}"
        
        pos_base_raw, pos_diff_raw, tick_base_raw, tick_diff_raw = r.mget(key_pos_base, key_pos_diff, key_base, key_diff)
        
        if pos_base_raw is None or pos_diff_raw is None or tick_base_raw is None or tick_diff_raw is None:
            continue
            
        pos_base = int(pos_base_raw)
        pos_diff = int(pos_diff_raw)
        so_lenh_thuc_te = max(pos_base, pos_diff)

        # XÓA TRÍ NHỚ AN TOÀN KHI KHÔNG CÒN LỆNH
        if so_lenh_thuc_te == 0:
            if (time.time() - thoi_diem_vao_lenh_cuoi) > 5.0:
                if huong_dang_danh is not None or len(lich_su_vao_lenh) > 0:
                    huong_dang_danh = None
                    lich_su_vao_lenh.clear()
                    da_xu_ly_vao_lenh_cho_tick_nay = False 
                    luu_tri_nho() 
                    print("🧹 Cập nhật: Đã làm mới trí nhớ (Không còn lệnh nào trên sàn).")

        # BÁO ĐỘNG LỆCH CHÂN 
        if pos_base != pos_diff:
            if thoi_diem_phat_hien_lech_chan == 0:
                thoi_diem_phat_hien_lech_chan = time.time()
                thoi_diem_gui_canh_bao_cuoi = 0 
                
            elif (time.time() - thoi_diem_phat_hien_lech_chan) > thoi_gian_cho_phep_delay:
                if (time.time() - thoi_diem_gui_canh_bao_cuoi) > chu_ky_nhac_nho_lech_chan:
                    canh_bao = f"🚨 [BÁO ĐỘNG ĐỎ] LỆCH CHÂN!!! {cap_hien_tai['base_exchange']}: {pos_base} lệnh | {cap_hien_tai['diff_exchange']}: {pos_diff} lệnh."
                    print(canh_bao)
                    logging.warning(canh_bao)
                    
                    msg_canh_bao = f"🚨 <b>BÁO ĐỘNG LỆCH CHÂN ({args.pair_id})</b> 🚨\nBase: {pos_base} lệnh | Diff: {pos_diff} lệnh\n⚠️ Vui lòng xử lý gấp! Bot đang khóa nòng."
                    r.lpush("TELEGRAM_QUEUE", msg_canh_bao)
                    
                    thoi_diem_gui_canh_bao_cuoi = time.time()
                
                dang_trong_chu_ky_debounce = False
                continue
        else:
            thoi_diem_phat_hien_lech_chan = 0
            thoi_diem_gui_canh_bao_cuoi = 0
        
        # ========================================================
        # 🚀 3. TỐI ƯU JSON CACHE & CHECK MSC ĐỘC LẬP
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
            current_base_msc = tick_base['time_msc']
            if current_base_msc > last_base_msc:
                last_base_msc = current_base_msc
                local_nhan_base = time.time() 
                co_tick_moi = True

        if diff_co_bien_dong:
            current_diff_msc = tick_diff['time_msc']
            if current_diff_msc > last_diff_msc:
                last_diff_msc = current_diff_msc
                local_nhan_diff = time.time() 
                co_tick_moi = True

        if co_tick_moi:
            thoi_diem_nhan_tick_cuoi = time.time() 
            da_xu_ly_vao_lenh_cho_tick_nay = False

        # ==========================================
        # 🛡️ 4. CHỐT CHẶN MẠNG (TÍCH HỢP VỚI DEBOUNCE)
        # ==========================================
        mang_base_ok = tick_base.get("connected", False)
        mang_diff_ok = tick_diff.get("connected", False)

        if not mang_base_ok or not mang_diff_ok:
            print(f"⚠️ [MẤT MẠNG] Base: {'OK' if mang_base_ok else 'RỚT'} | Diff: {'OK' if mang_diff_ok else 'RỚT'} -> KHÓA NÒNG!      ", end='\r')
            thoi_diem_nhan_tick_cuoi = time.time()
            da_xu_ly_vao_lenh_cho_tick_nay = True 
            continue

        # ========================================================
        # 🛡️ 5. LỚP GIÁP BẢO VỆ CHỐNG TICK THIU (STALE QUOTE)
        # ========================================================
        now = time.time()
        tre_base = now - local_nhan_base
        tre_diff = now - local_nhan_diff
        
        if tre_base > max_tick_delay or tre_diff > max_tick_delay:
            san_bi_tre = []
            if tre_base > max_tick_delay: san_bi_tre.append(cap_hien_tai['base_exchange'])
            if tre_diff > max_tick_delay: san_bi_tre.append(cap_hien_tai['diff_exchange'])
            
            print(f"⚠️ [ĐÓNG BĂNG] Sàn {', '.join(san_bi_tre)} quá {max_tick_delay}s k có giá mới! -> Hoãn {stable_time_sec*1000:.0f}ms!      ", end='\r')
            da_xu_ly_vao_lenh_cho_tick_nay = True 
            continue

        # ==========================================
        # 🧠 6. GỌI QUÂN SƯ TÍNH TOÁN
        # ==========================================
        tin_hieu = check_tin_hieu_arbitrage(tick_base, tick_diff, cap_hien_tai, huong_dang_danh) 
        hanh_dong = tin_hieu["hanh_dong"]
        hold_time_sec = cap_hien_tai.get('hold_time', 180)

        # Chặn sớm nếu không có biến động
        if not co_tick_moi:
            if so_lenh_thuc_te == 0 and (hanh_dong != "VAO_LENH" or da_xu_ly_vao_lenh_cho_tick_nay):
                continue

        # Trí nhớ gồng lệnh
        if (time.time() - thoi_diem_vao_lenh_cuoi) > 5.0:
            if len(lich_su_vao_lenh) > so_lenh_thuc_te:
                if so_lenh_thuc_te > 0:
                    lich_su_vao_lenh = lich_su_vao_lenh[-so_lenh_thuc_te:]
                else:
                    lich_su_vao_lenh.clear()
                luu_tri_nho()

        # --------------------------------------------------
        # TRƯỜNG HỢP 1: CÓ TÍN HIỆU ĐÓNG LỆNH CHỐT LỜI
        # --------------------------------------------------
        if hanh_dong == "DONG_LENH" and so_lenh_thuc_te > 0:
            dang_cooldown_dong = (time.time() - thoi_diem_vua_ra_lenh_dong) < cooldown_close_sec
            
            if not dang_cooldown_dong:
                lenh_du_tuoi = [t for t in lich_su_vao_lenh if (time.time() - t) >= hold_time_sec]
                
                if len(lenh_du_tuoi) > 0:
                    thoi_gian_dung_im = time.time() - thoi_diem_nhan_tick_cuoi
                    
                    if thoi_gian_dung_im >= stable_time_sec:
                        if not da_xu_ly_vao_lenh_cho_tick_nay: 
                            chenh_lech_close = tin_hieu.get("chenh_lech", 0)
                            loai_dong = tin_hieu.get("loai_dong", "UNKNOWN") 
                            
                            msg_chot_loi = f"💰 GIÁ BĂNG {stable_time_sec*1000:.0f}ms! TỈA LỜI {loai_dong} - 1 LỆNH (Đợi {cooldown_close_sec}s). Lệch: {chenh_lech_close:.2f}."
                            print(msg_chot_loi)
                            logging.info(msg_chot_loi)
                            
                            base_comment = cap_hien_tai.get('comment_close', '')
                            close_comment = f"{base_comment} {loai_dong}".strip()
                            
                            chi_thi_dong = {"action": "CLOSE_OLDEST", "count": 1, "comment": close_comment}
                            r.lpush(f"QUEUE:ORDER:{cap_hien_tai['base_exchange'].upper()}", json.dumps(chi_thi_dong))
                            r.lpush(f"QUEUE:ORDER:{cap_hien_tai['diff_exchange'].upper()}", json.dumps(chi_thi_dong))
                                                                                
                            lenh_bi_dong = lenh_du_tuoi[0]
                            lich_su_vao_lenh.remove(lenh_bi_dong)
                            
                            thoi_diem_vua_ra_lenh_dong = time.time() 
                            da_xu_ly_vao_lenh_cho_tick_nay = True 
                            luu_tri_nho()

        # --------------------------------------------------
        # TRƯỜNG HỢP 2: CÓ TÍN HIỆU VÀO LỆNH (ÁP DỤNG JS DEBOUNCE)
        # --------------------------------------------------
        elif hanh_dong == "VAO_LENH":
            khung_gio_cho_phep = cap_hien_tai.get('trading_hours', [])
            if not kiem_tra_gio_giao_dich(khung_gio_cho_phep):
                continue

            loai_lenh_moi = tin_hieu["loai_lenh"] 

            dang_dao_chieu_lien_thanh = (time.time() - thoi_diem_vua_ra_lenh_dong) < 2.0
            so_lenh_hien_tai = 0 if dang_dao_chieu_lien_thanh else so_lenh_thuc_te
            dang_cooldown = (time.time() - thoi_diem_vao_lenh_cuoi) < cooldown_sec

            if so_lenh_hien_tai >= max_orders or (dang_cooldown and not dang_dao_chieu_lien_thanh) or (huong_dang_danh is not None and huong_dang_danh != loai_lenh_moi):
                pass 
            else:
                thoi_gian_dung_im = time.time() - thoi_diem_nhan_tick_cuoi
                
                if thoi_gian_dung_im >= stable_time_sec:
                    if not da_xu_ly_vao_lenh_cho_tick_nay:
                        chenh_lech = tin_hieu["chenh_lech"]
                        
                        if dang_dao_chieu_lien_thanh:
                            msg_vao_lenh = f"⚡ LIÊN THANH ĐẢO CHIỀU! Lệch {chenh_lech:.2f}. BÓP CÒ {loai_lenh_moi} NGAY LẬP TỨC!!!"
                        else:
                            msg_vao_lenh = f"🔥 GIÁ ĐÓNG BĂNG ĐỦ {stable_time_sec*1000:.0f}ms! Lệch {chenh_lech:.2f}. BÓP CÒ {loai_lenh_moi}!!!"
                            
                        print(msg_vao_lenh)
                        logging.info(msg_vao_lenh)

                        huong_dang_danh = loai_lenh_moi
                        volume_base = cap_hien_tai.get('volume_base', 0.01)
                        volume_diff = cap_hien_tai.get('volume_diff', 0.01)
                        order_comment = cap_hien_tai.get('comment_entry', '')
                        
                        chi_thi_base = {"action": tin_hieu["lenh_base"], "volume": volume_base, "comment": order_comment}
                        r.lpush(f"QUEUE:ORDER:{cap_hien_tai['base_exchange'].upper()}", json.dumps(chi_thi_base))
                        
                        chi_thi_diff = {"action": tin_hieu["lenh_diff"], "volume": volume_diff, "comment": order_comment}
                        r.lpush(f"QUEUE:ORDER:{cap_hien_tai['diff_exchange'].upper()}", json.dumps(chi_thi_diff))
                        
                        lich_su_vao_lenh.append(time.time())
                        thoi_diem_vao_lenh_cuoi = time.time() 
                        
                        da_xu_ly_vao_lenh_cho_tick_nay = True 
                        luu_tri_nho()

except KeyboardInterrupt:
    print(f"\n🛑 [MASTER {args.pair_id}] Đã tắt an toàn!")
    logging.info(f"=== TẮT MASTER {args.pair_id} ===")