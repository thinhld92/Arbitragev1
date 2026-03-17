import redis
import requests
import json
# import ujson as json
import time
import sys
import os
import datetime

# Lùi 1 bước từ 'services' ra 'src' để Python nhìn thấy thư mục 'utils'
thu_muc_src = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(thu_muc_src)

from utils.terminal import dan_tran_cua_so

os.system("title 📨 TELEGRAM SERVICE")
dan_tran_cua_so(1) # Telegram nằm tầng 1 (trên cùng)

print("📨 Khởi động Dịch vụ Telegram (Phiên bản Chống Spam Toàn Diện)...")

# ==========================================
# 1. ĐỌC CẤU HÌNH
# ==========================================
try:
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)

    redis_conf = config['redis']
    tele_conf = config.get('telegram', {})
    
    # Kiểm tra xem có bật chức năng gửi không
    is_enabled = tele_conf.get('enable', False)
    bot_token = tele_conf.get('bot_token', '')
    chat_id = tele_conf.get('chat_id', '')

except Exception as e:
    print(f"❌ Lỗi đọc config: {e}")
    quit()

# Nếu trong config "enable": false -> Tắt bot
if not is_enabled or not bot_token or not chat_id:
    print("⚠️ Dịch vụ Telegram đang bị TẮT hoặc thiếu cấu hình trong config.json.")
    print("Vui lòng bật 'enable': true và cấu hình token/chat_id để sử dụng.")
    quit()

# Kết nối Redis
r = redis.Redis(host=redis_conf['host'], port=redis_conf['port'], db=redis_conf['db'], decode_responses=True)
QUEUE_TELEGRAM = "TELEGRAM_QUEUE"

print("✅ Đã kết nối Redis! Đang chờ thông báo lỗi...")

# ==========================================
# 2. HÀM GỬI TIN NHẮN API
# ==========================================
def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id, 
        "text": text, 
        "parse_mode": "HTML"
    }
    try:
        response = requests.post(url, json=payload, timeout=5)
        if response.status_code != 200:
            print(f"❌ Lỗi Telegram API: {response.text}")
    except Exception as e:
        print(f"❌ Lỗi kết nối mạng khi gửi Telegram: {e}")

# ==========================================
# 3. VÒNG LẶP CHỜ TIN NHẮN (THROTTLE + ANTI-SPAM)
# ==========================================
so_lan_gui = 0
thoi_gian_gui_cuoi = 0

try:
    while True:
        # 1. Chờ lấy lỗi đầu tiên (Sẽ đứng im ở đây nếu không có lỗi, KHÔNG tốn CPU)
        result = r.blpop(QUEUE_TELEGRAM, timeout=5)
        if result is None:
            continue  # 🛡️ POT-5 FIX: Tránh crash khi timeout hoặc mất kết nối Redis
        queue_name, first_msg = result
        
        now = time.time()
        
        # 2. Reset bộ đếm nếu hệ thống đã yên ắng được 2 phút (120 giây)
        if thoi_gian_gui_cuoi > 0 and (now - thoi_gian_gui_cuoi > 120):
            so_lan_gui = 0
            print("\n🔄 Hệ thống đã yên ắng hơn 2 phút, reset bộ đếm Telegram về 0.")
            
        # 3. Xác định thời gian giãn cách (Cooldown)
        if so_lan_gui < 3:
            cooldown = 10 # 3 lần đầu: Nghỉ 10 giây
        else:
            cooldown = 120 # Từ lần thứ 4 trở đi: Nghỉ 120 giây
            
        thoi_gian_da_troi_qua = now - thoi_gian_gui_cuoi
        
        # 4. Nếu lỗi đến quá nhanh, cho Bot ngủ chờ đủ thời gian Cooldown
        if so_lan_gui > 0 and thoi_gian_da_troi_qua < cooldown:
            thoi_gian_cho = cooldown - thoi_gian_da_troi_qua
            print(f"⏳ Bot đang chặn Spam: Tạm chờ {thoi_gian_cho:.1f}s nữa mới xử lý tiếp...")
            time.sleep(thoi_gian_cho)
            
        # 5. Ngủ xong, Vét cạn hàng đợi (Gom hết các lỗi bị dồn lại trên Redis trong lúc ngủ)
        danh_sach_tin = [first_msg]
        while True:
            extra_msg = r.lpop(QUEUE_TELEGRAM)
            if extra_msg:
                # Lọc sơ bộ: Nếu lỗi này giống y hệt lỗi trước thì KHÔNG gom thêm vào cho đỡ dài
                if extra_msg not in danh_sach_tin:
                    danh_sach_tin.append(extra_msg)
                
                # Tránh gom quá nhiều làm tin nhắn quá dài (Telegram cấm gửi > 4096 ký tự)
                if len(danh_sach_tin) >= 10:
                    break
            else:
                break
                
        # 6. Đóng gói nội dung
        if len(danh_sach_tin) == 1:
            final_text = danh_sach_tin[0]
        else:
            final_text = f"📦 <b>[GOM {len(danh_sach_tin)} CẢNH BÁO CÙNG LÚC]</b>\n\n" + "\n➖➖➖➖\n".join(danh_sach_tin)
            
        # ==========================================
        # 🪄 THÊM GIA VỊ ĐỂ LÁCH LUẬT ANTI-SPAM
        # ==========================================
        # Tạo chuỗi thời gian hiện tại
        thoi_gian_thuc = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Tạo một ID ngẫu nhiên nhỏ từ thời gian thực
        ma_bam = f"#{so_lan_gui + 1}_{int(time.time() * 1000) % 10000}" 
        
        # Đóng mộc vào cuối tin nhắn
        final_text += f"\n\n🕒 <i>{thoi_gian_thuc} | MsgID: {ma_bam}</i>"
        # ==========================================

        # Hiển thị log ra màn hình Terminal
        trich_doan = first_msg.replace('<br>', '').replace('<b>', '').replace('</b>', '')[:40]
        print(f"🚀 Đang gửi báo cáo Lần {so_lan_gui + 1} (Gom {len(danh_sach_tin)} tin): {trich_doan}...")
        
        send_telegram_message(final_text)
        
        # 7. Cập nhật lại thời gian và số lần gửi
        so_lan_gui += 1
        thoi_gian_gui_cuoi = time.time()
        
except KeyboardInterrupt:
    print("\n🛑 Đã tắt Dịch vụ Telegram an toàn.")