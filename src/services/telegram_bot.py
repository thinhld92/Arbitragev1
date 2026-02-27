import redis
import requests
import json
import time
import sys
import os

# Lùi 1 bước từ 'services' ra 'src' để Python nhìn thấy thư mục 'utils'
thu_muc_src = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(thu_muc_src)

from utils.terminal import dan_tran_cua_so

os.system("title 📨 TELEGRAM SERVICE")
dan_tran_cua_so(1) # Telegram nằm tầng 1 (trên cùng)

print("📨 Khởi động Dịch vụ Telegram...")

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

print("✅ Đã kết nối Redis! Đang chờ tin nhắn...")

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
# 3. VÒNG LẶP CHỜ TIN NHẮN (BLPOP)
# ==========================================
try:
    while True:
        # Lấy tin nhắn (Sẽ đứng im chờ ở đây nếu hàng đợi rỗng, KHÔNG tốn CPU)
        queue_name, message = r.blpop(QUEUE_TELEGRAM, timeout=0)
        
        print(f"Đang gửi tin: {message.replace('<br>', '').replace('<b>', '').replace('</b>', '')[:50]}...")
        send_telegram_message(message)
        
        # Giãn cách xíu để không bị Telegram khóa vì spam quá nhanh (Giới hạn: ~30 tin/giây)
        time.sleep(0.1) 
        
except KeyboardInterrupt:
    print("\n🛑 Đã tắt Dịch vụ Telegram an toàn.")