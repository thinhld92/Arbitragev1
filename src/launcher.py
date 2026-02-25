import json
import subprocess
import time
import os

# Đổi tên cửa sổ chính của Launcher cho ngầu
os.system("title 🚀 TRUNG TÂM CHỈ HUY - BOT ARBITRAGE")

print("🚀 ĐANG KHỞI ĐỘNG HỆ THỐNG SIÊU BOT ARBITRAGE...")

# Đọc config
try:
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
except Exception as e:
    print(f"❌ Lỗi đọc config.json: {e}")
    quit()

danh_sach_cap = config.get('danh_sach_cap', [])

# ==========================================
# 0. BẬT ĐƯỜNG DÂY NÓNG TELEGRAM ĐẦU TIÊN
# ==========================================
if config.get('telegram', {}).get('enable', False):
    print("📨 Đang gọi lính liên lạc: Telegram Service...")
    subprocess.Popen(
        ['cmd', '/k', 'python', 'src/services/telegram_bot.py'], 
        creationflags=subprocess.CREATE_NEW_CONSOLE
    )
    time.sleep(2) # Đợi Telegram bot khởi động xong

# ==========================================
# 1. TÍNH TOÁN & BẬT CÁC TERMINAL CHO WORKER
# ==========================================
# Dùng Dictionary để lưu trữ và phân loại vai trò (Tránh mở trùng Worker)
dict_workers = {}
for cap in danh_sach_cap:
    b_key = (cap['base_exchange'], cap['base_symbol'])
    d_key = (cap['diff_exchange'], cap['diff_symbol'])
    
    # Gắn mác cho Base
    if b_key not in dict_workers:
        dict_workers[b_key] = "BASE"
    elif dict_workers[b_key] == "DIFF":
        dict_workers[b_key] = "BASE/DIFF" # Sàn này đang làm cả 2 nhiệm vụ cho nhiều cặp khác nhau
        
    # Gắn mác cho Diff
    if d_key not in dict_workers:
        dict_workers[d_key] = "DIFF"
    elif dict_workers[d_key] == "BASE":
        dict_workers[d_key] = "BASE/DIFF"

print("\n👷‍♂️ ĐANG BỐ TRÍ CÁC TRINH SÁT (WORKER)...")
for (broker, symbol), role in dict_workers.items():
    print(f"   👉 Đang gọi {role} Worker: {broker} - {symbol}")
    subprocess.Popen(
        ['cmd', '/k', 'python', 'src/worker.py', '--broker', broker, '--symbol', symbol, '--role', role], 
        creationflags=subprocess.CREATE_NEW_CONSOLE
    )
    time.sleep(3) # Cực kỳ cần thiết: Chờ 3s cho MT5 load xong để tránh kẹt I/O

# ==========================================
# 2. BẬT CÁC TERMINAL CHO MASTER
# ==========================================
print("\n🧠 ĐANG ĐÁNH THỨC TƯỚNG QUÂN (MASTER)...")
for cap in danh_sach_cap:
    pair_id = cap['id']
    print(f"   👉 Đang gọi Master cho cặp: {pair_id}")
    subprocess.Popen(
        ['cmd', '/k', 'python', 'src/mastery.py', '--pair_id', pair_id], 
        creationflags=subprocess.CREATE_NEW_CONSOLE
    )
    time.sleep(2)

print("\n✅ QUẢN ĐỐC ĐÃ BỐ TRÍ XONG TOÀN BỘ NHÂN SỰ!")
print("👀 Hãy theo dõi các cửa sổ Terminal để xem hệ thống hoạt động.")