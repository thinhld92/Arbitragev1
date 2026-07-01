# import json
import ujson as json
import subprocess
import time
import os
import redis

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

try:
    redis_conf = config['redis']
    redis.Redis(
        host=redis_conf['host'],
        port=redis_conf['port'],
        db=redis_conf['db'],
        decode_responses=True,
        socket_timeout=2.0,
        socket_connect_timeout=2.0,
    ).delete("SIGNAL:SHUTDOWN")
except Exception as e:
    print(f"⚠️ Không xóa được SIGNAL:SHUTDOWN cũ trong Redis: {e}")

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

    # Gắn mác cho sàn thứ 3 (copy_diff, copy_base, copy_multi)
    trade_mode = str(cap.get('trade_mode', 'hedge')).strip().lower()
    if trade_mode in ('copy_diff', 'copy_base'):
        execution = cap.get('execution') or {}
        exec_exchange = str(execution.get('exchange', '')).strip().upper()
        exec_symbol = str(execution.get('symbol', '')).strip().upper()
        if exec_exchange and exec_symbol:
            e_key = (exec_exchange, exec_symbol)
            if e_key not in dict_workers:
                dict_workers[e_key] = "COPY_DIFF" if trade_mode == 'copy_diff' else "COPY_BASE"
    elif trade_mode == 'copy_multi':
        executions = cap.get('executions') or []
        for ex in executions:
            exec_exchange = str(ex.get('exchange', '')).strip().upper()
            exec_symbol = str(ex.get('symbol', '')).strip().upper()
            if exec_exchange and exec_symbol:
                e_key = (exec_exchange, exec_symbol)
                copy_side = str(ex.get('copy_side', 'diff')).strip().upper()
                role_str = "COPY_DIFF" if copy_side == "DIFF" else "COPY_BASE"
                if e_key not in dict_workers:
                    dict_workers[e_key] = role_str

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
    trade_mode = str(cap.get('trade_mode', 'hedge')).strip().lower()
    if trade_mode not in ('hedge', 'single', 'copy_diff', 'copy_base', 'copy_multi'):
        print(f"❌ trade_mode không hợp lệ cho {pair_id}: {trade_mode}")
        quit()
    if trade_mode == 'single':
        master_script = 'src/master_single.py'
    elif trade_mode == 'copy_diff':
        master_script = 'src/master_copy_diff.py'
    elif trade_mode == 'copy_base':
        master_script = 'src/master_copy_base.py'
    elif trade_mode == 'copy_multi':
        master_script = 'src/master_copy_multi.py'
    else:
        master_script = 'src/mastery.py'
    print(f"   👉 Đang gọi Master cho cặp: {pair_id} | mode={trade_mode} | script={master_script}")
    subprocess.Popen(
        ['cmd', '/k', 'python', master_script, '--pair_id', pair_id], 
        creationflags=subprocess.CREATE_NEW_CONSOLE
    )
    time.sleep(2)

# ==========================================
# 3. BẬT TERMINAL KẾ TOÁN TRƯỞNG (ACCOUNTANT)
# ==========================================
print("\n🧠 ĐANG ĐÁNH THỨC KẾ TOÁN TRƯỞNG (ACCOUNTANT CHUNG TỔNG HỢP)...")
command = 'start "KETOAN_TONG" cmd /k python src/accountant.py'
subprocess.Popen(command, shell=True)
time.sleep(2)

print("\n✅ QUẢN ĐỐC ĐÃ BỐ TRÍ XONG TOÀN BỘ NHÂN SỰ!")
print("👀 Hãy theo dõi các cửa sổ Terminal để xem hệ thống hoạt động.")
