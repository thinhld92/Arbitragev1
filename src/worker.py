import MetaTrader5 as mt5
import redis
import json
import time
import argparse
import os
import threading 

# ==========================================
# 1. ĐỌC THAM SỐ TỪ TERMINAL
# ==========================================
parser = argparse.ArgumentParser()
parser.add_argument("--broker", required=True, help="Tên sàn (VD: EXNESS)")
parser.add_argument("--symbol", required=True, help="Mã giao dịch (VD: BTCUSD)")
parser.add_argument("--role", default="WORKER", help="Vai trò của sàn này (BASE/DIFF)")
args = parser.parse_args()

os.system(f"title 👷‍♂️ {args.role} - {args.broker} - {args.symbol}")

# ==========================================
# 2. ĐỌC FILE CONFIG ĐỂ TÌM ĐƯỜNG DẪN
# ==========================================
try:
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
        
    mt5_path = config['brokers'][args.broker]['path']
    redis_conf = config['redis']
    
    cap_cfg = next((cap for cap in config['danh_sach_cap'] if 
                   (cap['base_exchange'] == args.broker and cap['base_symbol'] == args.symbol) or 
                   (cap['diff_exchange'] == args.broker and cap['diff_symbol'] == args.symbol)), None)
    
    alert_equity = cap_cfg.get('alert_equity', 0) if cap_cfg else 0

except KeyError:
    print(f"❌ Lỗi: Không tìm thấy cấu hình cho sàn {args.broker} trong config.json")
    quit()

r = redis.Redis(host=redis_conf['host'], port=redis_conf['port'], db=redis_conf['db'], decode_responses=True)

REDIS_TICK_KEY = f"TICK:{args.broker.upper()}:{args.symbol.upper()}"
REDIS_POS_KEY = f"POSITION:{args.broker.upper()}:{args.symbol.upper()}"
REDIS_EQUITY_KEY = f"ACCOUNT:{args.broker.upper()}:EQUITY"
QUEUE_ORDER_KEY = f"QUEUE:ORDER:{args.broker.upper()}"
QUEUE_TELEGRAM = "TELEGRAM_QUEUE"

# ==========================================
# KHỞI TẠO KẾT NỐI MT5
# ==========================================
print(f"🚀 [{args.broker}] Đang kết nối tới MT5 tại: {mt5_path}")

if not mt5.initialize(path=mt5_path, portable=True, timeout=60000):
    print(f"❌ [{args.broker}] Khởi tạo MT5 thất bại! Mã lỗi: {mt5.last_error()}")
    quit()

print(f"✅ [{args.broker}] Kết nối thành công! Sẵn sàng chiến đấu.")

# ==========================================
# HÀM HỖ TRỢ: ĐÓNG 1 LỆNH (DÙNG CHO THREAD)
# ==========================================
def thuc_thi_dong_1_lenh(pos, current_tick, comment):
    """Hàm này sẽ được ném cho từng thằng Lính đánh thuê chạy độc lập"""
    close_type = mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
    price = current_tick.bid if close_type == mt5.ORDER_TYPE_SELL else current_tick.ask
    
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": args.symbol,
        "volume": pos.volume,
        "type": close_type,
        "position": pos.ticket, 
        "price": price,
        "deviation": 20,
        "magic": 999999,
        "comment": comment,
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    
    result = mt5.order_send(request)
    if result.retcode == mt5.TRADE_RETCODE_DONE:
        print(f"💰 [{args.broker}] ĐÃ ĐÓNG LỆNH #{pos.ticket} THÀNH CÔNG.")
        # r.lpush(QUEUE_TELEGRAM, f"💰 <b>[{args.broker}] ĐÃ CHỐT LỜI</b>\nLệnh: #{pos.ticket}")
    else:
        print(f"❌ [{args.broker}] LỖI ĐÓNG LỆNH #{pos.ticket}: {result.comment}")
        r.lpush(QUEUE_TELEGRAM, f"❌ <b>[{args.broker}] LỖI ĐÓNG LỆNH</b>\nTicket: #{pos.ticket} | Lỗi: {result.comment}")

# ==========================================
# HÀM BÓP CÒ CHÍNH (PHÂN LOẠI LỆNH)
# ==========================================
def thuc_thi_chi_thi(chi_thi, current_tick):
    action = chi_thi.get("action")
    volume = float(chi_thi.get("volume", 0.01))
    comment = chi_thi.get("comment", "ban tia")
    
    acc_info = mt5.account_info()
    if not acc_info:
        print("⚠️ Không lấy được thông tin tài khoản MT5!")
        return
        
    # XỬ LÝ LỆNH BUY / SELL MỞ MỚI
    if action in ["BUY", "SELL"]:
        if acc_info.equity < alert_equity:
            msg = f"⚠️ <b>[{args.broker}] CẢNH BÁO LOW EQUITY</b>\nTài khoản đang có {acc_info.equity:.2f}$, chạm mức cảnh báo ({alert_equity}$). Vui lòng kiểm tra và nạp thêm tiền!\n<i>*Bot vẫn đang tiếp tục vào lệnh...</i>"
            r.lpush(QUEUE_TELEGRAM, msg)
            print(f"\n{msg}")

        order_type = mt5.ORDER_TYPE_BUY if action == "BUY" else mt5.ORDER_TYPE_SELL
        price = current_tick.ask if action == "BUY" else current_tick.bid

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": args.symbol,
            "volume": volume,
            "type": order_type,
            "price": price,
            "deviation": 20, 
            "magic": 999999, 
            "comment": comment,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC, 
        }
        
        result = mt5.order_send(request)
        if result.retcode == mt5.TRADE_RETCODE_DONE:
            print(f"🔫 [{args.broker}] ĐÃ BẮN {action} {volume} LOT. Ticket: {result.order}")
            # r.lpush(QUEUE_TELEGRAM, f"✅ <b>[{args.broker}] VÀO LỆNH {action}</b>\nMã: {args.symbol} | Vol: {volume}\nGiá: {price}")
        else:
            print(f"❌ [{args.broker}] LỖI VÀO LỆNH {action}: {result.comment} ({result.retcode})")
            r.lpush(QUEUE_TELEGRAM, f"❌ <b>[{args.broker}] LỖI {action}</b>\nMã lỗi: {result.retcode} - {result.comment}")

    # XỬ LÝ LỆNH ĐÓNG CÁC LỆNH CŨ (BẮN SONG SONG BẰNG THREAD)
    elif action == "CLOSE_OLDEST":
        count = chi_thi.get("count", 1)
        positions = mt5.positions_get(symbol=args.symbol)
        comment = chi_thi.get("comment", "Close ban tia")
        
        if positions:
            lenh_sap_xep = sorted(positions, key=lambda x: x.time_msc)
            lenh_can_dong = lenh_sap_xep[:count] 
            
            for pos in lenh_can_dong:
                # --- TUYỆT KỸ PHÂN THÂN CHUYỂN BÓNG ---
                # Thay vì tự tay đóng từng lệnh bắt nhau phải chờ, gọi thẳng 1 Thread ra đóng
                threading.Thread(target=thuc_thi_dong_1_lenh, args=(pos, current_tick, comment)).start()
                # Vòng lặp lướt qua với tốc độ 0.001ms, gọi ra đủ số Thread rồi kết thúc ngay!

# ==========================================
# 3. VÒNG LẶP CHIẾN TRANH (MAIN LOOP)
# ==========================================
last_tick_time = 0

try:
    while True:
        tick = mt5.symbol_info_tick(args.symbol)

        if tick is not None:
            if tick.time_msc != last_tick_time:
                tick_data = {
                    "bid": tick.bid,
                    "ask": tick.ask,
                    "time_msc": tick.time_msc
                }
                r.set(REDIS_TICK_KEY, json.dumps(tick_data))
                print(f"[{args.broker}] BID: {tick.bid} | ASK: {tick.ask}", end='\r')
                last_tick_time = tick.time_msc
                
            thu_tu_master = r.rpop(QUEUE_ORDER_KEY)
            if thu_tu_master:
                chi_thi = json.loads(thu_tu_master)
                print(f"\n📨 [{args.broker}] Nhận lệnh từ Master: {chi_thi}")
                
                # Gọi Thread tổng để nhận thư và xử lý phân loại
                threading.Thread(target=thuc_thi_chi_thi, args=(chi_thi, tick)).start()
                
        else:
            mt5.symbol_select(args.symbol, True)
            time.sleep(1)
            
        positions = mt5.positions_get(symbol=args.symbol)
        so_lenh = len(positions) if positions else 0
        r.set(REDIS_POS_KEY, so_lenh)
        
        acc_info = mt5.account_info()
        if acc_info:
            r.set(REDIS_EQUITY_KEY, acc_info.equity)

        time.sleep(0.001)

except KeyboardInterrupt:
    print(f"\n🛑 [{args.broker}] Đã dừng an toàn.")
    mt5.shutdown()