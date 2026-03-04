import MetaTrader5 as mt5
import redis
# import json
import ujson as json
import time
import argparse
import os
import threading 
from utils.terminal import dan_tran_cua_so

# ==========================================
# 1. ĐỌC THAM SỐ TỪ TERMINAL
# ==========================================
parser = argparse.ArgumentParser()
parser.add_argument("--broker", required=True, help="Tên sàn (VD: EXNESS)")
parser.add_argument("--symbol", required=True, help="Mã giao dịch (VD: BTCUSD)")
parser.add_argument("--role", default="WORKER", help="Vai trò của sàn này (BASE/DIFF)")
args = parser.parse_args()

os.system(f"title 👷‍♂️ {args.role} - {args.broker} - {args.symbol}")

if args.role == "BASE":
    dan_tran_cua_so(2)
elif args.role == "DIFF":
    dan_tran_cua_so(3)

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

# Tối ưu Redis
r = redis.Redis(host=redis_conf['host'], port=redis_conf['port'], db=redis_conf['db'], decode_responses=True, health_check_interval=30)

REDIS_TICK_KEY = f"TICK:{args.broker.upper()}:{args.symbol.upper()}"
REDIS_POS_KEY = f"POSITION:{args.broker.upper()}:{args.symbol.upper()}"
REDIS_EQUITY_KEY = f"ACCOUNT:{args.broker.upper()}:EQUITY"
QUEUE_ORDER_KEY = f"QUEUE:ORDER:{args.broker.upper()}"
QUEUE_TELEGRAM = "TELEGRAM_QUEUE"

mt5_lock = threading.Lock()

# ==========================================
# KHỞI TẠO KẾT NỐI MT5
# ==========================================
print(f"🚀 [{args.broker}] Đang kết nối tới MT5 tại: {mt5_path}")

if not mt5.initialize(path=mt5_path, portable=True, timeout=60000):
    print(f"❌ [{args.broker}] Khởi tạo MT5 thất bại! Mã lỗi: {mt5.last_error()}")
    quit()

# ==========================================
# 🛡️ FIX: QUÉT VÀ LƯU CACHE FILLING MODE MỘT LẦN DUY NHẤT
# ==========================================
mt5.symbol_select(args.symbol, True)
symbol_info = mt5.symbol_info(args.symbol)

if symbol_info is None:
    print(f"❌ [{args.broker}] Không tìm thấy mã {args.symbol} trên sàn. Vui lòng kiểm tra lại!")
    mt5.shutdown()
    quit()

# TỰ ĐỊNH NGHĨA HẰNG SỐ BỊ THIẾU CỦA THƯ VIỆN MT5 PYTHON
SYMBOL_FILLING_FOK = 1
SYMBOL_FILLING_IOC = 2

# Quét bitmask để xem sàn hỗ trợ kiểu khớp lệnh nào
filling_mode_bitmask = symbol_info.filling_mode
CACHED_FILLING_MODE = mt5.ORDER_FILLING_IOC # Đặt dự phòng

if filling_mode_bitmask & SYMBOL_FILLING_IOC:
    CACHED_FILLING_MODE = mt5.ORDER_FILLING_IOC
    ten_filling = "IOC (Khớp hoặc Hủy phần dư)"
elif filling_mode_bitmask & SYMBOL_FILLING_FOK:
    CACHED_FILLING_MODE = mt5.ORDER_FILLING_FOK
    ten_filling = "FOK (Khớp đủ hoặc Hủy toàn bộ)"
else:
    CACHED_FILLING_MODE = mt5.ORDER_FILLING_RETURN
    ten_filling = "RETURN"

print(f"✅ [{args.broker}] Kết nối thành công! Cấu hình Filling Mode: {ten_filling}")

# -----------------------------------------------------
# 🛡️ KIỂM TRA QUYỀN GIAO DỊCH TÀI KHOẢN (Check 1 lần)
# -----------------------------------------------------
acc_info = mt5.account_info()
if acc_info is not None:
    if not acc_info.trade_allowed:
        print(f"❌ [{args.broker}] LỖI: Tài khoản không được phép trade (Đang dùng Pass View?)")
        mt5.shutdown()
        quit()
    if not acc_info.trade_expert:
        print(f"❌ [{args.broker}] LỖI: Sàn chặn không cho phép dùng Bot (Algo Trading) trên tài khoản này!")
        mt5.shutdown()
        quit()
else:
    print(f"❌ [{args.broker}] Không lấy được thông tin tài khoản. Vui lòng kiểm tra lại đăng nhập!")
    mt5.shutdown()
    quit()

# ==========================================
# HÀM HỖ TRỢ: ĐÓNG 1 LỆNH (DÙNG CHO THREAD)
# ==========================================
def thuc_thi_dong_1_lenh(pos, current_tick, comment):
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
        "magic": 0,
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": CACHED_FILLING_MODE, # <--- SỬ DỤNG CACHE
    }
    if comment:
        request["comment"] = comment
    
    with mt5_lock: 
        result = mt5.order_send(request)
        
    if result.retcode == mt5.TRADE_RETCODE_DONE:
        print(f"💰 [{args.broker}] ĐÃ ĐÓNG LỆNH #{pos.ticket} THÀNH CÔNG.")
    else:
        print(f"❌ [{args.broker}] LỖI ĐÓNG LỆNH #{pos.ticket}: {result.comment}")
        r.lpush(QUEUE_TELEGRAM, f"❌ <b>[{args.broker}] LỖI ĐÓNG LỆNH</b>\nTicket: #{pos.ticket} | Lỗi: {result.comment}")

# ==========================================
# HÀM BÓP CÒ CHÍNH (PHÂN LOẠI LỆNH)
# ==========================================
def thuc_thi_chi_thi(chi_thi, current_tick):
    action = chi_thi.get("action")
    volume = float(chi_thi.get("volume", 0.01))
    comment = chi_thi.get("comment", "")

    if action in ["BUY", "SELL"]:
        is_buy = (action == "BUY")
        order_type = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
        price = current_tick.ask if is_buy else current_tick.bid

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": args.symbol,
            "volume": volume,
            "type": order_type,
            "price": price,
            "deviation": 20, 
            "magic": 0, 
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": CACHED_FILLING_MODE,  # <--- SỬ DỤNG CACHE
        }
        if comment:
            request["comment"] = comment
        
        with mt5_lock: 
            result = mt5.order_send(request)
            
        if result.retcode == mt5.TRADE_RETCODE_DONE:
            print(f"🔫 [{args.broker}] ĐÃ BẮN {action} {volume} LOT. Ticket: {result.order}")
        else:
            print(f"❌ [{args.broker}] LỖI VÀO LỆNH {action}: {result.comment} ({result.retcode})")
            r.lpush(QUEUE_TELEGRAM, f"❌ <b>[{args.broker}] LỖI {action}</b>\nMã lỗi: {result.retcode} - {result.comment}")

    elif action == "CLOSE_OLDEST":
        count = chi_thi.get("count", 1)
        positions = mt5.positions_get(symbol=args.symbol) 
        
        if positions:
            lenh_sap_xep = sorted(positions, key=lambda x: x.time_msc)
            lenh_can_dong = lenh_sap_xep[:count] 
            
            for pos in lenh_can_dong:
                threading.Thread(target=thuc_thi_dong_1_lenh, args=(pos, current_tick, comment)).start()

    # 👉 THÊM CHIÊU CHÉM ĐÍCH DANH VÀO DƯỚI CÙNG HÀM thuc_thi_chi_thi
    elif action == "CLOSE_BY_TICKET":
        ticket_can_dong = chi_thi.get("ticket")
        # Gọi MT5 tìm đúng cái lệnh có Ticket đó
        positions = mt5.positions_get(ticket=ticket_can_dong) 
        if positions:
            # Tìm thấy thì ném cho Thread phụ đi chém
            threading.Thread(target=thuc_thi_dong_1_lenh, args=(positions[0], current_tick, comment)).start()
        else:
            print(f"⚠️ [SỔ CÁI] Lệnh tử hình Ticket #{ticket_can_dong} thất bại do không tìm thấy lệnh trên sàn (Đã bị StopOut trước đó?)")

# ==========================================
# 3. VÒNG LẶP CHIẾN TRANH (MAIN LOOP)
# ==========================================
last_tick_time = 0
thoi_gian_check_mang_cuoi = 0
dang_co_mang = True 

thoi_gian_check_tk_cuoi = 0 
equity_canh_bao_da_gui = False

try:
    while True:
        now = time.time()
        
        # 🛡️ KIỂM TRA MẠNG VÀ NÚT ALGO TRADING
        if now - thoi_gian_check_mang_cuoi > 1.0:
            terminal_info = mt5.terminal_info()
            # Có mạng VÀ phải đang bật nút Algo Trading thì mới tính là OK
            dang_co_mang = terminal_info.connected and terminal_info.trade_allowed if terminal_info else False
            thoi_gian_check_mang_cuoi = now
            
            if terminal_info is None:
                print(f"⚠️ [{args.broker}] Mất kết nối nội bộ! Đang thử khởi tạo lại...")
                mt5.initialize(path=mt5_path, portable=True, timeout=10000)
            elif not terminal_info.trade_allowed:
                # Nếu lỡ quên bật, in cảnh báo đỏ rực trên màn hình Worker
                print(f"⛔ [{args.broker}] ĐẠI CA QUÊN BẬT NÚT 'ALGO TRADING' TRÊN MT5! Bot đang khóa nòng...", end='\r')

        # 📈 LẤY GIÁ VÀ CẬP NHẬT TICK
        tick = mt5.symbol_info_tick(args.symbol)

        if tick is not None:
            if tick.time_msc != last_tick_time:
                tick_data = {
                    "bid": tick.bid,
                    "ask": tick.ask,
                    "time_msc": tick.time_msc,
                    "connected": dang_co_mang 
                }
                r.set(REDIS_TICK_KEY, json.dumps(tick_data))
                
                trang_thai_mang = "OK" if dang_co_mang else "RỚT"
                print(f"[{args.broker}] BID: {tick.bid} | ASK: {tick.ask} | Mạng: {trang_thai_mang}", end='\r')
                
                last_tick_time = tick.time_msc
                
            thu_tu_master = r.rpop(QUEUE_ORDER_KEY)
            if thu_tu_master:
                chi_thi = json.loads(thu_tu_master)
                print(f"\n📨 [{args.broker}] Nhận lệnh từ Master: {chi_thi}")
                threading.Thread(target=thuc_thi_chi_thi, args=(chi_thi, tick)).start()
                
        else:
            mt5.symbol_select(args.symbol, True)
            time.sleep(1)
            
        # 🧮 CẬP NHẬT TÀI KHOẢN VÀ DANH SÁCH TICKET
        if now - thoi_gian_check_tk_cuoi > 0.2:
            positions = mt5.positions_get(symbol=args.symbol)
            if positions:
                # Trích xuất ticket và thời gian tạo lệnh thành mảng JSON
                danh_sach_ticket = [{"ticket": pos.ticket, "time_msc": pos.time_msc} for pos in positions]
            else:
                danh_sach_ticket = []
            
            # Đẩy nguyên mảng JSON lên Redis thay vì 1 con số
            r.set(REDIS_POS_KEY, json.dumps(danh_sach_ticket))
            
            acc_info = mt5.account_info()
            if acc_info:
                r.set(REDIS_EQUITY_KEY, acc_info.equity)
                
                if acc_info.equity < alert_equity and not equity_canh_bao_da_gui:
                    msg = f"⚠️ <b>[{args.broker}] CẢNH BÁO LOW EQUITY</b>\nTài khoản đang có {acc_info.equity:.2f}$, chạm mức cảnh báo ({alert_equity}$). Vui lòng nạp thêm tiền!"
                    r.lpush(QUEUE_TELEGRAM, msg)
                    print(f"\n{msg}")
                    equity_canh_bao_da_gui = True
                
                elif acc_info.equity > alert_equity + 10:
                    equity_canh_bao_da_gui = False
                    
            thoi_gian_check_tk_cuoi = now

        time.sleep(0.001)

except KeyboardInterrupt:
    print(f"\n🛑 [{args.broker}] Đã dừng an toàn.")
    mt5.shutdown()