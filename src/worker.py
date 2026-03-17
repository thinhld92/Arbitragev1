import MetaTrader5 as mt5
import redis
import ujson as json
import time
import argparse
import os
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
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
    
    # 🌟 LẤY TÊN VPS VÀ GẮN BIỂN SỐ CHO WORKER
    vps_name = config.get('vps_name', 'LOCAL') 
    bot_name = f"[{vps_name} | {args.broker} | {args.symbol}]"

except KeyError:
    print(f"❌ Lỗi: Không tìm thấy cấu hình cho sàn {args.broker} trong config.json")
    quit()

# Tối ưu Redis
r = redis.Redis(host=redis_conf['host'], port=redis_conf['port'], db=redis_conf['db'], decode_responses=True, health_check_interval=30)

# 🛡️ Kiểm tra kết nối Redis ngay lập tức (Fail-fast)
try:
    r.ping()
except redis.ConnectionError:
    print(f"❌ Không kết nối được Redis tại {redis_conf['host']}:{redis_conf['port']}! Hãy kiểm tra Redis server.")
    quit()

REDIS_TICK_KEY = f"TICK:{args.broker.upper()}:{args.symbol.upper()}"
REDIS_POS_KEY = f"POSITION:{args.broker.upper()}:{args.symbol.upper()}"
REDIS_EQUITY_KEY = f"ACCOUNT:{args.broker.upper()}:EQUITY"
QUEUE_ORDER_KEY = f"QUEUE:ORDER:{args.broker.upper()}"
QUEUE_TELEGRAM = "TELEGRAM_QUEUE"

mt5_lock = threading.Lock()
executor = ThreadPoolExecutor(max_workers=5) # ⚡ Sử dụng Pool 5 chiến binh túc trực

# ==========================================
# KHỞI TẠO KẾT NỐI MT5
# ==========================================
print(f"🚀 {bot_name} Đang kết nối tới MT5 tại: {mt5_path}")

if not mt5.initialize(path=mt5_path, portable=True, timeout=60000):
    print(f"❌ {bot_name} Khởi tạo MT5 thất bại! Mã lỗi: {mt5.last_error()}")
    quit()

# ==========================================
# 🛡️ FIX: QUÉT VÀ LƯU CACHE FILLING MODE MỘT LẦN DUY NHẤT
# ==========================================
mt5.symbol_select(args.symbol, True)
symbol_info = mt5.symbol_info(args.symbol)

if symbol_info is None:
    print(f"❌ {bot_name} Không tìm thấy mã {args.symbol} trên sàn. Vui lòng kiểm tra lại!")
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

print(f"✅ {bot_name} Kết nối thành công! Cấu hình Filling Mode: {ten_filling}")

# -----------------------------------------------------
# 🛡️ KIỂM TRA QUYỀN GIAO DỊCH TÀI KHOẢN (Check 1 lần)
# -----------------------------------------------------
acc_info = mt5.account_info()
if acc_info is not None:
    if not acc_info.trade_allowed:
        print(f"❌ {bot_name} LỖI: Tài khoản không được phép trade (Đang dùng Pass View?)")
        mt5.shutdown()
        quit()
    if not acc_info.trade_expert:
        print(f"❌ {bot_name} LỖI: Sàn chặn không cho phép dùng Bot (Algo Trading) trên tài khoản này!")
        mt5.shutdown()
        quit()
else:
    print(f"❌ {bot_name} Không lấy được thông tin tài khoản. Vui lòng kiểm tra lại đăng nhập!")
    mt5.shutdown()
    quit()

# ==========================================
# HÀM HỖ TRỢ: ĐÓNG 1 LỆNH (DÙNG CHO THREAD)
# ==========================================
def thuc_thi_dong_1_lenh(pos, current_tick, comment, chi_thi):
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
        "type_filling": CACHED_FILLING_MODE,
    }
    if comment: request["comment"] = comment
    
    with mt5_lock: 
        result = mt5.order_send(request)
        
    if result.retcode == mt5.TRADE_RETCODE_DONE:
        print(f"💰 {bot_name} ĐÃ ĐÓNG LỆNH #{pos.ticket}. Đang đợi sàn chốt sổ...")
        
        # 👉 VÒNG LẶP SĂN MỒI CHỜ LỊCH SỬ (Tối đa 5 giây)
        da_chot_so = False
        deals = []
        for _ in range(25): # 25 lần x 0.2s = 5 giây
            time.sleep(0.2)
            deals = mt5.history_deals_get(position=pos.ticket)
            if deals:
                # Kiểm tra xem trong lịch sử đã có cái deal ĐÓNG LỆNH (OUT) chưa
                co_deal_out = any(d.entry in [mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_OUT_BY] for d in deals)
                if co_deal_out:
                    da_chot_so = True
                    break # Chốt sổ xong rồi, đập vỡ đồng hồ thoát ra thôi!
                    
        if da_chot_so and deals:
            # Bòn rút tiền thật, phí thật, giá thật
            tong_profit = sum(d.profit for d in deals)
            tong_fee = sum(d.commission + d.swap for d in deals)
            gia_vao = next((d.price for d in deals if d.entry == mt5.DEAL_ENTRY_IN), 0)
            gia_ra = next((d.price for d in deals if d.entry in [mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_OUT_BY]), 0)

            # Đóng gói Biên lai gửi cho Kế toán
            bien_lai = {
                "role": chi_thi.get("role", "UNKNOWN"),
                "ticket": pos.ticket,
                "volume": pos.volume,
                "profit": tong_profit,
                "fee": tong_fee,
                "open_price": gia_vao,
                "close_price": gia_ra,
                "context": chi_thi.get("context", {}) 
            }
            r.lpush("QUEUE:ACCOUNTANT", json.dumps(bien_lai))
        else:
            print(f"⚠️ {bot_name} Báo động: Lệnh #{pos.ticket} đã đóng nhưng MT5 không nhả lịch sử sau 5s!")
            
    else:
        print(f"❌ {bot_name} LỖI ĐÓNG LỆNH #{pos.ticket}: {result.comment}")
        r.lpush(QUEUE_TELEGRAM, f"❌ <b>{bot_name} LỖI ĐÓNG LỆNH</b>\nTicket: #{pos.ticket} | Lỗi: {result.comment}")

# ==========================================
# HÀM HỖ TRỢ: ĐỒNG BỘ LỊCH SỬ (CHO LỆNH STOPOUT)
# ==========================================
def thuc_thi_dong_bo_lich_su(chi_thi):
    ticket = chi_thi.get("ticket")
    print(f"🔍 {bot_name} Đang truy xuất dữ liệu lịch sử của lệnh #{ticket}...")
    
    # Không cần đợi vì sàn đã đóng lệnh từ trước
    deals = mt5.history_deals_get(position=ticket)
    if deals:
        tong_profit = sum(d.profit for d in deals)
        tong_fee = sum(d.commission + d.swap for d in deals)
        gia_vao = next((d.price for d in deals if d.entry == mt5.DEAL_ENTRY_IN), 0)
        gia_ra = next((d.price for d in deals if d.entry in [mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_OUT_BY]), 0)

        bien_lai = {
            "role": chi_thi.get("role", "UNKNOWN"),
            "ticket": ticket,
            "volume": deals[0].volume if deals else 0,
            "profit": tong_profit,
            "fee": tong_fee,
            "open_price": gia_vao,
            "close_price": gia_ra,
            "context": chi_thi.get("context", {}) 
        }
        r.lpush("QUEUE:ACCOUNTANT", json.dumps(bien_lai))
        print(f"✅ Đã gửi hồ sơ đối soát của #{ticket} cho Kế toán!")
    else:
        print(f"⚠️ Không tìm thấy dữ liệu lịch sử của #{ticket} trên MT5!")

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
            print(f"🔫 {bot_name} ĐÃ BẮN {action} {volume} LOT. Ticket: {result.order}")
            
            # 👉 BÁO CÁO KẾT QUẢ GIAO VIỆC LÊN CHO KẾ TOÁN (JOB_ID)
            context = chi_thi.get("context", {})
            job_id = context.get("job_id")
            if job_id:
                report = {
                    "job_id": job_id,
                    "role": chi_thi.get("role", "UNKNOWN"),
                    "ticket": result.order,
                    "chenh_vao": context.get("chenh_vao", 0),
                    "tinh_chat_vao": context.get("tinh_chat_vao", "UNKNOWN"),
                    # Kẹp thêm các thông số cấu hình và Hz cho Kế toán
                    "tick_hz_base_in": context.get("tick_hz_base_in", 0),
                    "tick_hz_diff_in": context.get("tick_hz_diff_in", 0)
                }
                # Gửi báo cáo vào hòm thư riêng của cặp này
                pair_id = context.get("pair_id")
                if pair_id:
                    r_lpush(f"QUEUE:ORDER_RESULT:{pair_id}", json_dumps(report))
                    
        else:
            print(f"❌ {bot_name} LỖI VÀO LỆNH {action}: {result.comment} ({result.retcode})")
            r_lpush(QUEUE_TELEGRAM, f"❌ <b>{bot_name} LỖI {action}</b>\nMã lỗi: {result.retcode} - {result.comment}")

    elif action == "CLOSE_OLDEST":
        count = chi_thi.get("count", 1)
        positions = mt5.positions_get(symbol=args.symbol) 
        
        if positions:
            lenh_sap_xep = sorted(positions, key=lambda x: x.time_msc)
            lenh_can_dong = lenh_sap_xep[:count] 
            
            for pos in lenh_can_dong:
                executor.submit(thuc_thi_dong_1_lenh, pos, current_tick, comment, chi_thi)

    # 👉 THÊM CHIÊU CHÉM ĐÍCH DANH VÀO DƯỚI CÙNG HÀM thuc_thi_chi_thi
    elif action == "CLOSE_BY_TICKET":
        ticket_can_dong = chi_thi.get("ticket")
        # Gọi MT5 tìm đúng cái lệnh có Ticket đó
        positions = mt5.positions_get(ticket=ticket_can_dong) 
        if positions:
            # Tìm thấy thì ném cho Thread phụ đi chém
            executor.submit(thuc_thi_dong_1_lenh, positions[0], current_tick, comment, chi_thi)
        else:
            print(f"⚠️ {bot_name} Lệnh tử hình Ticket #{ticket_can_dong} thất bại do không tìm thấy lệnh trên sàn (Đã bị StopOut trước đó?)")

    elif action == "FETCH_HISTORY_ONLY":
        executor.submit(thuc_thi_dong_bo_lich_su, chi_thi)
# ==========================================
# 3. VÒNG LẶP CHIẾN TRANH (MAIN LOOP)
# ==========================================
last_tick_time = 0
thoi_gian_check_mang_cuoi = 0
dang_co_mang = True 

thoi_gian_check_tk_cuoi = 0 
equity_canh_bao_da_gui = False
last_len_positions = -1

# Tối ưu biến cục bộ để gọi hàm nhanh hơn
time_time = time.time
sleep = time.sleep
json_dumps = json.dumps
r_set = r.set
r_rpop = r.rpop
r_lpush = r.lpush
mt5_symbol_info_tick = mt5.symbol_info_tick
mt5_positions_get = mt5.positions_get
mt5_account_info = mt5.account_info

# Khay đếm Tick 60 giây (Sliding Window)
tick_history = deque()

try:
    while True:
        now = time_time()
        
        # 🛡️ KIỂM TRA MẠNG VÀ NÚT ALGO TRADING
        if now - thoi_gian_check_mang_cuoi > 1.0:
            terminal_info = mt5.terminal_info()
            # Có mạng VÀ phải đang bật nút Algo Trading thì mới tính là OK
            dang_co_mang = terminal_info.connected and terminal_info.trade_allowed if terminal_info else False
            thoi_gian_check_mang_cuoi = now
            
            if terminal_info is None:
                print(f"⚠️ {bot_name} Mất kết nối nội bộ! Đang thử khởi tạo lại...")
                mt5.initialize(path=mt5_path, portable=True, timeout=10000)
            elif not terminal_info.trade_allowed:
                # Nếu lỡ quên bật, in cảnh báo đỏ rực trên màn hình Worker
                print(f"⛔ {bot_name} ĐẠI CA QUÊN BẬT NÚT 'ALGO TRADING' TRÊN MT5! Bot đang khóa nòng...", end='\r')

        # 📈 LẤY GIÁ VÀ CẬP NHẬT TICK
        tick = mt5_symbol_info_tick(args.symbol)

        if tick is not None:
            # 👉 Lọc rác Sliding Window 60s (Chạy liên tục dù có tick mới hay không)
            while tick_history and now - tick_history[0] > 60.0:
                tick_history.popleft()
                
            tick_count_60s = len(tick_history)
            
            if tick.time_msc != last_tick_time:
                tick_history.append(now)
                tick_count_60s = len(tick_history) # Tính lại sau khi append
                
                tick_data = {
                    "bid": tick.bid,
                    "ask": tick.ask,
                    "time_msc": tick.time_msc,
                    "connected": dang_co_mang,
                    "tick_hz": tick_count_60s # Mật độ nhảy giá 1 phút qua
                }
                r_set(REDIS_TICK_KEY, json_dumps(tick_data))
                
                trang_thai_mang = "OK" if dang_co_mang else "RỚT"
                print(f"{bot_name} BID: {tick.bid} | ASK: {tick.ask} | Mạng: {trang_thai_mang} | {tick_count_60s} t/p", end='\r')
                
                last_tick_time = tick.time_msc
                
            thu_tu_master = r_rpop(QUEUE_ORDER_KEY)
            if thu_tu_master:
                chi_thi = json.loads(thu_tu_master)
                print(f"\n📨 {bot_name} Nhận lệnh từ Master: {chi_thi}")
                executor.submit(thuc_thi_chi_thi, chi_thi, tick)
                
        else:
            mt5.symbol_select(args.symbol, True)
            sleep(1)
            
        # 🧮 CẬP NHẬT TÀI KHOẢN VÀ DANH SÁCH TICKET
        if now - thoi_gian_check_tk_cuoi > 0.2:
            positions = mt5_positions_get(symbol=args.symbol)
            
            # CHỈ ĐẨY LÊN REDIS KHI SỐ LƯỢNG LỆNH THAY ĐỔI
            current_len = len(positions) if positions else 0
            if current_len != last_len_positions:
                if positions:
                    danh_sach_ticket = [{"ticket": pos.ticket, "time_msc": pos.time_update_msc if hasattr(pos, 'time_update_msc') else pos.time_msc} for pos in positions]
                else:
                    danh_sach_ticket = []
                r_set(REDIS_POS_KEY, json_dumps(danh_sach_ticket))
                last_len_positions = current_len

            acc_info = mt5_account_info()
            if acc_info:
                r_set(REDIS_EQUITY_KEY, acc_info.equity)
                
                if acc_info.equity < alert_equity and not equity_canh_bao_da_gui:
                    msg = f"⚠️ <b>{bot_name} CẢNH BÁO LOW EQUITY</b>\nTài khoản đang có {acc_info.equity:.2f}$, chạm mức cảnh báo ({alert_equity}$). Vui lòng nạp thêm tiền!"
                    r_lpush(QUEUE_TELEGRAM, msg)
                    print(f"\n{msg}")
                    equity_canh_bao_da_gui = True
                
                elif acc_info.equity > alert_equity + 10:
                    equity_canh_bao_da_gui = False
                    
            thoi_gian_check_tk_cuoi = now

        sleep(0.001)

except KeyboardInterrupt:
    print(f"\n🛑 {bot_name} Đã dừng an toàn.")
    executor.shutdown(wait=False) # Dọn dẹp Pool
    mt5.shutdown()