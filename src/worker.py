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
from utils.gui_trader import DomTrader

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
    
    worker_key = (args.broker.upper(), args.symbol.upper())
    matching_caps = [
        cap for cap in config['danh_sach_cap']
        if (cap['base_exchange'].upper(), cap['base_symbol'].upper()) == worker_key
        or (cap['diff_exchange'].upper(), cap['diff_symbol'].upper()) == worker_key
    ]
    cap_cfg = matching_caps[0] if matching_caps else None

    alert_equity_candidates = []
    for cap in matching_caps:
        trade_mode = str(cap.get('trade_mode', 'hedge')).strip().lower()
        if trade_mode == 'single':
            execution = cap.get('execution') or {}
            execution_key = (
                str(execution.get('exchange', '')).strip().upper(),
                str(execution.get('symbol', '')).strip().upper()
            )
            if execution_key != worker_key:
                continue
        alert_equity_candidates.append(cap.get('alert_equity', 0))

    alert_equity = max(alert_equity_candidates) if alert_equity_candidates else 0
    
    # 🌟 LẤY TÊN VPS VÀ GẮN BIỂN SỐ CHO WORKER
    vps_name = config.get('vps_name', 'LOCAL') 
    bot_name = f"[{vps_name}|{args.broker}|{args.symbol}]"

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
# KHỞI TẠO DOM TRADER (NẾU GUI_MODE BẬT)
# ==========================================
gui_mode = config.get('gui_mode', False)
dom_trader = None
if gui_mode:
    # Lấy PID của tiến trình MT5 đang kết nối để tìm đúng DOM của sàn này
    terminal_info = mt5.terminal_info()
    mt5_pid = terminal_info.community_connection  # Fallback nếu không có trường PID
    # MT5 Python API không expose PID trực tiếp, dùng đường dẫn để tìm PID
    import subprocess
    try:
        # Tìm PID của terminal64.exe đang chạy từ đúng thư mục sàn này
        mt5_dir = os.path.dirname(mt5_path).replace("/", "\\\\")
        ps_cmd = f"Get-CimInstance Win32_Process -Filter \"Name='{os.path.basename(mt5_path)}'\" | Select-Object ProcessId, ExecutablePath | ConvertTo-Csv -NoTypeInformation"
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_cmd],
            capture_output=True, text=True, timeout=15
        )
        mt5_pid = None
        for line in result.stdout.strip().split("\n"):
            line = line.strip()
            if not line or "ProcessId" in line:
                continue
            # Dữ liệu CSV từ PowerShell có dạng: "1234","C:\Path\To\terminal64.exe"
            parts = line.split('","')
            if len(parts) >= 2:
                pid_str = parts[0].strip('"')
                exe_path = parts[1].strip('"')
                if mt5_dir.lower() in exe_path.lower():
                    mt5_pid = int(pid_str)
                    break
        if mt5_pid:
            print(f"🔍 {bot_name} [GUI] Tìm thấy MT5 PID={mt5_pid} tại {mt5_path}")
        else:
            print(f"⚠️ {bot_name} [GUI] Không xác định được PID MT5! Sẽ tìm DOM không lọc PID.")
    except Exception as e:
        mt5_pid = None
        print(f"⚠️ {bot_name} [GUI] Lỗi lấy PID: {e}. Sẽ tìm DOM không lọc PID.")
    
    dom_trader = DomTrader(args.symbol, bot_name, mt5_pid=mt5_pid)
    dom_trader.khoi_tao()

# ==========================================
# HÀM HỖ TRỢ: CHỜ TICKET MỚI (CHO DOM)
# ==========================================
def cho_doi_ticket_moi(positions_truoc, timeout_ms=5000):
    truoc_tickets = {p.ticket for p in positions_truoc} if positions_truoc else set()
    t_start = time.perf_counter()
    while (time.perf_counter() - t_start) * 1000 < timeout_ms:
        hien_tai = mt5.positions_get(symbol=args.symbol)
        if hien_tai:
            for p in hien_tai:
                if p.ticket not in truoc_tickets:
                    return p.ticket
        time.sleep(0.005)
    return None

# ==========================================
# HÀM HỖ TRỢ: BỌc ThreadPool chống nuốt exception
# ==========================================
def safe_submit(fn, *args):
    future = executor.submit(fn, *args)
    def _check(f):
        ex = f.exception()
        if ex:
            print(f"\n🔥 [THREAD CRASH] {fn.__name__}: {ex}")
    future.add_done_callback(_check)
    return future

# ==========================================
# HÀM HỖ TRỢ: ĐÓNG 1 LỆNH (DÙNG CHO THREAD)
# ==========================================
def thuc_thi_dong_1_lenh(pos, current_tick, comment, chi_thi, use_dom=False):
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
    
    thanh_cong = False
    retcode = -1
    comment_loi = ""
    
    with mt5_lock: 
        # 1. Thu DOM truoc
        if use_dom and gui_mode and dom_trader and dom_trader.kiem_tra_dom_con_song():
            print(f"🖱️ {bot_name} Dùng DOM click CLOSE lệnh #{pos.ticket}...")
            click_ok = dom_trader.close_position()
            if click_ok:
                t_start = time.perf_counter()
                da_dong = False
                while (time.perf_counter() - t_start) * 1000 < 5000:
                    hien_tai = mt5.positions_get(ticket=pos.ticket)
                    if not hien_tai:
                        da_dong = True
                        break
                    time.sleep(0.005)
                
                if da_dong:
                    thanh_cong = True
                    print(f"⚡ {bot_name} DOM Click CLOSE THANH CONG! Lệnh #{pos.ticket} đã biến mất")
                else:
                    comment_loi = "DOM Click CLOSE OK nhung lenh chua bien mat sau 5s"
                    print(f"❌ {bot_name} LỖI: {comment_loi}")
            else:
                comment_loi = "DOM Click CLOSE FAIL (Khong click duoc)"
                print(f"❌ {bot_name} LỖI: {comment_loi}")
                
        # 2. Fallback API
        if not thanh_cong and (not use_dom or not gui_mode or not dom_trader or not dom_trader.kiem_tra_dom_con_song()):
            if use_dom and gui_mode: print(f"⚠️ {bot_name} DOM khong kha dung! Fallback sang API...")
            result = mt5.order_send(request)
            if result.retcode == mt5.TRADE_RETCODE_DONE:
                thanh_cong = True
            else:
                retcode = result.retcode
                comment_loi = result.comment
        
    if thanh_cong:
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
            print(f"📤 [DEBUG] Đã gửi biên lai đóng lệnh #{pos.ticket} cho Kế toán | Role: {chi_thi.get('role')} | Token: {chi_thi.get('context', {}).get('pair_token', 'N/A')[:30]}")
        else:
            # 🛡️ VẪN GỬi biên lai (profit=0) để accountant không bị kẹt chờ
            print(f"⚠️ {bot_name} Lệnh #{pos.ticket} đã đóng nhưng MT5 không nhả lịch sử sau 5s! Gửi biên lai rỗng.")
            bien_lai = {
                "role": chi_thi.get("role", "UNKNOWN"),
                "ticket": pos.ticket,
                "volume": pos.volume,
                "profit": 0, "fee": 0,
                "open_price": 0, "close_price": 0,
                "context": chi_thi.get("context", {})
            }
            r.lpush("QUEUE:ACCOUNTANT", json.dumps(bien_lai))
            
    else:
        # 🛡️ VẪN GỬi biên lai khi lệnh đóng FAIL để accountant không kẹt
        print(f"❌ {bot_name} LỖI ĐÓNG LỆNH #{pos.ticket}: {comment_loi} ({retcode})")
        bien_lai = {
            "role": chi_thi.get("role", "UNKNOWN"),
            "ticket": pos.ticket,
            "volume": pos.volume,
            "profit": 0, "fee": 0,
            "open_price": 0, "close_price": 0,
            "context": chi_thi.get("context", {})
        }
        r.lpush("QUEUE:ACCOUNTANT", json.dumps(bien_lai))
        try:
            r_lpush(QUEUE_TELEGRAM, f"❌ <b>{bot_name} LỖI ĐÓNG LỆNH</b>\nTicket: #{pos.ticket} | Lỗi: {retcode} - {comment_loi}")
        except Exception:
            pass

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
        # 🛡️ VẪN GỬi biên lai rỗng khi không tìm thấy lịch sử
        print(f"⚠️ Không tìm thấy lịch sử của #{ticket}. Gửi biên lai rỗng.")
        bien_lai = {
            "role": chi_thi.get("role", "UNKNOWN"),
            "ticket": ticket,
            "volume": 0, "profit": 0, "fee": 0,
            "open_price": 0, "close_price": 0,
            "context": chi_thi.get("context", {})
        }
        r.lpush("QUEUE:ACCOUNTANT", json.dumps(bien_lai))

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
        # if comment: request["comment"] = comment
        
        thanh_cong = False
        ticket_moi = None
        retcode = -1
        comment_loi = ""

        with mt5_lock:
            # 1. Thu DOM truoc neu duoc bat
            clicked_dom = False
            if gui_mode and dom_trader and dom_trader.kiem_tra_dom_con_song():
                print(f"🖱️ {bot_name} Dùng DOM click {action} {volume} LOT...")
                pos_truoc = mt5.positions_get(symbol=args.symbol)
                click_ok = dom_trader.thuc_thi_lenh(action, volume)
                if click_ok:
                    clicked_dom = True
                    ticket_moi = cho_doi_ticket_moi(pos_truoc, timeout_ms=5000)
                    if ticket_moi:
                        thanh_cong = True
                        print(f"⚡ {bot_name} DOM Click THANH CONG! Ticket: {ticket_moi}")
                    else:
                        comment_loi = "DOM Click OK nhung khong thay ticket xuat hien sau 5s"
                        print(f"❌ {bot_name} LỖI: {comment_loi}")
                else:
                    comment_loi = "DOM Click FAIL (Loi Volume hoac Handle bi ngat)"
                    print(f"❌ {bot_name} LỖI: {comment_loi}")
            
            # 2. Fallback API neu DOM that bai
            # ĐIỀU KIỆN FALLBACK: Chua thanh cong va Chua tung click bừa vao DOM
            if not thanh_cong and ticket_moi is None and not clicked_dom:
                if gui_mode: print(f"⚠️ {bot_name} DOM khong kha dung hoac huy Click! Fallback sang API...")
                result = mt5.order_send(request)
                if result.retcode == mt5.TRADE_RETCODE_DONE:
                    thanh_cong = True
                    ticket_moi = result.order
                    print(f"🔫 {bot_name} ĐÃ BẮN {action} {volume} LOT (API). Ticket: {ticket_moi}")
                else:
                    retcode = result.retcode
                    comment_loi = result.comment
            
        if thanh_cong:
            # 👉 BÁO CÁO KẾT QUẢ GIAO VIỆC LÊN CHO KẾ TOÁN (JOB_ID)
            context = chi_thi.get("context", {})
            job_id = context.get("job_id")
            if job_id:
                report = {
                    "job_id": job_id,
                    "role": chi_thi.get("role", "UNKNOWN"),
                    "ticket": ticket_moi,
                    "trade_mode": context.get("trade_mode", "hedge"),
                    "execution_exchange": context.get("execution_exchange", args.broker),
                    "execution_symbol": context.get("execution_symbol", args.symbol),
                    "execution_role": context.get("execution_role", chi_thi.get("role", "UNKNOWN")),
                    "execution_action": context.get("execution_action", action),
                    "chenh_vao": context.get("chenh_vao", 0),
                    "chenh_vao_raw": context.get("chenh_vao_raw", context.get("chenh_vao", 0)),
                    "tinh_chat_vao": context.get("tinh_chat_vao", "UNKNOWN"),
                    "entry_spread_pivot": context.get("entry_spread_pivot", 0.0),
                    "conf_dev_entry": context.get("conf_dev_entry", 0),
                    "entry_stable_time": context.get("entry_stable_time", context.get("conf_stable_time", 0)),
                    # Kẹp thêm các thông số cấu hình và Hz cho Kế toán
                    "tick_hz_base_in": context.get("tick_hz_base_in", 0),
                    "tick_hz_diff_in": context.get("tick_hz_diff_in", 0)
                }
                # Gửi báo cáo vào hòm thư riêng của cặp này
                pair_id = context.get("pair_id")
                if pair_id:
                    r_lpush(f"QUEUE:ORDER_RESULT:{pair_id}", json_dumps(report))
                    
        else:
            print(f"❌ {bot_name} LỖI VÀO LỆNH {action}: {comment_loi} ({retcode})")
            try:
                r_lpush(QUEUE_TELEGRAM, f"❌ <b>{bot_name} LỖI {action}</b>\nMã lỗi: {retcode} - {comment_loi}")
            except Exception:
                pass

    elif action == "CLOSE_OLDEST":
        count = chi_thi.get("count", 1)
        positions = mt5.positions_get(symbol=args.symbol) 
        
        if positions:
            lenh_sap_xep = sorted(positions, key=lambda x: x.time_msc)
            lenh_can_dong = lenh_sap_xep[:count] 
            
            for pos in lenh_can_dong:
                safe_submit(thuc_thi_dong_1_lenh, pos, current_tick, comment, chi_thi, True)

    # 👉 THÊM CHIÊU CHÉM ĐÍCH DANH VÀO DƯỚI CÙNG HÀM thuc_thi_chi_thi
    elif action == "CLOSE_BY_TICKET":
        ticket_can_dong = chi_thi.get("ticket")
        # Gọi MT5 tìm đúng cái lệnh có Ticket đó
        positions = mt5.positions_get(ticket=ticket_can_dong) 
        if positions:
            # Phân loại: Chốt lời bình thường → DOM, Quản lý rủi ro → API
            context = chi_thi.get("context", {})
            action_type = context.get("action_type", "")
            risk_actions = ("FORCE_CLOSE", "SINGLE_CLOSE", "BLACKOUT_CLOSE")
            use_dom = action_type not in risk_actions
            safe_submit(thuc_thi_dong_1_lenh, positions[0], current_tick, comment, chi_thi, use_dom)
        else:
            # 🛡️ VẪN GỬi biên lai khi position không tồn tại để accountant không kẹt
            print(f"⚠️ {bot_name} Ticket #{ticket_can_dong} không tìm thấy trên sàn. Gửi biên lai rỗng.")
            bien_lai = {
                "role": chi_thi.get("role", "UNKNOWN"),
                "ticket": ticket_can_dong,
                "volume": 0, "profit": 0, "fee": 0,
                "open_price": 0, "close_price": 0,
                "context": chi_thi.get("context", {})
            }
            r.lpush("QUEUE:ACCOUNTANT", json.dumps(bien_lai))

    elif action == "FETCH_HISTORY_ONLY":
        safe_submit(thuc_thi_dong_bo_lich_su, chi_thi)


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

# 🛑 Tín hiệu tắt bot an toàn
SHUTDOWN_KEY = "SIGNAL:SHUTDOWN"
last_shutdown_check = 0
dang_shutdown = False

try:
    while True:
        now = time_time()
        
        # 🛡️ KIỂM TRA MẠNG VÀ NÚT ALGO TRADING
        if now - thoi_gian_check_mang_cuoi > 1.0:
            terminal_info = mt5.terminal_info()
            # Có mạng VÀ phải đang bật nút Algo Trading thì mới tính là OK
            dang_co_mang = terminal_info.connected and terminal_info.trade_allowed if terminal_info else False
            thoi_gian_check_mang_cuoi = now
            
            # 🛑 CHECK TÍN HIỆU TẮT BOT AN TOÀN (mỗi 1 giây)
            if r.get(SHUTDOWN_KEY):
                dang_shutdown = True
                print(f"\n🛑 [SHUTDOWN] {bot_name} Nhận tín hiệu tắt! Đợi lệnh đang xử lý xong...")
                # ĐỢI TẤT CẢ THREAD ĐANG CHẠY HOÀN TẤT (CHỐNG LỆCH CHÂN)
                executor.shutdown(wait=True)
                print(f"✅ {bot_name} Tất cả lệnh đã xử lý xong. Tắt MT5...")
                mt5.shutdown()
                print(f"👋 {bot_name} Đã thoát an toàn!")
                break
            
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
                print(f"{bot_name} B: {tick.bid} | A: {tick.ask} | M: {trang_thai_mang} | {tick_count_60s} t/p", end='\r')
                
                last_tick_time = tick.time_msc
                
            thu_tu_master = r_rpop(QUEUE_ORDER_KEY)
            if thu_tu_master:
                chi_thi = json.loads(thu_tu_master)
                # print(f"\n📨 {bot_name} Nhận lệnh từ Master: {chi_thi}")
                safe_submit(thuc_thi_chi_thi, chi_thi, tick)
                
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
                    danh_sach_ticket = [
                        {
                            "ticket": pos.ticket,
                            "time_msc": pos.time_update_msc if hasattr(pos, 'time_update_msc') else pos.time_msc,
                            "side": "BUY" if pos.type == mt5.ORDER_TYPE_BUY else "SELL",
                            "volume": pos.volume,
                        }
                        for pos in positions
                    ]
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
    print(f"\n🛑 {bot_name} Nhận tín hiệu tắt (Ctrl+C). Đợi lệnh đang xử lý...")
    executor.shutdown(wait=True) # ĐỢI THREAD XONG để tránh lệch chân
    mt5.shutdown()
    print(f"👋 {bot_name} Đã thoát an toàn!")
