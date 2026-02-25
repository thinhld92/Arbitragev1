import MetaTrader5 as mt5
import time

# --- ĐỊNH NGHĨA HÀM ON_TICK (GIỐNG MQL5) ---
def on_tick(tick):
    """
    Hàm này sẽ được thực thi MỖI KHI có tick mới.
    Đây là nơi bạn đặt logic tính toán hoặc đẩy vào Redis.
    """
    print(f"⚡ Event OnTick: {tick.time_msc} | Bid: {tick.bid} | Ask: {tick.ask}")
    
    # Ví dụ logic:
    # if tick.bid > 68500:
    #     dat_lenh_buy(...)

# --- CHƯƠNG TRÌNH CHÍNH (BỘ LẮNG NGHE) ---
if not mt5.initialize():
    quit()

symbol = "BTCUSD"
last_tick_time = 0

print(f"🤖 Bot đang lắng nghe sự kiện OnTick cho {symbol}...")

try:
    while True:
        # Lấy tick hiện tại
        current_tick = mt5.symbol_info_tick(symbol)
        
        if current_tick is not None:
            # Kiểm tra xem đây có phải là tick mới thật sự không
            if current_tick.time_msc != last_tick_time:
                
                # NẾU CÓ TICK MỚI -> GỌI HÀM ON_TICK
                on_tick(current_tick)
                
                # Cập nhật ID của tick vừa xử lý
                last_tick_time = current_tick.time_msc
        
        # Nghỉ cực ngắn để giảm tải CPU nhưng vẫn đảm bảo tốc độ bắt tick
        time.sleep(0.001)

except KeyboardInterrupt:
    print("\n🛑 Dừng Bot.")
    mt5.shutdown()