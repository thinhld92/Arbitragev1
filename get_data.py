import MetaTrader5 as mt5
import pandas as pd
import time # 1. Import thư viện thời gian

# 2. BẮT ĐẦU BẤM GIỜ (Ngay dòng đầu tiên của chương trình)
thoi_gian_bat_dau = time.time()

# 1. Khởi tạo kết nối với MT5
if not mt5.initialize():
    print("❌ Khởi tạo thất bại! Lỗi:", mt5.last_error())
    quit()

# ==========================================
# 2. ĐỊNH NGHĨA CÁC HÀM (BẮT BUỘC ĐỂ Ở ĐÂY)
# ==========================================  
def dat_lenh(symbol, loai_lenh, khoi_luong):
    # 1. Định nghĩa chuẩn lệnh của MT5
    if loai_lenh == "BUY":
        order_type = mt5.ORDER_TYPE_BUY
        # Mua thì khớp bằng giá Ask (giá thị trường bán cho mình)
        gia_vao = mt5.symbol_info_tick(symbol).ask 
    else:
        order_type = mt5.ORDER_TYPE_SELL
        # Bán thì khớp bằng giá Bid (giá thị trường mua của mình)
        gia_vao = mt5.symbol_info_tick(symbol).bid

    # 2. Tạo Request (Giống hệt data gửi lên API)
    request = {
        "action": mt5.TRADE_ACTION_DEAL,  # Thực thi lệnh ngay lập tức
        "symbol": symbol,                 # Mã giao dịch (VD: BTCUSD)
        "volume": float(khoi_luong),      # Khối lượng (Lot)
        "type": order_type,               # Lệnh BUY hay SELL
        "price": gia_vao,                 # Giá khớp lệnh
        "deviation": 20,                  # Độ trượt giá cho phép (point)
        "magic": 123456,                  # Mã ID của Bot (để sau này phân biệt lệnh của bot và lệnh tự đánh)
        "comment": "Bot Python vao lenh", # Ghi chú
        "type_time": mt5.ORDER_TIME_GTC,  # Good till cancelled (Giữ lệnh đến khi hủy)
        "type_filling": mt5.ORDER_FILLING_IOC, # Cơ chế khớp lệnh (Tuỳ sàn, thường dùng IOC hoặc FOK)
    }

    # 3. Bắn lệnh vào MT5
    print(f"Đang gửi lệnh {loai_lenh} {khoi_luong} lot cho {symbol}...")
    result = mt5.order_send(request)

    # 4. Kiểm tra kết quả trả về
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        print(f"❌ Đặt lệnh thất bại! Mã lỗi: {result.retcode}")
    else:
        print(f"✅ THÀNH CÔNG! Đã khớp lệnh {loai_lenh} tại giá {result.price}")

# ==========================================
# 3. CHƯƠNG TRÌNH CHÍNH (LẤY GIÁ & PHÂN TÍCH)
# ==========================================
# 2. Thiết lập thông số lấy dữ liệu
symbol = "BTCUSD"          # Mã giao dịch (Nếu sàn của bạn dùng XAUUSD thì đổi lại nhé)
timeframe = mt5.TIMEFRAME_M15 # Khung thời gian: Nến 15 phút
so_luong_nen = 5          # Lấy 10 cây nến gần nhất

# 3. Lệnh cốt lõi: Lấy dữ liệu từ MT5
# Tham số 0 có nghĩa là lấy từ cây nến hiện tại (nến đang chạy) lùi về quá khứ
rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, so_luong_nen)

if rates is None:
    print(f"❌ Lỗi: Không lấy được dữ liệu của {symbol}.")
else:
    # 4. Đưa dữ liệu thô vào Pandas để biến thành "Bảng Excel"
    df = pd.DataFrame(rates)
    
    # Dữ liệu thời gian của MT5 là Unix Timestamp (đếm bằng giây), ta cần ép kiểu về ngày giờ dễ nhìn
    # df['time'] = pd.to_datetime(df['time'], unit='s')
    
    # In ra màn hình các cột quan trọng nhất: Thời gian, Giá Mở, Cao, Thấp, Đóng
    # print(f"\n📊 Dữ liệu {so_luong_nen} cây nến {symbol} gần nhất:")
    # print(df[['time', 'open', 'high', 'low', 'close']])
    
    # --- BƯỚC MỚI: PHÂN TÍCH TÍN HIỆU ---
    
    # Lấy cây nến đã chốt gần nhất (dòng áp chót trong bảng df)
    # Trong Python, chỉ số [-1] là dòng cuối cùng, [-2] là dòng áp chót
    nen_gan_nhat = df.iloc[-2] 
    
    print("\n🔍 ĐANG PHÂN TÍCH TÍN HIỆU...")
    
    # Trích xuất giá đóng và giá mở
    gia_mo = nen_gan_nhat['open']
    gia_dong = nen_gan_nhat['close']
    
    thoi_gian_ket_thuc = time.time()

    # Tính thời gian chạy (tính bằng giây)
    thoi_gian_chay = thoi_gian_ket_thuc - thoi_gian_bat_dau

    print(f"\n⏱️ Tổng thời gian xử lý dataFrame: {1000*thoi_gian_chay:.4f} ms")

    # Logic If/Else sinh tín hiệu
    if gia_dong > gia_mo:
        print(f"Giá đóng ({gia_dong}) > Giá mở ({gia_mo})")
        print("🟢 Nến XANH -> Khuyến nghị: BẮN LỆNH BUY!")
        dat_lenh(symbol, "BUY", 0.01) # Bot tự động Mua 0.01 lot
    elif gia_dong < gia_mo:
        print(f"Giá đóng ({gia_dong}) < Giá mở ({gia_mo})")
        print("🔴 Nến ĐỎ -> Khuyến nghị: BẮN LỆNH SELL!")
        dat_lenh(symbol, "SELL", 0.01) # Bot tự động Bán 0.01 lot
    else:
        print("⚪ Nến Doji (Giá không đổi) -> Đứng ngoài quan sát.")



# 5. Dọn dẹp, ngắt kết nối
mt5.shutdown()

# ==========================================
# 3. KẾT THÚC BẤM GIỜ & IN KẾT QUẢ
# ==========================================
thoi_gian_ket_thuc = time.time()

# Tính thời gian chạy (tính bằng giây)
thoi_gian_chay = thoi_gian_ket_thuc - thoi_gian_bat_dau

print(f"\n⏱️ Tổng thời gian xử lý vào lệnh: {1000*thoi_gian_chay:.4f} ms")