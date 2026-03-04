from datetime import datetime, timezone

# Lấy thời gian chuẩn UTC (GMT+0)
now_utc = datetime.now(timezone.utc)

# Format ra chuỗi HH:MM giống y hệt cách con Bot đang làm
chuoi_gio_bot_dung = now_utc.strftime("%H:%M")

print("🕒 KIỂM TRA ĐỒNG HỒ MÁY CHỦ (HỆ GMT+0) 🕒")
print("-" * 45)
print(f"▶ Giờ UTC đầy đủ       : {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"▶ Chuỗi giờ Bot sẽ đọc : {chuoi_gio_bot_dung}  <-- (Dùng cái này để so với config)")
print("-" * 45)
print("💡 Lời khuyên từ đệ: Đại ca cứ lấy cái chuỗi giờ này, ốp vào config là chuẩn bài!")