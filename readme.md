# 🤖 TBO Arbitrage Trading Bot (HFT Edition)

Hệ thống Bot giao dịch chênh lệch giá (Arbitrage) siêu tốc trên nền tảng MetaTrader 5 (MT5). Được thiết kế theo chuẩn kiến trúc **Microservices (SaaS Architecture)** với Redis làm trung tâm xử lý dữ liệu Real-time. Phiên bản HFT được tối ưu hóa cực hạn bằng lõi C (`ujson` & `hiredis`), đảm bảo tốc độ phản hồi và khớp lệnh tính bằng micro-giây.

## 🌟 Tính Năng Nổi Bật (Core Features)

* **Kiến trúc Tách Não (Microservices):** Tách biệt hoàn toàn `Worker` (Tay chân: Kéo data/Vào lệnh MT5) và `Master` (Trí não: Tính toán logic). Chống nghẽn cổ chai tuyệt đối, Master không bao giờ bị block bởi API của MT5.
* **Động cơ Siêu Tốc (HFT Engine):** Tích hợp `ujson` và `hiredis` giúp tăng tốc độ đọc/ghi và parse chuỗi JSON lên gấp 3 lần so với Python thuần. Đồng hồ Server được Cache 1 giây/lần giúp CPU nhàn rỗi tuyệt đối.
* **Tự Phục Hồi Đa Tầng (Multi-layer Self-healing):** Hệ thống "Bao Thanh Thiên" liên tục quét Sổ Cái. Tự động săn lùng và cắt bỏ các "lệnh góa phụ" (bị StopOut 1 chân) hoặc "lệnh mồ côi" (lỗi đẩy lệnh do sàn trượt giá), đảm bảo không bao giờ tồn tại lệnh lệch chân trên thị trường.
* **Máy Chém Giờ Cấm (Blackout Guillotine):** Tự động xả toàn bộ lệnh (cắt lỗ/chốt lời) trước thời điểm đóng phiên/giãn Spread. Hoạt động độc lập và chuẩn xác tuyệt đối theo múi giờ quốc tế **GMT+0**, bất chấp VPS đặt ở quốc gia nào.
* **Khóa An Toàn (Low Equity Lock):** Hệ thống giám sát vốn (Equity) theo thời gian thực. Tự động đóng băng cấm mở lệnh mới nếu vốn tụt dưới mức cảnh báo, tử thủ bảo toàn tài khoản.
* **Bất Tử Trạng Thái (State Persistence):** Lưu "trí nhớ" xuống Redis. Tắt Bot bật lại vẫn nhớ Sổ Cái đang ghép cặp những lệnh nào, gồng bao nhiêu lệnh.
* **Hot Reloading:** Thay đổi thông số (Mức lệch, Lot size, Giờ giao dịch) trực tiếp trong lúc Bot đang chạy bằng cách sửa file `config.json` mà không cần khởi động lại.

---

## 🛠️ Yêu Cầu Hệ Thống (Prerequisites)

1. **Python 3.9+** (Đảm bảo đã tick chọn *"Add Python to PATH"* khi cài đặt).
2. **Thư viện C++ cho Windows:** Chạy file cài đặt `vc_redist.x64.exe` (nằm trong thư mục `prereqs`) để kích hoạt engine C cho thư viện tốc độ cao.
3. **MetaTrader 5 (MT5):** Cài đặt nhiều bản MT5 khác nhau cho các sàn (VD: 1 bản Exness, 1 bản Tickmill).
4. **Memurai / Redis:** Bản port của Redis dành cho Windows (Chạy file `.msi` trong thư mục `prereqs` hoặc tải bản Developer từ trang chủ Memurai).

---

## ⚙️ Hướng Dẫn Cài Đặt (Installation)

**Bước 1: Cài đặt thư viện cốt lõi** Mở Terminal (CMD) tại thư mục dự án và chạy lệnh sau để nạp đạn cho Bot:
```bash
pip install redis MetaTrader5 requests
pip install ujson hiredis
(Lưu ý: Đảm bảo đã chạy file vc_redist.x64.exe trong thư mục prereqs trước khi cài ujson và hiredis để tránh lỗi thiếu DLL).

Bước 2: Setup MT5 1. Mở các phần mềm MT5 lên và đăng nhập vào tài khoản giao dịch.
2. Nhấn Ctrl + O (Options) -> Tab Expert Advisors.
3. Tick chọn "Allow algorithmic trading". Bỏ tick các ô cấm. Nhấn OK.
4. Mở sẵn Chart của các cặp tiền muốn đánh (VD: XAUUSD, BTCUSD).

Bước 3: Cấu hình hệ thống Sao chép file config.example.json thành config.json và điền thông số.
(Hệ thống có chú thích sẵn các tham số trong file, lưu ý: Toàn bộ khung giờ trading_hours và force_close_hours đều phải setup theo múi giờ GMT+0).

🚀 Khởi Chạy Bot (Running the Bot)
Chỉ cần nháy đúp vào file start_bot.bat hoặc gõ lệnh:
python launcher.py
Hệ thống Launcher sẽ tự động phân bổ:

Mở các cửa sổ Terminal cho Worker (Trinh sát tiền tuyến) kết nối MT5.

Mở các cửa sổ Terminal cho Master Brain (Tướng Quân) lên tính toán logic.

Mở Terminal cho Telegram Service (Lính Liên Lạc).

📚 Mẹo Quản Trị Hệ Thống (Pro-Tips)
1. ⚠️ CẢNH BÁO CHẾ ĐỘ QUICKEDIT CỦA WINDOWS
Đây là "bẫy chuột" chí mạng của Windows. Nếu bạn vô tình click chuột vào màn hình đen (CMD) lúc Bot đang chạy, toàn bộ tiến trình sẽ bị ĐÓNG BĂNG.
👉 Cách khắc phục: Click chuột phải vào thanh tiêu đề của cửa sổ CMD đang chạy Bot -> Chọn Properties -> Bỏ tick ô "QuickEdit Mode" -> Nhấn OK.

2. Tẩy Não Sổ Cái (Clear Cache)
Trong quá trình Test, nếu muốn xóa sạch "trí nhớ" của hệ thống (Sổ Cái ghép cặp, Hàng đợi lệnh, Tick giá cũ), mở CMD lên và gõ:

Bash
memurai-cli FLUSHALL
(Master sẽ tự động nhận diện và báo: "Bắt đầu với Sổ Cái trống rỗng").

3. Quản Lý Log Thông Minh
Dữ liệu chạy Bot thực tế (Vào lệnh, Chốt lời, Cắt lệnh mồ côi) được lưu tự động bằng cơ chế Cuốn chiếu (Log Rotation) trong thư mục logs. Mỗi file giới hạn 5MB để không làm đầy ổ cứng VPS. Các vòng lặp kiểm tra giá thông thường chỉ chạy ngầm trong Cache để tiết kiệm tối đa tài nguyên Máy chủ.

4. Công Tắc Khẩn Cấp (Hot Reload)
Gặp bão tin tức cực mạnh? Bạn không cần tắt Bot. Hãy mở config.json, xóa sạch danh sách trading_hours thành [] rồi lưu lại (Ctrl+S). Master sẽ lập tức "khóa cò súng" cấm vào lệnh mới, nhưng vẫn thức để canh chốt lời các cặp lệnh cũ đang gồng!

Phát triển và Thiết kế Kiến trúc bởi Hehehe ☕💻 - Built for High-Frequency Arbitrage.