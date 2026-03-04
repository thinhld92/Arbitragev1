🤖 TBO Arbitrage Trading Bot (MT5)
Hệ thống Bot giao dịch chênh lệch giá (Arbitrage) tốc độ cao trên nền tảng MetaTrader 5 (MT5). Sử dụng kiến trúc Microservices với Redis làm trung tâm xử lý dữ liệu Real-time, đảm bảo tốc độ phản hồi tính bằng mili-giây.

🌟 Tính năng Nổi bật (Core Features)
Kiến trúc Microservices: Tách biệt hoàn toàn Worker (Kéo data/Vào lệnh) và Master (Tính toán logic) giúp chống nghẽn cổ chai.

Động cơ Real-time (Tick Guard): Bắt giá chớp nhoáng với cơ chế "Ép xử lý" không bỏ sót mili-giây nào khi vào thế ngắm bắn.

Debounce & Chống Lật Mặt: Loại bỏ tín hiệu nhiễu, gồng lời mức chênh lệch tối đa và chống kẹt logic khi giá đảo chiều đột ngột.

Bất tử Trạng thái (State Persistence): Lưu "trí nhớ" xuống Redis. Tắt Bot bật lại vẫn nhớ đang đánh hướng nào, gồng bao nhiêu lệnh.

Hot Reloading: Đổi thông số (Mức lệch, Lot size, Giờ giao dịch) ngay trong lúc Bot đang chạy mà không cần khởi động lại.

Cầu dao Lệch chân (Circuit Breaker): Tự động khóa nòng và réo Telegram báo động khi số lệnh 2 sàn không khớp nhau.

Giới hạn Khung giờ: Tự động đi ngủ ngoài giờ hành chính (nhưng vẫn thức để chốt lời các lệnh cũ).

🛠️ Yêu cầu Hệ thống (Prerequisites)
Python 3.9+ (Đảm bảo đã tick chọn "Add Python to PATH" khi cài đặt).

MetaTrader 5 (MT5): Cài đặt nhiều bản MT5 khác nhau cho các sàn (VD: 1 cái Exness, 1 cái Tickmill).

Memurai: Bản port của Redis dành cho Windows (Tải và cài đặt bản Developer miễn phí từ trang chủ Memurai).

⚙️ Hướng dẫn Cài đặt (Installation)
Bước 1: Clone Code và Cài thư viện
Mở Terminal (CMD) tại thư mục dự án và chạy lệnh sau để cài đặt toàn bộ "đạn dược" cho Bot:

Bash
pip install redis MetaTrader5 requests
pip install ujson hiredis
Nhớ cài đặt các file có trong folder prereqs để có thể chạy trơn tru

Bước 2: Setup MT5
Mở các phần mềm MT5 lên và đăng nhập vào tài khoản giao dịch.

Trên mỗi MT5, nhấn Ctrl + O (Options) -> Tab Expert Advisors.

Tick chọn "Allow algorithmic trading". Bỏ tick các ô cấm. Nhấn OK.

Mở sẵn Chart của các cặp tiền muốn đánh (VD: XAUUSD, BTCUSD).

Bước 3: Cấu hình config.json
Tạo/Mở file config.json và điền thông số. Dưới đây là cấu trúc mẫu:

JSON
{
  "redis": {
    "host": "127.0.0.1",
    "port": 6379,
    "db": 0
  },
  "telegram": {
    "enable": true,
    "bot_token": "TOKEN_CUA_BAN",
    "chat_id": "-1001234567890" 
  },
  "danh_sach_cap": [
    {
      "id": "BTCUSD_TICKMILL_EXNESS",
      "base_exchange": "TICKMILL",
      "base_symbol": "BTCUSD",
      "diff_exchange": "EXNESS",
      "diff_symbol": "BTCUSD",
      "lot_size": 0.1,
      "max_orders": 3,
      "deviation_entry": 17.0,
      "deviation_close": 9.0,
      "stable_time": 250,
      "cooldown_second": 60,
      "hold_time": 180,
      "trading_hours": ["08:00-11:30", "13:30-22:00"]
    }
  ]
}
(Lưu ý: chat_id của Group Private trên Telegram luôn là một số ÂM).

🚀 Khởi chạy Bot (Running the Bot)
Chỉ cần nháy đúp vào file start_bot.bat hoặc gõ lệnh:

Bash
python launcher.py
Hệ thống Quản đốc sẽ tự động phân bổ:

Mở các Terminal cho Worker (Trinh sát) đi thu thập Data (Có mác BASE, DIFF, hoặc BASE/DIFF rõ ràng).

Chờ 3 giây để Worker nạp đạn và kết nối MT5.

Mở các Terminal cho Master Brain (Tướng Quân) lên tính toán và ra lệnh.

Mở Terminal cho Telegram Service (Lính Liên Lạc).

📚 Mẹo quản trị (Pro-Tips)
1. Tẩy não Redis (Clear Cache)
Trong quá trình Test, nếu Bot bị ôm lệnh ảo do lỗi mạng hoặc đóng lệnh tay, bạn có thể xóa sạch bộ nhớ (State/Queue/Tick) của hệ thống bằng cách mở CMD lên và gõ:

Bash
redis-cli FLUSHALL
memurai-cli FLUSHALL
(Khởi động lại Bot sau khi chạy lệnh này, Master sẽ báo "Bắt đầu với trí nhớ trống rỗng").

2. Quản lý Log
Dữ liệu chạy Bot thực tế (Vào lệnh, Chốt lời, Lệch chân) được lưu tự động bằng cơ chế Cuốn chiếu (Log Rotation) trong các file log_master_XXX.txt.

Các file log sẽ tự động giới hạn dung lượng tối đa 5MB/file để không làm đầy ổ cứng VPS. Các "Kèo lỏ" sẽ chỉ in ra màn hình Console, không ghi vào Log để tránh rác hệ thống.

3. Hot Reload (Công tắc khẩn cấp)
Nếu có tin tức cực mạnh (Non-farm, CPI...) mà bạn muốn Bot ngừng vào lệnh mới ngay lập tức nhưng vẫn tiếp tục chốt lời các lệnh cũ, hãy mở config.json, xóa mảng "trading_hours": [] (hoặc để trống) rồi ấn Ctrl + S. Master sẽ nhận lệnh khóa cò súng ngay tức khắc!

Phát triển bởi [gà] ☕💻