import redis
import sys

def verify_optimized_connection():
    print("--- Bắt đầu kiểm tra hệ thống ---")
    try:
        # Sử dụng cấu hình mặc định phổ biến nhất
        # Nếu bạn dùng Docker hoặc Server khác, hãy đổi '127.0.0.1'
        r = redis.Redis(
            host='127.0.0.1', 
            port=6379, 
            decode_responses=True,
            socket_connect_timeout=3 # Tăng thêm thời gian chờ để tránh timeout giả
        )

        # Kiểm tra parser mà không dùng get_connection(command_name) để tránh DeprecationWarning
        # Chúng ta lấy trực tiếp từ connection_pool
        connection = r.connection_pool.get_connection('PING')
        parser_type = connection._parser.__class__.__name__
        print(f"Trình phân tích đang sử dụng: {parser_type}")

        # Thực hiện lệnh kiểm tra thực tế
        if r.ping():
            print("Chúc mừng! Kết nối tới Redis đã thông suốt.")
            
    except redis.exceptions.TimeoutError:
        print("Lỗi: Kết nối bị quá hạn (Timeout). Máy chủ Redis có thể đang quá tải hoặc không phản hồi.")
    except redis.exceptions.ConnectionError:
        print("Lỗi: Không thể kết nối vật lý tới Redis. Vui lòng chạy lệnh 'redis-server' trong terminal.")
    except Exception as e:
        print(f"Phát hiện lỗi không xác định: {e}")
    finally:
        print("--- Kết thúc kiểm tra ---")

if __name__ == "__main__":
    verify_optimized_connection()