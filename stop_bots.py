import ujson as json
import redis
import time

print("🛑 ĐANG GỬI TÍN HIỆU TẮT BOT AN TOÀN...")

try:
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    redis_conf = config['redis']
    r = redis.Redis(
        host=redis_conf['host'], 
        port=redis_conf['port'], 
        db=redis_conf['db'], 
        decode_responses=True
    )
    
    # Gửi tín hiệu tắt (tồn tại 60 giây để đảm bảo mọi bot đều nhận được)
    r.setex("SIGNAL:SHUTDOWN", 60, "1")
    
    print("✅ Đã gửi tín hiệu SHUTDOWN lên Redis!")
    print("⏳ Các bot sẽ tự tắt theo thứ tự an toàn:")
    print("   1. Master → Ngưng ra lệnh mới")
    print("   2. Worker → Đợi lệnh đang xử lý xong → Tắt MT5")
    print("   3. Accountant → Ghi sổ nốt biên lai còn lại → Thoát")
    print()
    print("🔍 Hãy quan sát các cửa sổ Terminal tự đóng dần...")

except Exception as e:
    print(f"❌ Lỗi: {e}")
