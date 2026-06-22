import time
import statistics
import sys
import os

# Add src to path so we can import utils
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from utils.gui_trader import DomTrader, _random_delay

def run_benchmark(symbol="XAUUSD", iterations=30):
    print(f"==================================================")
    print(f"🚀 BẮT ĐẦU BENCHMARK DOM HFT (Lặp {iterations} lần)")
    print(f"==================================================")
    dom = DomTrader(symbol, bot_name="[BENCHMARK]")
    
    if not dom.khoi_tao():
        print("❌ LỖI: Không tìm thấy bảng DOM. Đại ca vui lòng mở MT5 và bật bảng DOM (MiniFrame) lên nhé!")
        return
        
    print("✅ Đã kết nối DOM thành công! Bắt đầu đo đạc (Sẽ không click BUY/SELL)...\n")
    
    times_read = []
    times_type_new = []
    times_verify = []
    times_skip_0ms = []
    
    vol_a = "0.01"
    vol_b = "0.19"
    
    for i in range(iterations):
        target_vol = vol_a if i % 2 == 0 else vol_b
        
        # --- BƯỚC 1: ĐO TỐC ĐỘ ĐỌC VOL CŨ (lay_volume_hien_tai) ---
        t0 = time.perf_counter()
        current_vol = dom.lay_volume_hien_tai().strip()
        t1 = time.perf_counter()
        times_read.append((t1 - t0) * 1000)
        
        vol_str_expected = target_vol.replace(".", ",") if "," in current_vol else target_vol
        
        # --- BƯỚC 2: ĐO TỐC ĐỘ GÕ PHÍM (dat_volume) ---
        t2 = time.perf_counter()
        dom.dat_volume(target_vol)
        t3 = time.perf_counter()
        times_type_new.append((t3 - t2) * 1000)
        
        # --- BƯỚC 3: ĐO TỐC ĐỘ SLEEP + VERIFY ---
        t4 = time.perf_counter()
        time.sleep(0.02) # Cho MT5 luu bo nho
        verify_vol = dom.lay_volume_hien_tai().strip()
        t5 = time.perf_counter()
        times_verify.append((t5 - t4) * 1000)
        
        if verify_vol != vol_str_expected:
            print(f"❌ [CRITICAL] Lỗi verify ở vòng lặp {i+1}!")
            
        # --- BƯỚC 4: ĐO TỐC ĐỘ TỐI ƯU 0ms (Thử gõ lại chính số vừa gõ) ---
        t6 = time.perf_counter()
        # Mô phỏng logic khi volume config == volume tren DOM
        test_vol_hien_tai = dom.lay_volume_hien_tai().strip()
        if test_vol_hien_tai == vol_str_expected:
            # BO QUA BUOC GO PHIM
            pass
        t7 = time.perf_counter()
        times_skip_0ms.append((t7 - t6) * 1000)
        
        # Xóa delay để chạy full tốc độ
        sys.stdout.write(f"\rĐang benchmark vòng {i+1}/{iterations}...")
        sys.stdout.flush()

    print("\n\n📊 KẾT QUẢ BENCHMARK (Đơn vị: Mili-giây / ms):")
    print(f"{'Thao tác':<35} | {'Trung bình':<10} | {'Nhanh nhất':<10} | {'Chậm nhất':<10}")
    print("-" * 75)
    print(f"{'1. Đọc số trên DOM (API GetText)':<35} | {statistics.mean(times_read):.3f} ms   | {min(times_read):.3f} ms   | {max(times_read):.3f} ms")
    print(f"{'2. Xóa & Gõ 4 ký tự (API Char)':<35} | {statistics.mean(times_type_new):.3f} ms   | {min(times_type_new):.3f} ms   | {max(times_type_new):.3f} ms")
    print(f"{'3. Chờ MT5 vẽ UI & Verify lại':<35} | {statistics.mean(times_verify):.3f} ms   | {min(times_verify):.3f} ms   | {max(times_verify):.3f} ms")
    
    total_latency = statistics.mean(times_type_new) + statistics.mean(times_verify)
    print("-" * 75)
    print(f"=> TỔNG TRỄ KHI ĐỔI SỐ VOL MỚI (2+3): {total_latency:.3f} ms")
    print(f"=> TỔNG TRỄ TỐI ƯU (NẾU SỐ KHỚP):     {statistics.mean(times_skip_0ms):.5f} ms (Tuyệt đối 0ms)")
    
    # TINH TOAN TAN SUAT CHAM (OUTLIERS)
    print("==================================================")
    print("📈 TẦN SUẤT CHẬM (PHÂN TÍCH OS JITTER ĐỌC SỐ):")
    p90 = statistics.quantiles(times_read, n=100)[89]
    p95 = statistics.quantiles(times_read, n=100)[94]
    p99 = statistics.quantiles(times_read, n=100)[98]
    
    lag_over_3ms = sum(1 for t in times_read if t > 3)
    lag_over_5ms = sum(1 for t in times_read if t > 5)
    lag_over_10ms = sum(1 for t in times_read if t > 10)
    
    print(f"- 90% số lệnh hoàn thành dưới: {p90:.3f} ms")
    print(f"- 95% số lệnh hoàn thành dưới: {p95:.3f} ms")
    print(f"- 99% số lệnh hoàn thành dưới: {p99:.3f} ms")
    print("-" * 50)
    print(f"- Số lần chậm > 3ms : {lag_over_3ms} / {iterations} lần ({lag_over_3ms/iterations*100:.1f}%)")
    print(f"- Số lần chậm > 5ms : {lag_over_5ms} / {iterations} lần ({lag_over_5ms/iterations*100:.1f}%)")
    print(f"- Số lần chậm > 10ms: {lag_over_10ms} / {iterations} lần ({lag_over_10ms/iterations*100:.1f}%)")
    print("==================================================")

if __name__ == "__main__":
    # Lay symbol tu arguments hoac mac dinh XAUUSD
    sym = sys.argv[1] if len(sys.argv) > 1 else "XAUUSD"
    iters = int(sys.argv[2]) if len(sys.argv) > 2 else 30
    run_benchmark(sym, iterations=iters)
