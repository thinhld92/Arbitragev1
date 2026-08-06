"""
spread_viewer.py — Web Dashboard hiển thị Spread Realtime.

Process độc lập đọc cùng dữ liệu Redis mà Worker/Master sử dụng,
tính spread bằng cùng công thức trading_logic.py, push qua WebSocket.

Usage:
    python src/spread_viewer.py --pair_id XAUUSD_TICKMILL_HFM
    python src/spread_viewer.py --pair_id XAUUSD_TICKMILL_HFM --port 8899

Truy cập: http://localhost:8899
"""

import os
import sys
import argparse
import asyncio
import time
import ctypes
import io

# Fix Windows console encoding cho emoji
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

try:
    kernel32 = ctypes.windll.kernel32
    kernel32.SetConsoleMode(kernel32.GetStdHandle(-10), 128)
except Exception:
    pass

# Import JSON - ưu tiên ujson nếu có
try:
    import ujson as json
except ImportError:
    import json

import redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

# Thêm src vào path để import trading_logic
sys.path.insert(0, os.path.dirname(__file__))
from utils.trading_logic import check_tin_hieu_arbitrage, lay_spread_pivot

# ==========================================
# 1. ĐỌC THAM SỐ
# ==========================================
parser = argparse.ArgumentParser(description="Spread Viewer Web Dashboard")
parser.add_argument("--pair_id", required=True, help="ID cặp giao dịch (VD: XAUUSD_TICKMILL_HFM)")
parser.add_argument("--port", type=int, default=8899, help="Port cho web server (mặc định: 8899)")
args = parser.parse_args()

# Đổi tên cửa sổ terminal
try:
    ctypes.windll.kernel32.SetConsoleTitleW(f"📊 SPREAD VIEWER — {args.pair_id}")
except Exception:
    pass

# ==========================================
# 2. ĐỌC CONFIG
# ==========================================
CONFIG_FILE = "config.json"
last_config_modified = 0


def doc_config():
    """Đọc config.json và trả về (config_full, config_cap)."""
    global last_config_modified
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        config = json.load(f)
    last_config_modified = os.path.getmtime(CONFIG_FILE)
    cap = next((c for c in config.get("danh_sach_cap", []) if c["id"] == args.pair_id), None)
    return config, cap


config, cap_hien_tai = doc_config()
if cap_hien_tai is None:
    print(f"❌ Không tìm thấy pair_id '{args.pair_id}' trong {CONFIG_FILE}!")
    sys.exit(1)

vps_name = config.get("vps_name", "LOCAL")

# ==========================================
# 3. KẾT NỐI REDIS
# ==========================================
redis_conf = config["redis"]
r = redis.Redis(
    host=redis_conf["host"],
    port=redis_conf["port"],
    db=redis_conf["db"],
    decode_responses=True,
    socket_timeout=2.0,
    socket_connect_timeout=2.0,
)

try:
    r.ping()
    print(f"✅ Redis OK ({redis_conf['host']}:{redis_conf['port']})")
except redis.ConnectionError:
    print(f"❌ Không kết nối được Redis!")
    sys.exit(1)


def build_redis_keys(cap):
    """Xây dựng danh sách Redis keys từ config cap."""
    return {
        "tick_base": f"TICK:{cap['base_exchange'].upper()}:{cap['base_symbol'].upper()}",
        "tick_diff": f"TICK:{cap['diff_exchange'].upper()}:{cap['diff_symbol'].upper()}",
        "pos_base": f"POSITION:{cap['base_exchange'].upper()}:{cap['base_symbol'].upper()}",
        "pos_diff": f"POSITION:{cap['diff_exchange'].upper()}:{cap['diff_symbol'].upper()}",
        "eq_base": f"ACCOUNT:{cap['base_exchange'].upper()}:EQUITY",
        "eq_diff": f"ACCOUNT:{cap['diff_exchange'].upper()}:EQUITY",
        "state": f"STATE:MASTER:{cap['id']}",
    }



redis_keys = build_redis_keys(cap_hien_tai)

# Pub/Sub channels
PUB_TICK_BASE = f"TICK_PUB:{cap_hien_tai['base_exchange'].upper()}:{cap_hien_tai['base_symbol']}"
PUB_TICK_DIFF = f"TICK_PUB:{cap_hien_tai['diff_exchange'].upper()}:{cap_hien_tai['diff_symbol']}"

# ==========================================
# 4. BACKEND BUFFER (deque lưu 15 phút spread history)
# ==========================================
import threading
from collections import deque

CHART_WINDOW_SEC = 60 * 60  # Mặc định backend có thể không cần xài biến này nữa, frontend tự lo
spread_history = deque(maxlen=360_000)  # Chứa thừa sức 60 phút tick data liên tục
latest_payload = {}  # Payload mới nhất để gửi cho WebSocket
payload_lock = threading.Lock()

# Tick cache — lưu tick mới nhất từ mỗi worker
latest_tick_base = {}
latest_tick_diff = {}
tick_lock = threading.Lock()

# Connected WebSocket clients
ws_clients = set()
ws_clients_lock = threading.Lock()


def build_config_snapshot(cap):
    """Tạo snapshot config để gửi cho frontend."""
    return {
        "pair_id": cap["id"],
        "trade_mode": cap.get("trade_mode", "hedge"),
        "base_exchange": cap["base_exchange"],
        "base_symbol": cap["base_symbol"],
        "diff_exchange": cap["diff_exchange"],
        "diff_symbol": cap["diff_symbol"],
        "deviation_entry": cap.get("deviation_entry", 0),
        "deviation_close": cap.get("deviation_close", 0),
        "spread_pivot": lay_spread_pivot(cap),
        "stable_mode": cap.get("stable_mode", "freeze"),
        "stable_time": cap.get("stable_time", 0),
        "volume_base": cap.get("volume_base", 0),
        "volume_diff": cap.get("volume_diff", 0),
        "max_orders": cap.get("max_orders", 0),
        "alert_equity": cap.get("alert_equity", 0),
        "filter_entry": cap.get("filter_entry", "none"),
        "filter_close": cap.get("filter_close", "none"),
        "cooldown_second": cap.get("cooldown_second", 0),
        "cooldown_close_second": cap.get("cooldown_close_second", 0),
        "hold_time": cap.get("hold_time", 0),
        "max_orphan_count": cap.get("max_orphan_count", 3),
        "orphan_cooldown_second": cap.get("orphan_cooldown_second", 300),
        "max_tick_hz_base": cap.get("max_tick_hz_base", 0),
        "max_tick_hz_diff": cap.get("max_tick_hz_diff", 0),
        "trading_hours": cap.get("trading_hours", []),
        "force_close_hours": cap.get("force_close_hours", []),
        "viewer_config": config.get("viewer_config", {}),
        "vps_name": vps_name,
    }

def doc_context_redis():
    """Đọc context bổ sung từ Redis (equity, positions, state) — KHÔNG đọc tick."""
    keys = redis_keys
    try:
        eq_base_raw, eq_diff_raw, state_raw = r.mget(
            keys["eq_base"], keys["eq_diff"], keys["state"]
        )
    except Exception:
        return 0.0, 0.0, {}

    equity_base = float(eq_base_raw) if eq_base_raw else 0.0
    equity_diff = float(eq_diff_raw) if eq_diff_raw else 0.0
    master_state = json.loads(state_raw) if state_raw else {}
    return equity_base, equity_diff, master_state


last_percentile_calc = 0
last_p98_th1 = 0.0
last_p99_th1 = 0.0
last_p98_th2 = 0.0
last_p99_th2 = 0.0
last_stats = {}

def calc_percentiles(now):
    global last_percentile_calc, last_p98_th1, last_p99_th1, last_p98_th2, last_p99_th2, last_stats
    if now - last_percentile_calc < 60:
        return
    last_percentile_calc = now
    
    cutoff = now - 900  # 15 phút
    hist = [x for x in list(spread_history) if x[0] > cutoff]
    
    th1_vals = [x[1] for x in hist]
    th2_vals = [x[2] for x in hist]
    
    if th1_vals:
        th1_vals.sort()
        n1 = len(th1_vals)
        last_p98_th1 = th1_vals[min(int(n1 * 0.98), n1 - 1)]
        last_p99_th1 = th1_vals[min(int(n1 * 0.99), n1 - 1)]
    if th2_vals:
        th2_vals.sort()
        n2 = len(th2_vals)
        last_p98_th2 = th2_vals[min(int(n2 * 0.98), n2 - 1)]
        last_p99_th2 = th2_vals[min(int(n2 * 0.99), n2 - 1)]
        
    stats = {
        "th1_p98": {"c200": 0, "c300": 0, "c500": 0},
        "th1_p99": {"c200": 0, "c300": 0, "c500": 0},
        "th2_p98": {"c200": 0, "c300": 0, "c500": 0},
        "th2_p99": {"c200": 0, "c300": 0, "c500": 0}
    }
    
    t1_p98_start = t1_p99_start = t2_p98_start = t2_p99_start = None
    
    for pt in hist:
        t, v1, v2 = pt
        if v1 >= last_p98_th1:
            if t1_p98_start is None: t1_p98_start = t
            dur = t - t1_p98_start
            if dur >= 0.5: stats["th1_p98"]["c500"] += 1
            if dur >= 0.3: stats["th1_p98"]["c300"] += 1
            if dur >= 0.2: stats["th1_p98"]["c200"] += 1
        else:
            t1_p98_start = None

        if v1 >= last_p99_th1:
            if t1_p99_start is None: t1_p99_start = t
            dur = t - t1_p99_start
            if dur >= 0.5: stats["th1_p99"]["c500"] += 1
            if dur >= 0.3: stats["th1_p99"]["c300"] += 1
            if dur >= 0.2: stats["th1_p99"]["c200"] += 1
        else:
            t1_p99_start = None

        if v2 >= last_p98_th2:
            if t2_p98_start is None: t2_p98_start = t
            dur = t - t2_p98_start
            if dur >= 0.5: stats["th2_p98"]["c500"] += 1
            if dur >= 0.3: stats["th2_p98"]["c300"] += 1
            if dur >= 0.2: stats["th2_p98"]["c200"] += 1
        else:
            t2_p98_start = None

        if v2 >= last_p99_th2:
            if t2_p99_start is None: t2_p99_start = t
            dur = t - t2_p99_start
            if dur >= 0.5: stats["th2_p99"]["c500"] += 1
            if dur >= 0.3: stats["th2_p99"]["c300"] += 1
            if dur >= 0.2: stats["th2_p99"]["c200"] += 1
        else:
            t2_p99_start = None

    last_stats = stats

def tinh_va_buffer(tick_base, tick_diff):
    """Tính spread và lưu vào deque buffer."""
    global latest_payload

    if not tick_base or not tick_diff:
        return None

    now = time.time()
    spread_pivot = lay_spread_pivot(cap_hien_tai)
    chenh_th1_raw = tick_base["bid"] - tick_diff["ask"]
    chenh_th2_raw = tick_diff["bid"] - tick_base["ask"]
    chenh_th1 = chenh_th1_raw - spread_pivot
    chenh_th2 = chenh_th2_raw + spread_pivot

    # Lưu vào deque
    spread_point = (now, round(chenh_th1, 4), round(chenh_th2, 4))
    spread_history.append(spread_point)

    # Đọc context bổ sung (throttled — chỉ đọc khi cần gửi WebSocket)
    equity_base, equity_diff, master_state = doc_context_redis()
    huong_dang_danh = master_state.get("huong_dang_danh")
    so_lenh = len(master_state.get("lich_su_vao_lenh", []))

    # Tính tín hiệu
    try:
        tin_hieu = check_tin_hieu_arbitrage(
            tick_base, tick_diff, cap_hien_tai, huong_dang_danh
        )
        hanh_dong = tin_hieu["hanh_dong"]
    except Exception:
        hanh_dong = "CHO"

    calc_percentiles(now)
    
    payload = {
        "tick_base": tick_base,
        "tick_diff": tick_diff,
        "spread_th1": round(chenh_th1, 4),
        "spread_th2": round(chenh_th2, 4),
        "spread_th1_raw": round(chenh_th1_raw, 4),
        "spread_th2_raw": round(chenh_th2_raw, 4),
        "p98_th1": last_p98_th1,
        "p99_th1": last_p99_th1,
        "p98_th2": last_p98_th2,
        "p99_th2": last_p99_th2,
        "stats": last_stats,
        "hanh_dong": hanh_dong,
        "huong_dang_danh": huong_dang_danh,
        "so_lenh": so_lenh,
        "equity_base": equity_base,
        "equity_diff": equity_diff,
        "config": build_config_snapshot(cap_hien_tai),
        "timestamp": now,
    }

    with payload_lock:
        latest_payload = payload

    return payload


# ==========================================
# 5. REDIS PUB/SUB LISTENER (Background Thread)
# ==========================================
def pubsub_listener():
    """Thread lắng nghe per-tick từ Worker qua Pub/Sub."""
    global latest_tick_base, latest_tick_diff, config, cap_hien_tai, redis_keys

    # Tạo Redis connection riêng cho Pub/Sub (bắt buộc — Pub/Sub chiếm connection)
    r_sub = redis.Redis(
        host=redis_conf["host"],
        port=redis_conf["port"],
        db=redis_conf["db"],
        decode_responses=True,
        socket_timeout=None,  # Block vô thời hạn chờ message
    )
    ps = r_sub.pubsub(ignore_subscribe_messages=True)
    ps.subscribe(PUB_TICK_BASE, PUB_TICK_DIFF)
    print(f"📡 Subscribed: {PUB_TICK_BASE}, {PUB_TICK_DIFF}")

    last_config_check = 0

    for msg in ps.listen():
        if msg["type"] != "message":
            continue

        channel = msg["channel"]
        tick_data = json.loads(msg["data"])

        with tick_lock:
            if channel == PUB_TICK_BASE:
                latest_tick_base = tick_data
            elif channel == PUB_TICK_DIFF:
                latest_tick_diff = tick_data

            tb = latest_tick_base
            td = latest_tick_diff

        # Chỉ tính spread khi có cả 2 tick
        if tb and td:
            payload = tinh_va_buffer(tb, td)

            # Push tới tất cả WebSocket clients
            if payload:
                with ws_clients_lock:
                    clients = list(ws_clients)
                for q in clients:
                    try:
                        q.put_nowait(payload)
                    except Exception:
                        pass

        # Hot-reload config mỗi 2 giây
        now = time.time()
        if now - last_config_check >= 2.0:
            last_config_check = now
            try:
                current_modified = os.path.getmtime(CONFIG_FILE)
                if current_modified != last_config_modified:
                    config, cap_hien_tai = doc_config()
                    if cap_hien_tai:
                        redis_keys = build_redis_keys(cap_hien_tai)
                        print("🔄 [HOT RELOAD] Config đã cập nhật")
            except Exception:
                pass


# Khởi chạy Pub/Sub listener thread (daemon — tự chết khi main process tắt)
pubsub_thread = threading.Thread(target=pubsub_listener, daemon=True)
pubsub_thread.start()

# ==========================================
# 6. FASTAPI APP
# ==========================================
import queue

app = FastAPI(title="Spread Viewer")

static_dir = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(static_dir, exist_ok=True)


@app.get("/", response_class=HTMLResponse)
async def serve_index():
    """Serve trang dashboard chính."""
    index_path = os.path.join(static_dir, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return HTMLResponse("<h1>index.html not found in src/static/</h1>")


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """WebSocket: gửi history trước, rồi push per-tick."""
    await ws.accept()
    print("🔗 Browser đã kết nối WebSocket")

    # Tạo queue riêng cho client này
    client_q = queue.Queue(maxsize=500)

    with ws_clients_lock:
        ws_clients.add(client_q)

    try:
        # 1. Gửi spread history (backend buffer) để chart vẽ ngay lập tức
        history = list(spread_history)  # Snapshot deque
        if history:
            history_payload = {
                "type": "history",
                "data": [
                    {"timestamp": ts, "spread_th1": th1, "spread_th2": th2}
                    for ts, th1, th2 in history
                ],
            }
            await ws.send_json(history_payload)
            print(f"   📜 Đã gửi {len(history)} điểm history")

        # 2. Gửi payload hiện tại (config, equity, tick prices...)
        with payload_lock:
            current = latest_payload.copy() if latest_payload else None
        if current:
            current["type"] = "tick"
            await ws.send_json(current)

        # 3. Push per-tick từ queue
        while True:
            try:
                payload = await asyncio.get_event_loop().run_in_executor(
                    None, client_q.get, True, 1.0  # Block 1 giây rồi thử lại
                )
                payload["type"] = "tick"
                await ws.send_json(payload)
            except queue.Empty:
                # Gửi heartbeat để giữ connection
                try:
                    await ws.send_json({"type": "heartbeat"})
                except Exception:
                    break
            except Exception:
                break

    except WebSocketDisconnect:
        print("🔌 Browser đã ngắt kết nối")
    except Exception as e:
        print(f"⚠️ WebSocket lỗi: {e}")
    finally:
        with ws_clients_lock:
            ws_clients.discard(client_q)


# ==========================================
# 7. KHỞI ĐỘNG
# ==========================================
if __name__ == "__main__":
    print(f"📊 SPREAD VIEWER đang khởi động...")
    print(f"   Pair: {args.pair_id}")
    print(f"   Port: {args.port}")
    print(f"   Mode: Per-tick (Pub/Sub) + Backend Buffer (60 phút)")
    print(f"   URL: http://localhost:{args.port}")
    print(f"   Redis: {redis_conf['host']}:{redis_conf['port']}")
    print()

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=args.port,
        log_level="warning",
        access_log=False,
    )

