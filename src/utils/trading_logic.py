def lay_spread_pivot(config_cap):
    spread_pivot = config_cap.get('spread_pivot')
    if spread_pivot is None:
        spread_pivot = config_cap.get('mean', config_cap.get('pivot', 0.0))

    try:
        return float(spread_pivot)
    except (TypeError, ValueError):
        return 0.0


def check_tin_hieu_arbitrage(tick_base, tick_diff, config_cap, huong_dang_danh=None):
    """
    Phân tích giá và trả về tín hiệu VÀO LỆNH hoặc ĐÓNG LỆNH.
    Chỉ check điều kiện đóng khi có biến huong_dang_danh.
    """
    base_bid = tick_base['bid']
    base_ask = tick_base['ask']
    diff_bid = tick_diff['bid']
    diff_ask = tick_diff['ask']
    
    dev_entry = config_cap['deviation_entry']
    dev_close = config_cap['deviation_close']
    spread_pivot = lay_spread_pivot(config_cap)
    
    # 🚀 Cache kết quả tính toán chênh lệch (tránh tính lại nhiều lần)
    chenh_th1_raw = base_bid - diff_ask
    chenh_th2_raw = diff_bid - base_ask

    chenh_th1 = chenh_th1_raw - spread_pivot  # TH1: Base cao hơn Diff
    chenh_th2 = chenh_th2_raw + spread_pivot  # TH2: Diff cao hơn Base

    # ==========================================
    # 1. KIỂM TRA TÍN HIỆU ĐÓNG LỆNH (Chỉ check nếu đang giữ lệnh hướng đó)
    # ==========================================
    
    if huong_dang_danh == "TH2":
        # Đóng TH2: Đang giữ BUY Base, SELL Diff -> Chờ chênh lệch thu hẹp để đóng (Bán Base, Mua lại Diff)
        if chenh_th1 >= dev_close:
            return {
                "hanh_dong": "DONG_LENH",
                "chenh_lech": chenh_th1,
                "chenh_lech_raw": chenh_th1_raw,
                "spread_pivot": spread_pivot,
                "loai_dong": "TH2" 
            }
            
    elif huong_dang_danh == "TH1":
        # Đóng TH1: Đang giữ SELL Base, BUY Diff -> Chờ chênh lệch thu hẹp để đóng (Mua lại Base, Bán Diff)
        if chenh_th2 >= dev_close:
            return {
                "hanh_dong": "DONG_LENH",
                "chenh_lech": chenh_th2,
                "chenh_lech_raw": chenh_th2_raw,
                "spread_pivot": spread_pivot,
                "loai_dong": "TH1"
            }

    # ==========================================
    # 2. KIỂM TRA TÍN HIỆU VÀO LỆNH (Luôn check để nhồi lệnh hoặc vô lệnh mới)
    # ==========================================
    
    # Vào lệnh TH1: Base cao hơn Diff (Sell Base, Buy Diff)
    if chenh_th1 >= dev_entry:
        return {
            "hanh_dong": "VAO_LENH",
            "loai_lenh": "TH1",
            "lenh_base": "SELL",
            "lenh_diff": "BUY",
            "chenh_lech": chenh_th1,
            "chenh_lech_raw": chenh_th1_raw,
            "spread_pivot": spread_pivot
        }
        
    # Vào lệnh TH2: Diff cao hơn Base (Buy Base, Sell Diff)
    elif chenh_th2 >= dev_entry:
        return {
            "hanh_dong": "VAO_LENH",
            "loai_lenh": "TH2",
            "lenh_base": "BUY",
            "lenh_diff": "SELL",
            "chenh_lech": chenh_th2,
            "chenh_lech_raw": chenh_th2_raw,
            "spread_pivot": spread_pivot
        }
        
    return {"hanh_dong": "CHO_DOI"}
