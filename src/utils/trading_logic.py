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
    
    ket_qua = {"hanh_dong": "CHO_DOI"}

    # ==========================================
    # 1. KIỂM TRA TÍN HIỆU ĐÓNG LỆNH (Chỉ check nếu đang giữ lệnh hướng đó)
    # ==========================================
    
    if huong_dang_danh == "TH2":
        # Đóng TH2: Đang giữ BUY Base, SELL Diff -> Chờ chênh lệch thu hẹp để đóng (Bán Base, Mua lại Diff)
        if (base_bid - diff_ask) >= dev_close:
            return {
                "hanh_dong": "DONG_LENH",
                "chenh_lech": base_bid - diff_ask,
                "loai_dong": "TH2" 
            }
            
    elif huong_dang_danh == "TH1":
        # Đóng TH1: Đang giữ SELL Base, BUY Diff -> Chờ chênh lệch thu hẹp để đóng (Mua lại Base, Bán Diff)
        if (diff_bid - base_ask) >= dev_close:
            return {
                "hanh_dong": "DONG_LENH",
                "chenh_lech": diff_bid - base_ask,
                "loai_dong": "TH1"
            }

    # ==========================================
    # 2. KIỂM TRA TÍN HIỆU VÀO LỆNH (Luôn check để nhồi lệnh hoặc vô lệnh mới)
    # ==========================================
    
    # Vào lệnh TH1: Base cao hơn Diff (Sell Base, Buy Diff)
    if (base_bid - diff_ask) >= dev_entry:
        return {
            "hanh_dong": "VAO_LENH",
            "loai_lenh": "TH1",
            "lenh_base": "SELL",
            "lenh_diff": "BUY",
            "chenh_lech": base_bid - diff_ask
        }
        
    # Vào lệnh TH2: Diff cao hơn Base (Buy Base, Sell Diff)
    elif (diff_bid - base_ask) >= dev_entry:
        return {
            "hanh_dong": "VAO_LENH",
            "loai_lenh": "TH2",
            "lenh_base": "BUY",
            "lenh_diff": "SELL",
            "chenh_lech": diff_bid - base_ask
        }
        
    return ket_qua