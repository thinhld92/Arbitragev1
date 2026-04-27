# Single Trade Mode

Tai lieu nay mo ta che do `single` cua bot. Che do nay van lay tin hieu arbitrage tu 2 san Base/Diff, nhung chi vao va dong lenh that tren 1 san duoc chi dinh trong `execution`.

## Cau hinh

Vi du:

```json
{
  "id": "XAUUSD_TICKMILL_HFM",
  "trade_mode": "single",
  "base_exchange": "TICKMILL",
  "base_symbol": "XAUUSD",
  "diff_exchange": "HFM",
  "diff_symbol": "XAUUSD",
  "execution": {
    "exchange": "TICKMILL",
    "symbol": "XAUUSD",
    "volume": 0.01
  }
}
```

`execution.exchange` va `execution.symbol` bat buoc phai trung voi mot trong hai chan Base hoac Diff.

## Gia tri trade_mode

- `hedge`: che do cu, vao lenh ca Base va Diff, co ghep cap va chong lech chan.
- `single`: van dung gia Base/Diff de tao tin hieu, nhung chi vao/dong lenh tren san trong `execution`.

Neu khong khai bao `trade_mode`, launcher mac dinh chay `hedge`.

## Mapping lenh

Neu `execution` trung Base:

| Tin hieu | Lenh that |
| --- | --- |
| TH1 | SELL Base |
| TH2 | BUY Base |

Neu `execution` trung Diff:

| Tin hieu | Lenh that |
| --- | --- |
| TH1 | BUY Diff |
| TH2 | SELL Diff |

## Luong chay

1. Launcher doc `trade_mode`.
2. Neu la `single`, launcher goi `src/master_single.py`.
3. Worker van duoc mo cho ca Base va Diff de cap tick.
4. Master single goi lai logic tin hieu trong `src/utils/trading_logic.py`.
5. Khi co tin hieu vao lenh, Master chi day 1 lenh vao queue cua broker execution.
6. Worker execution ban lenh va bao ticket ve `QUEUE:ORDER_RESULT:<pair_id>`.
7. Master single luu ticket vao state rieng `STATE:SINGLE_MASTER:<pair_id>`.
8. Khi co tin hieu dong, Master day `CLOSE_BY_TICKET` cho dung ticket tren broker execution.
9. Worker dong lenh, lay history deal, va day bien lai sang Accountant.
10. Accountant thay `trade_mode=single` thi ghi CSV ngay, khong cho ghep du Base/Diff.

## Quan ly lenh

Single khong dung so cai ghep `base_ticket + diff_ticket`. Moi lenh duoc quan ly rieng theo ticket:

- `ticket`
- `action` BUY/SELL
- `loai_lenh` TH1/TH2
- `time_entry`
- spread/dev luc vao
- mode vao `[F]` hoac `[C]`
- tick Hz luc vao

Khi restart, Master single khoi phuc state tu Redis. Neu thay position dang ton tai ma chua co trong state, Master se adopt neu xac dinh duoc huong; neu khong ro huong thi cat bang ticket de tranh ket lenh.

## Dieu kien vao/dong lenh

Single van ton trong cac tham so chinh:

- `trading_hours`
- `force_close_hours`
- `max_tick_delay`
- `max_tick_hz_base`
- `max_tick_hz_diff`
- `alert_equity` cua san execution
- `cooldown_second`
- `cooldown_close_second`
- `max_orders`
- `hold_time`
- `stable_mode`
- `stable_time`
- `filter_entry`
- `filter_close`

## Gio cam va stopout

- Trong `force_close_hours`, Master single xa toan bo ticket dang quan ly bang `CLOSE_BY_TICKET`.
- Neu ticket bien mat khoi position Redis, Master coi nhu stopout hoac dong tay, va gui `FETCH_HISTORY_ONLY` de Worker lay history ghi ke toan.
- Khong co cat chan doi ung vi single chi co 1 chan lenh that.

## Accountant

CSV van giu cac cot hedge cu. Single bo sung cac cot cuoi:

- `Trade_Mode`
- `Execution_Exchange`
- `Execution_Symbol`
- `Execution_Role`
- `Execution_Ticket`
- `Execution_Side`

## Chuyen ve hedge

De quay lai che do cu:

```json
"trade_mode": "hedge"
```

Hoac xoa `trade_mode` khoi config cap, vi mac dinh la `hedge`.
