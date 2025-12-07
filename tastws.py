import websocket, json, threading, time

CHANNEL_SNAPSHOT = "lightning_board_snapshot_FX_BTC_JPY"
CHANNEL_BOARD = "lightning_board_FX_BTC_JPY"

def on_message(ws, message):
    print(f"受信: {message[:200]}...")  # 先頭だけ表示

def on_error(ws, error):
    print(f"エラー: {error}")

def on_close(ws, close_status_code, close_msg):
    print("接続終了")

def on_open(ws):
    print("接続しました。購読リクエストを送信します...")
    ws.send(json.dumps({"method": "subscribe", "params": {"channel": CHANNEL_SNAPSHOT}}))
    ws.send(json.dumps({"method": "subscribe", "params": {"channel": CHANNEL_BOARD}}))

if __name__ == "__main__":
    ws_url = "wss://ws.lightstream.bitflyer.com/json-rpc"
    ws = websocket.WebSocketApp(ws_url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
    t = threading.Thread(target=ws.run_forever)
    t.start()
    time.sleep(10)
    ws.close()
    print("テスト終了")
