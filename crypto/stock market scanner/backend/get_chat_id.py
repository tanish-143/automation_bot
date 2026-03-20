"""Poll Telegram getUpdates to discover chat_id. Run, then send /start to the bot."""
import httpx, time

TOKEN = "8631546842:AAFicbRAxMkFga82OpmCAKXGbOGXJGHWHWM"
BASE = f"https://api.telegram.org/bot{TOKEN}"

print("Waiting for a message to @Crypto_market_scanner_bot ...")
print("Send /start or any text to the bot NOW.\n")

for i in range(24):  # ~2 minutes
    try:
        r = httpx.get(f"{BASE}/getUpdates", params={"timeout": 5, "allowed_updates": '["message"]'}, timeout=10.0)
        data = r.json()
        if data.get("result"):
            for u in data["result"]:
                msg = u.get("message", {})
                chat = msg.get("chat", {})
                cid = chat.get("id")
                name = chat.get("first_name", "") + " " + chat.get("username", "")
                print(f"SUCCESS  chat_id = {cid}   ({name.strip()})")
                # send confirmation
                httpx.post(f"{BASE}/sendMessage", json={
                    "chat_id": cid,
                    "text": "✅ Bot connected! You will receive scanner alerts here.",
                })
            break
    except Exception as e:
        print(f"  retry ({e})")
    time.sleep(0.5)
else:
    print("No message received. Please try again.")
