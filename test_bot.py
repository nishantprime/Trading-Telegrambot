import urllib.request
import json

TOKEN = "9153649370:AAEliFqY7yivQKQSQzHd4XpS039MmxsfGV4"
raw_chat_id = "-5561018888"

print("--- 1. Testing Bot Info ---")
try:
    req = urllib.request.Request(f"https://api.telegram.org/bot{TOKEN}/getMe")
    with urllib.request.urlopen(req, timeout=10) as resp:
        bot_info = json.loads(resp.read().decode("utf-8"))
        print("Bot is valid:", bot_info.get("result", {}).get("username"))
except Exception as e:
    print("Error getting bot info:", e)

print("\n--- 2. Checking getUpdates for Chat ID ---")
try:
    req = urllib.request.Request(f"https://api.telegram.org/bot{TOKEN}/getUpdates")
    with urllib.request.urlopen(req, timeout=10) as resp:
        updates = json.loads(resp.read().decode("utf-8"))
        print(f"Updates found: {len(updates.get('result', []))}")
        for u in updates.get("result", []):
            chat = None
            if "message" in u:
                chat = u["message"].get("chat")
            elif "my_chat_member" in u:
                chat = u["my_chat_member"].get("chat")
            elif "channel_post" in u:
                chat = u["channel_post"].get("chat")
            if chat:
                print(f"Found chat: id={chat.get('id')}, title={chat.get('title')}, type={chat.get('type')}")
except Exception as e:
    print("Error getting updates:", e)

print("\n--- 3. Testing Send Message ---")
clean_num = raw_chat_id.replace("-", "").replace("#", "").strip()
candidate_ids = [
    raw_chat_id,
    f"-{clean_num}",
    f"-100{clean_num}",
    clean_num
]

for cid in candidate_ids:
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": cid,
        "text": "🔔 TradeFlow Scanner: Bot connected successfully! Testing alert system."
    }).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f" SUCCESS! Message delivered to chat_id: {cid}")
            break
    except Exception as e:
        print(f" Attempt with {cid} failed: {e}")
