import requests
import json
import time
import random

# Ganti dengan URL API Gateway kamu
API_GATEWAY_URL = "https://your-api-id.execute-api.region.amazonaws.com/prod/events"

# Fungsi untuk membuat data event random
def generate_event():
    return {
        "device_id": f"device-{random.randint(1, 100)}",
        "event_type": random.choice(["temperature", "humidity", "motion"]),
        "value": round(random.uniform(10.0, 100.0), 2),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

# Fungsi untuk mengirim data ke API Gateway
def send_event():
    event_data = generate_event()
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(API_GATEWAY_URL, json=event_data, headers=headers)
        if response.status_code == 200:
            print(f"✅ Event Sent: {json.dumps(event_data, indent=2)}")
        else:
            print(f"❌ Failed to send event: {response.text}")
    except Exception as e:
        print(f"❌ Error: {e}")

# Looping untuk mengirim data secara berkala
if __name__ == "__main__":
    while True:
        send_event()
        time.sleep(5)  # Kirim event setiap 5 detik
