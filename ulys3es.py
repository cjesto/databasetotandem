import requests
import json
import base64
import time
from datetime import datetime

# ------------------------------ CONFIGURATION ------------------------------

username = "<confidential>"
password = "<confidential>"

sources = [
    "AQ_1", "AQ_2", "AQ_3", "AQ_4",
    "ENVI_1", "ENVI_2", "ENVI_3", "ENVI_4", "ENVI_5"
]

base_url = "https://api-staging.upcare.ph/api/data/care_2425_pgh?organization=care_2425_pgh&source="

aq_node_mapping = {
    "AQ_1": "https://:KwOveeAPSD2d8dXjt9TQyg@tandem.autodesk.com/api/v1/timeseries/models/urn:adsk.dtm:rU_2QJXPT5qZaiYROQybNQ/streams/AQAAAJlyY6nJd0yevntiklr2nQIAAAAA",
    "AQ_2": "https://:pT3RKureS_yecJf1YNhmiA@tandem.autodesk.com/api/v1/timeseries/models/urn:adsk.dtm:rU_2QJXPT5qZaiYROQybNQ/streams/AQAAAEWGLqfxb0wArUDsMlcyFpYAAAAA",
    "AQ_3": "https://:IfcMgJZtSYGMHreNz7d4FQ@tandem.autodesk.com/api/v1/timeseries/models/urn:adsk.dtm:rU_2QJXPT5qZaiYROQybNQ/streams/AQAAAKDWli0Q6Eu-iY47PW-TuLYAAAAA",
    "AQ_4": "https://:YVQOqhhXRziTC2GzvQrQWw@tandem.autodesk.com/api/v1/timeseries/models/urn:adsk.dtm:rU_2QJXPT5qZaiYROQybNQ/streams/AQAAAAteOLATMk-sim113K-eoW8AAAAA",
}

env_node_mapping = {
    "ENVI_1": "https://:SdvJc5DuRuGippRqNMP_Xw@tandem.autodesk.com/api/v1/timeseries/models/urn:adsk.dtm:rU_2QJXPT5qZaiYROQybNQ/streams/AQAAAJp82M2abUtOqNP6T779l-sAAAAA",
    "ENVI_2": "https://:Q4_ETvXuT96LaxAStXbxiA@tandem.autodesk.com/api/v1/timeseries/models/urn:adsk.dtm:rU_2QJXPT5qZaiYROQybNQ/streams/AQAAAA3CpJBimkAFsN9wirlUdgkAAAAA",
    "ENVI_3": "https://:VonnX8U5QPigNFIzT31bUg@tandem.autodesk.com/api/v1/timeseries/models/urn:adsk.dtm:rU_2QJXPT5qZaiYROQybNQ/streams/AQAAAGPbzeor0EfAlJcEQM8XZ7cAAAAA",
    "ENVI_4": "https://:9jtJFLMfTZ-iqwbx8dT4bA@tandem.autodesk.com/api/v1/timeseries/models/urn:adsk.dtm:rU_2QJXPT5qZaiYROQybNQ/streams/AQAAAGIDzzCfLE4VoZULVk0lRLYAAAAA",
    "ENVI_5": "https://:MXthNPiyTPmLS2n7DLXEwQ@tandem.autodesk.com/api/v1/timeseries/models/urn:adsk.dtm:rU_2QJXPT5qZaiYROQybNQ/streams/AQAAACl026Z34UBjrRosKF3YpEoAAAAA"
}

CARE_HIVE_TOKEN = base64.b64encode(f"{username}:{password}".encode()).decode()

CARE_HIVE_HEADERS = {
    "accept": "application/json",
    "content-type": "application/json",
    "authorization": f"Basic {CARE_HIVE_TOKEN}"
}

TANDEM_HEADERS = {"Content-Type": "application/json"}

# ------------------------------ FETCH & SEND ------------------------------

def fetch_and_send_latest(source):
    url = f"{base_url}{source}"
    try:
        response = requests.get(url, headers=CARE_HIVE_HEADERS, timeout=10)
        response.raise_for_status()
        json_data = response.json()

        # Check if data exists
        if "data" in json_data and json_data["data"]:
            # Sort and pick only the most recent entry
            latest_entry = max(json_data["data"], key=lambda x: x.get("local_time", ""))

            local_time = latest_entry.get("local_time")
            if not local_time:
                return

            # Clean entry
            tandem_data = {
                k: v for k, v in latest_entry.items()
                if v is not None and k not in ["created_at", "topic", "type"]
            }
            tandem_data["local_time"] = local_time

            # Determine destination
            if source in aq_node_mapping:
                tandem_url = aq_node_mapping[source]
            elif source in env_node_mapping:
                tandem_url = env_node_mapping[source]
            else:
                print(f"⚠️ Unknown source: {source}")
                return

            # Send to Tandem
            tandem_response = requests.post(
                tandem_url,
                headers=TANDEM_HEADERS,
                data=json.dumps(tandem_data)
            )

            if tandem_response.status_code == 200:
                print(f"Sent latest {source} data at {local_time}")
            else:
                print(f"Failed to send {source} data: {tandem_response.status_code}")
        else:
            print(f"ℹNo data for {source}")
    except Exception as e:
        print(f"Exception with {source}: {e}")

# ------------------------------ MAIN LOOP ------------------------------

if __name__ == "__main__":
    while True:
        print(f"\n[{datetime.now().isoformat()}] Checking real-time updates...")
        for src in sources:
            fetch_and_send_latest(src)
        time.sleep(15)
