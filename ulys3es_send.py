import requests
import json
import base64

# ------------------------------ CONFIGURATIONS ------------------------------

username = "cjesto"
password = "Chajane12"
url = "https://api-staging.upcare.ph/api/data/care_2425_pgh?organization=care_2425_pgh&local_time_start=2025-05-20T07:00:00&local_time_end=2025-05-20T15:59:59"

# Autodesk Tandem nodes
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

# ------------------------------ FETCH & SEND FUNCTION ------------------------------

def fetch_and_send_to_tandem():
    response = requests.get(url, headers=CARE_HIVE_HEADERS)

    if response.status_code == 200:
        data = response.json()

        if "data" in data and len(data["data"]) > 0:
            # Sort the data by 'local_time' in ascending order
            data["data"].sort(key=lambda x: x.get("local_time"))

            for entry in data["data"]:
                source = entry.get("source")
                local_time = entry.get("local_time")

                if not local_time or not source:
                    continue

                # Remove null values and keep local_time
                tandem_data = {
                    key: value for key, value in entry.items()
                    if value is not None and key != "created_at" and key != "topic" and key != "type"
                }

                # Ensure local_time is included
                tandem_data["local_time"] = local_time

                if source in aq_node_mapping:
                    tandem_url = aq_node_mapping[source]
                elif source in env_node_mapping:
                    tandem_url = env_node_mapping[source]
                else:
                    print(f"Unknown source: {source}")
                    continue

                # Send cleaned data to Tandem
                tandem_response = requests.post(tandem_url, headers=TANDEM_HEADERS, data=json.dumps(tandem_data))

                if tandem_response.status_code == 200:
                    print(f"✅ Data sent to {source} at {local_time}")
                else:
                    print(f"❌ Failed to send data to {source}: {tandem_response.status_code}")
        else:
            print("No data found in CARE HIVE.")
    else:
        print(f"Error fetching data: {response.status_code}, {response.text}")

# ------------------------------ MAIN LOOP ------------------------------

if __name__ == "__main__":
    while True:
        print("\nFetching data from CARE HIVE and sending to Autodesk Tandem...")
        fetch_and_send_to_tandem()
        # time.sleep(5)
