import requests
import json

with open("generate_data/keys/patient_api_key.json", "r") as key_gen:
    key = json.load(key_gen)

with open("generate_data/data/patient_data.json", "r") as customer_data:
    data = json.load(customer_data)

API_KEY = key["API_KEYS"][0]["API_KEY"] 
BIN_API_URL = "https://api.jsonbin.io/v3/b"

headers = {
    "Content-Type": "application/json",
    "X-Master-Key": API_KEY,
}


response = requests.post(BIN_API_URL, json={"record": data}, headers=headers)

if response.status_code in [200, 201]:
    bin_id = response.json()["metadata"]["id"]
    print(f"Data successfully stored. Bin ID: {bin_id}")
else:
    print(f"Error: {response.text}")

if bin_id:
    key["API_KEYS"][0]["BIN_ID"].append({"ID": bin_id}) 
    with open("generate_data/keys/patient_api_key.json", "w") as key_data:
        json.dump(key, key_data, indent= 4)

print("BIN_ID added successfully!")