import os
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")
BIN_ID = os.getenv("BIN_ID")
BIN_API_URL = f"https://api.jsonbin.io/v3/b/{BIN_ID}/latest"

headers = {
    "X-Master-Key": API_KEY,
}


def get_jsonbin_api():
    response = requests.get(BIN_API_URL, headers=headers)

    if response.status_code == 200:
        return response.json()["record"]
    else:
        print(f"Error: {response.text} with status code ", response.status_code)
        return None


if __name__ == "__main__":
    data = get_jsonbin_api()
    print(
        "Retrieved Data: {data_returned}".format(data_returned=data)
        if data
        else "Empty data returned"
    )
