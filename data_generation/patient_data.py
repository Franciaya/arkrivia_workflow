import json
import requests
import random
from faker import Faker

fake = Faker()

API_URL = "https://api.postcodes.io/random/postcodes"


def get_random_city_postcode():
    """Fetch a random city and postcode from an API, ensuring it's in England or Wales."""
    while True:
        response = requests.get(API_URL)
        if response.status_code == 200:
            data = response.json()
            country = data["result"]["country"]
            if country in ["England", "Wales"]:  # Ensure it's from England or Wales
                return data["result"]["admin_district"], data["result"]["postcode"]
    return "Unknown", "Unknown"


def generate_patient_data(n=1000):
    """Generate fake patient data for patients in England and Wales only."""
    patients = []
    diseases = ["Depression", "Schizophrenia", "Cancer", "Diabetes", "Hypertension"]

    for patient_id in range(1, n + 1):  # Generate sequential integer IDs
        city, postcode = get_random_city_postcode()
        patient = {
            "PatientId": patient_id,  # Use integer IDs
            "PatientName": fake.name(),
            "City": city,
            "PostCode": postcode,
            "Disease": random.choice(diseases),
        }
        # while len(patients) < n:
        #     city, postcode = get_random_city_postcode()
        #     patient = {
        #         "PatientId": str(fake.uuid4()),  # Ensure UUID is string
        #         "PatientName": fake.name(),
        #         "City": city,
        #         "PostCode": postcode,
        #         "Disease": random.choice(diseases)
        #     }
        patients.append(patient)

    return patients


if __name__ == "__main__":

    data = generate_patient_data()
    with open("data/patient_data.json", "w") as f:
        json.dump(data, f, indent=4)

    print("Generated patient data saved to data/patient_data.json")
