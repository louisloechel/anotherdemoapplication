import os
import random
import uuid
from faker import Faker
from datetime import datetime, timedelta
import json

# Initialize Faker for generating usernames
fake = Faker()

# Define ranges for physiological parameters
def generate_respiratory_rate():
    return random.randint(10, 30)

def generate_oxygen_saturation():
    return random.randint(85, 100)

def generate_blood_pressure_systolic():
    return random.randint(90, 200)

def generate_blood_pressure_diastolic():
    return random.randint(60, 120)

def generate_pulse():
    return random.randint(50, 130)

def generate_body_temperature():
    return round(random.uniform(35.0, 40.0), 1)

def generate_newstotal():
    return random.randint(1, 10)

def generate_newsrepeat():
    return random.choice(["1 hour", "4 hours", "12 hours"])

def generate_acvpu():
    return random.choice(["Alert", "Confused", "Voice", "Pain", "Unresponsive"])

def generate_oxcode():
    return random.choice(["", "RM", "N", "V40", "H28"])

def generate_oxpercent():
    return random.choice(["", "28", "40", "60"])

def generate_oxflow():
    return random.choice(["", "10", "14", "20"])

def generate_escalation():
    return random.choice(["Yes", "No"])

# Generate unique users with consistent userids
def generate_users(num_users=10):
    users = []
    for _ in range(num_users):
        user_id = "03" + str(uuid.uuid4().int)[:5]  # Unique user ID with length 7 and starting with 03
        username = fake.name()
        user_initials = username[0] + username.split()[-1][0]
        users.append({
            "userid": user_id,
            "username": username,
            "userinitials": user_initials.upper()
        })
    return users

# Generate synthetic data with multiple users for each timestamp
def generate_synthetic_data_for_user(user, num_timestamps=100, interval_minutes=10):
    start_date = datetime.now() - timedelta(days=1)  # Start date for entries
    user_data = []

    # decide randomly whether user is registered correctly or not [QA use case]
    # chance of user being registered correctly is 80%
    if random.random() < 0.2:
        user["username"] = "Unknown"
        user["userinitials"] = "XX"

    # Generate entries for each timestamp for this user
    for timestamp_index in range(num_timestamps):
        current_time = start_date + timedelta(minutes=interval_minutes * timestamp_index)
        entry_date = current_time.strftime("%d-%b")
        entry_time = current_time.strftime("%H:%M")

        entry = {
            "recordid": str(uuid.uuid4().int)[:5],  # Unique entry identifier
            "date": entry_date,
            "time": entry_time,
            "resp": generate_respiratory_rate(),
            "oxcode": generate_oxcode(),
            "oxpercent": generate_oxpercent(),
            "oxflow": generate_oxflow(),
            "oxcodename": fake.word(ext_word_list=["reservoir mask", "nasal cannula", "Venturi mask", "humidified oxygen"]),
            "oxsat": generate_oxygen_saturation(),
            "oxsatscale": str(random.randint(1, 2)),
            "bps": generate_blood_pressure_systolic(),
            "bpd": generate_blood_pressure_diastolic(),
            "pulse": generate_pulse(),
            "acvpu": generate_acvpu(),
            "temp": generate_body_temperature(),
            "newstotal": generate_newstotal(),
            "newsrepeat": generate_newsrepeat(),
            "userinitials": user["userinitials"],
            "username": user["username"],           # firstname lastname
            "userid": user["userid"],
            "escalation": generate_escalation()
        }
        user_data.append(entry)
    
    return user_data

# Main function to create output folder and save individual JSON files
def generate_and_save_user_data(num_users, num_timestamps, interval_minutes):
    # Create output folder
    output_folder = "output_json_files"
    os.makedirs(output_folder, exist_ok=True)

    # Generate unique users
    users = generate_users(num_users)

    # Generate data and save a JSON file for each user
    for user in users:
        user_data = generate_synthetic_data_for_user(user, num_timestamps, interval_minutes)
        file_path = os.path.join(output_folder, f"{user['username'].replace(' ', '_')}_{user['userid']}.json")
        
        with open(file_path, "w") as json_file:
            json.dump(user_data, json_file, indent=2)
        print(f"Data saved for user {user['username']} with userid {user['userid']} at {file_path}")

# Run the function
generate_and_save_user_data(num_users=10, num_timestamps=100, interval_minutes=10)