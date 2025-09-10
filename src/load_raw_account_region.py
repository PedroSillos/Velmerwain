import argparse
import os
import requests
from datetime import datetime
import json

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api_key", type=str)

    args = parser.parse_args()
    return args.api_key

def get_file_path(file_name: str, file_dir: str):
    current_path = os.path.abspath(__file__)
    project_path = "/".join(current_path.split("/")[:-2])
    file_path = f"{project_path}/{file_dir}/{file_name}"

    return file_path

def get_stored_puuids():
    puuids = []

    file_path = get_file_path(file_name="raw_account.json", file_dir="bronze")

    with open(file_path, "r", encoding="utf-8") as file:
        accounts = json.load(file)
    
    for account in accounts:
        puuids.append(account["puuid"])

    return puuids

def get_account_region_data(puuids: str, api_key: str):
    account_region_data = []

    for puuid in puuids:
        url = f"https://americas.api.riotgames.com/riot/account/v1/region/by-game/lol/by-puuid/{puuid}"
        headers = {"X-Riot-Token": api_key}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        account_region_data.append(response.json())
    
    return account_region_data

def save_account_region_data(file_path: str, account_region_data: dict):
    for account in account_region_data:
        account['modifiedOn'] = datetime.now().strftime("%Y-%m-%d %H-%M-%S")

    os.makedirs(file_path.replace("/raw_account_region.json",""), exist_ok=True)

    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(account_region_data, file, indent=4)

if __name__ == "__main__":
    api_key = get_args()

    puuids = get_stored_puuids()

    account_region_data = get_account_region_data(puuids, api_key)

    file_path = get_file_path(file_name="raw_account_region.json", file_dir="bronze")

    save_account_region_data(file_path, account_region_data)