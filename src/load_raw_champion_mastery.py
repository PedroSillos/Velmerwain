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

def get_puuid_regions():
    puuid_regions = []

    file_path = get_file_path(file_name="raw_account_region.json", file_dir="bronze")

    with open(file_path, "r", encoding="utf-8") as file:
        account_regions = json.load(file)
    
    for account_region in account_regions:
        puuid_regions.append({"puuid":account_region["puuid"],"region":account_region["region"]})

    return puuid_regions

def get_champion_masteries(puuid_regions: str, api_key: str):
    champion_masteries = []

    for puuid_region in puuid_regions:
        url = f"https://{puuid_region["region"]}.api.riotgames.com/lol/champion-mastery/v4/champion-masteries/by-puuid/{puuid_region["puuid"]}"
        headers = {"X-Riot-Token": api_key}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        for champion_mastery in response.json():
            champion_masteries.append(champion_mastery)
    
    return champion_masteries

def save_champion_masteries(file_path: str, champion_masteries: dict):
    for champion_mastery in champion_masteries:
        champion_mastery['modifiedOn'] = datetime.now().strftime("%Y-%m-%d %H-%M-%S")

    os.makedirs(file_path.replace("/raw_champion_mastery.json",""), exist_ok=True)

    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(champion_masteries, file, indent=4)

if __name__ == "__main__":
    api_key = get_args()

    puuid_regions = get_puuid_regions()

    champion_masteries = get_champion_masteries(puuid_regions, api_key)

    file_path = get_file_path(file_name="raw_champion_mastery.json", file_dir="bronze")
    
    save_champion_masteries(file_path, champion_masteries)