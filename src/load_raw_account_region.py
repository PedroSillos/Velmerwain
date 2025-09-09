import argparse
import os
import requests
from datetime import datetime
import json

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--game_name", type=str)
    parser.add_argument("--tag_line", type=str)
    parser.add_argument("--api_key", type=str)

    args = parser.parse_args()
    return args.game_name, args.tag_line, args.api_key

def get_file_path(file_name: str, file_dir: str):
    current_path = os.path.abspath(__file__)
    project_path = "/".join(current_path.split("/")[:-2])
    file_path = f"{project_path}/{file_dir}/{file_name}"

    return file_path

def get_account_data(game_name: str, tag_line: str, api_key: str):
    url = f"https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}"
    headers = {"X-Riot-Token": api_key}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def save_account_data(file_path: str, account_data: dict):
    account_data['modifiedOn'] = datetime.now().strftime("%Y-%m-%d %H-%M-%S")

    os.makedirs(file_path.replace("/raw_account.json",""), exist_ok=True)
    
    if os.path.isfile(file_path):
        users = []
        updated = False
        
        with open(file_path, "r", encoding="utf-8") as file:
            users = json.load(file)
        
        for user in users:
            if user["puuid"] == account_data["puuid"]:
                user["gameName"] = account_data["gameName"]
                user["tagLine"] = account_data["tagLine"]
                user["modifiedOn"] = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
                updated = True
        
        if not updated:
            users.append(account_data)
        
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(users, file, indent=4)

    else:
        users = []
        users.append(account_data)

        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(users, file, indent=4)

if __name__ == "__main__":
    game_name, tag_line, api_key = get_args()

    account_data = get_account_data(game_name, tag_line, api_key)

    file_path = get_file_path(file_name="raw_account.json", file_dir="bronze")

    save_account_data(file_path, account_data)