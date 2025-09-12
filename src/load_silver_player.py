import os
import requests
from datetime import datetime
import json

def get_file_path(file_name: str, file_dir: str):
    current_path = os.path.abspath(__file__)
    project_path = "/".join(current_path.split("/")[:-2])
    file_path = f"{project_path}/{file_dir}/{file_name}"

    return file_path

def get_puuids_and_accounts():
    puuids = []
    accounts = []

    file_path = get_file_path(file_name="raw_account.json", file_dir="bronze")

    with open(file_path, "r", encoding="utf-8") as file:
        accounts = json.load(file)
    
    for account in accounts:
        puuids.append(account["puuid"])

    return puuids, accounts

def get_account_regions(puuids: list, accounts: list):
    file_path = get_file_path(file_name="raw_account_region.json", file_dir="bronze")

    with open(file_path, "r", encoding="utf-8") as file:
        account_regions = json.load(file)
    
    for puuid in puuids:
        for account in accounts:
            if puuid == account["puuid"]:
                for account_region in account_regions:
                    if puuid == account_region["puuid"]:
                        account["game"] = account_region["game"]
                        account["region"] = account_region["region"]
                        break
                break

    return accounts

def save_summoners(file_path: str, summoners: dict):
    for summoner in summoners:
        summoner['modifiedOn'] = datetime.now().strftime("%Y-%m-%d %H-%M-%S")

    os.makedirs(file_path.replace("/raw_summoner.json",""), exist_ok=True)

    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(summoners, file, indent=4)

if __name__ == "__main__":
    puuids, accounts = get_puuids_and_accounts()

    account_regions = get_account_regions(puuids, accounts)

    #file_path = get_file_path(file_name="raw_summoner.json", file_dir="bronze")

    #save_summoners(file_path, summoners)