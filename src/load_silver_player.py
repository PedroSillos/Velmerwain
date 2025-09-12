import os
from datetime import datetime
import json
import csv

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

def get_account_regions(puuids: list, players: list):
    file_path = get_file_path(file_name="raw_account_region.json", file_dir="bronze")

    with open(file_path, "r", encoding="utf-8") as file:
        account_regions = json.load(file)
    
    for puuid in puuids:
        for player in players:
            if puuid == player["puuid"]:
                for account_region in account_regions:
                    if puuid == account_region["puuid"]:
                        player["game"] = account_region["game"]
                        player["region"] = account_region["region"]
                        break
                break

    return players

def get_summoners(puuids: list, players: list):
    file_path = get_file_path(file_name="raw_summoner.json", file_dir="bronze")

    with open(file_path, "r", encoding="utf-8") as file:
        summoners = json.load(file)
    
    for puuid in puuids:
        for player in players:
            if puuid == player["puuid"]:
                for summoner in summoners:
                    if puuid == summoner["puuid"]:
                        player["profileIconId"] = summoner["profileIconId"]
                        player["revisionDate"] = summoner["revisionDate"]
                        player["summonerLevel"] = summoner["summonerLevel"]
                        break
                break

    return players

def save_players(file_path: str, players: dict):
    for player in players:
        player['sourceModifiedOn'] = player['modifiedOn']
        player['modifiedOn'] = datetime.now().strftime("%Y-%m-%d %H-%M-%S")

    os.makedirs(file_path.replace("/silver_player.csv",""), exist_ok=True)

    field_names = players[0].keys()
    
    with open(file_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=field_names)

if __name__ == "__main__":
    puuids, players = get_puuids_and_accounts()

    players = get_account_regions(puuids, players)

    players = get_summoners(puuids, players)

    file_path = get_file_path(file_name="silver_player.csv", file_dir="silver")

    save_players(file_path, players)