import argparse
import os
import requests
import csv
from datetime import datetime

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage_file_name", type=str)
    parser.add_argument("--game_name", type=str)
    parser.add_argument("--tag_line", type=str)
    parser.add_argument("--api_key", type=str)

    args = parser.parse_args()
    return args.stage_file_name, args.game_name, args.tag_line, args.api_key

def get_file_path(stage_file_name: str):
    src_path = os.path.abspath(__file__)
    src_dir_path = os.path.dirname(src_path)
    project_path = src_dir_path.replace("/src","")
    stage_file_path = f"{project_path}/data/{stage_file_name}"

    return stage_file_path

def get_puuid_by_riot_id(game_name: str, tag_line: str, api_key: str):
    url = f"https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}"
    headers = {"X-Riot-Token": api_key}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return data.get("puuid")

def get_summoner_data_by_puuid(tag_line: str, puuid: str, api_key: str):
    url = f"https://{tag_line}.api.riotgames.com/lol/summoner/v4/summoners/by-puuid/{puuid}"
    headers = {"X-Riot-Token": api_key}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return data.get("profileIconId"), data.get("revisionDate"), data.get("summonerLevel")

def create_player_csv(
    stage_file_path:str,
    puuid: str,
    game_name: str,
    tag_line: str,
    profile_icon_id: int,
    revision_date: int,
    summoner_level: int,
    now:str
):
    with open(stage_file_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["puuid", "game_name", "tag_line", "profile_icon_id", "revision_date", "summoner_level", "datetime"])
        writer.writerow([puuid, game_name, tag_line, profile_icon_id, revision_date, summoner_level, now])
    return

def get_updated_rows(
    stage_file_path:str,
    puuid: str,
    game_name: str,
    tag_line: str,
    profile_icon_id: int,
    revision_date: int,
    summoner_level: int,
    now:str
):
    rows = []
    with open(stage_file_path, mode="r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        updated = False
        for row in reader:
            if row["puuid"] == puuid:
                row["game_name"] = game_name
                row["tag_line"] = tag_line
                row["profile_icon_id"] = str(profile_icon_id)
                row["revision_date"] = str(revision_date)
                row["summoner_level"] = str(summoner_level)
                row["datetime"] = now
                updated = True
            rows.append(row)
        if not updated:
            newRow = {"puuid": puuid,
                "game_name": game_name,
                "tag_line": tag_line,
                "profile_icon_id": str(profile_icon_id),
                "revision_date": str(revision_date),
                "summoner_level": str(summoner_level),
                "datetime": now
            }
            rows.append(newRow)
    return rows

def update_player_csv(
    stage_file_path:str,
    puuid: str,
    game_name: str,
    tag_line: str,
    profile_icon_id: int,
    revision_date: int,
    summoner_level: int,
    now:str
):
    rows = get_updated_rows(stage_file_path, puuid, game_name, tag_line, profile_icon_id, revision_date, summoner_level, now)
    
    with open(stage_file_path, mode="w", newline="", encoding="utf-8") as file:
        column_names = rows[0].keys()
        writer = csv.DictWriter(file, fieldnames=column_names)
        writer.writeheader()
        writer.writerows(rows)

def save_player_to_csv(
    puuid: str,
    game_name: str,
    tag_line: str,
    profile_icon_id: int,
    revision_date: int,
    summoner_level: int,
    stage_file_path:str
):
    stage_file_dir = stage_file_path.replace("/stage_player.csv","")
    if not os.path.exists(stage_file_dir):
        os.makedirs(stage_file_dir)
    
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    file_exists = os.path.isfile(stage_file_path)
    if not file_exists:
        create_player_csv(stage_file_path, puuid, game_name, tag_line, profile_icon_id, revision_date, summoner_level, now)
    else:
        update_player_csv(stage_file_path, puuid, game_name, tag_line, profile_icon_id, revision_date, summoner_level, now)

if __name__ == "__main__":
    # How to run:
    # python [...]/load_player.py --stage_file_name stage_player.csv --game_name OTalDoPedrinho --tag_line BR1 --api_key <api_key>
    
    stage_file_name, game_name, tag_line, api_key = get_args()

    stage_file_path = get_file_path(stage_file_name)

    puuid = get_puuid_by_riot_id(game_name, tag_line, api_key)

    profile_icon_id, revision_date, summoner_level = get_summoner_data_by_puuid(tag_line, puuid, api_key)

    save_player_to_csv(puuid, game_name, tag_line, profile_icon_id, revision_date, summoner_level, stage_file_path)