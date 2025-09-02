import argparse
import os
import requests
import csv
from datetime import datetime

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage_player_file_name", type=str)
    parser.add_argument("--stage_match_file_name", type=str)
    parser.add_argument("--api_key", type=str)

    args = parser.parse_args()
    return args.stage_player_file_name, args.stage_match_file_name, args.api_key

def get_file_path(stage_file_name: str):
    src_path = os.path.abspath(__file__)
    src_dir_path = os.path.dirname(src_path)
    project_path = src_dir_path.replace("/src","")
    stage_file_path = f"{project_path}/data/{stage_file_name}"

    return stage_file_path

def get_puuids(stage_player_file_path: str):
    puuids = []

    with open(stage_player_file_path, mode="r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            puuids.append(row["puuid"])

    return puuids

def get_match_ids_by_puuid(puuid, api_key):
    url = f"https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?queue=420&type=ranked&start=0&count=100&api_key={api_key}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"{response.status_code} - {response.text}")
    return response.json()

def get_match_data(match_ids, api_key, stage_match_file_path):
    matches_data = []

    if os.path.isfile(stage_match_file_path):
        for match_id in match_ids:
            match_id_exists = False
            
            with open(stage_match_file_path, mode="r", newline="", encoding="utf-8") as file:
                reader = list(csv.reader(file))
                rows = reader[1:]
                for row in rows:
                    if row[0] == match_id:
                        match_id_exists = True
            
            if not match_id_exists:
                url = f"https://americas.api.riotgames.com/lol/match/v5/matches/{match_id}?api_key={api_key}"
                response = requests.get(url)
                if response.status_code != 200:
                    print(f"{match_id}: {response.status_code} - {response.text}")
                else:
                    matches_data.append(response.json())
    else:
        for match_id in match_ids:
            url = f"https://americas.api.riotgames.com/lol/match/v5/matches/{match_id}?api_key={api_key}"
            response = requests.get(url)
            if response.status_code != 200:
                print(f"{match_id}: {response.status_code} - {response.text}")
            else:
                matches_data.append(response.json())
    return matches_data

def save_matches_to_csv(puuid, matches_data, stage_match_file_path):
    if not matches_data:
        return

    if os.path.isfile(stage_match_file_path):
        with open(stage_match_file_path, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            for match in matches_data:
                player_index_in_match = -1
                player_puuids = match.get("metadata").get("participants")

                for player_puuid in player_puuids:
                    if player_puuid == puuid:
                        player_index_in_match = player_puuids.index(puuid)
                
                info = match.get("info")
                writer.writerow([
                    match.get("metadata").get("matchId"),
                    info.get("gameDuration"),
                    info.get("gameCreation"),
                    info.get("gameVersion"),
                    info.get("participants")[player_index_in_match].get("puuid"),
                    info.get("participants")[player_index_in_match].get("assists"),
                    info.get("participants")[player_index_in_match].get("deaths"),
                    info.get("participants")[player_index_in_match].get("kills"),
                    info.get("participants")[player_index_in_match].get("champLevel"),
                    info.get("participants")[player_index_in_match].get("championId"),
                    info.get("participants")[player_index_in_match].get("championName"),
                    info.get("participants")[player_index_in_match].get("goldEarned"),
                    info.get("participants")[player_index_in_match].get("individualPosition"),
                    info.get("participants")[player_index_in_match].get("totalDamageDealt"),
                    info.get("participants")[player_index_in_match].get("visionScore"),
                    info.get("participants")[player_index_in_match].get("win"),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ])

    else:
        headers = ["match_id","game_duration","game_creation","game_version","puuid","assists","deaths","kills","champ_level","champion_id","champion_name","gold_earned","individual_position","total_damage_dealt","vision_score","win","datetime"]

        with open(stage_match_file_path, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            for match in matches_data:
                player_index_in_match = -1
                player_puuids = match.get("metadata").get("participants")
                
                for player_puuid in player_puuids:
                    if player_puuid == puuid:
                        player_index_in_match = player_puuids.index(puuid)

                info = match.get("info")
                writer.writerow([
                    match.get("metadata").get("matchId"),
                    info.get("gameDuration"),
                    info.get("gameCreation"),
                    info.get("gameVersion"),
                    info.get("participants")[player_index_in_match].get("puuid"),
                    info.get("participants")[player_index_in_match].get("assists"),
                    info.get("participants")[player_index_in_match].get("deaths"),
                    info.get("participants")[player_index_in_match].get("kills"),
                    info.get("participants")[player_index_in_match].get("champLevel"),
                    info.get("participants")[player_index_in_match].get("championId"),
                    info.get("participants")[player_index_in_match].get("championName"),
                    info.get("participants")[player_index_in_match].get("goldEarned"),
                    info.get("participants")[player_index_in_match].get("individualPosition"),
                    info.get("participants")[player_index_in_match].get("totalDamageDealt"),
                    info.get("participants")[player_index_in_match].get("visionScore"),
                    info.get("participants")[player_index_in_match].get("win"),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ])

def load_stage_table(puuids, api_key, stage_match_file_path):
    for puuid in puuids:
        match_ids = get_match_ids_by_puuid(puuid, api_key)
        matches_data = get_match_data(match_ids, api_key, stage_match_file_path)
        save_matches_to_csv(puuid, matches_data, stage_match_file_path)

if __name__ == "__main__":
    # How to run:
    # python [...]/load_match.py --stage_player_file_name stage_player.csv --stage_match_file_name stage_match.csv --api_key <api_key>
    
    stage_player_file_name, stage_match_file_name, api_key = get_args()

    stage_player_file_path = get_file_path(stage_player_file_name)
    stage_match_file_path = get_file_path(stage_match_file_name)

    puuids = get_puuids(stage_player_file_path)

    load_stage_table(puuids, api_key, stage_match_file_path)