import argparse
import os
import requests
import csv
from datetime import datetime

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stageFileName", type=str)
    parser.add_argument("--gameName", type=str)
    parser.add_argument("--tagLine", type=str)
    parser.add_argument("--region", type=str)
    parser.add_argument("--apiKey", type=str)

    args = parser.parse_args()
    return args.stageFileName, args.gameName, args.tagLine, args.region, args.apiKey

def get_project_paths(stageFileName:str):
    srcPath = os.path.abspath(__file__)
    srcDirPath = os.path.dirname(srcPath)
    projectPath = srcDirPath.replace("\\src","")
    stageFilePath = f"{projectPath}\\data\\{stageFileName}"

    return stageFilePath

def get_puuid_by_riot_id(tagLine: str, gameName: str, region: str, apiKey: str):
    url = f"https://{region}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{gameName}/{tagLine}"
    headers = {"X-Riot-Token": apiKey}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return data.get("puuid")

def get_summoner_data_by_puuid(tagLine: str, puuid: str, apiKey: str):
    url = f"https://{tagLine}.api.riotgames.com/lol/summoner/v4/summoners/by-puuid/{puuid}"
    headers = {"X-Riot-Token": apiKey}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return data.get("profileIconId"), data.get("revisionDate"), data.get("summonerLevel")

def create_player_csv(stageFilePath:str, puuid: str, gameName: str, tagLine: str, profileIconId: int, revisionDate: int, summonerLevel: int, now:str):
    with open(stageFilePath, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["puuid", "gameName", "tagLine", "profileIconId", "revisionDate", "summonerLevel", "datetime"])
        writer.writerow([puuid, gameName, tagLine, profileIconId, revisionDate, summonerLevel, now])
    return

def get_updated_rows(stageFilePath:str, puuid: str, gameName: str, tagLine: str, profileIconId: int, revisionDate: int, summonerLevel: int, now:str):
    rows = []
    with open(stageFilePath, mode="r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        updated = False
        for row in reader:
            if row["puuid"] == puuid:
                row["gameName"] = gameName
                row["tagLine"] = tagLine
                row["profileIconId"] = str(profileIconId)
                row["revisionDate"] = str(revisionDate)
                row["summonerLevel"] = str(summonerLevel)
                row["datetime"] = now
                updated = True
            rows.append(row)
        if not updated:
            newRow = {'puuid': puuid,
                'gameName': gameName,
                'tagLine': tagLine,
                'profileIconId': str(profileIconId),
                'revisionDate': str(revisionDate),
                'summonerLevel': str(summonerLevel),
                'datetime': now
            }
            rows.append(newRow)
    return rows

def update_player_csv(stageFilePath:str, puuid: str, gameName: str, tagLine: str, profileIconId: int, revisionDate: int, summonerLevel: int, now:str):
    rows = get_updated_rows(stageFilePath, puuid, gameName, tagLine, profileIconId, revisionDate, summonerLevel, now)
    
    with open(stageFilePath, mode="w", newline="", encoding="utf-8") as file:
        columnNames = rows[0].keys()
        writer = csv.DictWriter(file, fieldnames=columnNames)
        writer.writeheader()
        writer.writerows(rows)

def save_player_to_csv(puuid: str, gameName: str, tagLine: str, profileIconId: int, revisionDate: int, summonerLevel: int, stageFilePath:str):
    stageFileDir = stageFilePath.replace("\\stage_player.csv","")
    if not os.path.exists(stageFileDir):
        os.makedirs(stageFileDir)
    
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    fileExists = os.path.isfile(stageFilePath)
    if not fileExists:
        create_player_csv(stageFilePath, puuid, gameName, tagLine, profileIconId, revisionDate, summonerLevel, now)
    else:
        update_player_csv(stageFilePath, puuid, gameName, tagLine, profileIconId, revisionDate, summonerLevel, now)

if __name__ == "__main__":
    # How to run:
    # python src\load_player.py --stageFileName stage_player.csv --gameName OTalDoPedrinho --tagLine BR1 --region americas --apiKey <apiKey>
    
    stageFileName, gameName, tagLine, region, apiKey = get_args()

    stageFilePath = get_project_paths(stageFileName)

    puuid = get_puuid_by_riot_id(tagLine, gameName, region, apiKey)

    profileIconId, revisionDate, summonerLevel = get_summoner_data_by_puuid(tagLine, puuid, apiKey)

    save_player_to_csv(puuid, gameName, tagLine, profileIconId, revisionDate, summonerLevel, stageFilePath)