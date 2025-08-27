import argparse
import os
import requests
import csv
from datetime import datetime

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stagePlayerFileName", type=str)
    parser.add_argument("--stageMatchFileName", type=str)
    parser.add_argument("--region", type=str)
    parser.add_argument("--apiKey", type=str)

    args = parser.parse_args()
    return args.stagePlayerFileName, args.stageMatchFileName, args.region, args.apiKey

def get_file_path(stageFileName:str):
    srcPath = os.path.abspath(__file__)
    srcDirPath = os.path.dirname(srcPath)
    projectPath = srcDirPath.replace("\\src","")
    stageFilePath = f"{projectPath}\\data\\{stageFileName}"

    return stageFilePath

def get_puuids(stagePlayerFilePath: str):
    puuids = []

    with open(stagePlayerFilePath, mode="r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            puuids.append(row["puuid"])

    return puuids

def get_match_ids_by_puuid(puuid, region, apiKey):
    url = f"https://{region}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?queue=420&type=ranked&start=0&count=100&api_key={apiKey}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"{response.status_code} - {response.text}")
    return response.json()

def get_matches_data(matchIds, region, apiKey, stageMatchFilePath):
    matchesData = []
    fileExists = os.path.isfile(stageMatchFilePath)

    if fileExists:
        for matchId in matchIds:
            matchIdExists = False
            
            with open(stageMatchFilePath, mode="r", newline="", encoding="utf-8") as file:
                reader = list(csv.reader(file))
                rows = reader[1:]
                for row in rows:
                    if row[0] == matchId:
                        matchIdExists = True
            
            if not matchIdExists:
                url = f"https://{region}.api.riotgames.com/lol/match/v5/matches/{matchId}?api_key={apiKey}"
                response = requests.get(url)
                if response.status_code != 200:
                    print(f"{matchId}: {response.status_code} - {response.text}")
                else:
                    matchesData.append(response.json())
    else:
        for matchId in matchIds:
            url = f"https://{region}.api.riotgames.com/lol/match/v5/matches/{matchId}?api_key={apiKey}"
            response = requests.get(url)
            if response.status_code != 200:
                print(f"{matchId}: {response.status_code} - {response.text}")
            else:
                matchesData.append(response.json())
    return matchesData

def save_matches_to_csv(puuid, matchesData, stageMatchFilePath):
    if not matchesData:
        return

    if os.path.isfile(stageMatchFilePath):
        with open(stageMatchFilePath, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            for match in matchesData:
                playerIndexInMatch = -1
                playerPuuids = match.get("metadata").get("participants")

                for playerPuuid in playerPuuids:
                    if playerPuuid == puuid:
                        playerIndexInMatch = playerPuuids.index(puuid)
                
                info = match.get("info")
                writer.writerow([
                    match.get("metadata").get("matchId"),
                    info.get("gameDuration"),
                    info.get("gameCreation"),
                    info.get("gameVersion"),
                    info.get("participants")[playerIndexInMatch].get("puuid"),
                    info.get("participants")[playerIndexInMatch].get("assists"),
                    info.get("participants")[playerIndexInMatch].get("deaths"),
                    info.get("participants")[playerIndexInMatch].get("kills"),
                    info.get("participants")[playerIndexInMatch].get("champLevel"),
                    info.get("participants")[playerIndexInMatch].get("championId"),
                    info.get("participants")[playerIndexInMatch].get("championName"),
                    info.get("participants")[playerIndexInMatch].get("goldEarned"),
                    info.get("participants")[playerIndexInMatch].get("individualPosition"),
                    info.get("participants")[playerIndexInMatch].get("totalDamageDealt"),
                    info.get("participants")[playerIndexInMatch].get("visionScore"),
                    info.get("participants")[playerIndexInMatch].get("win"),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ])

    else:
        headers = ["matchId","gameDuration","gameCreation","gameVersion","puuid","assists","deaths","kills","champLevel","championId","championName","goldEarned","individualPosition","totalDamageDealt","visionScore","win","datetime"]

        with open(stageMatchFilePath, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            for match in matchesData:
                playerIndexInMatch = -1
                playerPuuids = match.get("metadata").get("participants")
                
                for playerPuuid in playerPuuids:
                    if playerPuuid == puuid:
                        playerIndexInMatch = playerPuuids.index(puuid)

                info = match.get("info")
                writer.writerow([
                    match.get("metadata").get("matchId"),
                    info.get("gameDuration"),
                    info.get("gameCreation"),
                    info.get("gameVersion"),
                    info.get("participants")[playerIndexInMatch].get("puuid"),
                    info.get("participants")[playerIndexInMatch].get("assists"),
                    info.get("participants")[playerIndexInMatch].get("deaths"),
                    info.get("participants")[playerIndexInMatch].get("kills"),
                    info.get("participants")[playerIndexInMatch].get("champLevel"),
                    info.get("participants")[playerIndexInMatch].get("championId"),
                    info.get("participants")[playerIndexInMatch].get("championName"),
                    info.get("participants")[playerIndexInMatch].get("goldEarned"),
                    info.get("participants")[playerIndexInMatch].get("individualPosition"),
                    info.get("participants")[playerIndexInMatch].get("totalDamageDealt"),
                    info.get("participants")[playerIndexInMatch].get("visionScore"),
                    info.get("participants")[playerIndexInMatch].get("win"),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ])

def loadStageTable(puuids, region, apiKey, stageMatchFilePath):
    for puuid in puuids:
        matchIds = get_match_ids_by_puuid(puuid, region, apiKey)
        matchesData = get_matches_data(matchIds, region, apiKey, stageMatchFilePath)
        save_matches_to_csv(puuid, matchesData, stageMatchFilePath)

if __name__ == "__main__":
    # How to run:
    # python src\load_match.py --stagePlayerFileName stage_player.csv --stageMatchFileName stage_match.csv --region americas --apiKey <apiKey>
    
    stagePlayerFileName, stageMatchFileName, region, apiKey = get_args()

    stagePlayerFilePath = get_file_path(stagePlayerFileName)
    stageMatchFilePath = get_file_path(stageMatchFileName)

    puuids = get_puuids(stagePlayerFilePath)

    loadStageTable(puuids, region, apiKey, stageMatchFilePath)