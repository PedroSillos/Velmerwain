import argparse
import os
import requests
import csv
from datetime import datetime

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stageFileName", type=str)
    parser.add_argument("--puuid", type=str)
    parser.add_argument("--region", type=str)
    parser.add_argument("--apiKey", type=str)

    args = parser.parse_args()
    return args.stageFileName, args.puuid, args.region, args.apiKey

def get_project_path(stageFileName:str):
    srcPath = os.path.abspath(__file__)
    srcDirPath = os.path.dirname(srcPath)
    projectPath = srcDirPath.replace("\\src","")
    stageFilePath = f"{projectPath}\\data\\{stageFileName}"

    return stageFilePath

def get_match_ids_by_puuid(puuid, region, apiKey):
    url = f"https://{region}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?queue=420&type=ranked&start=0&count=100&api_key={apiKey}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"{response.status_code} - {response.text}")
    return response.json()

def get_matches_data(matchIds, region, apiKey, stageFileName):
    matches_data = []
    for matchId in matchIds:
        file_exists = os.path.isfile(stageFileName)
        matchIdExists = False

        if file_exists:
            with open(stageFileName, mode="r", newline="", encoding="utf-8") as file:
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
                continue
            matches_data.append(response.json())
    return matches_data

def save_matches_to_csv(puuid, matches_data, stageFileName):
    if not matches_data:
        return

    if os.path.isfile(stageFileName):
        with open(stageFileName, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            for match in matches_data:
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

        with open(stageFileName, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            for match in matches_data:
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

def loadStageTable(puuid, region, apiKey, stageFilePath):
    matchIds = get_match_ids_by_puuid(puuid, region, apiKey)
    matches_data = get_matches_data(matchIds, region, apiKey, stageFilePath)
    save_matches_to_csv(puuid, matches_data, stageFilePath)

if __name__ == "__main__":
    # How to run:
    # python src\load_match.py --stageFileName stage_match.csv --puuid mz3C0mvreZqMH_Xe8s5Glc7dPuQbcQgUuy5q_NWvR7IC8yKYBqtYxiEtgn5tt_vio2ah9ORvJpu3DA --region americas --apiKey <apiKey>
    
    stageFileName, puuid, region, apiKey = get_args()

    stageFilePath = get_project_path(stageFileName)

    loadStageTable(puuid, region, apiKey, stageFilePath)