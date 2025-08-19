import requests
import csv
import os
from datetime import datetime

def get_match_ids_by_puuid(puuid, region, apiKey):
    url = f"https://{region}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?queue=420&type=ranked&start=0&count=100&api_key={apiKey}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"{response.status_code} - {response.text}")
    return response.json()

def get_matches_data(matchIds, region, apiKey, filename):
    matches_data = []
    for matchId in matchIds:
        file_exists = os.path.isfile(filename)
        matchIdExists = False

        if file_exists:
            with open(filename, mode="r", newline="", encoding="utf-8") as file:
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

def save_matches_to_csv(puuid, matches_data, filename):
    if not matches_data:
        return

    if os.path.isfile(filename):
        with open(filename, mode="a", newline="", encoding="utf-8") as file:
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
        headers = ["matchId","gameDuration","gameCreation","gameVersion","assists","deaths","kills","champLevel","championId","championName","goldEarned","individualPosition","totalDamageDealt","visionScore","win","datetime"]

        with open(filename, mode="w", newline="", encoding="utf-8") as file:
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

def loadStageTable(puuid, region, apiKey):
    matchIds = get_match_ids_by_puuid(puuid, region, apiKey)
    matches_data = get_matches_data(matchIds, region, apiKey, filename=stage_file_name)
    save_matches_to_csv(puuid, matches_data, filename=stage_file_name)

if __name__ == "__main__":
    puuid = "mz3C0mvreZqMH_Xe8s5Glc7dPuQbcQgUuy5q_NWvR7IC8yKYBqtYxiEtgn5tt_vio2ah9ORvJpu3DA"
    apiKey = "RGAPI-5efab3ae-29f1-4712-af7b-4e9f6408ddba"
    region = "americas"
    stage_file_name = "stage_match.csv"

    loadStageTable(puuid, region, apiKey)