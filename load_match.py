import requests
import time
import csv

def get_match_ids_by_puuid(puuid, region, apiKey):
    url = f"https://{region}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?queue=420&type=ranked&start=0&count=5&api_key={apiKey}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"{response.status_code} - {response.text}")
    return response.json()

def get_matches_data(matchIds, region, apiKey):
    matches_data = []
    for matchId in matchIds:
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

    headers = ["matchId", "gameDuration", "gameCreation", "gameVersion", "assists"]

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
                info.get("participants")[playerIndexInMatch].get("win")
            ])

if __name__ == "__main__":
    puuid = "mz3C0mvreZqMH_Xe8s5Glc7dPuQbcQgUuy5q_NWvR7IC8yKYBqtYxiEtgn5tt_vio2ah9ORvJpu3DA"
    apiKey = "RGAPI-5efab3ae-29f1-4712-af7b-4e9f6408ddba"
    region = "americas"
    
    matchIds = get_match_ids_by_puuid(puuid, region, apiKey)
    matches_data = get_matches_data(matchIds, region, apiKey)
    save_matches_to_csv(puuid, matches_data, filename="match.csv")