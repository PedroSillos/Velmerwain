import requests
import csv
from datetime import datetime
import os

def get_puuid_by_riot_id(tagLine: str, gameName: str, apiKey: str) -> str:
    url = f"https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{gameName}/{tagLine}"
    headers = {"X-Riot-Token": apiKey}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return data.get("puuid")

def get_summoner_data_by_puuid(puuid: str, apiKey: str) -> tuple:
    url = f"https://br1.api.riotgames.com/lol/summoner/v4/summoners/by-puuid/{puuid}"
    headers = {"X-Riot-Token": apiKey}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return data.get("profileIconId"), data.get("revisionDate"), data.get("summonerLevel")

def save_player_to_csv(puuid: str, gameName: str, tagLine: str, profileIconId: int, revisionDate: int, summonerLevel: int):
    file_exists = os.path.isfile("player.csv")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("player.csv", mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(["puuid", "gameName", "tagLine", "profileIconId", "revisionDate", "summonerLevel", "datetime"])
            writer.writerow([puuid, gameName, tagLine, profileIconId, revisionDate, summonerLevel, now])
        else:
            writer.writerow([puuid, gameName, tagLine, profileIconId, revisionDate, summonerLevel, now])

if __name__ == "__main__":
    apiKey = "RGAPI-09367fe4-7a48-42f1-bc97-285e7ea50894"
    gameName = "OTalDoPedrinho"
    tagLine = "BR1"

    puuid = get_puuid_by_riot_id(tagLine, gameName, apiKey)
    profileIconId, revisionDate, summonerLevel = get_summoner_data_by_puuid(puuid, apiKey)
    save_player_to_csv(puuid, gameName, tagLine, profileIconId, revisionDate, summonerLevel)