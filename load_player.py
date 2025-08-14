import requests
import pandas as pd

def get_puuid(game_name:str, tag_line:str):
    account_url = f"https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}?api_key={api_key}"
    puuid = requests.get(account_url).json()["puuid"]
    return puuid

def get_summoner_info(puuid:str, region:str):
    summoner_url = f"https://{region}.api.riotgames.com/lol/summoner/v4/summoners/by-puuid/{puuid}?api_key={api_key}"
    data = requests.get(summoner_url).json()
    return data["profileIconId"], data["revisionDate"], data["summonerLevel"]

api_key = "RGAPI-09367fe4-7a48-42f1-bc97-285e7ea50894"
game_name = "OTalDoPedrinho"
tag_line = "BR1"

puuid = get_puuid(game_name, tag_line)
region = tag_line.lower()
profile_icon_id, revision_date, summoner_level = get_summoner_info(puuid, region)

df = pd.DataFrame([{
    "puuid": puuid,
    "game_name": game_name,
    "tag_line": tag_line,
    "profile_icon_id": profile_icon_id,
    "revision_date": revision_date,
    "summoner_level": summoner_level
}])

print(df)

df.to_csv(path_or_buf="player.csv",header=True,index=False,mode='w')