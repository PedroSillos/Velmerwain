import requests
from datetime import datetime, timedelta
import json

def get_puuid_by_game_name(game_name:str,tag_line:str,api_key:str):
    request_body = 'https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/'

    request_url = request_body+game_name+'/'+tag_line+'?api_key='+api_key

    response = requests.get(request_url)

    if str(response) == '<Response [200]>':
        account_info = response.json()
        return account_info['puuid']

##### Not used #####
#def get_start_date_timestamp():
#    start_date = (datetime.now()+timedelta(days=-14))
#    start_timestamp = str(start_date.timestamp()).replace('.','')[0:13]
#    return [start_date,start_timestamp]

def get_match_ids_by_puuid(puuid:str,api_key:str):
    
    request_body = 'https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/'

    request_url = request_body+puuid+'/ids?queue=420&type=ranked&start=0&count=100&api_key='+api_key #queue=420 means SoloQ

    response = requests.get(request_url)

    if str(response) == '<Response [200]>':
        match_ids = response.json()
        return match_ids

game_name = 'OTalDoPedrinho'
tag_line = 'BR1'
api_key = 'RGAPI-490fcd82-d42f-4a17-b122-9907fe8a0bc7'

puuid = get_puuid_by_game_name(game_name,tag_line,api_key)

#print(puuid)

match_ids = get_match_ids_by_puuid(puuid,api_key)