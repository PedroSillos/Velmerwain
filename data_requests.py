import requests
from datetime import datetime, timedelta

def get_account_information(game_name:str,tag_line:str,api_key:str):
    request_body = 'https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/'

    request_url = request_body+game_name+'/'+tag_line+'?api_key='+api_key

    response = requests.get(request_url)

    if str(response) == '<Response [200]>':
        account_info = response.json()
        return account_info

def get_start_date_timestamp():
    start_date = (datetime.now()+timedelta(days=-14))
    start_timestamp = str(start_date.timestamp()).replace('.','')[0:13]
    return [start_date,start_timestamp]

game_name = 'OTalDoPedrinho'
tag_line = 'BR1'
api_key = 'RGAPI-e77f870d-ea99-4f05-9770-b4f0cdaf3a2f'

account_info = get_account_information(game_name,tag_line,api_key)

print(account_info['puuid'])
print(account_info['gameName'])
print(account_info['tagLine'])

print(get_start_date_timestamp())