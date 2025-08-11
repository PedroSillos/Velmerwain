import requests

request_body = 'https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/'
game_name = 'OTalDoPedrinho'
tag_line = 'BR1'
api_key = 'RGAPI-e77f870d-ea99-4f05-9770-b4f0cdaf3a2f'

request_url = request_body+game_name+'/'+tag_line+'?api_key='+api_key

response = requests.get(request_url)

if str(response) == '<Response [200]>':
    account_info = response.json()
    print(account_info['puuid'])
    print(account_info['gameName'])
    print(account_info['tagLine'])