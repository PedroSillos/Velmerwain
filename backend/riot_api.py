import os
import requests

RIOT_API_BASE = "https://americas.api.riotgames.com"

class RiotAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.headers = {"X-Riot-Token": self.api_key}

    def get_by_riot_id(self, game_name, tag_line):
        url = f"{RIOT_API_BASE}/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}"
        r = requests.get(url, headers=self.headers)
        r.raise_for_status()
        return r.json()

    def get_match_ids(self, puuid, start, count):
        url = f"{RIOT_API_BASE}/lol/match/v5/matches/by-puuid/{puuid}/ids"
        params = {"queue": 420, "type": "ranked", "start": start, "count": count}
        r = requests.get(url, headers=self.headers, params=params)
        r.raise_for_status()
        return r.json()

    def get_match(self, match_id):
        url = f"{RIOT_API_BASE}/lol/match/v5/matches/{match_id}"
        r = requests.get(url, headers=self.headers)
        r.raise_for_status()
        return r.json()
