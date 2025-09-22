from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message": "VelMerWain API running"}


import os
import json
from datetime import datetime   
from pydantic import BaseModel

from riot_api import RiotAPI
from analytics import most_played_champions, highest_winrate_champions

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
os.makedirs(BRONZE_DIR, exist_ok=True)

class PlayerRequest(BaseModel):
    gameName: str
    tagLine: str
    apiKey: str

@app.post("/player")
def get_player_info(req: PlayerRequest):
    api = RiotAPI(req.apiKey)
    player = api.get_by_riot_id(req.gameName, req.tagLine)
    player["createdOn"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Save player info
    player_path = os.path.join(BRONZE_DIR, f"player.json")

    players = []

    if os.path.exists(player_path):
        with open(player_path, 'r') as f:
            players = json.load(f)
        
        player_exists = False
        for p in players:
            if p['puuid'] == player['puuid']:
                player_exists = True
                break
        if not player_exists:
            players.append(player)

        with open(player_path, 'w') as f:
            json.dump(players, f, indent=4)
    else:
        with open(player_path, 'w') as f:
            players.append(player)
            json.dump(players, f, indent=4)
    
    return player

class MatchIdsRequest(BaseModel):
    puuid: str
    apiKey: str

@app.post("/matchids")
def get_match_ids(req: MatchIdsRequest):
    api = RiotAPI(req.apiKey)
    all_ids = []
    start = 0
    while True:
        ids = api.get_match_ids(puuid=req.puuid, start=start, count=100)
        if ids == []:
            break
        all_ids.extend(ids)
        start += 100
    # Save match ids
    ids_path = os.path.join(BRONZE_DIR, f"matchids_{req.puuid}.json")
    with open(ids_path, 'w') as f:
        json.dump(all_ids, f, indent=4)
    return {"matchIds": all_ids}

class MatchRequest(BaseModel):
    puuid: str
    matchId: str
    apiKey: str

@app.post("/match")
def get_match(req: MatchRequest):
    match_path = os.path.join(BRONZE_DIR, f"matches_{req.puuid}.json")
    
    matches = []
    
    if os.path.exists(match_path):
        with open(match_path) as f:
            matches = json.load(f)
            for match in matches:
                if match['matchId'] == req.matchId:
                    return

            api = RiotAPI(req.apiKey)  
            match = api.get_match(req.matchId)

            player_index_in_match = -1

            for i, participant in enumerate(match['metadata']['participants']):
                if participant == req.puuid:
                    player_index_in_match = i
                    break

            match = {
                "puuid": match["info"]["participants"][player_index_in_match]["puuid"],
                "matchId": match["metadata"]["matchId"],
                "gameCreation": match["info"]["gameCreation"],
                "gameDuration": match["info"]["gameDuration"],
                "gameVersion": match["info"]["gameVersion"],
                "assists": match["info"]["participants"][player_index_in_match]["assists"],
                "baronKills": match["info"]["participants"][player_index_in_match]["baronKills"],
                "champExperience": match["info"]["participants"][player_index_in_match]["champExperience"],
                "champLevel": match["info"]["participants"][player_index_in_match]["champLevel"],
                "championId": match["info"]["participants"][player_index_in_match]["championId"],
                "championName": match["info"]["participants"][player_index_in_match]["championName"],
                "damageDealtToBuildings": match["info"]["participants"][player_index_in_match]["damageDealtToBuildings"],
                "damageSelfMitigated": match["info"]["participants"][player_index_in_match]["damageSelfMitigated"],
                "deaths": match["info"]["participants"][player_index_in_match]["deaths"],
                "detectorWardsPlaced": match["info"]["participants"][player_index_in_match]["detectorWardsPlaced"],
                "doubleKills": match["info"]["participants"][player_index_in_match]["doubleKills"],
                "dragonKills": match["info"]["participants"][player_index_in_match]["dragonKills"],
                "firstBloodAssist": match["info"]["participants"][player_index_in_match]["firstBloodAssist"],
                "firstBloodKill": match["info"]["participants"][player_index_in_match]["firstBloodKill"],
                "firstTowerAssist": match["info"]["participants"][player_index_in_match]["firstTowerAssist"],
                "firstTowerKill": match["info"]["participants"][player_index_in_match]["firstTowerKill"],
                "goldEarned": match["info"]["participants"][player_index_in_match]["goldEarned"],
                "individualPosition": match["info"]["participants"][player_index_in_match]["individualPosition"],
                "inhibitorKills": match["info"]["participants"][player_index_in_match]["inhibitorKills"],
                "inhibitorTakedowns": match["info"]["participants"][player_index_in_match]["inhibitorTakedowns"],
                "inhibitorsLost": match["info"]["participants"][player_index_in_match]["inhibitorsLost"],
                "item0": match["info"]["participants"][player_index_in_match]["item0"],
                "item1": match["info"]["participants"][player_index_in_match]["item1"],
                "item2": match["info"]["participants"][player_index_in_match]["item2"],
                "item3": match["info"]["participants"][player_index_in_match]["item3"],
                "item4": match["info"]["participants"][player_index_in_match]["item4"],
                "item5": match["info"]["participants"][player_index_in_match]["item5"],
                "item6": match["info"]["participants"][player_index_in_match]["item6"],
                "killingSprees": match["info"]["participants"][player_index_in_match]["killingSprees"],
                "kills": match["info"]["participants"][player_index_in_match]["kills"],
                "largestKillingSpree": match["info"]["participants"][player_index_in_match]["largestKillingSpree"],
                "largestMultiKill": match["info"]["participants"][player_index_in_match]["largestMultiKill"],
                "longestTimeSpentLiving": match["info"]["participants"][player_index_in_match]["longestTimeSpentLiving"],
                "objectivesStolen": match["info"]["participants"][player_index_in_match]["objectivesStolen"],
                "objectivesStolenAssists": match["info"]["participants"][player_index_in_match]["objectivesStolenAssists"],
                "pentaKills": match["info"]["participants"][player_index_in_match]["pentaKills"],
                "statPerksDefense": match["info"]["participants"][player_index_in_match]["perks"]["statPerks"]["defense"],
                "statPerksFlex": match["info"]["participants"][player_index_in_match]["perks"]["statPerks"]["flex"],
                "statPerksOffense": match["info"]["participants"][player_index_in_match]["perks"]["statPerks"]["offense"],
                "primaryStyle1": match["info"]["participants"][player_index_in_match]["perks"]["styles"][0]["selections"][0]["perk"],
                "primaryStyle2": match["info"]["participants"][player_index_in_match]["perks"]["styles"][0]["selections"][1]["perk"],
                "primaryStyle3": match["info"]["participants"][player_index_in_match]["perks"]["styles"][0]["selections"][2]["perk"],
                "primaryStyle4": match["info"]["participants"][player_index_in_match]["perks"]["styles"][0]["selections"][3]["perk"],
                "subStyle1": match["info"]["participants"][player_index_in_match]["perks"]["styles"][1]["selections"][0]["perk"],
                "subStyle2": match["info"]["participants"][player_index_in_match]["perks"]["styles"][1]["selections"][1]["perk"],
                "quadraKills": match["info"]["participants"][player_index_in_match]["quadraKills"],
                "role": match["info"]["participants"][player_index_in_match]["role"],
                "timeCCingOthers": match["info"]["participants"][player_index_in_match]["timeCCingOthers"],
                "totalDamageDealtToChampions": match["info"]["participants"][player_index_in_match]["totalDamageDealtToChampions"],
                "totalDamageShieldedOnTeammates": match["info"]["participants"][player_index_in_match]["totalDamageShieldedOnTeammates"],
                "totalHeal": match["info"]["participants"][player_index_in_match]["totalHeal"],
                "totalHealsOnTeammates": match["info"]["participants"][player_index_in_match]["totalHealsOnTeammates"],
                "totalMinionsKilled": match["info"]["participants"][player_index_in_match]["totalMinionsKilled"],
                "tripleKills": match["info"]["participants"][player_index_in_match]["tripleKills"],
                "turretKills": match["info"]["participants"][player_index_in_match]["turretKills"],
                "turretTakedowns": match["info"]["participants"][player_index_in_match]["turretTakedowns"],
                "turretsLost": match["info"]["participants"][player_index_in_match]["turretsLost"],
                "visionScore": match["info"]["participants"][player_index_in_match]["visionScore"],
                "visionWardsBoughtInGame": match["info"]["participants"][player_index_in_match]["visionWardsBoughtInGame"],
                "wardsKilled": match["info"]["participants"][player_index_in_match]["wardsKilled"],
                "wardsPlaced": match["info"]["participants"][player_index_in_match]["wardsPlaced"],
                "win": match["info"]["participants"][player_index_in_match]["win"]
            }

            matches.append(match)
            with open(match_path, 'w') as f:
                json.dump(matches, f, indent=4)
    
    else:
        api = RiotAPI(req.apiKey)  
        match = api.get_match(req.matchId)

        player_index_in_match = -1

        for i, participant in enumerate(match['metadata']['participants']):
            if participant == req.puuid:
                player_index_in_match = i
                break

        match = {
            "puuid": match["info"]["participants"][player_index_in_match]["puuid"],
            "matchId": match["metadata"]["matchId"],
            "gameCreation": match["info"]["gameCreation"],
            "gameDuration": match["info"]["gameDuration"],
            "gameVersion": match["info"]["gameVersion"],
            "assists": match["info"]["participants"][player_index_in_match]["assists"],
            "baronKills": match["info"]["participants"][player_index_in_match]["baronKills"],
            "champExperience": match["info"]["participants"][player_index_in_match]["champExperience"],
            "champLevel": match["info"]["participants"][player_index_in_match]["champLevel"],
            "championId": match["info"]["participants"][player_index_in_match]["championId"],
            "championName": match["info"]["participants"][player_index_in_match]["championName"],
            "damageDealtToBuildings": match["info"]["participants"][player_index_in_match]["damageDealtToBuildings"],
            "damageSelfMitigated": match["info"]["participants"][player_index_in_match]["damageSelfMitigated"],
            "deaths": match["info"]["participants"][player_index_in_match]["deaths"],
            "detectorWardsPlaced": match["info"]["participants"][player_index_in_match]["detectorWardsPlaced"],
            "doubleKills": match["info"]["participants"][player_index_in_match]["doubleKills"],
            "dragonKills": match["info"]["participants"][player_index_in_match]["dragonKills"],
            "firstBloodAssist": match["info"]["participants"][player_index_in_match]["firstBloodAssist"],
            "firstBloodKill": match["info"]["participants"][player_index_in_match]["firstBloodKill"],
            "firstTowerAssist": match["info"]["participants"][player_index_in_match]["firstTowerAssist"],
            "firstTowerKill": match["info"]["participants"][player_index_in_match]["firstTowerKill"],
            "goldEarned": match["info"]["participants"][player_index_in_match]["goldEarned"],
            "individualPosition": match["info"]["participants"][player_index_in_match]["individualPosition"],
            "inhibitorKills": match["info"]["participants"][player_index_in_match]["inhibitorKills"],
            "inhibitorTakedowns": match["info"]["participants"][player_index_in_match]["inhibitorTakedowns"],
            "inhibitorsLost": match["info"]["participants"][player_index_in_match]["inhibitorsLost"],
            "item0": match["info"]["participants"][player_index_in_match]["item0"],
            "item1": match["info"]["participants"][player_index_in_match]["item1"],
            "item2": match["info"]["participants"][player_index_in_match]["item2"],
            "item3": match["info"]["participants"][player_index_in_match]["item3"],
            "item4": match["info"]["participants"][player_index_in_match]["item4"],
            "item5": match["info"]["participants"][player_index_in_match]["item5"],
            "item6": match["info"]["participants"][player_index_in_match]["item6"],
            "killingSprees": match["info"]["participants"][player_index_in_match]["killingSprees"],
            "kills": match["info"]["participants"][player_index_in_match]["kills"],
            "largestKillingSpree": match["info"]["participants"][player_index_in_match]["largestKillingSpree"],
            "largestMultiKill": match["info"]["participants"][player_index_in_match]["largestMultiKill"],
            "longestTimeSpentLiving": match["info"]["participants"][player_index_in_match]["longestTimeSpentLiving"],
            "objectivesStolen": match["info"]["participants"][player_index_in_match]["objectivesStolen"],
            "objectivesStolenAssists": match["info"]["participants"][player_index_in_match]["objectivesStolenAssists"],
            "pentaKills": match["info"]["participants"][player_index_in_match]["pentaKills"],
            "statPerksDefense": match["info"]["participants"][player_index_in_match]["perks"]["statPerks"]["defense"],
            "statPerksFlex": match["info"]["participants"][player_index_in_match]["perks"]["statPerks"]["flex"],
            "statPerksOffense": match["info"]["participants"][player_index_in_match]["perks"]["statPerks"]["offense"],
            "primaryStyle1": match["info"]["participants"][player_index_in_match]["perks"]["styles"][0]["selections"][0]["perk"],
            "primaryStyle2": match["info"]["participants"][player_index_in_match]["perks"]["styles"][0]["selections"][1]["perk"],
            "primaryStyle3": match["info"]["participants"][player_index_in_match]["perks"]["styles"][0]["selections"][2]["perk"],
            "primaryStyle4": match["info"]["participants"][player_index_in_match]["perks"]["styles"][0]["selections"][3]["perk"],
            "subStyle1": match["info"]["participants"][player_index_in_match]["perks"]["styles"][1]["selections"][0]["perk"],
            "subStyle2": match["info"]["participants"][player_index_in_match]["perks"]["styles"][1]["selections"][1]["perk"],
            "quadraKills": match["info"]["participants"][player_index_in_match]["quadraKills"],
            "role": match["info"]["participants"][player_index_in_match]["role"],
            "timeCCingOthers": match["info"]["participants"][player_index_in_match]["timeCCingOthers"],
            "totalDamageDealtToChampions": match["info"]["participants"][player_index_in_match]["totalDamageDealtToChampions"],
            "totalDamageShieldedOnTeammates": match["info"]["participants"][player_index_in_match]["totalDamageShieldedOnTeammates"],
            "totalHeal": match["info"]["participants"][player_index_in_match]["totalHeal"],
            "totalHealsOnTeammates": match["info"]["participants"][player_index_in_match]["totalHealsOnTeammates"],
            "totalMinionsKilled": match["info"]["participants"][player_index_in_match]["totalMinionsKilled"],
            "tripleKills": match["info"]["participants"][player_index_in_match]["tripleKills"],
            "turretKills": match["info"]["participants"][player_index_in_match]["turretKills"],
            "turretTakedowns": match["info"]["participants"][player_index_in_match]["turretTakedowns"],
            "turretsLost": match["info"]["participants"][player_index_in_match]["turretsLost"],
            "visionScore": match["info"]["participants"][player_index_in_match]["visionScore"],
            "visionWardsBoughtInGame": match["info"]["participants"][player_index_in_match]["visionWardsBoughtInGame"],
            "wardsKilled": match["info"]["participants"][player_index_in_match]["wardsKilled"],
            "wardsPlaced": match["info"]["participants"][player_index_in_match]["wardsPlaced"],
            "win": match["info"]["participants"][player_index_in_match]["win"]
        }
        
        matches.append(match)
        with open(match_path, 'w') as f:
            json.dump(matches, f, indent=4)
        return

class AnalyticsRequest(BaseModel):
    puuid: str

@app.post("/analytics")
def get_analytics(req: AnalyticsRequest):
    most_played = most_played_champions(req.puuid, BRONZE_DIR)
    highest_winrate = highest_winrate_champions(req.puuid, BRONZE_DIR)
    return {
        "mostPlayed": most_played,
        "highestWinrate": highest_winrate
    }