import getpass
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import requests
import time

def init_spark():
    builder = SparkSession.builder.appName("VelMerWain") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return configure_spark_with_delta_pip(builder).getOrCreate()

def load_match_bronze(spark, api_key):
    # Get all match_ids from match_id table
    source_match_ids = []
    try:
        source_match_id_rows = spark.read.format("delta").load("data/bronze/match_id").collect()
        for row in source_match_id_rows:
            source_match_ids.append({"puuid": row["puuid"], "matchId": row["matchId"]})
    except:
        print("\nNo match_id table found")
        return

    # Stop if there are no match_ids
    if not source_match_ids:
        print("\nNo match_ids found in match_id table")
        return

    # Get all match_ids from match table
    destination_match_ids = []
    try:
        destination_match_id_rows = spark.read.format("delta").load("data/bronze/match").collect()
        for row in destination_match_id_rows:
            destination_match_ids.append({"puuid": row["puuid"], "matchId": row["matchId"]})
    except:
        print("\nNo match table found")
    
    new_match_ids = []
    # Get new match_ids
    for row in source_match_ids:
        if row not in destination_match_ids:
            new_match_ids.append(row)

    print(f"\nFound {len(new_match_ids)} new match_ids")
    
    # Counter to know how many matches were loaded
    count = 1
    # Empty list to hold new matches
    new_matches = []
    # Only fetch matches for new match ids
    for new_match_id in new_match_ids:
        # Fetch match for new_match_id from Riot API
        url = f"https://americas.api.riotgames.com/lol/match/v5/matches/{new_match_id}"
        response = requests.get(url, headers={"X-Riot-Token": api_key})
        # Check if rate limit was exceeded
        while response.status_code == 429:
            # If rate limit exceeded, wait and retry
            print("Rate limit exceeded. Sleeping for 30 seconds...")
            time.sleep(30)
            url = f"https://americas.api.riotgames.com/lol/match/v5/matches/{new_match_id}"
            response = requests.get(url, headers={"X-Riot-Token": api_key})
        # If successful, load the match
        if response.status_code == 200:
            match_api_data = response.json()
            # Get participants and iterate (there will be a row in bronze/match for every participant)
            participants = match_api_data["info"]["participants"]
            for participant in participants:
                if participant["puuid"] == new_match_id["puuid"]:
                    team_index = 0
                    if participant["teamId"] == 200:
                        team_index = 1
                    try:
                        team_feats = match_api_data["info"]["teams"][team_index]["feats"]
                    except:
                        team_feats = {
                            "EPIC_MONSTER_KILL": {
                                "featState": -1
                            },
                            "FIRST_BLOOD": {
                                "featState": -1
                            },
                            "FIRST_TURRET": {
                                "featState": -1
                            }
                        }
                    try:
                        team_objectives = match_api_data["info"]["teams"][team_index]["objectives"]
                        team_atakhan_kills = team_objectives["atakhan"]["kills"]
                    except:
                        team_objectives = {
                            "atakhan": {
                                "kills": 0
                            },
                            "baron": {
                                "kills": 0
                            },
                            "champion": {
                                "kills": 0
                            },
                            "dragon": {
                                "kills": 0
                            },
                            "horde": {
                                "kills": 0
                            },
                            "inhibitor": {
                                "kills": 0
                            },
                            "riftHerald": {
                                "kills": 0
                            },
                            "tower": {
                                "kills": 0
                            }
                        }
                    participant_match = {
                        "matchId": match_api_data["metadata"]["matchId"],
                        "puuid": participant["puuid"],
                        "riotIdGameName": participant["riotIdGameName"],
                        "riotIdTagline": participant["riotIdTagline"],
                        "gameDuration": match_api_data["info"]["gameDuration"],
                        "gameVersion": '.'.join(match_api_data["info"]["gameVersion"].split('.')[:2]),
                        "assists": participant["assists"],
                        "baronKills": participant["baronKills"],
                        "champLevel": participant["champLevel"],
                        "championName": participant["championName"],
                        "damageDealtToBuildings": participant["damageDealtToBuildings"],
                        "damageDealtToObjectives": participant["damageDealtToObjectives"],
                        "damageSelfMitigated": participant["damageSelfMitigated"],
                        "deaths": participant["deaths"],
                        "detectorWardsPlaced": participant["detectorWardsPlaced"],
                        "dragonKills": participant["dragonKills"],
                        "firstBloodAssist": participant["firstBloodAssist"],
                        "firstBloodKill": participant["firstBloodKill"],
                        "firstTowerAssist": participant["firstTowerAssist"],
                        "firstTowerKill": participant["firstTowerKill"],
                        "gameEndedInEarlySurrender": participant["gameEndedInEarlySurrender"],
                        "gameEndedInSurrender": participant["gameEndedInSurrender"],
                        "goldEarned": participant["goldEarned"],
                        "goldSpent": participant["goldSpent"],
                        "individualPosition": participant["individualPosition"],
                        "inhibitorKills": participant["inhibitorKills"],
                        "inhibitorTakedowns": participant["inhibitorTakedowns"],
                        "inhibitorsLost": participant["inhibitorsLost"],
                        "item0": participant["item0"],
                        "item1": participant["item1"],
                        "item2": participant["item2"],
                        "item3": participant["item3"],
                        "item4": participant["item4"],
                        "item5": participant["item5"],
                        "item6": participant["item6"],
                        "itemsPurchased": participant["itemsPurchased"],
                        "killingSprees": participant["killingSprees"],
                        "kills": participant["kills"],
                        "lane": participant["lane"],
                        "largestKillingSpree": participant["largestKillingSpree"],
                        "largestMultiKill": participant["largestMultiKill"],
                        "longestTimeSpentLiving": participant["longestTimeSpentLiving"],
                        "magicDamageDealtToChampions": participant["magicDamageDealtToChampions"],
                        "magicDamageTaken": participant["magicDamageTaken"],
                        "neutralMinionsKilled": participant["neutralMinionsKilled"],
                        "nexusKills": participant["nexusKills"],
                        "nexusLost": participant["nexusLost"],
                        "nexusTakedowns": participant["nexusTakedowns"],
                        "objectivesStolen": participant["objectivesStolen"],
                        "objectivesStolenAssists": participant["objectivesStolenAssists"],
                        "runeStatOffense": participant["perks"]["statPerks"]["offense"],
                        "runeStatFlex": participant["perks"]["statPerks"]["flex"],
                        "runeStatDefense": participant["perks"]["statPerks"]["defense"],
                        "runeStylePrimary": participant["perks"]["styles"][0]["style"],
                        "runeStylePrimaryFirst": participant["perks"]["styles"][0]["selections"][0]["perk"],
                        "runeStylePrimarySecond": participant["perks"]["styles"][0]["selections"][1]["perk"],
                        "runeStylePrimaryThird": participant["perks"]["styles"][0]["selections"][2]["perk"],
                        "runeStylePrimaryFourth": participant["perks"]["styles"][0]["selections"][3]["perk"],
                        "runeStyleSecondary": participant["perks"]["styles"][1]["style"],
                        "runeStyleSecondaryFirst": participant["perks"]["styles"][1]["selections"][0]["perk"],
                        "runeStyleSecondarySecond": participant["perks"]["styles"][1]["selections"][1]["perk"],
                        "physicalDamageDealtToChampions": participant["physicalDamageDealtToChampions"],
                        "physicalDamageTaken": participant["physicalDamageTaken"],
                        "summoner1Id": participant["summoner1Id"],
                        "summoner2Id": participant["summoner2Id"],
                        "teamId": participant["teamId"],
                        "teamPosition": participant["teamPosition"],
                        "totalDamageDealtToChampions": participant["totalDamageDealtToChampions"],
                        "totalDamageShieldedOnTeammates": participant["totalDamageShieldedOnTeammates"],
                        "totalDamageTaken": participant["totalDamageTaken"],
                        "totalHeal": participant["totalHeal"],
                        "totalHealsOnTeammates": participant["totalHealsOnTeammates"],
                        "totalMinionsKilled": participant["totalMinionsKilled"],
                        "trueDamageDealtToChampions": participant["trueDamageDealtToChampions"],
                        "turretKills": participant["turretKills"],
                        "turretTakedowns": participant["turretTakedowns"],
                        "turretsLost": participant["turretsLost"],
                        "visionScore": participant["visionScore"],
                        "visionWardsBoughtInGame": participant["visionWardsBoughtInGame"],
                        "wardsKilled": participant["wardsKilled"],
                        "wardsPlaced": participant["wardsPlaced"],
                        "win": participant["win"],
                        "featEpicMonsterKill": team_feats["EPIC_MONSTER_KILL"]["featState"],
                        "featFirstBlood": team_feats["FIRST_BLOOD"]["featState"],
                        "featFirstTurret": team_feats["FIRST_TURRET"]["featState"],
                        "teamAtakhanKills": team_objectives["atakhan"]["kills"],
                        "teamBaronKills": team_objectives["baron"]["kills"],
                        "teamChampionKills": team_objectives["champion"]["kills"],
                        "teamDragonKills": team_objectives["dragon"]["kills"],
                        "teamHordeKills": team_objectives["horde"]["kills"],
                        "teamInhibitorKills": team_objectives["inhibitor"]["kills"],
                        "teamRiftHeraldKills": team_objectives["riftHerald"]["kills"],
                        "teamTowerKills": team_objectives["tower"]["kills"]
                    }
                    new_matches.append(participant_match)
        else:
            print(f"\nAPI Error: {response.status_code}")
            print(f"for match_id {new_match_id['matchId']}")
            return
        # Save new matches to match table
        # Save every 30 matches to reduce number of writes (but also save at the end)
        if new_matches and (count % 30 == 0 or count == len(new_match_ids)):
            df = spark.createDataFrame(new_matches)
            df.write.format("delta").mode("append").save("data/bronze/match")
            # Print progress
            print(f"\nSaved {len(new_matches)} matches for match_ids until {count}/{len(new_match_ids)}")
            # Reset new_match_ids list (avoid re-insert)
            new_matches = []
        # Increment player counter
        count += 1

def load_match():
    print("\n ***** Start load matches ***** \n")
    api_key = getpass.getpass("API Key: ")
    spark = init_spark()
    load_match_bronze(spark, api_key)
    spark.stop()
    print("\n ***** End load matches ***** \n")