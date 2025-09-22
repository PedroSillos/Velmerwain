# Prompt 1
I want to build a data-driven tool to help League of Legends players increase their win rate. 

### I want you to create a solution, using:
 - python
 - pyspark
 - delta lake
 - bronze (raw), silver and gold layers

Add any other tools if needed, but keep it free to run locally.

### For now, this is the flow of the app:
1) The user types in the terminal:
 - gameName
 - tagLine
 - apiKey (must not be visible in the terminal)

2) Save player (bronze) layer - Check if gameName and tagLine are already stored:
 - If not stored, pass gameName and tagLine to https://developer.riotgames.com/apis#account-v1/GET_getByRiotId and save puuid,gameName,tagLine and createdOn (datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
 - If stored, updated createdOn

3) Save matchId (bronze) layer:
 - Now that we have the puuid for this player, get the matchIds from https://developer.riotgames.com/apis#match-v5/GET_getMatchIdsByPUUID passing parameters queue=420, type=ranked, count=100. Run this changing the "start" parameter (0,100,200...) until there are no more matchIds to return.

4) Save match (bronze) layer - Get the new matchIds for this player and compare with the matchIds for the matches already stored.
 - For every matchId that does not have a match stored, pass the id to https://developer.riotgames.com/apis#match-v5/GET_getMatch and return match data

Save player, matchId and match as parquet in delta lake format.