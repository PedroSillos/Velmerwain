# Prompt 1
I want to build a data-driven tool to help League of Legends players increase their win rate. 

### I want you to create a solution, using:
 - python
 - pyspark (https://spark.apache.org/docs/latest/api/python/)
 - delta lake (https://delta.io/)
 - containers
 - bronze (raw), silver and gold layers

Use minimal imports and make code as simple and short as possible

### For now, this is the flow of the app:
1) The user types in the terminal:
 - gameName
 - tagLine
 - apiKey (must not be visible in the terminal)

2) Save player (bronze) layer - check if gameName and tagLine are already stored:
 - If not stored, pass gameName and tagLine to https://developer.riotgames.com/apis#account-v1/GET_getByRiotId and save puuid, gameName,tagLine and modifiedOn (datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
 - If stored, update modifiedOn