# Prompt 1
Lets start a Data Analytics project.

### Here are all 8 pillars of a Data Analytics project together:
 - Data Collection – Gathering relevant data.
 - Data Cleaning – Ensuring data quality.
 - Data Analysis – Identifying patterns and insights.
 - Visualization & Reporting – Presenting findings clearly.
 - Decision Support – Guiding business or operational decisions.
 - Monitoring & Optimization – Tracking outcomes and refining processes.
 - Infrastructure – Scalable storage, computing resources, and data pipelines.
 - Security & Compliance – Data privacy, access controls, and regulatory adherence.

### Considering this pillars, I want you to create a solution, using:
 - python.
 - containers.
 - Layered (N-tier) Architecture, data will be stored in bronze (raw), silver and gold layers
 - Infraestructure as a code. It will run locally for now, but make it as easy as possible to migrate to AWS, Azure or GCP.

The user will interact with the application in a web page. Use html, css and javascript to build it.

Add any other tools if needed, but keep it free to run locally.

### For now, this is the flow of the app:
1) The user types their gameName, tagLine and apiKey in a web page.

2) The system passes these as parameters to https://developer.riotgames.com/apis#account-v1/GET_getByRiotId and stores the results.

3) Now that we have the puuid for this player, get the matchIds from https://developer.riotgames.com/apis#match-v5/GET_getMatchIdsByPUUID passing parameters queue=420, type=ranked, count=100

4) Run this n times, changing the "start" parameter from 0, to 100, 200, etc, to make this return all matchIds for this player
Store this matchids

5) Now that we have this matchIds, pass one by one to https://developer.riotgames.com/apis#match-v5/GET_getMatch, and return the match that for the player in the match. Only try to get match data if we do not already have it stored.

6) After storing player, matchids and match_info, return in another page the 10 most played champions by the player and return the 10 highest win rate for the player.