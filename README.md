# VelMerWain
A data-driven tool to help League of Legends players increase their win rate. 

Vel’Koz – The Eye of the Void: a void creature that decomposes life to extract knowledge.
Heimerdinger – The Revered Inventor: converts wisdom into revolutionary breakthroughs.
Swain – The Noxian Grand General: turns knowledge into power.

## Local Setup & Usage

### Prerequisites
- Docker & Docker Compose installed
- Riot Games API key (https://developer.riotgames.com/)

### 1. Clone the repo

```
git clone https://github.com/PedroSillos/Velmerwain.git
```

### 2. Build and run with Docker Compose

```
cd infrastructure
docker compose up --build
```

### 3. Usage Flow
1. Open the frontend in your browser (http://localhost:8080)
2. Enter your gameName, tagLine, and Riot API key
3. The app will fetch your player info, match IDs, and match data, then analyze and display:
	- 10 most played champions
	- 10 highest win rate champions (min 5 games)

### Cloud Migration
- Infrastructure is ready for future migration to AWS/Azure/GCP (see `infrastructure/terraform`)