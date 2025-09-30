# VelMerWain - Copilot Prompts

A data-driven tool to help League of Legends players increase their win rate.

## Tech Stack
- Python 3.13.7
- PySpark with Delta Lake
- Docker containers
- Riot Games API integration
- Bronze, Silver and Gold data storage

## Current App Flow

### 1. User Input
The user runs the app and chooses an action:
- `add` - to add a new player and fetch their match data
- `list` - to display all stored data

### 2. Add Player Flow
When adding a player, the user provides:
- **gameName** - League of Legends summoner name
- **tagLine** - Riot ID tag (e.g., #NA1)
- **apiKey** - Riot Games API key (hidden input using getpass)

The app then executes a complete data pipeline:

 - Step 1: Player Storage

 - Match IDs Collection

 - Match Details Collection

### 3. List/Display Flow
Displays comprehensive data overview:

 -  Players Table

 -  Match IDs Summary

 -  Matches Summary

### 4. Data Storage Structure
```
data/bronze/
├── players/
├── match_ids/
└── matches/
```

### 5. Container Setup
- **Base**: Python 3.13.7 with OpenJDK 21
- **Dependencies**: pyspark, delta-spark, requests
- **Volumes**: `./data` and `./src` mounted for persistence and development
- **Command**: `python src/main.py`

### 6. API Integration Details
- **Rate Limiting**: 1.2s delay between match detail requests
- **Error Handling**: HTTP status code validation
- **Batch Processing**: 100 match IDs per API call
- **Incremental Updates**: Only fetches new match data
- **Queue Filter**: Ranked Solo/Duo games only (queue=420)