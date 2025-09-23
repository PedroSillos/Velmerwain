# VelMerWain - Copilot Prompts

A data-driven tool to help League of Legends players increase their win rate.

## Tech Stack
- Python 3.13.7
- PySpark with Delta Lake
- Docker containers
- Bronze (raw), Silver, and Gold data layers

## Current App Flow

### 1. User Input
The user runs the app and chooses an action:
- `add` - to add a new player
- `list` - to display all stored players

### 2. Add Player Flow
When adding a player, the user provides:
- **gameName** - League of Legends summoner name
- **tagLine** - Riot ID tag (e.g., #NA1)
- **apiKey** - Riot Games API key (hidden input using getpass)

### 3. Bronze Layer - Player Storage
The app checks if the player exists in `data/bronze/players/`:
- **If player exists**: Updates the `modifiedOn` timestamp
- **If player doesn't exist**: 
  - Calls Riot API: `https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{gameName}/{tagLine}`
  - Saves: `puuid`, `gameName`, `tagLine`, `modifiedOn`
  - Stores data in Delta Lake format

### 4. List Players Flow
Displays all stored players in a formatted table showing:
- Game Name
- Tag Line  
- PUUID
- Last Modified timestamp

### 5. Container Setup
- Uses Docker with Python 3.13.7 and OpenJDK 21
- Mounts `./data` volume for persistent storage
- Interactive terminal support with `stdin_open` and `tty`