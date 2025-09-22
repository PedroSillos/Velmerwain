import os
import json
from collections import Counter, defaultdict

def load_matches(puuid, data_dir):
    matches = []
    match_path = os.path.join(data_dir, f"matches_{puuid}.json")
    if os.path.exists(match_path):
        with open(match_path) as mf:
            matches = json.load(mf)
    return matches

def most_played_champions(puuid, data_dir, top_n=10):
    matches = load_matches(puuid, data_dir)
    champ_counter = Counter()
    for match in matches:
        champ_counter[match["championName"]] += 1
    return champ_counter.most_common(top_n)

def highest_winrate_champions(puuid, data_dir, top_n=10, min_games=5):
    matches = load_matches(puuid, data_dir)
    champ_stats = defaultdict(lambda: {'games': 0, 'wins': 0})
    for match in matches:
        champ = match['championName']
        champ_stats[champ]['games'] += 1
        if match['win']:
            champ_stats[champ]['wins'] += 1
    winrates = [
        (champ, stats['wins'] / stats['games'], stats['games'])
        for champ, stats in champ_stats.items() if stats['games'] >= min_games
    ]
    winrates.sort(key=lambda x: x[1], reverse=True)
    return winrates[:top_n]
