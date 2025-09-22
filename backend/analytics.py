import os
import json
from collections import Counter, defaultdict

def load_matches(puuid, data_dir):
    ids_path = os.path.join(data_dir, f"matchids_{puuid}.json")
    if not os.path.exists(ids_path):
        return []
    with open(ids_path) as f:
        match_ids = json.load(f)
    matches = []
    for match_id in match_ids:
        match_path = os.path.join(data_dir, f"match_{match_id}.json")
        if os.path.exists(match_path):
            with open(match_path) as mf:
                matches.append(json.load(mf))
    return matches

def most_played_champions(puuid, data_dir, top_n=10):
    matches = load_matches(puuid, data_dir)
    champ_counter = Counter()
    for match in matches:
        for p in match['info']['participants']:
            if p['puuid'] == puuid:
                champ_counter[p['championName']] += 1
    return champ_counter.most_common(top_n)

def highest_winrate_champions(puuid, data_dir, top_n=10, min_games=5):
    matches = load_matches(puuid, data_dir)
    champ_stats = defaultdict(lambda: {'games': 0, 'wins': 0})
    for match in matches:
        for p in match['info']['participants']:
            if p['puuid'] == puuid:
                champ = p['championName']
                champ_stats[champ]['games'] += 1
                if p['win']:
                    champ_stats[champ]['wins'] += 1
    winrates = [
        (champ, stats['wins'] / stats['games'], stats['games'])
        for champ, stats in champ_stats.items() if stats['games'] >= min_games
    ]
    winrates.sort(key=lambda x: x[1], reverse=True)
    return winrates[:top_n]
