document.getElementById('playerForm').addEventListener('submit', async function(e) {
    e.preventDefault();
    
    const gameName = document.getElementById('gameName').value;
    const tagLine = document.getElementById('tagLine').value;
    const apiKey = document.getElementById('apiKey').value;
    const resultsDiv = document.getElementById('results');
    
    // 1. Get player info
    resultsDiv.innerText = 'Fetching player info...';
    const playerRes = await fetch('http://localhost:8000/player', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ gameName, tagLine, apiKey })
    });
    const player = await playerRes.json();
    if (!player.puuid) {
        resultsDiv.innerText = 'Error fetching player info.';
        return;
    }
    
    // 2. Get match IDs
    resultsDiv.innerText = 'Fetching match IDs...';
    const matchIdsRes = await fetch('http://localhost:8000/matchids', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ puuid: player.puuid, apiKey })
    });
    const matchIdsData = await matchIdsRes.json();
    const matchIds = matchIdsData.matchIds || [];
    
    // 3. Fetch match data
    resultsDiv.innerText = `Fetched ${matchIds.length} match IDs. Fetching match data...`;
    for (let i = 0; i < matchIds.length; i++) {
        await fetch('http://localhost:8000/match', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ puuid: player.puuid, matchId: matchIds[i], apiKey })
        });
        resultsDiv.innerText = `Fetched ${i+1} of ${matchIds.length} matches...`;
    }
    
    // 4. Get analytics
    resultsDiv.innerText = 'Analyzing data...';
    const analyticsRes = await fetch('http://localhost:8000/analytics', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ puuid: player.puuid })
    });
    const analytics = await analyticsRes.json();
    // 5. Display analytics
    let html = '<h2>Most Played Champions</h2><ol>';
    analytics.mostPlayed.forEach(([champ, count]) => {
        html += `<li>${champ}: ${count} games</li>`;
    });
    html += '</ol><h2>Highest Win Rate Champions (min 5 games)</h2><ol>';
    analytics.highestWinrate.forEach(([champ, winrate, games]) => {
        html += `<li>${champ}: ${(winrate*100).toFixed(1)}% win rate (${games} games)</li>`;
    });
    html += '</ol>';
    resultsDiv.innerHTML = html;
});
