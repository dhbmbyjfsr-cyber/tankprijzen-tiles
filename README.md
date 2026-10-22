# tankprijzen-tiles

Static JSON tiles with **estimated** fuel prices for the TankPrijzen app, rebuilt daily and hosted
on GitHub Pages. The app's API runs on a free instance that sleeps; these files never sleep, so the
app can show stations immediately and swap in live prices when the API answers.

```
https://dhbmbyjfsr-cyber.github.io/tankprijzen-tiles/v1/index.json
https://dhbmbyjfsr-cyber.github.io/tankprijzen-tiles/v1/cells/{x}/{y}.json   x = floor(lng/0.18), y = floor(lat/0.12)
```

Prices are estimates (EUR/L), **not** live pump prices: national averages from CBS (NL) and the EU
Weekly Oil Bulletin, adjusted for station type and brand (`estimate.py`).

## Data and licences
- Station locations: © OpenStreetMap contributors (ODbL)
- CBS open data (CC BY 4.0), EU Weekly Oil Bulletin (© European Union)

Build: `python scripts/build_fuel_tiles.py --out public` (needs `httpx`, `aiosqlite`).
