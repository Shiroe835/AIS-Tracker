# AIS ship tracker (multi-ship)

1. Create a free key at https://aisstream.io
2. `docker compose up -d --build` (no `.env` needed)
3. Open http://localhost:8000 (or http://<vm-ip>:8000), click **API key** and paste your key, then add ships by MMSI in the side panel

The key is stored in the SQLite database (`./data/ais.db`). Anyone who can open the page can replace it, so protect the page (reverse proxy with a password, firewall) if the VM is reachable from the internet.

History is stored in `./data/ais.db` (SQLite). Back it up by copying that file.
An existing single-ship database is migrated automatically at first start.
Up to `MAX_SHIPS` ships (default 3) are tracked. The connection to AISStream is renewed every `RECONNECT_HOURS` hours (default 6).

The ⏸ button stops tracking a ship but keeps its history (▶ resumes it). Only tracked ships count toward `MAX_SHIPS`; × deletes a ship and its history.
