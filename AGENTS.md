# Repository Guidelines

## Project Structure & Module Organization

This repository is a small Python/FastAPI AIS ship tracker. Application logic and API routes live in `app/main.py`; the browser interface and icons are in `app/static/`. `Dockerfile` builds the service, while `docker-compose.yml` runs it and mounts persistent state at `./data/ais.db`. There is no separate test or package directory in the current repository.

## Build, Test, and Development Commands

- `docker compose up -d --build` builds the image and starts the tracker on port 8000.
- `docker compose logs -f ais-tracker` follows application logs; stop the service with `docker compose down`.
- For local development, install dependencies with `python -m pip install -r requirements.txt`, then run `uvicorn app.main:app --reload --port 8000` from the repository root. Set `DB_PATH` to a writable local path if `/data/ais.db` is unavailable.

No automated test or lint command is currently configured. When adding tests, document the chosen runner and invocation in this guide and keep tests isolated from any live AISStream connection.

## Coding Style & Naming Conventions

Follow the existing Python style: four-space indentation, `snake_case` for functions and variables, and uppercase names for module-level constants. Keep database access parameterized, protect shared SQLite operations with the existing lock, and avoid logging API keys or other secrets. Keep browser assets under `app/static/` and update the API and interface consistently when changing behavior.

## Testing Guidelines

There is no test suite at present. For a change, run the service locally or with Docker and exercise the affected API or UI path. Use a temporary SQLite database for checks that modify persisted data; do not overwrite a user's `./data/ais.db`.

## Commit & Pull Request Guidelines

The available Git history contains only `first` commits, so it does not establish a commit-message convention. Use short imperative subjects that describe the change (for example, `Add vessel history export`). Pull requests should explain user-visible behavior, configuration or migration effects, and how the change was checked. Include screenshots for interface changes and link a related issue when one exists.

## Security & Configuration

The AISStream key can be entered in the page and is stored in SQLite. Key changes require the admin password; its salted scrypt hash is stored in the password file beside the database. Restrict network access or put authentication in front of deployments exposed beyond a trusted network. Optional settings include `AISSTREAM_API_KEY`, `SHIP_MMSI`, `MIN_INTERVAL_SECONDS`, `MAX_SHIPS`, and `RECONNECT_HOURS`; keep secrets out of commits.
