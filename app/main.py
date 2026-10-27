import asyncio
import hashlib
import hmac
import itertools
import json
import logging
import os
import re
import secrets
import sqlite3
import threading
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import websockets
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

ENV_API_KEY = os.getenv("AISSTREAM_API_KEY", "").strip()  # optional fallback; the page can set the key
SEED_MMSI = os.getenv("SHIP_MMSI", "")  # optional, comma-separated, used to seed an empty list
DB_PATH = os.getenv("DB_PATH", "/data/ais.db")
MIN_INTERVAL = int(os.getenv("MIN_INTERVAL_SECONDS", "60"))
WS_URL = os.getenv("AISSTREAM_URL", "wss://stream.aisstream.io/v0/stream")
MAX_SHIPS = int(os.getenv("MAX_SHIPS", "3"))  # max ships tracked / subscribed
RECONNECT_HOURS = float(os.getenv("RECONNECT_HOURS", "6"))  # forced periodic reconnection
PASSWORD_FILE = os.getenv("PASSWORD_FILE", DB_PATH + ".password")
MMSI_RE = re.compile(r"^\d{9}$")
PALETTE = ["#1565c0", "#c62828", "#2e7d32", "#ef6c00", "#6a1b9a",
           "#00838f", "#ad1457", "#4e342e", "#546e7a", "#9e9d24"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ais")

_lock = threading.RLock()
os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.row_factory = sqlite3.Row

now_iso = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

db.executescript(
    """
    CREATE TABLE IF NOT EXISTS ships (
        mmsi TEXT PRIMARY KEY,
        name TEXT,                    -- name broadcast by the ship
        label TEXT,                   -- your own label (optional)
        color TEXT NOT NULL,
        added_at TEXT NOT NULL,
        tracking INTEGER NOT NULL DEFAULT 1   -- 0 = paused (history kept, no subscription)
    );
    CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
    """
)

if "tracking" not in [r[1] for r in db.execute("PRAGMA table_info(ships)")]:
    db.execute("ALTER TABLE ships ADD COLUMN tracking INTEGER NOT NULL DEFAULT 1")
    db.commit()

# --- migration from the single-ship version (no mmsi column) -----------------
_cols = [r[1] for r in db.execute("PRAGMA table_info(positions)")]
if _cols and "mmsi" not in _cols:
    legacy = SEED_MMSI.split(",")[0].strip() or "000000000"
    log.info("Migrating legacy positions to multi-ship schema (MMSI %s)", legacy)
    row = db.execute("SELECT value FROM meta WHERE key='ship_name'").fetchone()
    db.execute("DROP INDEX IF EXISTS idx_positions_ts")
    db.execute("ALTER TABLE positions RENAME TO positions_old")
    db.execute(
        "INSERT OR IGNORE INTO ships(mmsi, name, color, added_at) VALUES (?,?,?,?)",
        (legacy, row["value"] if row else None, PALETTE[0], now_iso()),
    )
    db.commit()
    _legacy_ship = legacy
else:
    _legacy_ship = None

db.executescript(
    """
    CREATE TABLE IF NOT EXISTS positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mmsi TEXT NOT NULL,
        ts TEXT NOT NULL,             -- ISO 8601 UTC
        lat REAL NOT NULL,
        lon REAL NOT NULL,
        sog REAL,                     -- speed over ground (knots)
        cog REAL,                     -- course over ground (deg)
        heading REAL,
        UNIQUE (mmsi, ts)
    );
    CREATE INDEX IF NOT EXISTS idx_positions_mmsi_ts ON positions(mmsi, ts);
    """
)
if _legacy_ship:
    db.execute(
        "INSERT OR IGNORE INTO positions(mmsi, ts, lat, lon, sog, cog, heading) "
        "SELECT ?, ts, lat, lon, sog, cog, heading FROM positions_old",
        (_legacy_ship,),
    )
    db.execute("DROP TABLE positions_old")
    db.commit()


def add_ship(mmsi: str, label: str | None = None) -> bool:
    with _lock:
        if db.execute("SELECT 1 FROM ships WHERE mmsi=?", (mmsi,)).fetchone():
            return False
        used = {r["color"] for r in db.execute("SELECT color FROM ships")}
        free = [c for c in PALETTE if c not in used]
        color = free[0] if free else PALETTE[len(used) % len(PALETTE)]
        db.execute(
            "INSERT INTO ships(mmsi, label, color, added_at) VALUES (?,?,?,?)",
            (mmsi, label or None, color, now_iso()),
        )
        db.commit()
    return True


# seed from env when the list is empty
with _lock:
    if db.execute("SELECT COUNT(*) c FROM ships").fetchone()["c"] == 0:
        for m in SEED_MMSI.split(",")[:MAX_SHIPS]:
            if MMSI_RE.match(m.strip()):
                add_ship(m.strip())

tracked: set[str] = set()
_last_stored: dict[str, datetime] = {}
changed = asyncio.Event()  # set whenever the ship list changes
POSITION_TYPES = ["PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport"]
status = {"connected": False, "connected_since": None, "last_error": None,
          "last_message_at": None, "messages": 0, "max_ships": MAX_SHIPS, "subscribed": 0, "subscribed_mmsis": []}
events: deque = deque(maxlen=300)    # debug log: commands sent, connections, errors
recv_log: deque = deque(maxlen=500)  # debug log: raw messages received from AISStream
_seq = itertools.count(1)


def log_event(kind: str, text: str):
    entry = {"n": next(_seq), "at": now_iso(), "kind": kind, "text": text}
    (recv_log if kind == "recv" else events).append(entry)
seen: dict[str, dict] = {}  # mmsi -> {"at": iso, "type": message type}


def subscribed_mmsis() -> list[str]:
    with _lock:
        rows = db.execute(
            "SELECT mmsi FROM ships WHERE tracking=1 ORDER BY added_at, mmsi LIMIT ?", (MAX_SHIPS,)).fetchall()
    return [r["mmsi"] for r in rows]


KEY_RE = re.compile(r"^[A-Za-z0-9_.\-]{8,200}$")


def get_api_key() -> tuple[str, str | None]:
    """Key saved from the web page wins over the optional environment variable."""
    with _lock:
        row = db.execute("SELECT value FROM meta WHERE key='api_key'").fetchone()
    if row and row["value"]:
        return row["value"], "page"
    if ENV_API_KEY:
        return ENV_API_KEY, "env"
    return "", None


def password_configured() -> bool:
    return os.path.isfile(PASSWORD_FILE)


def set_admin_password(password: str):
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt(password.encode(), salt=salt, n=2**14, r=8, p=1)
    os.makedirs(os.path.dirname(PASSWORD_FILE) or ".", exist_ok=True)
    tmp = PASSWORD_FILE + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(f"scrypt${salt.hex()}${digest.hex()}\n")
    os.replace(tmp, PASSWORD_FILE)
    os.chmod(PASSWORD_FILE, 0o600)


def check_admin_password(password: str) -> bool:
    try:
        with open(PASSWORD_FILE) as f:
            algorithm, salt_hex, digest_hex = f.read().strip().split("$")
        if algorithm != "scrypt":
            return False
        actual = hashlib.scrypt(password.encode(), salt=bytes.fromhex(salt_hex), n=2**14, r=8, p=1)
        return hmac.compare_digest(actual.hex(), digest_hex)
    except (OSError, ValueError):
        return False


def require_password(password: str):
    if not password_configured() or not check_admin_password(password):
        raise HTTPException(401, "Incorrect password")


def refresh_tracked():
    with _lock:
        tracked.clear()
        tracked.update(r["mmsi"] for r in db.execute("SELECT mmsi FROM ships WHERE tracking=1"))


refresh_tracked()


def _parse_ts(raw: str) -> datetime:
    # AISStream format: "2022-12-29 18:22:32.318353 +0000 UTC"
    return datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def store_position(msg: dict):
    meta = msg.get("MetaData") or msg.get("Metadata") or {}  # AISStream spells it "MetaData"
    mmsi = str(meta.get("MMSI", ""))
    if mmsi not in tracked:
        return
    mtype = msg.get("MessageType", "")
    if mmsi not in seen:
        log.info("First message from %s (%s)", mmsi, mtype)
    seen[mmsi] = {"at": now_iso(), "type": mtype}
    pr = msg.get("Message", {}).get(mtype, {})
    lat = pr.get("Latitude", meta.get("latitude"))
    lon = pr.get("Longitude", meta.get("longitude"))
    if lat is None or lon is None or (lat == 0 and lon == 0) or abs(lat) > 90 or abs(lon) > 180:
        return
    ts = _parse_ts(meta["time_utc"])
    last = _last_stored.get(mmsi)
    if last and (ts - last).total_seconds() < MIN_INTERVAL:
        return
    heading = pr.get("TrueHeading")
    with _lock:
        db.execute(
            "INSERT OR IGNORE INTO positions(mmsi, ts, lat, lon, sog, cog, heading) VALUES (?,?,?,?,?,?,?)",
            (mmsi, ts.strftime("%Y-%m-%dT%H:%M:%SZ"), lat, lon, pr.get("Sog"), pr.get("Cog"),
             None if heading in (None, 511) else heading),
        )
        name = (meta.get("ShipName") or "").strip()
        if name:
            db.execute("UPDATE ships SET name=? WHERE mmsi=?", (name, mmsi))
        db.commit()
    _last_stored[mmsi] = ts
    log.info("Stored %s: %.5f, %.5f at %s", mmsi, lat, lon, ts)


async def session(mmsis: list[str], api_key: str):
    payload = {
        "APIKey": api_key,
        "BoundingBoxes": [[[-90, -180], [90, 180]]],
        "FiltersShipMMSI": mmsis,
        "FilterMessageTypes": POSITION_TYPES,
    }
    shown = {**payload, "APIKey": f"<hidden, {len(api_key)} chars>"}
    try:
        log_event("info", f"Connecting to {WS_URL}")
        async with websockets.connect(WS_URL, ping_interval=20) as ws:
            await ws.send(json.dumps(payload))
            log_event("sent", json.dumps(shown))
            status.update(connected=True, connected_since=now_iso(), last_error=None,
                          subscribed=len(mmsis), subscribed_mmsis=mmsis)
            log.info("Subscribed to AISStream for %d ship(s)", len(mmsis))
            async for raw in ws:
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode("utf-8", errors="replace")
                msg = json.loads(raw)
                status["messages"] += 1
                status["last_message_at"] = now_iso()
                if "error" in msg:
                    log_event("error", "AISStream said: " + str(raw)[:300])
                    raise RuntimeError(msg["error"])
                log_event("recv", str(raw)[:800])
                if msg.get("MessageType") in POSITION_TYPES:
                    store_position(msg)
    finally:
        status.update(connected=False, subscribed=0, subscribed_mmsis=[])
        log_event("info", "Disconnected")


async def collector():
    delay = 5
    while True:
        refresh_tracked()
        api_key, _src = get_api_key()
        if not api_key:
            status["last_error"] = "No API key configured"
            log_event("info", "No API key configured, waiting for one to be entered")
            await changed.wait()
            changed.clear()
            continue
        mmsis = subscribed_mmsis()
        if len(tracked) > len(mmsis):
            log_event("info", f"{len(tracked)} ships in the list but only the first {len(mmsis)} are subscribed (MAX_SHIPS={MAX_SHIPS})")
        if not mmsis:
            log_event("info", "No ship tracked, waiting for one to be added")
            await changed.wait()
            changed.clear()
            continue
        run = asyncio.create_task(session(mmsis, api_key))
        watch = asyncio.create_task(changed.wait())
        timer = asyncio.create_task(asyncio.sleep(RECONNECT_HOURS * 3600))
        done, pending = await asyncio.wait({run, watch, timer}, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
            try:
                await t
            except BaseException:
                pass
        if watch in done:  # ship list changed: resubscribe
            log_event("info", "Ship list changed, resubscribing")
            changed.clear()
            delay = 5
            await asyncio.sleep(1)
            continue
        if timer in done:  # scheduled reconnection
            log_event("info", f"Scheduled reconnection (every {RECONNECT_HOURS:g} h)")
            delay = 5
            await asyncio.sleep(1)
            continue
        try:
            run.result()
            status["last_error"] = "Connection closed by AISStream"
        except Exception as e:
            status["last_error"] = str(e)
            log_event("error", f"{type(e).__name__}: {e}")
            log.warning("Connection lost (%s), retrying in %ss", e, delay)
        await asyncio.sleep(delay)
        delay = min(delay * 2, 300)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(collector())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)


class ShipIn(BaseModel):
    mmsi: str
    label: str | None = None


@app.get("/api/ships")
def list_ships():
    with _lock:
        rows = db.execute(
            """
            SELECT s.mmsi, s.name, s.label, s.color, s.tracking,
                   (SELECT COUNT(*) FROM positions WHERE mmsi = s.mmsi) AS count,
                   p.ts, p.lat, p.lon, p.sog, p.cog
            FROM ships s
            LEFT JOIN positions p
              ON p.id = (SELECT id FROM positions WHERE mmsi = s.mmsi ORDER BY ts DESC LIMIT 1)
            ORDER BY s.added_at, s.mmsi
            """
        ).fetchall()
    out = []
    for r in rows:
        last = None if r["ts"] is None else {
            k: r[k] for k in ("ts", "lat", "lon", "sog", "cog")}
        out.append({"mmsi": r["mmsi"], "name": r["name"], "label": r["label"],
                    "color": r["color"], "tracking": bool(r["tracking"]), "count": r["count"], "last": last,
                    "seen": seen.get(r["mmsi"])})
    return out


@app.get("/api/debug")
def get_debug(view: str = "all"):
    if view == "sent":
        items = [e for e in events if e["kind"] == "sent"]
    elif view == "recv":
        items = list(recv_log)
    else:
        items = sorted([*events, *recv_log], key=lambda e: e["n"])
    return {"status": status, "tracked": sorted(tracked), "events": items,
            "recv_total": status["messages"]}


class KeyIn(BaseModel):
    key: str
    password: str


class PasswordIn(BaseModel):
    password: str


@app.get("/api/settings/password")
def get_password_state():
    return {"configured": password_configured()}


@app.post("/api/settings/password")
def create_password(body: PasswordIn):
    if len(body.password) < 12:
        raise HTTPException(400, "Password must be at least 12 characters")
    with _lock:
        if password_configured():
            raise HTTPException(409, "Password is already configured")
        set_admin_password(body.password)
    return {"configured": True}


@app.get("/api/settings")
def get_settings():
    key, src = get_api_key()
    return {"has_key": bool(key), "source": src, "hint": key[-4:] if len(key) >= 8 else ""}


@app.post("/api/settings/key")
async def save_key(body: KeyIn):
    require_password(body.password)
    key = body.key.strip()
    if not KEY_RE.match(key):
        raise HTTPException(400, "This does not look like an API key (8+ letters/digits, no spaces or quotes)")
    with _lock:
        db.execute("INSERT OR REPLACE INTO meta(key, value) VALUES ('api_key', ?)", (key,))
        db.commit()
    status["last_error"] = None
    log_event("info", f"API key saved from the web page ({len(key)} chars)")
    changed.set()
    return {"saved": True}


@app.delete("/api/settings/key")
async def delete_key(body: PasswordIn):
    require_password(body.password)
    with _lock:
        db.execute("DELETE FROM meta WHERE key='api_key'")
        db.commit()
    log_event("info", "API key removed from the database")
    changed.set()
    return {"removed": True}


@app.get("/api/status")
def get_status():
    return status


@app.post("/api/ships", status_code=201)
async def create_ship(ship: ShipIn):
    mmsi = ship.mmsi.strip()
    if not MMSI_RE.match(mmsi):
        raise HTTPException(400, "MMSI must be 9 digits")
    with _lock:
        count = db.execute("SELECT COUNT(*) c FROM ships WHERE tracking=1").fetchone()["c"]
    if count >= MAX_SHIPS:
        raise HTTPException(400, f"Limit of {MAX_SHIPS} tracked ships reached (pause or remove one)")
    label = (ship.label or "").strip()[:60] or None
    if not add_ship(mmsi, label):
        raise HTTPException(409, "Already tracked")
    refresh_tracked()
    changed.set()
    return {"mmsi": mmsi}


class TrackingIn(BaseModel):
    on: bool


@app.post("/api/ships/{mmsi}/tracking")
async def set_tracking(mmsi: str, body: TrackingIn):
    """Pause (on=false) or resume (on=true) tracking; history is always kept."""
    if not MMSI_RE.match(mmsi):
        raise HTTPException(400, "Bad MMSI")
    with _lock:
        row = db.execute("SELECT tracking FROM ships WHERE mmsi=?", (mmsi,)).fetchone()
        if not row:
            raise HTTPException(404, "Unknown ship")
        if body.on and not row["tracking"]:
            active = db.execute("SELECT COUNT(*) c FROM ships WHERE tracking=1").fetchone()["c"]
            if active >= MAX_SHIPS:
                raise HTTPException(400, f"Limit of {MAX_SHIPS} tracked ships reached (pause another first)")
        db.execute("UPDATE ships SET tracking=? WHERE mmsi=?", (int(body.on), mmsi))
        db.commit()
    _last_stored.pop(mmsi, None)
    log_event("info", f"{'Resumed' if body.on else 'Paused'} tracking of {mmsi}")
    refresh_tracked()
    changed.set()
    return {"mmsi": mmsi, "tracking": body.on}


@app.delete("/api/ships/{mmsi}")
async def delete_ship(mmsi: str):
    if not MMSI_RE.match(mmsi):
        raise HTTPException(400, "Bad MMSI")
    with _lock:
        db.execute("DELETE FROM positions WHERE mmsi=?", (mmsi,))
        cur = db.execute("DELETE FROM ships WHERE mmsi=?", (mmsi,))
        db.commit()
    if cur.rowcount == 0:
        raise HTTPException(404, "Unknown ship")
    _last_stored.pop(mmsi, None)
    refresh_tracked()
    changed.set()
    return {"deleted": mmsi}


@app.get("/api/tracks")
def tracks(mmsis: str, hours: int = Query(24, ge=1, le=24 * 365)):
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    result = {}
    with _lock:
        for m in mmsis.split(","):
            m = m.strip()
            if not MMSI_RE.match(m):
                continue
            rows = db.execute(
                "SELECT ts, lat, lon, sog, cog FROM positions WHERE mmsi=? AND ts>=? ORDER BY ts",
                (m, since),
            ).fetchall()
            result[m] = [dict(r) for r in rows]
    return result


@app.get("/")
def index():
    return FileResponse("app/static/index.html")


app.mount("/static", StaticFiles(directory="app/static"), name="static")
