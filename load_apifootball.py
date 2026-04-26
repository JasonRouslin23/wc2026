"""
WC2026 API-Football Player Stats Loader
=========================================
Pulls season player stats from API-Football for all leagues
relevant to WC2026 squads. Matches to our DB players by name
and stores in player_apifootball_stats table.

Usage:
  python load_apifootball.py --dry-run   # fetch + match, don't save
  python load_apifootball.py             # full save to DB

Env vars:
  DATABASE_URL
  APIFOOTBALL_KEY
"""

import os
import json
import time
import logging
import argparse
import re
from collections import defaultdict
from pathlib import Path

import requests
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb as Json
from dotenv import load_dotenv
from unidecode import unidecode

load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("apifootball")

DATABASE_URL    = os.environ["DATABASE_URL"]
APIFOOTBALL_KEY = os.environ.get("APIFOOTBALL_KEY", "d50fc38c12e6599794eef0b5b6f830da")
AF_BASE         = "https://v3.football.api-sports.io"
RATE_LIMIT      = 0.5  # seconds between calls

# ── Leagues to pull ───────────────────────────────────────────────────────────
# (league_id, season, league_name, weight)
# Season is the year the league started (2024 = 2024/25)

LEAGUES = [
    # Top 5 European
    (39,  2024, "Premier League",        1.20),
    (140, 2024, "La Liga",               1.20),
    (78,  2024, "Bundesliga",            1.20),
    (135, 2024, "Serie A",               1.20),
    (61,  2024, "Ligue 1",               1.15),
    # Other European
    (88,  2024, "Eredivisie",            1.05),
    (94,  2024, "Primeira Liga",         1.05),
    (203, 2024, "Super Lig",             1.00),
    (144, 2024, "Belgian Pro League",    1.00),
    (179, 2024, "Scottish Premiership",  0.95),
    (113, 2024, "Allsvenskan",           0.90),
    (103, 2024, "Eliteserien",           0.90),  # Norway
    (307, 2024, "Saudi Pro League",      0.90),
    # Americas
    (253, 2024, "MLS",                   0.95),
    (262, 2024, "Liga MX",               2024),
    (71,  2024, "Serie A Brazil",        1.00),
    (128, 2024, "Liga Profesional",      1.00),  # Argentina
    (239, 2024, "Superliga",             0.90),  # Colombia
    (242, 2024, "Primera Division",      0.90),  # Uruguay
    # Asia/Africa
    (17,  2024, "AFC Asian Cup",         0.90),
    (33,  2023, "Africa Cup of Nations", 0.90),
    # Uzbekistan/Central Asia
    (686, 2024, "Uzbekistan League",     0.80),
]

# ── Name normalization ────────────────────────────────────────────────────────

def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = unidecode(str(name))
    name = name.lower()
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def get_conn():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


# ── API-Football helpers ──────────────────────────────────────────────────────

def af_get(path: str, params: dict = None) -> dict:
    url = f"{AF_BASE}/{path}"
    headers = {"x-apisports-key": APIFOOTBALL_KEY}
    resp = requests.get(url, headers=headers, params=params or {}, timeout=20)
    resp.raise_for_status()
    time.sleep(RATE_LIMIT)
    return resp.json()


def fetch_league_players(league_id: int, season: int) -> list[dict]:
    """Fetch all players for a league/season, handling pagination."""
    players = []
    page = 1
CACHE_DIR = Path.home() / ".wc2026_cache"
CACHE_DIR.mkdir(exist_ok=True)


def fetch_league_players(league_id: int, season: int) -> list[dict]:
    """Fetch all players for a league/season, with local file cache."""
    cache_file = CACHE_DIR / f"league_{league_id}_{season}.json"

    if cache_file.exists():
        log.info(f"    Using cache: {cache_file.name}")
        return json.loads(cache_file.read_text())

    players = []
    page = 1

    while True:
        log.info(f"    Page {page} …")
        data = af_get("players", {
            "league": league_id,
            "season": season,
            "page":   page,
        })

        results = data.get("response", [])
        if not results:
            break

        players.extend(results)

        paging = data.get("paging", {})
        if page >= paging.get("total", 1):
            break
        page += 1

    # Save to cache
    cache_file.write_text(json.dumps(players))
    log.info(f"    Cached {len(players)} players to {cache_file.name}")
    return players


# ── Ensure table ──────────────────────────────────────────────────────────────

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS player_apifootball_stats (
                player_id           TEXT PRIMARY KEY,
                af_player_id        INT,
                af_name             TEXT,
                af_nationality      TEXT,
                af_position         TEXT,
                af_age              INT,
                league_name         TEXT,
                league_id           INT,
                season              INT,
                -- Appearances
                appearances         INT,
                lineups             INT,
                minutes             INT,
                -- Goals & assists
                goals               INT,
                assists             INT,
                -- Shots
                shots_total         INT,
                shots_on             INT,
                -- Passes
                passes_total        INT,
                passes_key          INT,
                pass_accuracy       NUMERIC,
                -- Dribbles
                dribbles_attempts   INT,
                dribbles_success    INT,
                -- Defense
                tackles_total       INT,
                interceptions       INT,
                duels_total         INT,
                duels_won           INT,
                -- Cards & fouls
                yellow_cards        INT,
                red_cards           INT,
                fouls_committed     INT,
                fouls_drawn         INT,
                -- Rating
                rating              NUMERIC(4,2),
                -- Computed
                apifootball_rating  NUMERIC(6,2),
                raw_json            JSONB,
                last_synced         TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        conn.commit()
    log.info("player_apifootball_stats table ready")


# ── Match players to DB ───────────────────────────────────────────────────────

def match_player(af_player: dict, db_lookup: dict) -> str | None:
    """
    Try to match an API-Football player to a DB player ID.
    db_lookup: {normalized_name: player_id}
    """
    name = af_player.get("name", "")
    firstname = af_player.get("firstname", "")
    lastname  = af_player.get("lastname", "")

    candidates = []

    # Full name
    if name:
        candidates.append(normalize_name(name))

    # First + Last
    if firstname and lastname:
        candidates.append(normalize_name(f"{firstname} {lastname}"))
        candidates.append(normalize_name(f"{lastname} {firstname}"))
        # First initial + last
        if firstname:
            candidates.append(normalize_name(f"{firstname[0]} {lastname}"))

    for c in candidates:
        if len(c.split()) < 2:
            continue
        if c in db_lookup:
            return db_lookup[c]

        # Try first + last only
        parts = c.split()
        if len(parts) > 2:
            fl = f"{parts[0]} {parts[-1]}"
            if fl in db_lookup:
                return db_lookup[fl]

    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = get_conn()
    ensure_table(conn)

    # Load WC squad players
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.name, p.position, p.nationality, p.country_code,
                   t.name as team_name, t.group_name
            FROM players p
            JOIN teams t ON t.id = p.team_id
            WHERE t.group_name IS NOT NULL
        """)
        db_players = [dict(r) for r in cur.fetchall()]

    log.info(f"Loaded {len(db_players)} WC squad players")

    # Build DB name lookup
    db_lookup: dict[str, str] = {}
    for p in db_players:
        name = p["name"]
        # SR uses "Last, First" format
        if "," in name:
            parts = name.split(",", 1)
            first_last = f"{parts[1].strip()} {parts[0].strip()}"
            db_lookup[normalize_name(first_last)] = p["id"]
            # Also last first
            db_lookup[normalize_name(name.replace(",", ""))] = p["id"]
        else:
            db_lookup[normalize_name(name)] = p["id"]

        # First + last only variant
        norm = normalize_name(name.replace(",", ""))
        parts = norm.split()
        if len(parts) >= 2:
            db_lookup[f"{parts[0]} {parts[-1]}"] = p["id"]

    log.info(f"DB lookup built: {len(db_lookup)} name variants")

    # Pull stats per league
    all_matched: dict[str, dict] = {}  # player_id → best stats

    for league_id, season, league_name, weight in LEAGUES:
        if not isinstance(weight, float) and not isinstance(weight, int):
            weight = 1.0
        weight = float(weight) if weight <= 2.0 else 1.0

        log.info(f"Fetching {league_name} ({league_id}) season {season} …")
        try:
            players_data = fetch_league_players(league_id, season)
        except Exception as e:
            log.warning(f"  Failed: {e}")
            continue

        log.info(f"  {len(players_data)} players fetched")
        matched_count = 0

        for entry in players_data:
            player_info = entry.get("player", {})
            statistics   = entry.get("statistics", [{}])
            if not statistics:
                continue

            # Pick the stats with most minutes
            stats = max(statistics,
                        key=lambda s: s.get("games", {}).get("minutes") or 0)

            pid = match_player(player_info, db_lookup)
            if not pid:
                continue

            games    = stats.get("games", {})
            goals    = stats.get("goals", {})
            shots    = stats.get("shots", {})
            passes   = stats.get("passes", {})
            dribbles = stats.get("dribbles", {})
            tackles  = stats.get("tackles", {})
            duels    = stats.get("duels", {})
            cards    = stats.get("cards", {})
            fouls    = stats.get("fouls", {})

            new_minutes = games.get("minutes") or 0
            if new_minutes == 0:
                continue

            rating_str = games.get("rating")
            try:
                rating = float(rating_str) if rating_str else None
            except (ValueError, TypeError):
                rating = None

            new_entry = {
                "_weight":           weight,
                "_minutes_list":     [new_minutes],
                "_rating_list":      [rating] if rating else [],
                "af_player_id":      player_info.get("id"),
                "af_name":           player_info.get("name"),
                "af_nationality":    player_info.get("nationality"),
                "af_position":       games.get("position"),
                "af_age":            player_info.get("age"),
                "league_name":       league_name,
                "league_id":         league_id,
                "season":            season,
                "appearances":       games.get("appearences") or 0,
                "lineups":           games.get("lineups") or 0,
                "minutes":           new_minutes,
                "goals":             goals.get("total") or 0,
                "assists":           goals.get("assists") or 0,
                "shots_total":       shots.get("total") or 0,
                "shots_on":          shots.get("on") or 0,
                "passes_total":      passes.get("total") or 0,
                "passes_key":        passes.get("key") or 0,
                "pass_accuracy":     passes.get("accuracy"),
                "dribbles_attempts": dribbles.get("attempts") or 0,
                "dribbles_success":  dribbles.get("success") or 0,
                "tackles_total":     tackles.get("total") or 0,
                "interceptions":     tackles.get("interceptions") or 0,
                "duels_total":       duels.get("total") or 0,
                "duels_won":         duels.get("won") or 0,
                "yellow_cards":      cards.get("yellow") or 0,
                "red_cards":         cards.get("red") or 0,
                "fouls_committed":   fouls.get("committed") or 0,
                "fouls_drawn":       fouls.get("drawn") or 0,
                "rating":            rating,
                "raw_json":          Json(entry),
            }

            existing = all_matched.get(pid)
            if existing:
                # Aggregate cumulative stats
                for key in ("appearances", "lineups", "minutes", "goals", "assists",
                            "shots_total", "shots_on", "passes_total", "passes_key",
                            "dribbles_attempts", "dribbles_success", "tackles_total",
                            "interceptions", "duels_total", "duels_won",
                            "yellow_cards", "red_cards", "fouls_committed", "fouls_drawn"):
                    existing[key] = (existing.get(key) or 0) + new_entry[key]

                # Weighted average rating by minutes
                existing["_minutes_list"].append(new_minutes)
                if rating:
                    existing["_rating_list"].append(rating)

                total_mins = sum(existing["_minutes_list"])
                if existing["_rating_list"] and total_mins > 0:
                    # Weight rating by minutes played in each league
                    existing["rating"] = sum(existing["_rating_list"]) / len(existing["_rating_list"])

                # Use highest-weight league name for display
                if weight > existing.get("_weight", 0):
                    existing["league_name"] = league_name
                    existing["league_id"]   = league_id
                    existing["_weight"]     = weight
            else:
                all_matched[pid] = new_entry

            matched_count += 1

        log.info(f"  Matched {matched_count} WC players in {league_name}")

    log.info(f"\nTotal WC players matched: {len(all_matched)}/{len(db_players)}")

    # Print sample
    log.info("\nSample matched players:")
    log.info(f"{'Name':<30} {'League':<25} {'Apps':<6} {'G':<5} {'A':<5} {'Rating':<8} {'Min'}")
    log.info("-" * 90)
    player_lookup = {p["id"]: p for p in db_players}
    for pid, stats in sorted(all_matched.items(),
                              key=lambda x: float(x[1].get("rating") or 0),
                              reverse=True)[:30]:
        p = player_lookup.get(pid, {})
        log.info(
            f"{p.get('name',''):<30} {stats.get('league_name',''):<25} "
            f"{stats.get('appearances') or 0:<6} "
            f"{stats.get('goals') or 0:<5} "
            f"{stats.get('assists') or 0:<5} "
            f"{stats.get('rating') or '?':<8} "
            f"{stats.get('minutes') or 0}"
        )

    if args.dry_run:
        log.info("\nDry run — not saving to DB")
        conn.close()
        return

    # Save to DB
    log.info(f"\nSaving {len(all_matched)} player stats …")
    with conn.cursor() as cur:
        for pid, s in all_matched.items():
            cur.execute("""
                INSERT INTO player_apifootball_stats (
                    player_id, af_player_id, af_name, af_nationality, af_position, af_age,
                    league_name, league_id, season,
                    appearances, lineups, minutes,
                    goals, assists, shots_total, shots_on,
                    passes_total, passes_key, pass_accuracy,
                    dribbles_attempts, dribbles_success,
                    tackles_total, interceptions, duels_total, duels_won,
                    yellow_cards, red_cards, fouls_committed, fouls_drawn,
                    rating, raw_json, last_synced
                ) VALUES (
                    %(player_id)s, %(af_player_id)s, %(af_name)s, %(af_nationality)s,
                    %(af_position)s, %(af_age)s, %(league_name)s, %(league_id)s, %(season)s,
                    %(appearances)s, %(lineups)s, %(minutes)s,
                    %(goals)s, %(assists)s, %(shots_total)s, %(shots_on)s,
                    %(passes_total)s, %(passes_key)s, %(pass_accuracy)s,
                    %(dribbles_attempts)s, %(dribbles_success)s,
                    %(tackles_total)s, %(interceptions)s, %(duels_total)s, %(duels_won)s,
                    %(yellow_cards)s, %(red_cards)s, %(fouls_committed)s, %(fouls_drawn)s,
                    %(rating)s, %(raw_json)s, NOW()
                )
                ON CONFLICT (player_id) DO UPDATE SET
                    af_name          = EXCLUDED.af_name,
                    league_name      = EXCLUDED.league_name,
                    appearances      = EXCLUDED.appearances,
                    minutes          = EXCLUDED.minutes,
                    goals            = EXCLUDED.goals,
                    assists          = EXCLUDED.assists,
                    shots_total      = EXCLUDED.shots_total,
                    shots_on         = EXCLUDED.shots_on,
                    passes_key       = EXCLUDED.passes_key,
                    pass_accuracy    = EXCLUDED.pass_accuracy,
                    tackles_total    = EXCLUDED.tackles_total,
                    interceptions    = EXCLUDED.interceptions,
                    rating           = EXCLUDED.rating,
                    raw_json         = EXCLUDED.raw_json,
                    last_synced      = NOW()
            """, {**s, "player_id": pid})

        conn.commit()

    log.info(f"✅ Saved {len(all_matched)} player stats to DB")
    conn.close()


if __name__ == "__main__":
    main()
