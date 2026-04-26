"""
WC2026 FBref Stats Scraper
===========================
Pulls player season stats from FBref via soccerdata for all leagues
relevant to WC2026 squads. Matches players to our DB by name + nationality
and stores in player_fbref_stats table.

Usage:
  python scrape_fbref.py              # full scrape
  python scrape_fbref.py --dry-run    # scrape but don't save to DB
  python scrape_fbref.py --match-only # just run name matching, don't re-scrape

Env vars:
  DATABASE_URL
"""

import os
import re
import time
import logging
import argparse
from collections import defaultdict
from unidecode import unidecode

import pandas as pd
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv
import soccerdata as sd

load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("fbref")

DATABASE_URL = os.environ["DATABASE_URL"]

# ── Leagues to scrape ─────────────────────────────────────────────────────────
# Format: (soccerdata league name, season, weight)
# Weight reflects competition quality for rating purposes

LEAGUES = [
    # Top 5 European leagues — current season
    ("ENG-Premier League",       "2425", 1.20),
    ("ESP-La Liga",              "2425", 1.20),
    ("GER-Bundesliga",           "2425", 1.20),
    ("ITA-Serie A",              "2425", 1.20),
    ("FRA-Ligue 1",              "2425", 1.15),
    # Other European
    ("NED-Eredivisie",           "2425", 1.05),
    ("POR-Primeira Liga",        "2425", 1.05),
    ("TUR-Super Lig",            "2425", 1.00),
    ("BEL-First Division A",     "2425", 1.00),
    ("SCO-Scottish Premiership", "2425", 0.95),
    # Americas
    ("USA-MLS",                  "2025", 0.95),
    ("MEX-Liga MX",              "2425", 0.95),
    ("BRA-Serie A",              "2024", 1.00),
    ("ARG-Liga Profesional",     "2024", 1.00),
    # Asia/Other
    ("SAU-Saudi Pro League",     "2425", 0.90),
]

# Stat types to pull
STAT_TYPES = ["standard", "shooting", "misc", "playing_time"]

# ── Name normalization ────────────────────────────────────────────────────────

def normalize_name(name: str) -> str:
    """Normalize player name for fuzzy matching."""
    if not name:
        return ""
    # Remove accents
    name = unidecode(str(name))
    # Lowercase
    name = name.lower()
    # Remove punctuation except spaces
    name = re.sub(r"[^\w\s]", "", name)
    # Collapse spaces
    name = re.sub(r"\s+", " ", name).strip()
    return name


def name_variants(name: str) -> list[str]:
    """Generate name variants for matching (FBref uses 'Last, First' sometimes)."""
    norm = normalize_name(name)
    parts = norm.split()
    variants = [norm]
    if len(parts) >= 2:
        # Try reversed
        variants.append(f"{parts[-1]} {' '.join(parts[:-1])}")
        # First + last only
        variants.append(f"{parts[0]} {parts[-1]}")
        # Last name only (for single-name players)
        variants.append(parts[-1])
    return variants


def get_conn():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


# ── Scrape FBref ──────────────────────────────────────────────────────────────

def scrape_league(league: str, season: str) -> dict[str, dict]:
    """
    Scrape all stat types for a league/season.
    Returns {normalized_name: {stat_dict}} merged across all stat types.
    """
    log.info(f"  Scraping {league} {season} …")
    fb = sd.FBref([league], season)

    all_stats = {}

    for stat_type in STAT_TYPES:
        try:
            df = fb.read_player_season_stats(stat_type)

            # Flatten multi-level columns
            new_cols = []
            for col in df.columns:
                if isinstance(col, tuple):
                    a, b = col[0], col[1]
                    if a in ("", "nation", "pos", "age", "born"):
                        new_cols.append(b)
                    else:
                        new_cols.append(f"{a}_{b}")
                else:
                    new_cols.append(str(col))
            df.columns = new_cols
            df = df.reset_index()

            # Rename player column if it's in the index
            if "player" not in df.columns:
                # Try to find it
                for col in df.columns:
                    if col.lower() == "player":
                        df = df.rename(columns={col: "player"})
                        break

            for idx in range(len(df)):
                row = df.iloc[idx]
                player_name = str(row.get("player", "")).strip()
                if not player_name or player_name in ("nan", "Player", ""):
                    continue

                norm = normalize_name(player_name)
                if not norm:
                    continue

                if norm not in all_stats:
                    nation_val = row.get("nation", "")
                    pos_val = row.get("pos", row.get("Pos", ""))
                    age_val = row.get("age", row.get("Age", ""))
                    team_val = row.get("team", "")

                    all_stats[norm] = {
                        "fbref_name": player_name,
                        "nation":     str(nation_val) if pd.notna(nation_val) else "",
                        "pos":        str(pos_val) if pd.notna(pos_val) else "",
                        "age":        age_val,
                        "team":       str(team_val) if pd.notna(team_val) else "",
                        "league":     league,
                        "season":     season,
                    }

                # Merge stats — iterate columns safely
                for col in df.columns:
                    if col in ("player", "nation", "pos", "age", "born", "team",
                               "league", "season", "Player", "Nation", "Pos",
                               "Age", "Born", "Team", "Squad"):
                        continue
                    try:
                        val = row[col]
                        if pd.isna(val):
                            continue
                        if str(val) in ("", "nan", "NaN"):
                            continue
                        try:
                            all_stats[norm][col] = float(val)
                        except (ValueError, TypeError):
                            all_stats[norm][col] = str(val)
                    except Exception:
                        continue

        except Exception as e:
            log.warning(f"    {stat_type} failed for {league}: {e}")
            continue

    log.info(f"    {league}: {len(all_stats)} players scraped")
    return all_stats


# ── Match to DB players ───────────────────────────────────────────────────────

def match_players(fbref_data: dict[str, dict], db_players: list[dict]) -> dict[str, str]:
    """
    Match FBref player names to our DB player IDs.
    Returns {player_id: normalized_fbref_name}
    """
    # Build lookup from normalized name variants to FBref norm name
    fbref_lookup: dict[str, str] = {}
    for norm_name in fbref_data:
        for variant in name_variants(norm_name):
            fbref_lookup[variant] = norm_name

    matched = {}
    unmatched = []

    for player in db_players:
        pid = player["id"]
        db_name = player["name"]

        # SR names are often "Last, First" format
        # Try both orderings
        search_names = name_variants(db_name)

        found = None
        for variant in search_names:
            if variant in fbref_lookup:
                found = fbref_lookup[variant]
                break

        if found:
            matched[pid] = found
        else:
            unmatched.append(db_name)

    log.info(f"Matched {len(matched)}/{len(db_players)} players "
             f"({len(unmatched)} unmatched)")

    if unmatched[:10]:
        log.info(f"Sample unmatched: {unmatched[:10]}")

    return matched


# ── Ensure table ──────────────────────────────────────────────────────────────

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS player_fbref_stats (
                player_id           TEXT PRIMARY KEY,
                fbref_name          TEXT,
                league              TEXT,
                season              TEXT,
                pos                 TEXT,
                age                 INT,
                -- Playing time
                minutes_played      NUMERIC,
                matches_played      INT,
                starts              INT,
                minutes_per_90      NUMERIC,
                -- Standard
                goals               NUMERIC,
                assists             NUMERIC,
                goals_per90         NUMERIC,
                assists_per90       NUMERIC,
                ga_per90            NUMERIC,
                -- Shooting
                shots               NUMERIC,
                shots_on_target     NUMERIC,
                shot_pct            NUMERIC,
                shots_per90         NUMERIC,
                sot_per90           NUMERIC,
                goals_per_shot      NUMERIC,
                goals_per_sot       NUMERIC,
                -- Misc / defense
                yellow_cards        INT,
                red_cards           INT,
                fouls_committed     NUMERIC,
                fouls_drawn         NUMERIC,
                interceptions       NUMERIC,
                tackles_won         NUMERIC,
                offsides            NUMERIC,
                -- Composite rating (filled by build_player_ratings.py)
                fbref_rating        NUMERIC(6,2),
                raw_json            JSONB,
                last_synced         TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        conn.commit()
    log.info("player_fbref_stats table ready")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run",    action="store_true")
    parser.add_argument("--match-only", action="store_true",
                        help="Skip scraping, just re-run matching from cached data")
    args = parser.parse_args()

    conn = get_conn()
    ensure_table(conn)

    # Load WC squad players from DB
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.name, p.position, p.date_of_birth,
                   p.nationality, p.country_code,
                   t.name as team_name, t.group_name
            FROM players p
            JOIN teams t ON t.id = p.team_id
            WHERE t.group_name IS NOT NULL
        """)
        db_players = [dict(r) for r in cur.fetchall()]

    log.info(f"Loaded {len(db_players)} WC squad players from DB")

    # Scrape FBref
    all_fbref: dict[str, dict] = {}

    if not args.match_only:
        for league, season, weight in LEAGUES:
            try:
                league_data = scrape_league(league, season)
                # Add league weight to each player's data
                for norm, stats in league_data.items():
                    stats["league_weight"] = weight
                    # Keep the highest-weight league for each player
                    if norm not in all_fbref or weight > all_fbref[norm].get("league_weight", 0):
                        all_fbref[norm] = stats
            except Exception as e:
                log.warning(f"League {league} failed entirely: {e}")
                continue
            # Small delay between leagues to be nice to FBref
            time.sleep(3)

        log.info(f"Total unique players scraped from FBref: {len(all_fbref)}")
    else:
        log.info("--match-only: skipping scrape")

    # Match players
    matched = match_players(all_fbref, db_players)

    if args.dry_run:
        # Print some matched players
        log.info("\nSample matches:")
        for pid, fbref_norm in list(matched.items())[:20]:
            db_p = next(p for p in db_players if p["id"] == pid)
            fbref_p = all_fbref.get(fbref_norm, {})
            g = fbref_p.get("Performance_Gls", fbref_p.get("Standard_Gls", "?"))
            a = fbref_p.get("Performance_Ast", "?")
            log.info(f"  {db_p['name']} ({db_p['team_name']}) → "
                     f"{fbref_p.get('fbref_name','?')} | G:{g} A:{a} "
                     f"League:{fbref_p.get('league','?')}")
        log.info("\nDry run — not saving to DB")
        conn.close()
        return

    # Save to DB
    log.info(f"\nSaving {len(matched)} player FBref stats to DB …")
    saved = 0

    with conn.cursor() as cur:
        for pid, fbref_norm in matched.items():
            stats = all_fbref.get(fbref_norm, {})
            if not stats:
                continue

            def get_stat(*keys, default=None):
                for k in keys:
                    v = stats.get(k)
                    if v is not None and str(v) not in ("nan", ""):
                        try:
                            return float(v)
                        except (ValueError, TypeError):
                            pass
                return default

            cur.execute("""
                INSERT INTO player_fbref_stats (
                    player_id, fbref_name, league, season, pos, age,
                    minutes_played, matches_played, starts, minutes_per_90,
                    goals, assists, goals_per90, assists_per90, ga_per90,
                    shots, shots_on_target, shot_pct, shots_per90, sot_per90,
                    goals_per_shot, goals_per_sot,
                    yellow_cards, red_cards, fouls_committed, fouls_drawn,
                    interceptions, tackles_won, offsides,
                    raw_json, last_synced
                ) VALUES (
                    %(player_id)s, %(fbref_name)s, %(league)s, %(season)s,
                    %(pos)s, %(age)s,
                    %(minutes_played)s, %(matches_played)s, %(starts)s, %(minutes_per_90)s,
                    %(goals)s, %(assists)s, %(goals_per90)s, %(assists_per90)s, %(ga_per90)s,
                    %(shots)s, %(shots_on_target)s, %(shot_pct)s, %(shots_per90)s, %(sot_per90)s,
                    %(goals_per_shot)s, %(goals_per_sot)s,
                    %(yellow_cards)s, %(red_cards)s, %(fouls_committed)s, %(fouls_drawn)s,
                    %(interceptions)s, %(tackles_won)s, %(offsides)s,
                    %(raw_json)s, NOW()
                )
                ON CONFLICT (player_id) DO UPDATE SET
                    fbref_name      = EXCLUDED.fbref_name,
                    league          = EXCLUDED.league,
                    goals           = EXCLUDED.goals,
                    assists         = EXCLUDED.assists,
                    goals_per90     = EXCLUDED.goals_per90,
                    assists_per90   = EXCLUDED.assists_per90,
                    shots_per90     = EXCLUDED.shots_per90,
                    sot_per90       = EXCLUDED.sot_per90,
                    interceptions   = EXCLUDED.interceptions,
                    tackles_won     = EXCLUDED.tackles_won,
                    minutes_played  = EXCLUDED.minutes_played,
                    raw_json        = EXCLUDED.raw_json,
                    last_synced     = NOW()
            """, {
                "player_id":      pid,
                "fbref_name":     stats.get("fbref_name"),
                "league":         stats.get("league"),
                "season":         stats.get("season"),
                "pos":            stats.get("pos"),
                "age":            int(stats["age"]) if stats.get("age") and str(stats.get("age")) != "nan" else None,
                "minutes_played": get_stat("Playing Time_Min"),
                "matches_played": int(get_stat("Playing Time_MP", default=0)),
                "starts":         int(get_stat("Starts_Starts", default=0)),
                "minutes_per_90": get_stat("Playing Time_90s"),
                "goals":          get_stat("Performance_Gls"),
                "assists":        get_stat("Performance_Ast"),
                "goals_per90":    get_stat("Per 90 Minutes_Gls"),
                "assists_per90":  get_stat("Per 90 Minutes_Ast"),
                "ga_per90":       get_stat("Per 90 Minutes_G+A"),
                "shots":          get_stat("Standard_Sh"),
                "shots_on_target": get_stat("Standard_SoT"),
                "shot_pct":       get_stat("Standard_SoT%"),
                "shots_per90":    get_stat("Standard_Sh/90"),
                "sot_per90":      get_stat("Standard_SoT/90"),
                "goals_per_shot": get_stat("Standard_G/Sh"),
                "goals_per_sot":  get_stat("Standard_G/SoT"),
                "yellow_cards":   int(get_stat("Performance_CrdY", default=0)),
                "red_cards":      int(get_stat("Performance_CrdR", default=0)),
                "fouls_committed": get_stat("Performance_Fls"),
                "fouls_drawn":    get_stat("Performance_Fld"),
                "interceptions":  get_stat("Performance_Int"),
                "tackles_won":    get_stat("Performance_TklW"),
                "offsides":       get_stat("Performance_Off"),
                "raw_json":       psycopg.types.json.Jsonb(stats),
            })
            saved += 1

        conn.commit()

    log.info(f"✅ Saved {saved} player FBref stats to DB")
    conn.close()


if __name__ == "__main__":
    main()
