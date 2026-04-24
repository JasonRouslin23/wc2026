"""
WC2026 Data Loader
==================
Fetches and caches into Railway PostgreSQL:
  1. All 48 WC teams (with group assignments, jersey colors, manager)
  2. Full squads (players, positions, DOB, nationality)
  3. Recent form (last N matches per team: friendlies, qualifiers, Nations League)

Usage:
  python load_data.py --all          # full sync (teams → squads → form)
  python load_data.py --teams        # only team profiles
  python load_data.py --squads       # only squad sync (requires teams loaded)
  python load_data.py --form         # only recent form
  python load_data.py --standings    # season standings

Env vars (set in Railway or .env):
  DATABASE_URL       postgres://...
  SPORTRADAR_KEY     P7TCC23BCU2BEoNsDM292gRFtLQdnDm4lhh8JNF1
"""

import os
import sys
import time
import argparse
import logging
from datetime import datetime, timezone

import requests
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

# ── Config ───────────────────────────────────────────────────────────────────

SR_KEY         = os.environ.get("SPORTRADAR_KEY", "P7TCC23BCU2BEoNsDM292gRFtLQdnDm4lhh8JNF1")
SR_BASE        = "https://api.sportradar.com/soccer/production/v4/en"
WC_SEASON_ID   = "sr:season:101177"
WC_COMP_ID     = "sr:competition:16"
DATABASE_URL   = os.environ["DATABASE_URL"]

# How many recent matches to load per team (form window)
FORM_WINDOW    = 10
# Seconds to wait between API calls (rate limit: 1 req/sec on base tier)
RATE_LIMIT     = 1.1

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("wc2026")


# ── SportRadar helpers ────────────────────────────────────────────────────────

def sr_get(path: str, params: dict = None) -> dict:
    """GET from SportRadar v4, rate-limited. Raises on HTTP error."""
    url = f"{SR_BASE}/{path}"
    p = {"api_key": SR_KEY, **(params or {})}
    resp = requests.get(url, params=p, timeout=20)
    if resp.status_code == 429:
        log.warning("Rate limited — sleeping 10s")
        time.sleep(10)
        resp = requests.get(url, params=p, timeout=20)
    resp.raise_for_status()
    time.sleep(RATE_LIMIT)
    return resp.json()


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def log_sync(cur, job: str, status: str, records: int = 0, message: str = ""):
    cur.execute(
        "INSERT INTO sync_log (job, status, records, message) VALUES (%s, %s, %s, %s)",
        (job, status, records, message),
    )


# ── Group name lookup ─────────────────────────────────────────────────────────

# Hardcoded from the confirmed groups endpoint draw — update if draw changes.
# This is the 2026 FIFA World Cup group stage draw (48 teams, 12 groups A-L).
# Maps sr:competitor:XXXXX → group letter.
# Populated by load_groups() at startup if the API exposes stages_groups.

GROUP_MAP: dict[str, str] = {}  # filled at runtime


def load_groups(conn):
    """
    Fetch season stages/groups to build GROUP_MAP.
    Endpoint: /seasons/{season_id}/stages_groups_cup_rounds.json
    """
    log.info("Fetching group structure …")
    try:
        data = sr_get(f"seasons/{WC_SEASON_ID}/stages_groups_cup_rounds.json")
    except Exception as e:
        log.error(f"Could not load group structure: {e}")
        return

    stages = data.get("stages", [])
    for stage in stages:
        for group in stage.get("groups", []):
            group_name = group.get("name", "").replace("Group ", "").strip()  # "A" .. "L"
            for competitor_ref in group.get("competitors", []):
                tid = competitor_ref.get("id") or competitor_ref.get("competitor", {}).get("id")
                if tid and group_name:
                    GROUP_MAP[tid] = group_name

    log.info(f"Group map loaded: {len(GROUP_MAP)} teams mapped")

    # Ensure groups table is populated
    with conn.cursor() as cur:
        for letter in [chr(c) for c in range(ord("A"), ord("M"))]:  # A-L
            cur.execute(
                "INSERT INTO groups (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
                (letter,),
            )
        conn.commit()


# ── 1. Teams ──────────────────────────────────────────────────────────────────

def load_teams(conn):
    """
    Fetch all competitors for the WC season, then enrich each with
    the full competitor profile (jersey colors, manager, country).
    """
    log.info("Fetching season competitors …")
    data = sr_get(f"seasons/{WC_SEASON_ID}/competitors.json")
    competitors = data.get("season_competitors", [])
    log.info(f"Found {len(competitors)} competitors in season")

    upserted = 0
    with conn.cursor() as cur:
        for comp in competitors:
            cid = comp.get("id")
            if not cid:
                continue

            # Enrich with full profile
            try:
                profile = sr_get(f"competitors/{cid}/profile.json")
            except Exception as e:
                log.warning(f"Profile fetch failed for {cid}: {e}")
                profile = {}

            c = profile.get("competitor", comp)
            jerseys = c.get("jerseys", [{}])

            def jersey_color(side: str) -> str | None:
                for j in jerseys:
                    if j.get("type", "").lower() == side:
                        return j.get("base") or j.get("base_color")
                return None

            manager_obj = c.get("manager", {})

            cur.execute(
                """
                INSERT INTO teams (
                    id, name, short_name, abbreviation, country, country_code,
                    group_name, jersey_primary, jersey_secondary, jersey_goalkeeper,
                    manager_id, manager_name, manager_nationality,
                    raw_json, last_synced
                ) VALUES (
                    %(id)s, %(name)s, %(short_name)s, %(abbreviation)s,
                    %(country)s, %(country_code)s, %(group_name)s,
                    %(jersey_primary)s, %(jersey_secondary)s, %(jersey_goalkeeper)s,
                    %(manager_id)s, %(manager_name)s, %(manager_nationality)s,
                    %(raw_json)s, NOW()
                )
                ON CONFLICT (id) DO UPDATE SET
                    name               = EXCLUDED.name,
                    short_name         = EXCLUDED.short_name,
                    abbreviation       = EXCLUDED.abbreviation,
                    country            = EXCLUDED.country,
                    country_code       = EXCLUDED.country_code,
                    group_name         = EXCLUDED.group_name,
                    jersey_primary     = EXCLUDED.jersey_primary,
                    jersey_secondary   = EXCLUDED.jersey_secondary,
                    jersey_goalkeeper  = EXCLUDED.jersey_goalkeeper,
                    manager_id         = EXCLUDED.manager_id,
                    manager_name       = EXCLUDED.manager_name,
                    manager_nationality = EXCLUDED.manager_nationality,
                    raw_json           = EXCLUDED.raw_json,
                    last_synced        = NOW()
                """,
                {
                    "id":                  cid,
                    "name":                c.get("name", ""),
                    "short_name":          c.get("short_name"),
                    "abbreviation":        c.get("abbreviation"),
                    "country":             c.get("country"),
                    "country_code":        c.get("country_code"),
                    "group_name":          GROUP_MAP.get(cid),
                    "jersey_primary":      jersey_color("home"),
                    "jersey_secondary":    jersey_color("away"),
                    "jersey_goalkeeper":   jersey_color("goalkeeper"),
                    "manager_id":          manager_obj.get("id"),
                    "manager_name":        manager_obj.get("name"),
                    "manager_nationality": manager_obj.get("nationality"),
                    "raw_json":            psycopg2.extras.Json(c),
                },
            )
            upserted += 1
            log.info(f"  ✓ {c.get('name', cid)} ({GROUP_MAP.get(cid, '?')})")

        log_sync(cur, "teams", "ok", upserted)
        conn.commit()

    log.info(f"Teams loaded: {upserted}")


# ── 2. Squads ─────────────────────────────────────────────────────────────────

def load_squads(conn):
    """
    For each team in DB, fetch the full squad from the competitor profile.
    Players are upserted with position, DOB, nationality, shirt number.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM teams")
        teams = cur.fetchall()

    log.info(f"Loading squads for {len(teams)} teams …")
    total_players = 0

    with conn.cursor() as cur:
        for team in teams:
            tid = team["id"]
            tname = team["name"]
            try:
                profile = sr_get(f"competitors/{tid}/profile.json")
            except Exception as e:
                log.warning(f"Squad fetch failed for {tname}: {e}")
                continue

            players = profile.get("players", [])
            log.info(f"  {tname}: {len(players)} players")

            for p in players:
                pid = p.get("id")
                if not pid:
                    continue

                dob = None
                dob_str = p.get("date_of_birth")
                if dob_str:
                    try:
                        dob = datetime.strptime(dob_str, "%Y-%m-%d").date()
                    except ValueError:
                        pass

                cur.execute(
                    """
                    INSERT INTO players (
                        id, team_id, name, date_of_birth, nationality,
                        country_code, position, shirt_number,
                        height_cm, weight_kg, raw_json, last_synced
                    ) VALUES (
                        %(id)s, %(team_id)s, %(name)s, %(dob)s, %(nationality)s,
                        %(country_code)s, %(position)s, %(shirt_number)s,
                        %(height_cm)s, %(weight_kg)s, %(raw_json)s, NOW()
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        team_id      = EXCLUDED.team_id,
                        name         = EXCLUDED.name,
                        date_of_birth = EXCLUDED.date_of_birth,
                        nationality  = EXCLUDED.nationality,
                        country_code = EXCLUDED.country_code,
                        position     = EXCLUDED.position,
                        shirt_number = EXCLUDED.shirt_number,
                        height_cm    = EXCLUDED.height_cm,
                        weight_kg    = EXCLUDED.weight_kg,
                        raw_json     = EXCLUDED.raw_json,
                        last_synced  = NOW()
                    """,
                    {
                        "id":           pid,
                        "team_id":      tid,
                        "name":         p.get("name", ""),
                        "dob":          dob,
                        "nationality":  p.get("nationality"),
                        "country_code": p.get("country_code"),
                        "position":     p.get("type"),   # SR uses 'type' for position
                        "shirt_number": p.get("jersey_number"),
                        "height_cm":    p.get("height"),
                        "weight_kg":    p.get("weight"),
                        "raw_json":     psycopg2.extras.Json(p),
                    },
                )
                total_players += 1

        log_sync(cur, "squads", "ok", total_players)
        conn.commit()

    log.info(f"Squads loaded: {total_players} players total")


# ── 3. Recent Form ────────────────────────────────────────────────────────────

def load_form(conn):
    """
    Fetch recent match summaries per team.
    Uses the competitor's last matches endpoint:
      GET /competitors/{id}/summaries.json
    Stores matches + events (goals, cards).
    """
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM teams")
        teams = cur.fetchall()

    log.info(f"Loading recent form for {len(teams)} teams …")
    total_matches = 0

    with conn.cursor() as cur:
        for team in teams:
            tid = team["id"]
            tname = team["name"]
            try:
                # summaries returns last ~10 completed matches
                data = sr_get(f"competitors/{tid}/summaries.json")
            except Exception as e:
                log.warning(f"Form fetch failed for {tname}: {e}")
                continue

            summaries = data.get("summaries", [])[:FORM_WINDOW]
            log.info(f"  {tname}: {len(summaries)} recent matches")

            for summary in summaries:
                sport_event = summary.get("sport_event", {})
                results     = summary.get("sport_event_status", {})
                se_id       = sport_event.get("id")
                if not se_id:
                    continue

                competitors = sport_event.get("sport_event_context", {}).get("competition", {})
                comp_obj    = sport_event.get("sport_event_context", {}).get("competition", {})
                season_obj  = sport_event.get("sport_event_context", {}).get("season", {})
                stage_obj   = sport_event.get("sport_event_context", {}).get("stage", {})
                round_obj   = sport_event.get("sport_event_context", {}).get("round", {})
                groups_obj  = sport_event.get("sport_event_context", {}).get("groups", [{}])
                venue_obj   = sport_event.get("venue", {})

                # Determine home/away
                comps = sport_event.get("competitors", [])
                home_team = away_team = None
                home_score = away_score = None
                for comp in comps:
                    if comp.get("qualifier") == "home":
                        home_team = comp.get("id")
                    elif comp.get("qualifier") == "away":
                        away_team = comp.get("id")

                period_scores = results.get("period_scores", [])
                home_score_ht = away_score_ht = None
                if period_scores:
                    ht = period_scores[0]
                    home_score_ht = ht.get("home_score")
                    away_score_ht = ht.get("away_score")

                kickoff_str = sport_event.get("start_time")
                kickoff_utc = None
                if kickoff_str:
                    try:
                        kickoff_utc = datetime.fromisoformat(kickoff_str.replace("Z", "+00:00"))
                    except ValueError:
                        pass

                # Group from context
                group_name = None
                if groups_obj:
                    gn = groups_obj[0].get("name", "")
                    group_name = gn.replace("Group ", "").strip() or None

                cur.execute(
                    """
                    INSERT INTO matches (
                        id, season_id, competition_id, competition_name,
                        home_team_id, away_team_id, kickoff_utc, status,
                        stage, group_name, round,
                        home_score, away_score, home_score_ht, away_score_ht,
                        venue_id, venue_name, venue_city, venue_country,
                        raw_json, last_synced
                    ) VALUES (
                        %(id)s, %(season_id)s, %(competition_id)s, %(competition_name)s,
                        %(home_team_id)s, %(away_team_id)s, %(kickoff_utc)s, %(status)s,
                        %(stage)s, %(group_name)s, %(round)s,
                        %(home_score)s, %(away_score)s, %(home_score_ht)s, %(away_score_ht)s,
                        %(venue_id)s, %(venue_name)s, %(venue_city)s, %(venue_country)s,
                        %(raw_json)s, NOW()
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        status          = EXCLUDED.status,
                        home_score      = EXCLUDED.home_score,
                        away_score      = EXCLUDED.away_score,
                        home_score_ht   = EXCLUDED.home_score_ht,
                        away_score_ht   = EXCLUDED.away_score_ht,
                        raw_json        = EXCLUDED.raw_json,
                        last_synced     = NOW()
                    """,
                    {
                        "id":               se_id,
                        "season_id":        season_obj.get("id"),
                        "competition_id":   comp_obj.get("id"),
                        "competition_name": comp_obj.get("name"),
                        "home_team_id":     home_team,
                        "away_team_id":     away_team,
                        "kickoff_utc":      kickoff_utc,
                        "status":           results.get("status"),
                        "stage":            stage_obj.get("type"),
                        "group_name":       group_name,
                        "round":            str(round_obj.get("number", "")) or None,
                        "home_score":       results.get("home_score"),
                        "away_score":       results.get("away_score"),
                        "home_score_ht":    home_score_ht,
                        "away_score_ht":    away_score_ht,
                        "venue_id":         venue_obj.get("id"),
                        "venue_name":       venue_obj.get("name"),
                        "venue_city":       venue_obj.get("city_name"),
                        "venue_country":    venue_obj.get("country_name"),
                        "raw_json":         psycopg2.extras.Json(summary),
                    },
                )

                # Store match events (goals, cards)
                for event in summary.get("timeline", []):
                    etype = event.get("type", "")
                    if etype not in ("score_change", "yellow_card", "red_card",
                                     "yellow_red_card", "substitution"):
                        continue
                    player_obj = event.get("player") or event.get("scorer", {})
                    team_obj   = event.get("team", {})

                    cur.execute(
                        """
                        INSERT INTO match_events (
                            match_id, team_id, player_id,
                            event_type, minute, extra_time, method, raw_json
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (
                            se_id,
                            team_obj.get("id"),
                            player_obj.get("id") if player_obj else None,
                            etype,
                            event.get("time"),
                            event.get("added_time"),
                            event.get("method"),
                            psycopg2.extras.Json(event),
                        ),
                    )

                total_matches += 1

        log_sync(cur, "form", "ok", total_matches)
        conn.commit()

    log.info(f"Form loaded: {total_matches} match records")


# ── 4. Standings ──────────────────────────────────────────────────────────────

def load_standings(conn):
    """
    Fetch season standings and cache by group.
    Endpoint: /seasons/{season_id}/standings.json
    """
    log.info("Fetching standings …")
    try:
        data = sr_get(f"seasons/{WC_SEASON_ID}/standings.json")
    except Exception as e:
        log.error(f"Standings fetch failed: {e}")
        return

    standings_raw = data.get("standings", [])
    total = 0

    with conn.cursor() as cur:
        for standing in standings_raw:
            for group in standing.get("groups", []):
                group_name = group.get("name", "").replace("Group ", "").strip()
                for entry in group.get("team_standings", []):
                    team_obj = entry.get("team", {})
                    tid = team_obj.get("id")
                    if not tid:
                        continue
                    cur.execute(
                        """
                        INSERT INTO standings (
                            season_id, group_name, team_id, rank,
                            played, wins, draws, losses,
                            goals_for, goals_against, points, raw_json, last_synced
                        ) VALUES (
                            %(season_id)s, %(group_name)s, %(team_id)s, %(rank)s,
                            %(played)s, %(wins)s, %(draws)s, %(losses)s,
                            %(goals_for)s, %(goals_against)s, %(points)s, %(raw_json)s, NOW()
                        )
                        ON CONFLICT (season_id, group_name, team_id) DO UPDATE SET
                            rank          = EXCLUDED.rank,
                            played        = EXCLUDED.played,
                            wins          = EXCLUDED.wins,
                            draws         = EXCLUDED.draws,
                            losses        = EXCLUDED.losses,
                            goals_for     = EXCLUDED.goals_for,
                            goals_against = EXCLUDED.goals_against,
                            points        = EXCLUDED.points,
                            raw_json      = EXCLUDED.raw_json,
                            last_synced   = NOW()
                        """,
                        {
                            "season_id":    WC_SEASON_ID,
                            "group_name":   group_name,
                            "team_id":      tid,
                            "rank":         entry.get("rank"),
                            "played":       entry.get("played", 0),
                            "wins":         entry.get("win", 0),
                            "draws":        entry.get("draw", 0),
                            "losses":       entry.get("loss", 0),
                            "goals_for":    entry.get("goals_scored", 0),
                            "goals_against": entry.get("goals_conceded", 0),
                            "points":       entry.get("points", 0),
                            "raw_json":     psycopg2.extras.Json(entry),
                        },
                    )
                    total += 1

        log_sync(cur, "standings", "ok", total)
        conn.commit()

    log.info(f"Standings loaded: {total} entries")


# ── Entrypoint ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="WC2026 Data Loader")
    parser.add_argument("--all",       action="store_true", help="Full sync")
    parser.add_argument("--teams",     action="store_true")
    parser.add_argument("--squads",    action="store_true")
    parser.add_argument("--form",      action="store_true")
    parser.add_argument("--standings", action="store_true")
    args = parser.parse_args()

    if not any(vars(args).values()):
        parser.print_help()
        sys.exit(1)

    conn = get_conn()

    try:
        # Always load group map first (needed for team → group assignment)
        load_groups(conn)

        if args.all or args.teams:
            load_teams(conn)

        if args.all or args.squads:
            load_squads(conn)

        if args.all or args.form:
            load_form(conn)

        if args.all or args.standings:
            load_standings(conn)

        log.info("✅ Sync complete")

    except Exception as e:
        log.exception(f"Loader failed: {e}")
        with conn.cursor() as cur:
            log_sync(cur, "loader", "error", message=str(e))
        conn.commit()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
