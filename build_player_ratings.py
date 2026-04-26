"""
WC2026 Player Ratings Builder
==============================
Scores each WC squad player from their player_form data, builds
a starting XI quality score per team, and updates the players table.

Run this BEFORE build_power_ratings.py to feed squad strength into the model.

Usage:
  python build_player_ratings.py
  python build_player_ratings.py --dry-run
"""

import os
import math
import logging
import argparse
from collections import defaultdict

import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("player_ratings")

DATABASE_URL = os.environ["DATABASE_URL"]

# ── Competition prestige weights ──────────────────────────────────────────────
# Matched against competition_name from player_form table

PRESTIGE_RULES = [
    # UCL
    (["UEFA Champions League"], 1.5),
    # Europa / Conference knockout
    (["UEFA Europa League", "UEFA Conference League"], 1.3),
    # Top 5 leagues
    (["Premier League", "LaLiga", "Bundesliga", "Serie A", "Ligue 1"], 1.2),
    # Other top leagues
    (["Primeira Liga", "Eredivisie", "Pro League", "Super Lig",
      "Saudi Pro League", "MLS", "Liga MX", "Brasileirao",
      "Argentine Primera Division", "Scottish Premiership"], 1.05),
    # International competitive
    (["UEFA Nations League", "CONMEBOL Copa America", "UEFA European Championship",
      "CAF Africa Cup of Nations", "AFC Asian Cup", "CONCACAF Gold Cup"], 1.0),
    # WC qualifiers
    (["FIFA World Cup", "World Cup Qualification"], 0.90),
    # Cup competitions
    (["DFB Pokal", "FA Cup", "Copa del Rey", "Coppa Italia",
      "Coupe de France", "EFL Cup", "Carabao Cup"], 0.95),
    # Friendly
    (["International Friendly", "Friendly"], 0.60),
]

DEFAULT_PRESTIGE = 0.85  # unknown competitions


def get_prestige(competition_name: str) -> float:
    if not competition_name:
        return DEFAULT_PRESTIGE
    cn = competition_name.lower()
    for keywords, weight in PRESTIGE_RULES:
        for kw in keywords:
            if kw.lower() in cn:
                return weight
    return DEFAULT_PRESTIGE


# ── Position attack/defense classification ────────────────────────────────────

POSITION_ROLE = {
    "Goalkeeper":  "gk",
    "Defender":    "def",
    "Midfielder":  "mid",
    "Forward":     "fwd",
}


def score_player_match(row: dict, prestige: float) -> float:
    """
    Score a single player-match row. Returns a weighted contribution score.
    """
    score = 0.0

    # Starter bonus
    if row.get("starter"):
        score += 0.5

    # Attacking contributions
    score += (row.get("goals_scored") or 0) * 3.0
    score += (row.get("assists") or 0) * 2.0
    score += (row.get("shots_on_target") or 0) * 0.5
    score += (row.get("shots_off_target") or 0) * 0.2

    # Discipline
    score -= (row.get("yellow_cards") or 0) * 0.5
    score -= (row.get("red_cards") or 0) * 2.0
    score -= (row.get("yellow_red_cards") or 0) * 1.5

    # Own goals
    score -= (row.get("own_goals") or 0) * 2.0

    return score * prestige


def get_conn():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = get_conn()

    # Load all WC squad players with their team info
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.id, p.name, p.team_id, p.position,
                   t.name as team_name, t.group_name, t.country_code
            FROM players p
            JOIN teams t ON t.id = p.team_id
            WHERE t.group_name IS NOT NULL
            ORDER BY p.team_id, p.position, p.name
        """)
        players = [dict(r) for r in cur.fetchall()]

    log.info(f"Scoring {len(players)} WC squad players …")

    # Load all player form rows
    with conn.cursor() as cur:
        cur.execute("""
            SELECT player_id, competition_name, kickoff_utc,
                   starter, goals_scored, assists, shots_on_target,
                   shots_off_target, shots_blocked, yellow_cards,
                   red_cards, yellow_red_cards, own_goals
            FROM player_form
            ORDER BY player_id, kickoff_utc DESC
        """)
        form_rows = cur.fetchall()

    # Group form by player_id
    player_form: dict[str, list] = defaultdict(list)
    for row in form_rows:
        player_form[row["player_id"]].append(dict(row))

    # Score each player
    player_scores: dict[str, float] = {}
    decay = 0.92

    for player in players:
        pid = player["id"]
        matches = player_form.get(pid, [])

        if not matches:
            # No form data — assign a baseline based on position
            pos = player.get("position", "")
            player_scores[pid] = 2.0  # baseline
            continue

        total_score = 0.0
        total_weight = 0.0

        for i, match in enumerate(matches[:10]):
            prestige = get_prestige(match.get("competition_name", ""))
            match_score = score_player_match(match, prestige)
            weight = decay ** i
            total_score += match_score * weight
            total_weight += weight

        raw = total_score / total_weight if total_weight > 0 else 0.0
        # Shift to positive range (floor at 1.0)
        player_scores[pid] = max(1.0, raw + 5.0)

    # Normalize scores to 0-100 scale
    all_scores = list(player_scores.values())
    min_score = min(all_scores)
    max_score = max(all_scores)
    score_range = max_score - min_score if max_score > min_score else 1.0

    normalized: dict[str, float] = {
        pid: round(((s - min_score) / score_range) * 99 + 1, 2)
        for pid, s in player_scores.items()
    }

    # Print top 30 players
    player_lookup = {p["id"]: p for p in players}
    top_players = sorted(normalized.items(), key=lambda x: x[1], reverse=True)[:30]
    log.info("\nTop 30 Players:")
    log.info(f"{'Rank':<5} {'Name':<30} {'Team':<25} {'Pos':<12} {'Rating'}")
    log.info("-" * 85)
    for i, (pid, score) in enumerate(top_players):
        p = player_lookup.get(pid, {})
        log.info(f"{i+1:<5} {p.get('name',''):<30} {p.get('team_name',''):<25} "
                 f"{p.get('position',''):<12} {score:.1f}")

    # Build team squad ratings
    log.info("\nBuilding team squad ratings …")
    team_players: dict[str, list] = defaultdict(list)
    for player in players:
        pid = player["id"]
        team_players[player["team_id"]].append({
            "id": pid,
            "position": player.get("position", ""),
            "rating": normalized.get(pid, 50.0),
        })

    team_squad_ratings: dict[str, dict] = {}

    for team_id, squad in team_players.items():
        # Build best XI by position
        gks   = sorted([p for p in squad if p["position"] == "Goalkeeper"],
                       key=lambda x: x["rating"], reverse=True)
        defs  = sorted([p for p in squad if p["position"] == "Defender"],
                       key=lambda x: x["rating"], reverse=True)
        mids  = sorted([p for p in squad if p["position"] == "Midfielder"],
                       key=lambda x: x["rating"], reverse=True)
        fwds  = sorted([p for p in squad if p["position"] == "Forward"],
                       key=lambda x: x["rating"], reverse=True)

        # Typical 4-3-3 or best available
        xi = []
        xi += gks[:1]
        xi += defs[:4]
        xi += mids[:3]
        xi += fwds[:3]

        # Fill to 11 with next best remaining
        if len(xi) < 11:
            used = {p["id"] for p in xi}
            rest = sorted([p for p in squad if p["id"] not in used],
                          key=lambda x: x["rating"], reverse=True)
            xi += rest[:11 - len(xi)]

        if not xi:
            team_squad_ratings[team_id] = {
                "xi_avg": 50.0, "xi_attack": 50.0, "xi_defense": 50.0,
                "depth_avg": 50.0, "squad_rating": 50.0
            }
            continue

        xi_avg = sum(p["rating"] for p in xi) / len(xi)

        # Attack rating = avg of fwds + mids
        attack_players = [p for p in xi if p["position"] in ("Forward", "Midfielder")]
        xi_attack = (sum(p["rating"] for p in attack_players) / len(attack_players)
                     if attack_players else xi_avg)

        # Defense rating = avg of defenders + gk
        defense_players = [p for p in xi if p["position"] in ("Defender", "Goalkeeper")]
        xi_defense = (sum(p["rating"] for p in defense_players) / len(defense_players)
                      if defense_players else xi_avg)

        # Depth = avg of full squad (23 players)
        depth_avg = sum(p["rating"] for p in squad[:23]) / min(len(squad), 23)

        # Composite squad rating (weighted)
        squad_rating = xi_avg * 0.7 + depth_avg * 0.3

        team_squad_ratings[team_id] = {
            "xi_avg":       round(xi_avg, 2),
            "xi_attack":    round(xi_attack, 2),
            "xi_defense":   round(xi_defense, 2),
            "depth_avg":    round(depth_avg, 2),
            "squad_rating": round(squad_rating, 2),
        }

    # Print team squad ratings
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM teams WHERE group_name IS NOT NULL")
        team_names = {r["id"]: r["name"] for r in cur.fetchall()}

    sorted_teams = sorted(team_squad_ratings.items(),
                          key=lambda x: x[1]["squad_rating"], reverse=True)
    log.info(f"\n{'Rank':<5} {'Team':<25} {'Squad':<8} {'XI Avg':<8} {'Attack':<8} {'Defense':<8} {'Depth'}")
    log.info("-" * 70)
    for i, (tid, r) in enumerate(sorted_teams):
        log.info(f"{i+1:<5} {team_names.get(tid,''):<25} {r['squad_rating']:<8.1f} "
                 f"{r['xi_avg']:<8.1f} {r['xi_attack']:<8.1f} {r['xi_defense']:<8.1f} {r['depth_avg']:.1f}")

    if args.dry_run:
        log.info("\nDry run — not saving to DB")
        conn.close()
        return

    # Save player ratings to DB
    log.info("\nSaving player ratings …")
    with conn.cursor() as cur:
        for pid, rating in normalized.items():
            cur.execute(
                "UPDATE players SET player_rating = %s WHERE id = %s",
                (rating, pid)
            )

        # Save team squad ratings to a new table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS team_squad_ratings (
                team_id         TEXT PRIMARY KEY,
                xi_avg          NUMERIC(6,2),
                xi_attack       NUMERIC(6,2),
                xi_defense      NUMERIC(6,2),
                depth_avg       NUMERIC(6,2),
                squad_rating    NUMERIC(6,2),
                computed_at     TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        for tid, r in team_squad_ratings.items():
            cur.execute("""
                INSERT INTO team_squad_ratings
                    (team_id, xi_avg, xi_attack, xi_defense, depth_avg, squad_rating, computed_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (team_id) DO UPDATE SET
                    xi_avg       = EXCLUDED.xi_avg,
                    xi_attack    = EXCLUDED.xi_attack,
                    xi_defense   = EXCLUDED.xi_defense,
                    depth_avg    = EXCLUDED.depth_avg,
                    squad_rating = EXCLUDED.squad_rating,
                    computed_at  = NOW()
            """, (tid, r["xi_avg"], r["xi_attack"], r["xi_defense"],
                  r["depth_avg"], r["squad_rating"]))

        conn.commit()

    log.info(f"✅ Player ratings saved for {len(normalized)} players")
    log.info(f"✅ Squad ratings saved for {len(team_squad_ratings)} teams")
    conn.close()


if __name__ == "__main__":
    main()
