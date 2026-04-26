"""
WC2026 Flask Backend (proxy.py)
================================
Railway deployment. Serves all data to the Vercel frontend.

Endpoints:
  GET /api/groups                          → all 12 groups with teams + standings
  GET /api/teams                           → all 48 teams (lightweight)
  GET /api/teams/<team_id>                 → full team detail: profile + squad + form
  GET /api/power-rankings                  → ordered power ranking list
  GET /api/standings                       → all group standings
  GET /api/standings/<group>               → single group standings
  GET /api/matches/upcoming                → upcoming WC matches
  GET /api/matches/recent                  → recent completed matches
  GET /api/team/<team_id>/form             → last N matches for a team

  POST /api/admin/sync                     → trigger data loader (teams/squads/form)
  POST /api/admin/power-rankings           → upsert power ratings (body: [{id, rating, rank, tier}])

Deploy env vars (Railway):
  DATABASE_URL
  SPORTRADAR_KEY
  ADMIN_SECRET      (for admin endpoints)
"""

import os
import json
import logging
import subprocess
from decimal import Decimal
from datetime import datetime, timezone, date

import psycopg
from psycopg.rows import dict_row
from flask import Flask, jsonify, request, abort
from flask_cors import CORS

# ── Setup ─────────────────────────────────────────────────────────────────────

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-8s %(message)s")
log = logging.getLogger("wc2026")

DATABASE_URL  = os.environ["DATABASE_URL"]
ADMIN_SECRET  = os.environ.get("ADMIN_SECRET", "changeme")


# ── DB ────────────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def rows(cur) -> list[dict]:
    return [dict(r) for r in cur.fetchall()]


def row(cur) -> dict | None:
    r = cur.fetchone()
    return dict(r) if r else None


def serialize(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Not serialisable: {type(obj)}")


def jsonify_rows(data):
    return app.response_class(
        json.dumps(data, default=serialize),
        mimetype="application/json",
    )


# ── Auth helper ───────────────────────────────────────────────────────────────

def require_admin():
    secret = request.headers.get("X-Admin-Secret") or request.args.get("secret")
    if secret != ADMIN_SECRET:
        abort(403, "Forbidden")


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def health():
    return jsonify({"status": "ok", "service": "wc2026", "ts": datetime.now(timezone.utc).isoformat()})


# ── Groups ────────────────────────────────────────────────────────────────────

@app.route("/api/groups")
def get_groups():
    """All 12 groups with teams and current standings."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Teams by group
            cur.execute("""
                SELECT
                    t.id, t.name, t.short_name, t.abbreviation,
                    t.country, t.country_code, t.group_name,
                    t.jersey_primary, t.jersey_secondary,
                    t.manager_name, t.power_rating, t.power_rank, t.tier,
                    s.rank as standing_rank, s.played, s.wins, s.draws, s.losses,
                    s.goals_for, s.goals_against, s.goal_diff, s.points
                FROM teams t
                LEFT JOIN standings s
                    ON s.team_id = t.id
                WHERE t.group_name IS NOT NULL
                ORDER BY t.group_name, s.rank NULLS LAST, t.name
            """)
            team_rows = rows(cur)

        # Reshape into {A: {name, teams: [...]}, ...}
        groups: dict = {}
        for t in team_rows:
            g = t["group_name"]
            if g not in groups:
                groups[g] = {"name": g, "teams": []}
            groups[g]["teams"].append(t)

        # Sort groups A-L
        ordered = [groups[k] for k in sorted(groups.keys())]
        return jsonify_rows(ordered)
    finally:
        conn.close()


# ── Teams ─────────────────────────────────────────────────────────────────────

@app.route("/api/teams")
def get_teams():
    """Lightweight list of all 48 teams."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id, name, short_name, abbreviation, country, country_code,
                    group_name, jersey_primary, jersey_secondary,
                    manager_name, power_rating, power_rank, tier
                FROM teams
                ORDER BY group_name NULLS LAST, name
            """)
            return jsonify_rows(rows(cur))
    finally:
        conn.close()


@app.route("/api/teams/<path:team_id>")
def get_team_detail(team_id: str):
    """Full team detail: profile, squad, recent form, standings."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Team profile
            cur.execute("SELECT * FROM teams WHERE id = %s", (team_id,))
            team = row(cur)
            if not team:
                abort(404, "Team not found")

            # Squad by position with API-Football stats
            cur.execute("""
                SELECT p.id, p.name, p.date_of_birth, p.nationality, p.country_code,
                       p.position, p.shirt_number, p.height_cm, p.weight_kg, p.player_rating,
                       af.goals, af.assists, af.appearances, af.minutes, af.rating as af_rating,
                       af.shots_total, af.shots_on, af.passes_key, af.pass_accuracy,
                       af.tackles_total, af.interceptions, af.league_name as club_league
                FROM players p
                LEFT JOIN player_apifootball_stats af ON af.player_id = p.id
                WHERE p.team_id = %s
                ORDER BY
                    CASE p.position
                        WHEN 'Goalkeeper' THEN 1
                        WHEN 'Defender'   THEN 2
                        WHEN 'Midfielder' THEN 3
                        WHEN 'Forward'    THEN 4
                        ELSE 5
                    END,
                    p.shirt_number NULLS LAST
            """, (team_id,))
            squad = rows(cur)

            # Recent form (last 10 matches as home or away)
            cur.execute("""
                SELECT
                    m.id, m.kickoff_utc, m.status, m.stage, m.group_name,
                    m.home_score, m.away_score,
                    m.competition_name,
                    ht.name as home_team_name, ht.abbreviation as home_team_abbr,
                    ht.country_code as home_team_code,
                    at.name as away_team_name, at.abbreviation as away_team_abbr,
                    at.country_code as away_team_code,
                    m.home_team_id, m.away_team_id
                FROM matches m
                JOIN teams ht ON ht.id = m.home_team_id
                JOIN teams at ON at.id = m.away_team_id
                WHERE (m.home_team_id = %s OR m.away_team_id = %s)
                  AND m.status = 'closed'
                ORDER BY m.kickoff_utc DESC
                LIMIT 10
            """, (team_id, team_id))
            form_matches = rows(cur)

            # Add W/D/L result from this team's perspective
            for m in form_matches:
                is_home = m["home_team_id"] == team_id
                tf = m["home_score"] if is_home else m["away_score"]
                ta = m["away_score"] if is_home else m["home_score"]
                if tf is not None and ta is not None:
                    if tf > ta:
                        m["result"] = "W"
                    elif tf == ta:
                        m["result"] = "D"
                    else:
                        m["result"] = "L"
                    m["team_score"] = tf
                    m["opp_score"]  = ta
                else:
                    m["result"] = None

            # Standings row
            cur.execute("""
                SELECT rank, played, wins, draws, losses,
                       goals_for, goals_against, goal_diff, points
                FROM standings WHERE team_id = %s
            """, (team_id,))
            standing = row(cur)

        team["squad"]   = squad
        team["form"]    = form_matches
        team["standing"] = standing
        # Remove heavy raw_json from response
        team.pop("raw_json", None)

        return jsonify_rows(team)
    finally:
        conn.close()


# ── Power Rankings ────────────────────────────────────────────────────────────

@app.route("/api/power-rankings")
def get_power_rankings():
    """Ranked list of all teams for power rankings view."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    t.id, t.name, t.short_name, t.abbreviation, t.country_code, t.group_name,
                    t.jersey_primary, t.jersey_secondary,
                    t.power_rating, t.power_rank, t.tier,
                    pr.attack, pr.defense, pr.squad_rating,
                    pr.win_pct, pr.final_pct, pr.semi_pct, pr.qf_pct, pr.advance_pct,
                    pr.win_american, pr.final_american, pr.semi_american,
                    pr.qf_american, pr.advance_american,
                    pr.n_matches, pr.conf, pr.computed_at
                FROM teams t
                LEFT JOIN power_rankings pr ON pr.team_id = t.id
                WHERE t.power_rank IS NOT NULL
                ORDER BY t.power_rank
            """)
            return jsonify_rows(rows(cur))
    finally:
        conn.close()


# ── Player Detail ─────────────────────────────────────────────────────────────

@app.route("/api/players/<path:player_id>")
def get_player_detail(player_id: str):
    """Full player detail: profile + API-Football stats + recent form."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Base player info
            cur.execute("""
                SELECT p.*, t.name as team_name, t.group_name, t.country_code as team_country_code
                FROM players p
                JOIN teams t ON t.id = p.team_id
                WHERE p.id = %s
            """, (player_id,))
            player = row(cur)
            if not player:
                abort(404, "Player not found")
            player.pop("raw_json", None)

            # API-Football season stats
            cur.execute("""
                SELECT af_name, league_name, season, pos, age,
                       appearances, lineups, minutes,
                       goals, assists, shots_total, shots_on,
                       passes_total, passes_key, pass_accuracy,
                       dribbles_attempts, dribbles_success,
                       tackles_total, interceptions, duels_total, duels_won,
                       yellow_cards, red_cards, fouls_committed, fouls_drawn,
                       rating
                FROM player_apifootball_stats
                WHERE player_id = %s
            """, (player_id,))
            season_stats = row(cur)

            # Recent form (last 10 matches)
            cur.execute("""
                SELECT match_id, kickoff_utc, competition_name,
                       home_team_name, away_team_name,
                       home_score, away_score, starter,
                       goals_scored, assists, shots_on_target,
                       shots_off_target, shots_blocked,
                       yellow_cards, red_cards,
                       substituted_in, substituted_out
                FROM player_form
                WHERE player_id = %s
                ORDER BY kickoff_utc DESC
                LIMIT 10
            """, (player_id,))
            form = [dict(r) for r in cur.fetchall()]

        player["season_stats"] = season_stats
        player["form"] = form
        return jsonify_rows(player)
    finally:
        conn.close()


# ── Standings ─────────────────────────────────────────────────────────────────

@app.route("/api/standings")
def get_all_standings():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    s.group_name, s.rank, s.played, s.wins, s.draws, s.losses,
                    s.goals_for, s.goals_against, s.goal_diff, s.points,
                    t.id as team_id, t.name, t.abbreviation, t.country_code,
                    t.jersey_primary
                FROM standings s
                JOIN teams t ON t.id = s.team_id
                ORDER BY s.group_name, s.rank
            """)
            return jsonify_rows(rows(cur))
    finally:
        conn.close()


@app.route("/api/standings/<group>")
def get_group_standings(group: str):
    group = group.upper()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    s.rank, s.played, s.wins, s.draws, s.losses,
                    s.goals_for, s.goals_against, s.goal_diff, s.points,
                    t.id as team_id, t.name, t.abbreviation, t.country_code,
                    t.jersey_primary
                FROM standings s
                JOIN teams t ON t.id = s.team_id
                WHERE s.group_name = %s
                ORDER BY s.rank
            """, (group,))
            return jsonify_rows(rows(cur))
    finally:
        conn.close()


# ── Matches ───────────────────────────────────────────────────────────────────

@app.route("/api/matches/upcoming")
def get_upcoming_matches():
    limit = min(int(request.args.get("limit", 50)), 200)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    m.id, m.kickoff_utc, m.status, m.stage, m.group_name, m.round,
                    m.competition_name, m.venue_name, m.venue_city,
                    ht.id as home_team_id, ht.name as home_team, ht.abbreviation as home_abbr,
                    ht.country_code as home_code, ht.jersey_primary as home_color,
                    at.id as away_team_id, at.name as away_team, at.abbreviation as away_abbr,
                    at.country_code as away_code, at.jersey_primary as away_color
                FROM matches m
                JOIN teams ht ON ht.id = m.home_team_id
                JOIN teams at ON at.id = m.away_team_id
                WHERE m.status IN ('not_started', 'live')
                  AND m.kickoff_utc >= NOW()
                ORDER BY m.kickoff_utc
                LIMIT %s
            """, (limit,))
            return jsonify_rows(rows(cur))
    finally:
        conn.close()


@app.route("/api/matches/recent")
def get_recent_matches():
    limit = min(int(request.args.get("limit", 20)), 100)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    m.id, m.kickoff_utc, m.status, m.stage, m.group_name, m.round,
                    m.competition_name, m.venue_name, m.venue_city,
                    m.home_score, m.away_score,
                    ht.id as home_team_id, ht.name as home_team, ht.abbreviation as home_abbr,
                    ht.country_code as home_code, ht.jersey_primary as home_color,
                    at.id as away_team_id, at.name as away_team, at.abbreviation as away_abbr,
                    at.country_code as away_code, at.jersey_primary as away_color
                FROM matches m
                JOIN teams ht ON ht.id = m.home_team_id
                JOIN teams at ON at.id = m.away_team_id
                WHERE m.status = 'closed'
                ORDER BY m.kickoff_utc DESC
                LIMIT %s
            """, (limit,))
            return jsonify_rows(rows(cur))
    finally:
        conn.close()


@app.route("/api/team/<team_id>/form")
def get_team_form(team_id: str):
    limit = min(int(request.args.get("limit", 10)), 20)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    m.id, m.kickoff_utc, m.status, m.stage, m.competition_name,
                    m.home_score, m.away_score, m.home_team_id, m.away_team_id,
                    ht.name as home_team, ht.abbreviation as home_abbr, ht.country_code as home_code,
                    at.name as away_team, at.abbreviation as away_abbr, at.country_code as away_code
                FROM matches m
                JOIN teams ht ON ht.id = m.home_team_id
                JOIN teams at ON at.id = m.away_team_id
                WHERE (m.home_team_id = %s OR m.away_team_id = %s)
                  AND m.status = 'closed'
                ORDER BY m.kickoff_utc DESC
                LIMIT %s
            """, (team_id, team_id, limit))
            match_rows = rows(cur)

        for m in match_rows:
            is_home = m["home_team_id"] == team_id
            tf = m["home_score"] if is_home else m["away_score"]
            ta = m["away_score"] if is_home else m["home_score"]
            m["result"] = ("W" if tf > ta else "D" if tf == ta else "L") if (tf is not None and ta is not None) else None
            m["team_score"] = tf
            m["opp_score"]  = ta

        return jsonify_rows(match_rows)
    finally:
        conn.close()


# ── Admin ─────────────────────────────────────────────────────────────────────

@app.route("/api/players/<player_id>/form")
def get_player_form(player_id: str):
    limit = min(int(request.args.get("limit", 10)), 20)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    pf.match_id, pf.kickoff_utc, pf.competition_name,
                    pf.home_team_name, pf.away_team_name,
                    pf.home_score, pf.away_score, pf.starter,
                    pf.goals_scored, pf.assists, pf.shots_on_target,
                    pf.shots_off_target, pf.shots_blocked,
                    pf.yellow_cards, pf.red_cards, pf.substituted_in, pf.substituted_out
                FROM player_form pf
                WHERE pf.player_id = %s
                ORDER BY pf.kickoff_utc DESC
                LIMIT %s
            """, (player_id, limit))
            return jsonify_rows(rows(cur))
    finally:
        conn.close()


@app.route("/api/admin/sync", methods=["POST"])
def admin_sync():
    """Trigger load_data.py in the background. Pass ?job=teams|squads|form|standings|all."""
    require_admin()
    job = request.json.get("job", "all") if request.is_json else request.args.get("job", "all")
    valid_jobs = {"all", "teams", "squads", "form", "standings"}
    if job not in valid_jobs:
        abort(400, f"Invalid job. Must be one of: {valid_jobs}")

    flag = f"--{job}"
    try:
        proc = subprocess.Popen(
            ["python", "load_data.py", flag],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        stdout, _ = proc.communicate(timeout=300)
        return jsonify({
            "status": "ok" if proc.returncode == 0 else "error",
            "job": job,
            "returncode": proc.returncode,
            "output": stdout[-3000:],  # tail output
        })
    except subprocess.TimeoutExpired:
        proc.kill()
        return jsonify({"status": "timeout", "job": job}), 504
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/admin/power-rankings", methods=["POST"])
def admin_upsert_power_rankings():
    """
    Upsert power rankings.
    Body: [{"id": "sr:competitor:XXX", "rating": 87.5, "rank": 1, "tier": 1}, ...]
    """
    require_admin()
    data = request.get_json()
    if not isinstance(data, list):
        abort(400, "Expected JSON array")

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for entry in data:
                cur.execute("""
                    UPDATE teams SET
                        power_rating = %(rating)s,
                        power_rank   = %(rank)s,
                        tier         = %(tier)s
                    WHERE id = %(id)s
                """, entry)
            conn.commit()
        return jsonify({"status": "ok", "updated": len(data)})
    finally:
        conn.close()


@app.route("/api/admin/sync-log")
def admin_sync_log():
    require_admin()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM sync_log ORDER BY ran_at DESC LIMIT 50")
            return jsonify_rows(rows(cur))
    finally:
        conn.close()


# ── Error handlers ────────────────────────────────────────────────────────────

@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": str(e)}), 404


@app.errorhandler(403)
def forbidden(e):
    return jsonify({"error": str(e)}), 403


@app.errorhandler(400)
def bad_request(e):
    return jsonify({"error": str(e)}), 400


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
