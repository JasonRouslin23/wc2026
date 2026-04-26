from flask import Flask, request, jsonify, send_from_directory
import subprocess
import sys
import os
import csv
import math
import logging
import time
import uuid
import requests
from threading import Lock
from db import get_connection
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__, static_folder="parlay_ui")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from datetime import datetime, timezone, timedelta

EDT = timezone(timedelta(hours=-4))

def utc_to_edt(dt_str):
    """Convert UTC datetime string to EDT time string."""
    try:
        if isinstance(dt_str, str):
            dt = datetime.fromisoformat(dt_str.replace('Z','').replace(' ','T'))
        else:
            dt = dt_str
        dt_utc = dt.replace(tzinfo=timezone.utc)
        dt_edt = dt_utc.astimezone(EDT)
        return dt_edt.strftime("%I:%M %p EDT").lstrip("0")
    except:
        return ""

def get_time_slot(dt_str):
    """Return Morning/Brunch/Afternoon based on EDT time."""
    try:
        if isinstance(dt_str, str):
            dt = datetime.fromisoformat(dt_str.replace('Z','').replace(' ','T'))
        else:
            dt = dt_str
        dt_utc = dt.replace(tzinfo=timezone.utc)
        dt_edt = dt_utc.astimezone(EDT)
        hour = dt_edt.hour
        minute = dt_edt.minute
        total_mins = hour * 60 + minute
        if total_mins < 10 * 60:          return "Morning"
        elif total_mins < 14 * 60:        return "Brunch"
        else:                             return "Afternoon"
    except:
        return "All"

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
GAMBLYBOT_API_BASE_URL = os.getenv("GAMBLYBOT_API_BASE_URL", "").rstrip("/")
GAMBLYBOT_API_KEY = os.getenv("GAMBLYBOT_API_KEY", "")
GAMBLYBOT_CREATE_PATH = os.getenv("GAMBLYBOT_CREATE_PATH", "/v1/share-links")
GAMBLYBOT_STATUS_PATH = os.getenv("GAMBLYBOT_STATUS_PATH", "/v1/share-links/{request_id}")
GAMBLYBOT_REQUEST_TIMEOUT = float(os.getenv("GAMBLYBOT_REQUEST_TIMEOUT", "12"))
GAMBLYBOT_JOBS = {}
GAMBLYBOT_JOBS_LOCK = Lock()

REQUIRED_MATCH_COLUMNS = {
    "is_active": "BOOLEAN NOT NULL DEFAULT TRUE",
    "last_seen_at": "TIMESTAMPTZ",
    "api_updated_at": "TIMESTAMPTZ",
}

RESEARCH_MATCH_BASE_QUERY = """
WITH slate_matches AS (
    SELECT
        m.source_match_id,
        m.competition,
        m.match_datetime,
        m.home_team,
        m.away_team,
        m.status
    FROM matches m
    WHERE m.is_active = TRUE
      AND m.status IN ('TIMED', 'SCHEDULED')
      AND DATE(m.match_datetime AT TIME ZONE 'UTC') = %s
      AND (%s = '' OR m.competition = %s)
),
team_leagues AS (
    SELECT tpc.team, tpc.primary_competition
    FROM team_primary_competition tpc
),
match_context AS (
    SELECT
        sm.source_match_id,
        sm.competition,
        sm.match_datetime,
        sm.home_team,
        sm.away_team,
        sm.status,
        COALESCE(htl.primary_competition, sm.competition) AS home_rating_competition,
        COALESCE(atl.primary_competition, sm.competition) AS away_rating_competition
    FROM slate_matches sm
    LEFT JOIN team_leagues htl ON sm.home_team = htl.team
    LEFT JOIN team_leagues atl ON sm.away_team = atl.team
)
SELECT
    mc.source_match_id, mc.home_team, mc.away_team, mc.competition,
    mc.match_datetime, mc.status,
    ROUND(lb_home.avg_home_goals * h.attack_strength_adj * a.defense_strength_adj, 2) AS home_xg,
    ROUND(lb_away.avg_away_goals * a.attack_strength_adj * h.defense_strength_adj, 2) AS away_xg,
    h.attack_strength_adj AS home_atk, h.defense_strength_adj AS home_def,
    a.attack_strength_adj AS away_atk, a.defense_strength_adj AS away_def,
    lb_home.avg_home_goals + lb_home.avg_away_goals AS league_avg_total
FROM match_context mc
JOIN team_strength_compressed h
    ON mc.home_team = h.team
   AND mc.home_rating_competition = h.competition
JOIN team_strength_compressed a
    ON mc.away_team = a.team
   AND mc.away_rating_competition = a.competition
JOIN league_baselines lb_home
    ON mc.home_rating_competition = lb_home.competition
JOIN league_baselines lb_away
    ON mc.away_rating_competition = lb_away.competition
ORDER BY mc.competition, mc.match_datetime
"""


def ensure_matches_schema():
    """Safely add required `matches` columns if they do not exist."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        for col, col_type in REQUIRED_MATCH_COLUMNS.items():
            cur.execute("""
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'matches'
                  AND column_name = %s
            """, (col,))
            exists = cur.fetchone() is not None
            if not exists:
                cur.execute(f"ALTER TABLE matches ADD COLUMN IF NOT EXISTS {col} {col_type}")
                logger.info("Added missing matches.%s column", col)
        conn.commit()
    finally:
        cur.close()
        conn.close()


try:
    ensure_matches_schema()
except Exception:
    logger.exception("Startup schema check failed")


def run_script(cmd, cwd=PROJECT_DIR):
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    return result.stdout, result.stderr, result.returncode


def decimal_to_american(d):
    d = float(d)
    if d >= 2:
        return f"+{int(round((d - 1) * 100))}"
    if d > 1:
        return str(int(round(-100 / (d - 1))))
    return "N/A"


def load_pool(date):
    path = os.path.join(PROJECT_DIR, f"cheatsheets/model_parlay_candidate_pool_{date}.csv")
    if not os.path.exists(path):
        return None, path
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return rows, path


def filter_by_league(rows, leagues):
    if not leagues:
        return rows
    if isinstance(leagues, str):
        leagues = [leagues]
    leagues = [l for l in leagues if l and l != "All Leagues"]
    if not leagues:
        return rows
    return [r for r in rows if r.get("competition", "").strip() in leagues]


def get_game_time_for_match(match_key):
    """Look up EDT game time from matches table for a given match_key."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        parts = match_key.split(" | ")
        if len(parts) >= 2:
            teams = parts[1].split(" vs ")
            if len(teams) == 2:
                home = teams[0].strip()
                away = teams[1].strip()
                cur.execute("""
                    SELECT match_datetime FROM matches
                    WHERE LOWER(home_team) = LOWER(%s) AND LOWER(away_team) = LOWER(%s)
                      AND is_active = TRUE
                      AND status IN ('TIMED', 'SCHEDULED')
                    LIMIT 1
                """, (home, away))
                row = cur.fetchone()
                if row:
                    cur.close()
                    conn.close()
                    return utc_to_edt(str(row[0]))
        cur.close()
        conn.close()
    except:
        pass
    return ""


def build_csv_output(legs):
    lines = ["CSV_START", "home_team,away_team,market_type,bet_name,model_prob,fair_decimal_odds,combined_l5,combined_l10,combined_l20,match_key,game_time"]
    for row in legs:
        mk = row.get("match_key", "").replace(",", " ")
        game_time = get_game_time_for_match(row.get("match_key",""))
        lines.append(
            f"{row.get('home_team','')},{row.get('away_team','')},{row.get('market_type','')},"
            f"{row.get('bet_name','')},{float(row.get('model_prob', 0)):.6f},{float(row.get('fair_decimal_odds', 1)):.4f},"
            f"{row.get('combined_l5','')},{row.get('combined_l10','')},{row.get('combined_l20','')},{mk},{game_time}"
        )
    lines.append("CSV_END")
    return "\n".join(lines)


def build_summary(legs):
    combined = 1.0
    for row in legs:
        combined *= float(row.get("fair_decimal_odds", 1))
    return {
        "fair_american": decimal_to_american(combined),
        "valid_count": str(len(legs)),
        "pool_size": str(len(legs)),
    }


def pick_top_legs(rows, n, exclude_matches=None):
    """Pick top n legs by model_prob, one per match, excluding already used matches."""
    seen = set(exclude_matches or [])
    result = []
    for row in sorted(rows, key=lambda r: float(r.get("model_prob", 0)), reverse=True):
        mk = row.get("match_key", "")
        if mk not in seen:
            seen.add(mk)
            result.append(row)
        if len(result) >= n:
            break
    return result


def _gambly_headers():
    headers = {"Content-Type": "application/json"}
    if GAMBLYBOT_API_KEY:
        headers["Authorization"] = f"Bearer {GAMBLYBOT_API_KEY}"
    return headers


def _join_gambly_url(path):
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return f"{GAMBLYBOT_API_BASE_URL}{path}"


def _extract_value(payload, keys):
    for key in keys:
        if key in payload and payload.get(key):
            return payload.get(key)
    data = payload.get("data")
    if isinstance(data, dict):
        for key in keys:
            if key in data and data.get(key):
                return data.get(key)
    return None


def _parse_gambly_response(payload):
    share_url = _extract_value(payload, ["share_url", "shareUrl", "url", "link"])
    request_id = _extract_value(payload, ["request_id", "requestId", "id", "job_id", "jobId"])
    status = str(_extract_value(payload, ["status", "state"]) or "").lower()
    error = _extract_value(payload, ["error", "message"])
    if share_url:
        return {"status": "ready", "share_url": share_url, "request_id": request_id}
    if status in {"ready", "complete", "completed", "success", "succeeded"}:
        return {"status": "ready", "share_url": share_url, "request_id": request_id}
    if status in {"failed", "error", "cancelled"}:
        return {"status": "failed", "error": error or "GamblyBot request failed.", "request_id": request_id}
    return {"status": "pending", "request_id": request_id}


def _poll_gambly_status(external_request_id):
    if not external_request_id:
        return {"status": "pending"}
    status_path = GAMBLYBOT_STATUS_PATH.format(request_id=external_request_id)
    status_url = _join_gambly_url(status_path)
    resp = requests.get(status_url, headers=_gambly_headers(), timeout=GAMBLYBOT_REQUEST_TIMEOUT)
    resp.raise_for_status()
    payload = resp.json()
    return _parse_gambly_response(payload)


@app.route("/")
def index():
    response = send_from_directory("parlay_ui", "index.html")
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    return response


@app.route("/api/available-leagues", methods=["POST"])
def available_leagues():
    date = request.json.get("date")
    if not date:
        return jsonify({"leagues": []})
    rows, _ = load_pool(date)
    if not rows:
        return jsonify({"leagues": []})
    leagues = sorted(set(r.get("competition", "").strip() for r in rows if r.get("competition", "").strip()))
    return jsonify({"leagues": leagues})


@app.route("/api/gamblybot/create", methods=["POST"])
def create_gamblybot_link():
    if not GAMBLYBOT_API_BASE_URL:
        return jsonify({"error": "GamblyBot is not configured on this server."}), 500

    data = request.json or {}
    legs = data.get("legs") or []
    if not legs:
        return jsonify({"error": "At least one parlay leg is required."}), 400

    payload = {
        "sport": "soccer",
        "source": "soccer-model",
        "legs": legs,
    }
    create_url = _join_gambly_url(GAMBLYBOT_CREATE_PATH)

    try:
        response = requests.post(
            create_url,
            headers=_gambly_headers(),
            json=payload,
            timeout=GAMBLYBOT_REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        parsed = _parse_gambly_response(response.json())
    except Exception as exc:
        logger.exception("GamblyBot create failed")
        return jsonify({"error": f"Unable to create GamblyBot link: {exc}"}), 502

    job_id = str(uuid.uuid4())
    job = {
        "status": parsed.get("status", "pending"),
        "share_url": parsed.get("share_url"),
        "external_request_id": parsed.get("request_id"),
        "error": parsed.get("error"),
        "updated_at": time.time(),
    }
    with GAMBLYBOT_JOBS_LOCK:
        GAMBLYBOT_JOBS[job_id] = job

    return jsonify({
        "job_id": job_id,
        "status": job["status"],
        "share_url": job.get("share_url"),
    })


@app.route("/api/gamblybot/status/<job_id>", methods=["GET"])
def gamblybot_status(job_id):
    with GAMBLYBOT_JOBS_LOCK:
        job = GAMBLYBOT_JOBS.get(job_id)
    if not job:
        return jsonify({"error": "Unknown GamblyBot job."}), 404

    if job.get("status") == "ready":
        return jsonify({"status": "ready", "share_url": job.get("share_url")})
    if job.get("status") == "failed":
        return jsonify({"status": "failed", "error": job.get("error", "GamblyBot request failed.")})

    try:
        parsed = _poll_gambly_status(job.get("external_request_id"))
    except Exception as exc:
        logger.exception("GamblyBot status poll failed")
        return jsonify({"status": "pending", "warning": f"Polling delayed: {exc}"})

    job["status"] = parsed.get("status", "pending")
    job["share_url"] = parsed.get("share_url") or job.get("share_url")
    job["external_request_id"] = parsed.get("request_id") or job.get("external_request_id")
    job["error"] = parsed.get("error")
    job["updated_at"] = time.time()
    with GAMBLYBOT_JOBS_LOCK:
        GAMBLYBOT_JOBS[job_id] = job

    if job["status"] == "ready":
        return jsonify({"status": "ready", "share_url": job.get("share_url")})
    if job["status"] == "failed":
        return jsonify({"status": "failed", "error": job.get("error", "GamblyBot request failed.")})
    return jsonify({"status": "pending"})


@app.route("/api/run-parlay", methods=["POST"])
def run_parlay():
    data          = request.json
    date          = data.get("date")
    legs          = int(data.get("legs", 8))
    min_odds      = int(data.get("min_odds", 1000))
    max_odds      = int(data.get("max_odds", 100000))
    max_candidates = int(data.get("max_candidates", 40))
    mode          = data.get("mode", "mix")
    bet_filter    = data.get("bet_filter", None)
    league        = data.get("league", [])  # can be a list of leagues or empty
    custom_mix    = data.get("custom_mix", [])  # list of {bet, pct, min_prob}

    if not date:
        return jsonify({"error": "Date is required"}), 400

    # build cheatsheet if needed
    cheatsheet_path = os.path.join(PROJECT_DIR, f"cheatsheets/cheatsheet_{date}.csv")
    if not os.path.exists(cheatsheet_path):
        stdout, stderr, code = run_script([sys.executable, "build_cheatsheet.py", date])
        if code != 0:
            return jsonify({"error": f"build_cheatsheet failed: {stderr}"}), 500

    run_script([sys.executable, "generate_market_sheets.py", date])

    stdout, stderr, code = run_script([sys.executable, "generate_model_parlay_candidate_pool.py", date])
    if code != 0:
        return jsonify({"error": f"candidate pool failed: {stderr}"}), 500

    pool, pool_path = load_pool(date)
    if pool is None:
        return jsonify({"error": f"Candidate pool not found: {pool_path}"}), 500

    # apply league filter
    pool = filter_by_league(pool, league)
    if not pool:
        return jsonify({"error": f"No matches found for league: {league}"}), 200

    # ── BET FILTER MODE ──────────────────────────────────────────────────────
    if mode == "filter" and bet_filter:
        matched = []
        for row in pool:
            selection = (row.get("bet_name") or "").strip()
            market    = (row.get("market_type") or "").strip().lower()
            hit = False
            if bet_filter == "correct_score":
                hit = market == "correct_score"
            elif bet_filter == "corners":
                hit = market == "corners"
            elif bet_filter.startswith("Over") and "Corner" in bet_filter:
                hit = market == "corners" and selection.strip().lower() == bet_filter.strip().lower()
            elif bet_filter.startswith("Over") and "Shot" in bet_filter and "SOT" not in bet_filter:
                hit = market == "shots" and selection.strip().lower() == bet_filter.strip().lower()
            elif bet_filter.startswith("Over") and "SOT" in bet_filter:
                hit = market == "shots_on_target" and selection.strip().lower() == bet_filter.strip().lower()
            elif bet_filter == "Home Win":
                hit = market == "moneyline" and selection == row.get("home_team", "").strip()
            elif bet_filter == "Away Win":
                hit = market == "moneyline" and selection == row.get("away_team", "").strip()
            elif bet_filter == "or Draw":
                hit = selection.lower().endswith("or draw")
            elif bet_filter == "No Draw":
                hit = selection.strip().lower() == "no draw"
            else:
                hit = selection.strip().lower() == bet_filter.strip().lower()
            if hit:
                matched.append(row)

        if not matched:
            return jsonify({"error": f"No bets found matching '{bet_filter}' for {date}"}), 200

        unique = pick_top_legs(matched, legs)
        if len(unique) < legs:
            return jsonify({"error": f"Only found {len(unique)} matching bets — try fewer legs."}), 200

        return jsonify({"summary": build_summary(unique), "raw_output": build_csv_output(unique)})

    # ── CUSTOM MIX MODE ──────────────────────────────────────────────────────
    if mode == "custom" and custom_mix:
        import math as _math

        def row_matches_bet(row, bet, min_prob=0):
            selection = (row.get("bet_name") or "").strip()
            market    = (row.get("market_type") or "").strip().lower()
            prob      = float(row.get("model_prob", 0))
            if prob < min_prob:
                return False
            if bet == "correct_score":
                return market == "correct_score"
            elif bet == "corners":
                return market == "corners"
            elif bet.startswith("Over") and "Corner" in bet:
                return market == "corners" and selection.strip().lower() == bet.strip().lower()
            elif bet == "Home Win":
                return market == "moneyline" and selection == row.get("home_team", "").strip()
            elif bet == "Away Win":
                return market == "moneyline" and selection == row.get("away_team", "").strip()
            elif bet == "or Draw":
                return selection.lower().endswith("or draw")
            elif bet == "No Draw":
                return selection.strip().lower() == "no draw"
            else:
                return selection.strip().lower() == bet.strip().lower()

        def optimal_assignment_dp(match_keys, match_bucket_rows, bucket_targets):
            """
            Dynamic programming optimal assignment.
            Finds the assignment of matches to buckets that maximizes
            combined probability (product), respecting bucket size targets.
            Runs in O(n_matches * states) time — fast even for large slates.
            """
            n_buckets = len(bucket_targets)
            n_matches = len(match_keys)
            total_needed = sum(bucket_targets)
            initial_state = tuple([0] * n_buckets)
            dp = {initial_state: (0.0, [])}

            for i, mk in enumerate(match_keys):
                new_dp = {}
                rows = match_bucket_rows[mk]
                matches_left = n_matches - i - 1

                for state, (log_prob, path) in dp.items():
                    assigned_so_far = sum(state)
                    still_needed = total_needed - assigned_so_far

                    # option: skip this match
                    if matches_left >= still_needed:
                        if state not in new_dp or new_dp[state][0] < log_prob:
                            new_dp[state] = (log_prob, path)

                    # option: assign to bucket bi
                    for bi in range(n_buckets):
                        if state[bi] >= bucket_targets[bi]:
                            continue
                        row = rows[bi]
                        if row is None:
                            continue
                        p = float(row.get("model_prob", 0))
                        if p <= 0:
                            continue
                        new_state = list(state)
                        new_state[bi] += 1
                        new_state = tuple(new_state)
                        new_log_prob = log_prob + _math.log(p)
                        if new_state not in new_dp or new_dp[new_state][0] < new_log_prob:
                            new_dp[new_state] = (new_log_prob, path + [(mk, bi)])

                dp = new_dp

            target_state = tuple(bucket_targets)
            if target_state not in dp:
                return None
            _, assignment = dp[target_state]
            return assignment

        # build bucket specs
        bucket_specs = []
        for bucket in custom_mix:
            bet      = bucket.get("bet", "")
            pct      = float(bucket.get("pct", 0)) / 100
            min_prob = float(bucket.get("min_prob", 0)) / 100
            n_legs   = max(1, round(legs * pct))
            bucket_specs.append({"bet": bet, "n_legs": n_legs, "min_prob": min_prob})

        bucket_targets = [s["n_legs"] for s in bucket_specs]

        # get unique matches and best eligible row per match per bucket
        match_keys = list(dict.fromkeys(r.get("match_key", "") for r in pool))
        match_bucket_rows = {}
        for mk in match_keys:
            match_rows = [r for r in pool if r.get("match_key", "") == mk]
            per_bucket = []
            for spec in bucket_specs:
                eligible = [r for r in match_rows if row_matches_bet(r, spec["bet"], spec["min_prob"])]
                best = max(eligible, key=lambda r: float(r.get("model_prob", 0))) if eligible else None
                per_bucket.append(best)
            match_bucket_rows[mk] = per_bucket

        # filter to matches with at least one eligible bucket
        eligible_matches = [mk for mk in match_keys if any(match_bucket_rows[mk])]

        # run DP to find optimal assignment
        best_assignment = optimal_assignment_dp(eligible_matches, match_bucket_rows, bucket_targets)

        if not best_assignment:
            return jsonify({"error": "Could not find valid assignment for buckets. Try adjusting percentages or min probs."}), 200

        # build final legs
        final_legs = []
        used_matches = set()
        for mk, bi in best_assignment:
            row = match_bucket_rows[mk][bi]
            if row:
                final_legs.append(row)
                used_matches.add(mk)

        # fill remainder with best available
        remaining = legs - len(final_legs)
        if remaining > 0:
            remainder = pick_top_legs(pool, remaining, exclude_matches=used_matches)
            final_legs.extend(remainder)

        # sort by prob descending for display
        final_legs = sorted(final_legs, key=lambda r: float(r.get("model_prob", 0)), reverse=True)

        if not final_legs:
            return jsonify({"error": "No legs found for custom mix — try adjusting your buckets."}), 200

        return jsonify({"summary": build_summary(final_legs), "raw_output": build_csv_output(final_legs)})

    # ── MARKET MIX MODE ──────────────────────────────────────────────────────
    # write filtered pool to a temp file if league filtered
    # filter by time slot
    time_slot = data.get("time_slot", "All")
    if time_slot and time_slot != "All":
        try:
            conn_ts = get_connection()
            cur_ts = conn_ts.cursor()
            filtered_keys = []
            for row in pool:
                mk = row.get("match_key", "")
                parts = mk.split(" | ")
                if len(parts) >= 2:
                    teams = parts[1].split(" vs ")
                    if len(teams) == 2:
                        cur_ts.execute("""
                            SELECT match_datetime FROM matches
                            WHERE LOWER(home_team) = LOWER(%s) AND LOWER(away_team) = LOWER(%s)
                              AND is_active = TRUE
                              AND status IN ('TIMED', 'SCHEDULED')
                            LIMIT 1
                        """, (teams[0].strip(), teams[1].strip()))
                        r = cur_ts.fetchone()
                        if r and get_time_slot(str(r[0])) == time_slot:
                            filtered_keys.append(mk)
            cur_ts.close()
            conn_ts.close()
            pool = [r for r in pool if r.get("match_key","") in filtered_keys]
        except Exception as e:
            print(f"Time slot filter error: {e}")

    active_leagues = [l for l in (league if isinstance(league, list) else [league]) if l and l != "All Leagues"]
    if active_leagues:
        import tempfile
        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, dir=os.path.join(PROJECT_DIR, "cheatsheets"), prefix="tmp_pool_")
        writer = csv.DictWriter(tmp, fieldnames=pool[0].keys())
        writer.writeheader()
        writer.writerows(pool)
        tmp.close()
        pool_file = tmp.name
    else:
        pool_file = pool_path

    min_prob_ml       = float(data.get("min_prob_ml", 0.65))
    min_prob_dc       = float(data.get("min_prob_dc", 0.72))
    min_prob_btts     = float(data.get("min_prob_btts", 0.60))
    min_prob_totals   = float(data.get("min_prob_totals", 0.85))
    min_prob_corners  = float(data.get("min_prob_corners", 0.55))
    max_legs_ml       = int(data.get("max_legs_ml", 3))
    max_legs_dc       = int(data.get("max_legs_dc", 4))
    max_legs_btts     = int(data.get("max_legs_btts", 4))
    max_legs_totals   = int(data.get("max_legs_totals", 6))
    max_legs_corners  = int(data.get("max_legs_corners", 2))
    min_prob_shots    = float(data.get("min_prob_shots", 0.55))
    max_legs_shots    = int(data.get("max_legs_shots", 2))
    min_prob_sot      = float(data.get("min_prob_sot", 0.55))
    max_legs_sot      = int(data.get("max_legs_sot", 2))

    stdout, stderr, code = run_script([
        sys.executable, "optimize_model_parlay.py",
        "--date", date,
        "--legs", str(legs),
        "--min-odds", f"+{min_odds}",
        "--max-odds", f"+{max_odds}",
        "--max-candidates", str(max_candidates),
        "--min-prob-moneyline",     str(min_prob_ml),
        "--min-prob-double-chance", str(min_prob_dc),
        "--min-prob-btts",          str(min_prob_btts),
        "--min-prob-totals",        str(min_prob_totals),
        "--min-prob-corners",       str(min_prob_corners),
        "--max-legs-moneyline",     str(max_legs_ml),
        "--max-legs-double-chance", str(max_legs_dc),
        "--max-legs-btts",          str(max_legs_btts),
        "--max-legs-totals",        str(max_legs_totals),
        "--max-legs-corners",       str(max_legs_corners),
        "--min-prob-shots",         str(min_prob_shots),
        "--max-legs-shots",         str(max_legs_shots),
        "--min-prob-sot",           str(min_prob_sot),
        "--max-legs-sot",           str(max_legs_sot),
    ])

    # clean up temp file
    if league and league != "All Leagues" and os.path.exists(pool_file):
        os.unlink(pool_file)

    if code != 0:
        return jsonify({"error": f"optimizer failed: {stderr}"}), 500

    summary = {}
    for line in stdout.split("\n"):
        line = line.strip()
        if "Best parlay fair American odds:" in line:
            summary["fair_american"] = line.split(":")[-1].strip()
        elif "Valid parlays found:" in line:
            summary["valid_count"] = line.split(":")[-1].strip()
        elif "Candidate pool after market filters:" in line:
            summary["pool_size"] = line.split(":")[1].strip().split()[0]
        elif "No valid parlay found" in line:
            return jsonify({"error": "No valid parlay found. Try widening your odds range or reducing legs."}), 200

    return jsonify({"summary": summary, "raw_output": stdout})


@app.route("/api/match-bets", methods=["POST"])
def match_bets():
    """Return all available bets for a specific match from the candidate pool."""
    data = request.json
    date = data.get("date")
    match_key = data.get("match_key")

    if not date or not match_key:
        return jsonify({"bets": []}), 400

    pool, _ = load_pool(date)
    if not pool:
        return jsonify({"bets": []}), 200

    # find all bets for this match
    bets = []
    seen = set()
    for row in pool:
        if row.get("match_key", "").strip() == match_key.strip():
            bet_name = row.get("bet_name", "").strip()
            market   = row.get("market_type", "").strip()
            key      = f"{market}|{bet_name}"
            if key not in seen:
                seen.add(key)
                bets.append({
                    "bet_name":         bet_name,
                    "market_type":      market,
                    "model_prob":       float(row.get("model_prob", 0)),
                    "fair_decimal_odds": float(row.get("fair_decimal_odds", 1)),
                    "combined_l5":      row.get("combined_l5", ""),
                    "combined_l10":     row.get("combined_l10", ""),
                    "combined_l20":     row.get("combined_l20", ""),
                })

    # sort by prob descending
    bets.sort(key=lambda b: b["model_prob"], reverse=True)
    return jsonify({"bets": bets})


@app.route("/api/team-stats", methods=["POST"])
def team_stats():
    """Return full mini card data for a team."""
    data = request.json
    team       = data.get("team")
    market_type = data.get("market_type", "")
    bet_name   = data.get("bet_name", "")
    date       = data.get("date", "")

    if not team:
        return jsonify({"error": "Team required"}), 400

    try:
        conn = get_connection()
        cur  = conn.cursor()

        # ── recent results (last 8) ──────────────────────────────────────────
        cur.execute("""
            SELECT home_team, away_team, home_score_ft, away_score_ft,
                   match_datetime, competition
            FROM matches
            WHERE (home_team = %s OR away_team = %s)
              AND home_score_ft IS NOT NULL
              AND status = 'FINISHED'
            ORDER BY match_datetime DESC
            LIMIT 8
        """, (team, team))

        rows = cur.fetchall()
        results = []
        form    = []

        for row in rows:
            home, away, hs, aws, dt, comp = row
            is_home = (home == team)
            gf = hs if is_home else aws
            ga = aws if is_home else hs
            opp = away if is_home else home
            venue = "vs" if is_home else "@"

            if gf > ga:   wdl = "W"
            elif gf == ga: wdl = "D"
            else:          wdl = "L"

            form.append(wdl)
            results.append({
                "opponent":    opp,
                "venue":       venue,
                "score":       f"{gf}-{ga}",
                "wdl":         wdl,
                "date":        str(dt)[:10],
                "competition": comp,
            })

        # ── team strength ratings ────────────────────────────────────────────
        cur.execute("""
            SELECT t.attack_strength_adj, t.defense_strength_adj, t.competition,
                   lb.avg_home_goals, lb.avg_away_goals
            FROM team_strength_compressed t
            JOIN league_baselines lb ON t.competition = lb.competition
            WHERE t.team = %s
            LIMIT 1
        """, (team,))

        rating_row = cur.fetchone()
        ratings = {}
        if rating_row:
            atk, dfs, comp, avg_hg, avg_ag = rating_row
            ratings = {
                "attack":  round(float(atk), 3),
                "defense": round(float(dfs), 3),
                "competition": comp,
            }
            # grade attack and defense
            def grade(val, reverse=False):
                thresholds = [(1.3,"A+"),(1.2,"A"),(1.1,"A-"),(1.0,"B+"),(0.9,"B"),(0.8,"B-"),(0.7,"C+")]
                if reverse:
                    thresholds = [(0.7,"A+"),(0.8,"A"),(0.9,"A-"),(1.0,"B+"),(1.1,"B"),(1.2,"B-"),(1.3,"C+")]
                for threshold, g in thresholds:
                    if (val >= threshold and not reverse) or (val <= threshold and reverse):
                        return g
                return "C"
            ratings["attack_grade"]  = grade(float(atk))
            ratings["defense_grade"] = grade(float(dfs), reverse=True)

        # ── hit rate for the specific bet ────────────────────────────────────
        hit_rate = {}
        if market_type and bet_name:
            def calc_hit_rate(n):
                cur.execute("""
                    SELECT home_team, away_team, home_score_ft, away_score_ft
                    FROM matches
                    WHERE (home_team = %s OR away_team = %s)
                      AND home_score_ft IS NOT NULL
                      AND status = 'FINISHED'
                    ORDER BY match_datetime DESC
                    LIMIT %s
                """, (team, team, n))
                recent = cur.fetchall()
                if not recent: return None, 0

                hits = 0
                for h, a, hs, aws in recent:
                    total = hs + aws
                    hit = None
                    bn = bet_name.lower()
                    if "over 1.5" in bn:  hit = total > 1
                    elif "over 2.5" in bn: hit = total > 2
                    elif "over 3.5" in bn: hit = total > 3
                    elif "under 4.5" in bn: hit = total <= 4
                    elif "under 3.5" in bn: hit = total <= 3
                    elif "btts" in bn and "yes" in bn: hit = hs > 0 and aws > 0
                    elif "btts" in bn and "no" in bn:  hit = not (hs > 0 and aws > 0)
                    elif "or draw" in bn:
                        is_home = (h == team)
                        if is_home: hit = hs >= aws
                        else: hit = aws >= hs
                    elif "no draw" in bn: hit = hs != aws
                    elif market_type == "moneyline":
                        is_home = (h == team)
                        if team in bn:
                            if is_home: hit = hs > aws
                            else: hit = aws > hs
                        elif "draw" in bn: hit = hs == aws
                    if hit: hits += 1
                return f"{hits}/{len(recent)}", len(recent)

            l5, _  = calc_hit_rate(5)
            l10, _ = calc_hit_rate(10)
            l20, _ = calc_hit_rate(20)
            hit_rate = {"l5": l5, "l10": l10, "l20": l20}

        cur.close()
        conn.close()

        return jsonify({
            "team":     team,
            "results":  results,
            "form":     form[:8],
            "ratings":  ratings,
            "hit_rate": hit_rate,
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/match-preview", methods=["POST"])
def match_preview():
    """Return full match prediction data for a specific match."""
    data = request.json
    date      = data.get("date")
    match_key = data.get("match_key")

    if not date or not match_key:
        return jsonify({"error": "date and match_key required"}), 400

    pool, _ = load_pool(date)
    if not pool:
        return jsonify({"error": "No pool found"}), 200

    # get all rows for this match
    rows = [r for r in pool if r.get("match_key", "").strip() == match_key.strip()]
    if not rows:
        return jsonify({"error": "Match not found in pool"}), 200

    # build market prob map
    markets = {}
    home_team = rows[0].get("home_team", "")
    away_team = rows[0].get("away_team", "")
    competition = rows[0].get("competition", "")

    for row in rows:
        market = row.get("market_type", "").strip()
        bet    = row.get("bet_name",    "").strip()
        prob   = float(row.get("model_prob", 0))
        key    = f"{market}|{bet}"
        markets[key] = {"prob": prob, "fair_dec": float(row.get("fair_decimal_odds", 1)),
                        "combined_l5": row.get("combined_l5",""),
                        "combined_l10": row.get("combined_l10",""),
                        "combined_l20": row.get("combined_l20","")}

    def get_prob(market, bet):
        return markets.get(f"{market}|{bet}", {}).get("prob", None)

    def get_odds(market, bet):
        dec = markets.get(f"{market}|{bet}", {}).get("fair_dec", None)
        if not dec or dec <= 1: return None
        if dec >= 2: return f"+{int(round((dec-1)*100))}"
        return str(int(round(-100/(dec-1))))

    def get_hr(market, bet):
        row = markets.get(f"{market}|{bet}", {})
        return {"l5": row.get("combined_l5",""), "l10": row.get("combined_l10",""), "l20": row.get("combined_l20","")}

    # get projected xG from DB
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            SELECT 
                ROUND(lb_home.avg_home_goals * h.attack_strength_adj * a.defense_strength_adj, 2) AS home_xg,
                ROUND(lb_away.avg_away_goals * a.attack_strength_adj * h.defense_strength_adj, 2) AS away_xg
            FROM team_strength_compressed h
            JOIN team_strength_compressed a ON a.team = %s AND a.competition = h.competition
            JOIN league_baselines lb_home ON h.competition = lb_home.competition
            JOIN league_baselines lb_away ON h.competition = lb_away.competition
            WHERE h.team = %s
            LIMIT 1
        """, (away_team, home_team))
        xg_row = cur.fetchone()
        home_xg = float(xg_row[0]) if xg_row else None
        away_xg = float(xg_row[1]) if xg_row else None
        cur.close()
        conn.close()
    except:
        home_xg = away_xg = None

    # get corners projected from corners sheet
    corners_l = [r for r in rows if r.get("market_type","") == "corners"]
    proj_corners = None
    if corners_l:
        # find the line where prob crosses 50%
        over_lines = sorted(
            [(float(r.get("model_prob",0)), r.get("bet_name","")) for r in corners_l if "Over" in r.get("bet_name","")],
            key=lambda x: x[1]
        )
        if over_lines:
            for prob, bet in over_lines:
                line = bet.replace("Over","").replace("Corners","").strip()
                try:
                    if float(prob) > 0.5:
                        proj_corners = line
                        break
                except: pass

    key_markets = []
    for market, bet in [
        ("totals","Over 1.5"), ("totals","Over 2.5"), ("totals","Over 3.5"),
        ("totals","Under 4.5"), ("btts","Yes"), ("btts","No")
    ]:
        prob = get_prob(market, bet)
        if prob:
            key_markets.append({
                "label": bet, "market": market, "bet_name": bet,
                "prob":  round(prob, 4),
                "odds":  get_odds(market, bet),
                "hit_rates": get_hr(market, bet),
                "l10": get_hr(market, bet).get("l10","")
            })

    corners_markets = []
    for line in ["Over 7.5 Corners","Over 8.5 Corners","Over 9.5 Corners",
                 "Over 10.5 Corners","Over 11.5 Corners","Over 12.5 Corners"]:
        prob = get_prob("corners", line)
        if prob:
            corners_markets.append({
                "label": line.replace(" Corners",""), "market": "corners", "bet_name": line,
                "prob": round(prob,4), "odds": get_odds("corners", line), "l10": ""
            })

    match_markets = []
    for mkt, bet, label in [
        ("moneyline", home_team, f"{home_team} Win"),
        ("moneyline", "Draw", "Draw"),
        ("moneyline", away_team, f"{away_team} Win"),
        ("double_chance", f"{home_team} or Draw", "Home or Draw"),
        ("double_chance", f"{away_team} or Draw", "Away or Draw"),
        ("double_chance", "No Draw", "No Draw"),
    ]:
        prob = get_prob(mkt, bet)
        if prob:
            match_markets.append({
                "label": label, "market": mkt, "bet_name": bet,
                "prob": round(prob,4), "odds": get_odds(mkt, bet), "l10": ""
            })

    return jsonify({
        "home_team":   home_team,
        "away_team":   away_team,
        "competition": competition,
        "home_xg":     home_xg,
        "away_xg":     away_xg,
        "proj_total":  round(home_xg + away_xg, 2) if home_xg and away_xg else None,
        "proj_corners": proj_corners,
        "home_win":    get_prob("moneyline", home_team),
        "draw":        get_prob("moneyline", "Draw"),
        "away_win":    get_prob("moneyline", away_team),
        "key_markets": key_markets,
        "corners_markets": corners_markets,
        "match_markets": match_markets,
    })


@app.route("/research")
def research_page():
    return send_from_directory("parlay_ui", "soccer_research.html")


@app.route("/api/research-matches", methods=["POST"])
def research_matches():
    """Return full match intelligence data for all matches on a date."""
    data = request.json
    date = data.get("date")
    league = data.get("league", "")

    if not date:
        return jsonify({"error": "date required"}), 400

    try:
        conn = get_connection()
        cur  = conn.cursor()

        # compare strict join results vs slate to surface dropped matches
        cur.execute("""
            SELECT COUNT(*)
            FROM matches m
            WHERE DATE(m.match_datetime AT TIME ZONE 'UTC') = %s
              AND m.is_active = TRUE
              AND m.status IN ('TIMED', 'SCHEDULED')
              AND (%s = '' OR m.competition = %s)
        """, (date, league, league))
        slate_count = cur.fetchone()[0]

        # get all matches for this date using the same mapping/joins as build_cheatsheet.py
        cur.execute(RESEARCH_MATCH_BASE_QUERY, (date, league, league))
        match_rows = cur.fetchall()
        dropped_count = max(0, slate_count - len(match_rows))
        if dropped_count:
            logger.warning(
                "research-matches join drop: %s of %s slate matches removed (likely missing ratings/baselines). date=%s league=%s",
                dropped_count, slate_count, date, league or "ALL"
            )

        matches = []

        for row in match_rows:
            sid, home, away, comp, dt, status, hxg, axg, hatk, hdef, aatk, adef, league_avg_total = row
            hxg = float(hxg) if hxg else 1.2
            axg = float(axg) if axg else 1.0
            league_avg_total = float(league_avg_total) if league_avg_total else None
            total_xg = round(hxg + axg, 2)

            # goal chaos score (0-10) — blended absolute xG + league context
            # 75% absolute projected goal environment, 25% league-relative context.
            # This keeps high-total games high even in higher-scoring leagues like MLS.
            league_avg_for_chaos = league_avg_total if league_avg_total and league_avg_total > 0 else 2.7
            absolute_chaos = total_xg / 3.4 * 9.5
            relative_chaos = total_xg / league_avg_for_chaos * 5.0
            chaos = min(10, max(1, round((absolute_chaos * 0.75) + (relative_chaos * 0.25), 1)))

            # compute market probs using Poisson
            import math as _math
            def poisson(lam, k): return (_math.exp(-lam) * lam**k) / _math.factorial(k)
            def market_probs(hxg, axg):
                MAX = 8
                hp = [poisson(hxg, i) for i in range(MAX+1)]
                ap = [poisson(axg, i) for i in range(MAX+1)]
                hw = sum(hp[h]*ap[a] for h in range(MAX+1) for a in range(MAX+1) if h > a)
                dr = sum(hp[h]*ap[a] for h in range(MAX+1) for a in range(MAX+1) if h == a)
                aw = sum(hp[h]*ap[a] for h in range(MAX+1) for a in range(MAX+1) if h < a)
                o15 = sum(hp[h]*ap[a] for h in range(MAX+1) for a in range(MAX+1) if h+a > 1)
                o25 = sum(hp[h]*ap[a] for h in range(MAX+1) for a in range(MAX+1) if h+a > 2)
                o35 = sum(hp[h]*ap[a] for h in range(MAX+1) for a in range(MAX+1) if h+a > 3)
                u45 = sum(hp[h]*ap[a] for h in range(MAX+1) for a in range(MAX+1) if h+a <= 4)
                btts = sum(hp[h]*ap[a] for h in range(MAX+1) for a in range(MAX+1) if h > 0 and a > 0)
                score_probs = []
                for h in range(MAX+1):
                    for a in range(MAX+1):
                        score_probs.append({"h": h, "a": a, "prob": round(hp[h]*ap[a], 4)})
                top_scores = sorted(score_probs, key=lambda x: x["prob"], reverse=True)[:8]
                return {
                    "home_win": round(hw, 4), "draw": round(dr, 4), "away_win": round(aw, 4),
                    "over_15": round(o15, 4), "over_25": round(o25, 4), "over_35": round(o35, 4),
                    "under_45": round(u45, 4), "btts": round(btts, 4),
                    "top_scores": top_scores
                }

            probs = market_probs(hxg, axg)

            def fair_odds(p):
                if not p or p <= 0 or p >= 1: return None
                dec = 1/p
                if dec >= 2: return f"+{int(round((dec-1)*100))}"
                return str(int(round(-100/(dec-1))))

            # get team stats from match_statistics
            def get_team_stats(team, is_home, n=10):
                cur.execute("""
                    SELECT AVG(shots), AVG(shots_on_goal), AVG(corners), AVG(possession)
                    FROM (
                        SELECT shots, shots_on_goal, corners, possession
                        FROM match_statistics
                        WHERE team_name = %s AND is_home = %s
                        ORDER BY match_datetime DESC
                        LIMIT %s
                    ) sub
                """, (team, is_home, n))
                r = cur.fetchone()
                if r and r[0]:
                    return {
                        "shots": round(float(r[0]),1),
                        "sot":   round(float(r[1]),1) if r[1] else 0,
                        "corners": round(float(r[2]),1) if r[2] else 0,
                        "possession": round(float(r[3]),1) if r[3] else 0,
                    }
                return {"shots": 0, "sot": 0, "corners": 0, "possession": 0}

            home_stats = get_team_stats(home, True)
            away_stats = get_team_stats(away, False)

            # get recent results for each team
            def get_results(team, is_home, n=3):
                venue_filter = "is_home = %s" if is_home is not None else "1=1"
                cur.execute(f"""
                    SELECT ms.team_name, ms.opponent_name, ms.is_home,
                           m.home_score_ft, m.away_score_ft, m.match_datetime
                    FROM match_statistics ms
                    JOIN matches m ON ms.source_match_id = m.source_match_id
                    WHERE ms.team_name = %s
                      AND m.home_score_ft IS NOT NULL
                    ORDER BY m.match_datetime DESC
                    LIMIT %s
                """, (team, n))
                rows = cur.fetchall()
                results = []
                for r in rows:
                    tname, opp, ih, hs, aws, rdt = r
                    gf = int(hs) if ih else int(aws)
                    ga = int(aws) if ih else int(hs)
                    wdl = "W" if gf > ga else ("D" if gf == ga else "L")
                    opp_short = opp[:3].upper()
                    venue = "" if ih else "@"
                    results.append({"wdl": wdl, "score": f"{gf}-{ga}", "opp": f"{venue}{opp_short}", "date": str(rdt)[:10]})
                return results

            home_home_form = get_results(home, None, 5)
            home_away_form = []
            away_home_form = get_results(away, None, 5)
            away_away_form = []

            # projected corners (from avg)
            proj_corners = round((home_stats["corners"] + away_stats["corners"]), 1)
            proj_shots   = round((home_stats["shots"] + away_stats["shots"]), 1)
            proj_sot     = round((home_stats["sot"] + away_stats["sot"]), 1)

            # combined L10 hit rates for key markets
            def get_team_game_stats(team, n):
                cur.execute("""
                    SELECT ms.shots, ms.shots_on_goal, ms.corners, ms.is_home,
                           m.home_score_ft, m.away_score_ft
                    FROM match_statistics ms
                    JOIN matches m ON ms.source_match_id = m.source_match_id
                    WHERE ms.team_name = %s AND m.home_score_ft IS NOT NULL
                    ORDER BY m.match_datetime DESC LIMIT %s
                """, (team, n))
                return cur.fetchall()

            def calc_l10(selection, all_rows):
                hits = 0
                total = 0
                for r in all_rows:
                    shots, sot, corners, ih, hs, aws = r
                    hit = None
                    t = int(hs) + int(aws)
                    # goals markets
                    if selection == "Over 1.5":   hit = t > 1
                    elif selection == "Over 2.5":  hit = t > 2
                    elif selection == "Over 3.5":  hit = t > 3
                    elif selection == "Under 4.5": hit = t <= 4
                    elif selection == "BTTS Yes":  hit = int(hs) > 0 and int(aws) > 0
                    # shots markets — check team's shots vs their expected share (line/2)
                    elif "Shots" in selection and "SOT" not in selection:
                        try:
                            line = float(selection.replace("Over","").replace("Shots","").strip())
                            hit = (shots or 0) > (line / 2)
                        except: pass
                    # SOT markets — check team's SOT vs their expected share (line/2)
                    elif "SOT" in selection:
                        try:
                            line = float(selection.replace("Over","").replace("SOT","").strip())
                            hit = (sot or 0) > (line / 2)
                        except: pass
                    # match markets
                    elif selection == f"{home} Win":
                        hit = (int(hs) > int(aws)) if ih else (int(aws) > int(hs))
                    elif selection == f"{away} Win":
                        hit = (int(aws) > int(hs)) if not ih else (int(hs) > int(aws))
                    elif selection == "Draw":      hit = int(hs) == int(aws)
                    elif selection == "No Draw":   hit = int(hs) != int(aws)
                    elif "or Draw" in selection:
                        if home in selection:
                            hit = (int(hs) >= int(aws)) if ih else (int(aws) >= int(hs))
                        else:
                            hit = (int(aws) >= int(hs)) if not ih else (int(hs) >= int(aws))
                    if hit is not None:
                        hits += (1 if hit else 0)
                        total += 1
                if total == 0: return ""
                return f"{hits}/{total}"

            # get last 10 games for each team
            home_rows = get_team_game_stats(home, 10)
            away_rows = get_team_game_stats(away, 10)
            all_rows  = list(home_rows) + list(away_rows)

            def calc_team_l10(rows, check_fn):
                hits = sum(1 for r in rows if check_fn(r))
                return f"{hits}/{len(rows)}" if rows else ""

            market_l10 = {}

            # corners hit rates — need total game corners (both teams combined)
            def calc_corners_l10(team, line, n=10):
                cur.execute("""
                    SELECT ms.corners as team_corners,
                           opp.corners as opp_corners
                    FROM match_statistics ms
                    JOIN matches m ON ms.source_match_id = m.source_match_id
                    JOIN match_statistics opp ON opp.source_match_id = ms.source_match_id
                        AND opp.team_name != ms.team_name
                    WHERE ms.team_name = %s AND m.home_score_ft IS NOT NULL
                    ORDER BY m.match_datetime DESC LIMIT %s
                """, (team, n))
                rows = cur.fetchall()
                if not rows: return ""
                hits = sum(1 for r in rows if (r[0] or 0) + (r[1] or 0) > line)
                return f"{hits}/{len(rows)}"

            for line in [6.5, 7.5, 8.5, 9.5, 10.5, 11.5]:
                sel = f"Over {line} Corners"
                home_rate = calc_corners_l10(home, line)
                away_rate = calc_corners_l10(away, line)
                # combine: count hits across both teams' last 10 games
                if home_rate and away_rate:
                    h1, t1 = home_rate.split('/')
                    h2, t2 = away_rate.split('/')
                    market_l10[sel] = f"{int(h1)+int(h2)}/{int(t1)+int(t2)}"
                else:
                    market_l10[sel] = home_rate or away_rate or ""

            # goals/totals/btts — combined both teams
            for sel in ["Over 1.5","Over 2.5","Over 3.5","Under 4.5","BTTS Yes"]:
                market_l10[sel] = calc_l10(sel, all_rows)

            # shots — combined (all lines)
            for sel in ["Over 17.5 Shots","Over 19.5 Shots","Over 21.5 Shots",
                        "Over 23.5 Shots","Over 25.5 Shots","Over 27.5 Shots","Over 29.5 Shots"]:
                market_l10[sel] = calc_l10(sel, all_rows)

            # SOT — combined (all lines, note bet names from shots sheet)
            for sel in ["Over 5.5 SOT","Over 6.5 SOT","Over 7.5 SOT",
                        "Over 8.5 SOT","Over 9.5 SOT","Over 10.5 SOT"]:
                market_l10[sel] = calc_l10(sel, all_rows)

            # home win — home team only (out of 10)
            market_l10[f"{home} Win"] = calc_team_l10(home_rows,
                lambda r: (int(r[4]) > int(r[5])) if r[3] else (int(r[5]) > int(r[4])))

            # away win — away team only (out of 10)
            market_l10[f"{away} Win"] = calc_team_l10(away_rows,
                lambda r: (int(r[5]) > int(r[4])) if not r[3] else (int(r[4]) > int(r[5])))

            # draw — both teams combined (out of 20)
            market_l10["Draw"] = calc_l10("Draw", all_rows)

            # home or draw — home team only (out of 10)
            market_l10["Home or Draw"] = calc_team_l10(home_rows,
                lambda r: (int(r[4]) >= int(r[5])) if r[3] else (int(r[5]) >= int(r[4])))

            # away or draw — away team only (out of 10)
            market_l10["Away or Draw"] = calc_team_l10(away_rows,
                lambda r: (int(r[5]) >= int(r[4])) if not r[3] else (int(r[4]) >= int(r[5])))

            # no draw — both teams (out of 20)
            market_l10["No Draw"] = calc_l10("No Draw", all_rows)

            game_time_edt = utc_to_edt(str(dt))
            time_slot = get_time_slot(str(dt))

            # build key_markets, corners_markets, match_markets for bet slip
            fo = {
                "home_win": fair_odds(probs["home_win"]),
                "draw":     fair_odds(probs["draw"]),
                "away_win": fair_odds(probs["away_win"]),
                "over_15":  fair_odds(probs["over_15"]),
                "over_25":  fair_odds(probs["over_25"]),
                "over_35":  fair_odds(probs["over_35"]),
                "under_45": fair_odds(probs["under_45"]),
                "btts":     fair_odds(probs["btts"]),
            }

            key_markets = [
                {"label": "Over 1.5", "market": "totals", "bet_name": "Over 1.5", "prob": probs["over_15"], "odds": fo["over_15"], "l10": market_l10.get("Over 1.5","")},
                {"label": "Over 2.5", "market": "totals", "bet_name": "Over 2.5", "prob": probs["over_25"], "odds": fo["over_25"], "l10": market_l10.get("Over 2.5","")},
                {"label": "Over 3.5", "market": "totals", "bet_name": "Over 3.5", "prob": probs["over_35"], "odds": fo["over_35"], "l10": market_l10.get("Over 3.5","")},
                {"label": "Under 4.5","market": "totals", "bet_name": "Under 4.5","prob": probs["under_45"],"odds": fo["under_45"],"l10": market_l10.get("Under 4.5","")},
                {"label": "BTTS Yes", "market": "btts",   "bet_name": "Yes",       "prob": probs["btts"],    "odds": fo["btts"],    "l10": market_l10.get("BTTS Yes","")},
            ]

            # shots/SOT markets initialized empty — populated below from shots sheet
            shots_markets = []
            sot_markets   = []

            # corners from candidate pool for this match
            pool_path = os.path.join(PROJECT_DIR, "cheatsheets", f"model_parlay_candidate_pool_{date}.csv")
            corners_markets = []
            if os.path.exists(pool_path):
                import csv as _csv
                with open(pool_path, newline="", encoding="utf-8") as f:
                    reader = _csv.DictReader(f)
                    for row in reader:
                        bet_name = row.get("bet_name","").strip()
                        # only game-level corners (no team name prefix)
                        is_game_level = bet_name.startswith("Over") and not any(
                            t.lower() in bet_name.lower() for t in [home, away]
                        )
                        if (row.get("market_type","").strip() == "corners" and
                            row.get("home_team","").strip().lower() == home.lower() and
                            row.get("away_team","").strip().lower() == away.lower() and
                            is_game_level):
                            prob = float(row.get("model_prob",0))
                            dec = 1/prob if prob > 0 else 1
                            if dec >= 2: odds = f"+{int(round((dec-1)*100))}"
                            else: odds = str(int(round(-100/(dec-1))))
                            label = bet_name.replace(" Corners","")
                            corners_markets.append({
                                "label": label, "market": "corners",
                                "bet_name": bet_name,
                                "prob": round(prob,4), "odds": odds, "l10": row.get("combined_l10","")
                            })

            # shots and SOT from shots sheet
            shots_path = os.path.join(PROJECT_DIR, "cheatsheets", f"shots_sheet_{date}.csv")
            if os.path.exists(shots_path):
                import csv as _csv2
                with open(shots_path, newline="", encoding="utf-8") as f:
                    reader = _csv2.DictReader(f)
                    for row in reader:
                        ht = row.get("home_team","").strip().lower()
                        at = row.get("away_team","").strip().lower()
                        if ht != home.lower() or at != away.lower():
                            continue
                        prob = float(row.get("model_prob",0))
                        if prob <= 0 or prob >= 1: continue
                        dec = 1/prob
                        if dec >= 2: odds = f"+{int(round((dec-1)*100))}"
                        else: odds = str(int(round(-100/(dec-1))))
                        mkt = row.get("market_type","").strip()
                        bet = row.get("bet","").strip()
                        entry = {"label": bet, "market": mkt, "bet_name": bet, "prob": round(prob,4), "odds": odds, "l10": ""}
                        if mkt == "shots":
                            shots_markets.append(entry)
                        elif mkt == "shots_on_target":
                            sot_markets.append(entry)

            # update corners/shots/SOT with hit rates
            for entry in corners_markets:
                entry["l10"] = market_l10.get(entry["bet_name"], "")
            for entry in shots_markets:
                entry["l10"] = market_l10.get(entry["bet_name"], "")
            for entry in sot_markets:
                entry["l10"] = market_l10.get(entry["bet_name"], "")

            match_markets = [
                {"label": f"{home} Win", "market": "moneyline", "bet_name": home, "prob": probs["home_win"], "odds": fo["home_win"], "l10": market_l10.get(f"{home} Win","")},
                {"label": "Draw",        "market": "moneyline", "bet_name": "Draw", "prob": probs["draw"], "odds": fo["draw"], "l10": market_l10.get("Draw","")},
                {"label": f"{away} Win", "market": "moneyline", "bet_name": away, "prob": probs["away_win"], "odds": fo["away_win"], "l10": market_l10.get(f"{away} Win","")},
                {"label": "Home or Draw","market": "double_chance","bet_name": f"{home} or Draw","prob": round(probs["home_win"]+probs["draw"],4),"odds": fair_odds(probs["home_win"]+probs["draw"]),"l10": market_l10.get("Home or Draw","")},
                {"label": "Away or Draw","market": "double_chance","bet_name": f"{away} or Draw","prob": round(probs["away_win"]+probs["draw"],4),"odds": fair_odds(probs["away_win"]+probs["draw"]),"l10": market_l10.get("Away or Draw","")},
                {"label": "No Draw",     "market": "double_chance","bet_name": "No Draw","prob": round(probs["home_win"]+probs["away_win"],4),"odds": fair_odds(probs["home_win"]+probs["away_win"]),"l10": market_l10.get("No Draw","")},
            ]

            # team-specific markets
            def team_markets(team, is_home):
                try:
                    # get team expected goals
                    if is_home:
                        exp_goals = hxg
                        cur.execute("""
                            SELECT avg_home_corners_for, avg_home_corners_against,
                                   avg_home_shots_for, avg_home_sot_for
                            FROM team_corners_ratings cr
                            JOIN team_shots_ratings sr USING (team_name, competition)
                            WHERE cr.team_name = %s AND cr.competition = %s LIMIT 1
                        """, (team, comp))
                    else:
                        exp_goals = axg
                        cur.execute("""
                            SELECT avg_away_corners_for, avg_away_corners_against,
                                   avg_away_shots_for, avg_away_sot_for
                            FROM team_corners_ratings cr
                            JOIN team_shots_ratings sr USING (team_name, competition)
                            WHERE cr.team_name = %s AND cr.competition = %s LIMIT 1
                        """, (team, comp))
                    r = cur.fetchone()
                    exp_corners = float(r[0]) if r else 5.0
                    exp_shots   = float(r[2]) if r else 10.0
                    exp_sot     = float(r[3]) if r else 4.0
                except:
                    exp_corners = 5.0
                    exp_shots   = 10.0
                    exp_sot     = 4.0

                import math as _m
                def pois(lam, line):
                    thresh = _m.floor(line) + 1
                    under = sum(_m.exp(-lam) * lam**k / _m.factorial(k) for k in range(thresh))
                    return round(max(0, min(1, 1 - under)), 4)

                def fo(p):
                    if not p or p <= 0 or p >= 1: return None
                    dec = 1/p
                    if dec >= 2: return f"+{int(round((dec-1)*100))}"
                    return str(int(round(-100/(dec-1))))

                mkts = []
                # goals
                for line, label in [(0.5,"Over 0.5 Goals"),(1.5,"Over 1.5 Goals")]:
                    p = pois(exp_goals, line)
                    rows_team = get_team_game_stats(team, 10)
                    hits = sum(1 for r in rows_team if (
                        (int(r[4]) if r[3] else int(r[5])) > line
                    ))
                    l10 = f"{hits}/{len(rows_team)}" if rows_team else ""
                    mkts.append({"label": label, "market": "team_total_goals",
                                 "bet_name": f"{team} {label}", "prob": p, "odds": fo(p), "l10": l10})
                # corners
                for line, label in [(3.5,"Over 3.5 Corners"),(4.5,"Over 4.5 Corners")]:
                    p = pois(exp_corners, line)
                    rows_team = get_team_game_stats(team, 10)
                    hits = sum(1 for r in rows_team if (r[2] or 0) > line)
                    l10 = f"{hits}/{len(rows_team)}" if rows_team else ""
                    mkts.append({"label": label, "market": "team_corners",
                                 "bet_name": f"{team} {label}", "prob": p, "odds": fo(p), "l10": l10})
                # SOT
                for line, label in [(4.5,"Over 4.5 SOT"),(5.5,"Over 5.5 SOT")]:
                    p = pois(exp_sot, line)
                    rows_team = get_team_game_stats(team, 10)
                    hits = sum(1 for r in rows_team if (r[1] or 0) > line)
                    l10 = f"{hits}/{len(rows_team)}" if rows_team else ""
                    mkts.append({"label": label, "market": "team_shots_on_target",
                                 "bet_name": f"{team} {label}", "prob": p, "odds": fo(p), "l10": l10})
                # Shots
                for line, label in [(10.5,"Over 10.5 Shots"),(11.5,"Over 11.5 Shots")]:
                    p = pois(exp_shots, line)
                    rows_team = get_team_game_stats(team, 10)
                    hits = sum(1 for r in rows_team if (r[0] or 0) > line)
                    l10 = f"{hits}/{len(rows_team)}" if rows_team else ""
                    mkts.append({"label": label, "market": "team_shots",
                                 "bet_name": f"{team} {label}", "prob": p, "odds": fo(p), "l10": l10})
                return mkts

            home_team_markets = team_markets(home, True)
            away_team_markets = team_markets(away, False)

            matches.append({
                "home_team": home, "away_team": away,
                "competition": comp,
                "match_datetime": str(dt)[:16],
                "game_time_edt": game_time_edt,
                "time_slot": time_slot,
                "home_xg": hxg, "away_xg": axg, "total_xg": total_xg,
                "chaos_score": chaos,
                "probs": probs,
                "fair_odds": fo,
                "key_markets": key_markets,
                "corners_markets": corners_markets,
                "shots_markets": shots_markets,
                "sot_markets": sot_markets,
                "match_markets": match_markets,
                "home_team_markets": home_team_markets,
                "away_team_markets": away_team_markets,
                "home_stats": home_stats, "away_stats": away_stats,
                "home_home_form": home_home_form, "home_away_form": home_away_form,
                "away_home_form": away_home_form, "away_away_form": away_away_form,
                "proj_corners": proj_corners, "proj_shots": proj_shots, "proj_sot": proj_sot,
                "market_l10": market_l10,
                "top_scores": probs.get("top_scores", []),
                "home_atk": round(float(hatk),3) if hatk else None,
                "home_def": round(float(hdef),3) if hdef else None,
                "away_atk": round(float(aatk),3) if aatk else None,
                "away_def": round(float(adef),3) if adef else None,
            })

        # get crest URLs before closing connection
        all_teams = list(set(
            t for m in matches for t in [m["home_team"], m["away_team"]]
        ))
        crest_map = {}
        if all_teams:
            cur2 = conn.cursor()
            cur2.execute("SELECT team_name, crest_url FROM teams WHERE team_name = ANY(%s)", (all_teams,))
            for tname, curl in cur2.fetchall():
                crest_map[tname] = curl
            cur2.close()

        cur.close()
        conn.close()

        for m in matches:
            m["home_crest"] = crest_map.get(m["home_team"])
            m["away_crest"] = crest_map.get(m["away_team"])

        # apply time slot filter
        time_slot_filter = data.get("time_slot", "All")
        if time_slot_filter and time_slot_filter != "All":
            matches = [m for m in matches if m.get("time_slot") == time_slot_filter]

        # get available leagues for this date
        leagues = sorted(list(set(m["competition"] for m in matches)))
        time_slots = ["All"] + sorted(list(set(m.get("time_slot","All") for m in matches if m.get("time_slot"))))

        logger.info(
            "research-matches returned=%s date=%s league=%s time_slot=%s",
            len(matches), date, league or "ALL", data.get("time_slot", "All")
        )
        return jsonify({"matches": matches, "leagues": leagues, "time_slots": time_slots})

    except Exception as e:
        import traceback
        try:
            conn.rollback()
        except: pass
        try:
            conn.close()
        except: pass
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/available-dates", methods=["GET"])
def available_dates():
    cheatsheet_dir = os.path.join(PROJECT_DIR, "cheatsheets")
    dates = []
    if os.path.exists(cheatsheet_dir):
        for f in os.listdir(cheatsheet_dir):
            if f.startswith("cheatsheet_") and f.endswith(".csv"):
                dates.append(f.replace("cheatsheet_", "").replace(".csv", ""))
    dates.sort(reverse=True)
    return jsonify({"dates": dates})


@app.route("/health", methods=["GET"])
def health():
    payload = {"db_connected": False, "has_is_active": False, "match_count_today": 0}
    try:
        conn = get_connection()
        cur = conn.cursor()
        payload["db_connected"] = True

        cur.execute("""
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'matches'
              AND column_name = 'is_active'
        """)
        payload["has_is_active"] = cur.fetchone() is not None

        cur.execute("""
            SELECT COUNT(*)
            FROM matches
            WHERE DATE(match_datetime AT TIME ZONE 'UTC') = CURRENT_DATE
              AND is_active = TRUE
              AND status IN ('TIMED', 'SCHEDULED')
        """)
        payload["match_count_today"] = cur.fetchone()[0]
        cur.close()
        conn.close()
    except Exception:
        logger.exception("health check failed")
    return jsonify(payload)


def nightly_load_matches():
    """Load matches for today + next 7 days into the DB."""
    api_key = os.environ.get("FOOTBALL_DATA_API_KEY", "")
    if not api_key:
        logger.warning("nightly_load_matches: no API key, skipping")
        return
    logger.info("nightly_load_matches: starting 7-day load")
    today = datetime.now(EDT).date()
    for i in range(8):
        target = today + timedelta(days=i)
        date_str = target.strftime("%Y-%m-%d")
        try:
            script = os.path.join(os.path.dirname(__file__), "load_matches.py")
            result = subprocess.run(
                [sys.executable, script, "--date", date_str],
                capture_output=True, text=True, timeout=60
            )
            logger.info("nightly_load_matches %s: %s", date_str, result.stdout.strip())
            if result.returncode != 0:
                logger.warning("nightly_load_matches %s stderr: %s", date_str, result.stderr.strip())
        except Exception as e:
            logger.error("nightly_load_matches %s failed: %s", date_str, e)
    logger.info("nightly_load_matches: done")


def nightly_sr_load_matches():
    """Load matches + stats for next 7 days using SportRadar."""
    sr_key = os.environ.get("SR_API_KEY", "")
    if not sr_key:
        logger.warning("nightly_sr_load_matches: no SR_API_KEY, skipping")
        return
    logger.info("nightly_sr_load_matches: starting")
    try:
        script = os.path.join(os.path.dirname(__file__), "sr_load_matches.py")
        result = subprocess.run(
            [sys.executable, script, "--days-back", "3", "--days-forward", "7"],
            capture_output=True, text=True, timeout=1800,
            env={**os.environ, "SR_API_KEY": sr_key}
        )
        logger.info("nightly_sr_load_matches: %s", result.stdout.strip()[-500:])
        if result.returncode != 0:
            logger.warning("nightly_sr_load_matches stderr: %s", result.stderr.strip()[-500:])
    except Exception as e:
        logger.error("nightly_sr_load_matches failed: %s", e)
    logger.info("nightly_sr_load_matches: done")


def nightly_rebuild_ratings():
    """Rebuild team rating tables after new match stats loaded."""
    logger.info("nightly_rebuild_ratings: starting")
    try:
        script = os.path.join(os.path.dirname(__file__), "rebuild_ratings.py")
        result = subprocess.run(
            [sys.executable, script],
            capture_output=True, text=True, timeout=300
        )
        logger.info("nightly_rebuild_ratings: %s", result.stdout.strip())
        if result.returncode != 0:
            logger.warning("nightly_rebuild_ratings stderr: %s", result.stderr.strip())
    except Exception as e:
        logger.error("nightly_rebuild_ratings failed: %s", e)
    logger.info("nightly_rebuild_ratings: done")


scheduler = BackgroundScheduler(timezone="America/New_York")
scheduler.add_job(nightly_sr_load_matches,  "cron", hour=23, minute=0,  id="nightly_sr_load_matches")
scheduler.add_job(nightly_rebuild_ratings,  "cron", hour=23, minute=45, id="nightly_rebuild_ratings")
scheduler.start()
logger.info("Scheduler started — SR match load at 11:00 PM ET, ratings rebuild at 11:45 PM ET")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    debug = os.environ.get("RAILWAY_ENVIRONMENT") is None  # debug only locally
    app.run(debug=debug, host="0.0.0.0", port=port)
