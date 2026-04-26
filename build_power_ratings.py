"""
WC2026 Power Ratings Builder
=============================
Builds Poisson-based power ratings for all 48 WC teams.

Pipeline:
  1. Load recent form matches from DB (up to 10 per team)
  2. Calculate universal international baseline (avg goals/match)
  3. Build team metrics with exponential decay weighting (0.92 per game back)
  4. Calculate attack/defense strength vs baseline
  5. Apply confederation adjustment multipliers
  6. Compress extremes (50% dampening)
  7. Simulate group stage (100k iterations per group) → group win %
  8. Simulate full tournament bracket → outright win %
  9. Convert probabilities to fair American odds
 10. Upsert power_rating, power_rank, tier into teams table
 11. Write full breakdown to power_rankings table

Usage:
  python build_power_ratings.py
  python build_power_ratings.py --simulations 50000
  python build_power_ratings.py --dry-run   # print results, don't save

Env vars:
  DATABASE_URL
"""

import os
import math
import random
import argparse
import logging
from collections import defaultdict
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb as Json
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("ratings")

DATABASE_URL = os.environ["DATABASE_URL"]

# ── Confederation adjustment multipliers ──────────────────────────────────────
# Accounts for strength of schedule differences between confederations.
# UEFA and CONMEBOL play harder opponents → raw stats slightly understate quality.
# AFC/CAF/CONCACAF play weaker opposition on average.
CONF_ADJUSTMENT = {
    "UEFA":     1.08,
    "CONMEBOL": 1.06,
    "AFC":      0.96,
    "CAF":      0.94,
    "CONCACAF": 0.95,
    "OFC":      0.90,
}

# Manual confederation mapping by country_code
# (SportRadar doesn't expose confederation directly)
CONFEDERATION_MAP = {
    # UEFA
    "ESP": "UEFA", "FRA": "UEFA", "ENG": "UEFA", "GER": "UEFA", "POR": "UEFA",
    "NED": "UEFA", "BEL": "UEFA", "CRO": "UEFA", "SUI": "UEFA", "DEN": "UEFA",
    "AUT": "UEFA", "SCO": "UEFA", "NOR": "UEFA", "SWE": "UEFA", "CZE": "UEFA",
    "TUR": "UEFA", "BIH": "UEFA", "SRB": "UEFA", "UKR": "UEFA", "POL": "UEFA",
    # CONMEBOL
    "BRA": "CONMEBOL", "ARG": "CONMEBOL", "URU": "CONMEBOL", "COL": "CONMEBOL",
    "ECU": "CONMEBOL", "PAR": "CONMEBOL", "CHI": "CONMEBOL", "PER": "CONMEBOL",
    "BOL": "CONMEBOL", "VEN": "CONMEBOL",
    # CONCACAF
    "USA": "CONCACAF", "MEX": "CONCACAF", "CAN": "CONCACAF", "PAN": "CONCACAF",
    "HTI": "CONCACAF", "JAM": "CONCACAF", "CRC": "CONCACAF", "HON": "CONCACAF",
    "CUW": "CONCACAF",
    # AFC
    "KOR": "AFC", "JPN": "AFC", "AUS": "AFC", "IRN": "AFC", "SAU": "AFC",
    "QAT": "AFC", "IRQ": "AFC", "JOR": "AFC", "UZB": "AFC", "CHN": "AFC",
    # CAF
    "MAR": "CAF", "SEN": "CAF", "NGA": "CAF", "CMR": "CAF", "GHA": "CAF",
    "CIV": "CAF", "EGY": "CAF", "TUN": "CAF", "ALG": "CAF", "ZAF": "CAF",
    "CPV": "CAF", "COD": "CAF",
    # OFC
    "NZL": "OFC",
    # Haiti / Scotland / etc fallback handled below
}

# ── FanDuel Market Odds — April 24, 2026 ─────────────────────────────────────

# Outright winner odds
MARKET_WIN_ODDS = {
    "Spain": 430, "France": 470, "England": 650, "Brazil": 750,
    "Argentina": 850, "Germany": 1100, "Portugal": 1100, "Netherlands": 1900,
    "Norway": 2200, "Belgium": 3000, "Morocco": 4000, "Colombia": 4000,
    "Japan": 5500, "USA": 5500, "Uruguay": 6500, "Mexico": 6500,
    "Switzerland": 6500, "Turkiye": 8000, "Croatia": 8000, "Sweden": 8000,
    "Senegal": 8000, "Ecuador": 8000, "Austria": 10000, "Paraguay": 10000,
    "Canada": 15000, "Scotland": 15000, "Ivory Coast": 17500, "Egypt": 17500,
    "Czechia": 17500, "Ghana": 22500, "Algeria": 22500, "IR Iran": 30000,
    "Australia": 30000, "Tunisia": 30000, "Korea Republic": 30000,
    "Bosnia and Herzegovina": 30000, "Congo DR": 40000, "Saudi Arabia": 50000,
    "New Zealand": 50000, "Qatar": 50000, "Cape Verde": 50000,
    "Curacao": 50000, "Haiti": 50000, "Iraq": 50000, "Jordan": 50000,
    "Panama": 50000, "Uzbekistan": 50000, "South Africa": 50000,
}

# To reach final odds
MARKET_FINAL_ODDS = {
    "Spain": 240, "England": 300, "France": 320, "Brazil": 380,
    "Argentina": 380, "Portugal": 500, "Germany": 550, "Belgium": 900,
    "Netherlands": 1000, "Norway": 1300, "Colombia": 1600, "Uruguay": 1800,
    "USA": 1800, "Croatia": 2000, "Ecuador": 2200, "Mexico": 2200,
    "Morocco": 2200, "Switzerland": 2200, "Japan": 3000, "Austria": 3300,
    "Senegal": 4000, "Ivory Coast": 4000, "Canada": 4000, "Sweden": 4000,
    "Turkiye": 4000, "Algeria": 5000, "Paraguay": 5000, "Egypt": 6500,
    "IR Iran": 8000, "Scotland": 8000, "Ghana": 8000, "Korea Republic": 10000,
    "Czechia": 10000, "Congo DR": 15000, "Bosnia and Herzegovina": 15000,
    "Saudi Arabia": 22500, "Tunisia": 22500, "Qatar": 40000, "Uzbekistan": 40000,
    "Australia": 40000, "Panama": 40000, "South Africa": 40000,
    "Cape Verde": 40000, "Curacao": 40000, "New Zealand": 40000,
    "Jordan": 40000, "Haiti": 40000, "Iraq": 40000,
}

# To qualify from group (advance odds) — negative = favorite
MARKET_ADVANCE_ODDS = {
    # Group A
    "Mexico": -800, "Korea Republic": -280, "Czechia": -210, "South Africa": -110,
    # Group B
    "Switzerland": -1100, "Canada": -550, "Bosnia and Herzegovina": -250, "Qatar": 200,
    # Group C
    "Brazil": -7000, "Morocco": -800, "Scotland": -260, "Haiti": 950,
    # Group D
    "USA": -650, "Turkiye": -280, "Paraguay": -260, "Australia": 140,
    # Group E
    "Germany": -7000, "Ecuador": -1500, "Ivory Coast": -600, "Curacao": 1500,
    # Group F
    "Netherlands": -800, "Japan": -310, "Sweden": -230, "Tunisia": 125,
    # Group G
    "Belgium": -4000, "Egypt": -310, "IR Iran": -220, "New Zealand": 200,
    # Group H
    "Spain": -7000, "Uruguay": -800, "Saudi Arabia": -125, "Cape Verde": 270,
    # Group I
    "France": -7000, "Norway": -650, "Senegal": -230, "Iraq": 440,
    # Group J
    "Argentina": -7000, "Austria": -450, "Algeria": -310, "Jordan": 480,
    # Group K
    "Portugal": -7000, "Colombia": -650, "Congo DR": 100, "Uzbekistan": 200,
    # Group L
    "England": -7000, "Croatia": -450, "Ghana": -165, "Panama": 220,
}

# To reach semi-final odds
MARKET_SEMI_ODDS = {
    "Spain": 125, "France": 170, "England": 170, "Argentina": 190,
    "Brazil": 200, "Portugal": 230, "Germany": 280, "Belgium": 340,
    "Netherlands": 430, "Norway": 600, "Colombia": 650, "USA": 700,
    "Uruguay": 800, "Ecuador": 850, "Croatia": 900, "Switzerland": 900,
    "Mexico": 900, "Morocco": 900, "Japan": 1300, "Austria": 1300,
    "Ivory Coast": 1600, "Turkiye": 1600, "Senegal": 1800, "Canada": 1800,
    "Sweden": 1800, "Egypt": 2000, "Algeria": 2000, "Paraguay": 2000,
    "Ghana": 2500, "Korea Republic": 2700, "IR Iran": 2700, "Scotland": 2700,
    "Czechia": 4000, "Bosnia and Herzegovina": 4000, "Saudi Arabia": 6500,
    "Congo DR": 6500, "South Africa": 8000, "Tunisia": 8000, "Curacao": 10000,
    "Jordan": 10000, "New Zealand": 10000, "Cape Verde": 10000,
    "Australia": 10000, "Qatar": 10000, "Panama": 10000,
    "Haiti": 10000, "Uzbekistan": 10000, "Iraq": 10000,
}

# To reach quarter-final odds
MARKET_QF_ODDS = {
    "Spain": -140, "England": -140, "France": -125, "Brazil": -115,
    "Argentina": -115, "Portugal": 100, "Belgium": 125, "Germany": 135,
    "Netherlands": 175, "USA": 250, "Norway": 250, "Colombia": 280,
    "Mexico": 280, "Switzerland": 300, "Morocco": 320, "Ecuador": 320,
    "Uruguay": 320, "Croatia": 380, "Canada": 470, "Japan": 470,
    "Austria": 470, "Ivory Coast": 500, "Turkiye": 500, "Egypt": 650,
    "Paraguay": 650, "Senegal": 650, "Sweden": 650, "Algeria": 700,
    "Korea Republic": 800, "Scotland": 850, "IR Iran": 850, "Ghana": 900,
    "Bosnia and Herzegovina": 1000, "Czechia": 1000, "South Africa": 1800,
    "Saudi Arabia": 2000, "Congo DR": 2000, "Tunisia": 2500, "Australia": 2700,
    "Panama": 3300, "Uzbekistan": 4000, "New Zealand": 5000, "Qatar": 5000,
    "Iraq": 8000, "Cape Verde": 10000, "Curacao": 10000, "Jordan": 10000,
    "Haiti": 10000,
}

# Blend weights for composite market score
MARKET_BLEND = 0.75


def american_to_prob(odds: int) -> float:
    if odds > 0:
        return 100 / (odds + 100)
    else:
        return abs(odds) / (abs(odds) + 100)


def get_composite_market_prob(team_name: str) -> float | None:
    """
    Blend win, final, semi, qf, and advance market probs into a single
    composite team quality score normalized to win probability scale.
    Historical scaling factors (approximate):
      - reach QF:   ~25% of QF teams win it
      - reach SF:   ~15% of SF teams win it  
      - reach F:    ~50% of finalists win it... no, ~15% net
      - advance:    ~8% of group advancers win it
    """
    win_p     = american_to_prob(MARKET_WIN_ODDS[team_name])     if team_name in MARKET_WIN_ODDS     else None
    final_p   = american_to_prob(MARKET_FINAL_ODDS[team_name])   if team_name in MARKET_FINAL_ODDS   else None
    semi_p    = american_to_prob(MARKET_SEMI_ODDS[team_name])    if team_name in MARKET_SEMI_ODDS    else None
    qf_p      = american_to_prob(MARKET_QF_ODDS[team_name])      if team_name in MARKET_QF_ODDS      else None
    advance_p = american_to_prob(MARKET_ADVANCE_ODDS[team_name]) if team_name in MARKET_ADVANCE_ODDS else None

    if win_p is None:
        return None

    # Scale each market down to win probability scale
    final_as_win   = final_p   * 0.15 if final_p   else None
    semi_as_win    = semi_p    * 0.10 if semi_p    else None
    qf_as_win      = qf_p      * 0.07 if qf_p      else None
    advance_as_win = advance_p * 0.05 if advance_p else None

    # Weight: win odds get most weight, deeper rounds add signal
    components = []
    weights    = []
    for p, w in [(win_p, 0.40), (final_as_win, 0.25), (semi_as_win, 0.20),
                 (qf_as_win, 0.10), (advance_as_win, 0.05)]:
        if p is not None:
            components.append(p * w)
            weights.append(w)

    if not weights:
        return None
    return sum(components) / sum(weights)


TIER_THRESHOLDS = [
    (0.08, 1),   # >8% chance = Tier 1 (contenders)
    (0.04, 2),   # 4-8%       = Tier 2 (dark horses)
    (0.015, 3),  # 1.5-4%     = Tier 3 (solid
    (0.005, 4),  # 0.5-1.5%   = Tier 4 (longshots)
    (0.0,   5),  # <0.5%      = Tier 5 (minnows)
]

DECAY = 0.92        # exponential decay per game back
COMPRESS = 0.50     # strength compression factor
N_SIMS = 100_000    # tournament simulations


# ── DB ────────────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


# ── Step 1: Load data ─────────────────────────────────────────────────────────

def load_teams(conn) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT t.id, t.name, t.abbreviation, t.country_code, t.group_name,
                   COALESCE(sr.xi_attack, 50.0) as squad_attack,
                   COALESCE(sr.xi_defense, 50.0) as squad_defense,
                   COALESCE(sr.squad_rating, 50.0) as squad_rating
            FROM teams t
            LEFT JOIN team_squad_ratings sr ON sr.team_id = t.id
            WHERE t.group_name IS NOT NULL
            ORDER BY t.group_name, t.name
        """)
        return [dict(r) for r in cur.fetchall()]


def load_team_form(conn) -> dict[str, list[dict]]:
    """Returns {team_id: [match_rows sorted newest first]}"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                m.id as match_id,
                m.home_team_id, m.away_team_id,
                m.home_score, m.away_score,
                m.kickoff_utc
            FROM matches m
            WHERE m.status = 'closed'
              AND m.home_score IS NOT NULL
              AND m.away_score IS NOT NULL
              AND (
                m.home_team_id IN (SELECT id FROM teams WHERE group_name IS NOT NULL)
                OR
                m.away_team_id IN (SELECT id FROM teams WHERE group_name IS NOT NULL)
              )
            ORDER BY m.kickoff_utc DESC
        """)
        rows = cur.fetchall()

    form: dict[str, list] = defaultdict(list)
    for r in rows:
        r = dict(r)
        htid = r["home_team_id"]
        atid = r["away_team_id"]
        if htid:
            form[htid].append({
                "goals_for":     r["home_score"],
                "goals_against": r["away_score"],
                "is_home":       True,
                "kickoff_utc":   r["kickoff_utc"],
            })
        if atid:
            form[atid].append({
                "goals_for":     r["away_score"],
                "goals_against": r["home_score"],
                "is_home":       False,
                "kickoff_utc":   r["kickoff_utc"],
            })

    # Keep last 10 per team, newest first
    for tid in form:
        form[tid] = sorted(form[tid],
                           key=lambda x: x["kickoff_utc"] or datetime.min.replace(tzinfo=timezone.utc),
                           reverse=True)[:10]
    return dict(form)


# ── Step 2: Universal baseline ────────────────────────────────────────────────

def compute_baseline(form: dict[str, list]) -> tuple[float, float]:
    """
    Returns (avg_home_goals, avg_away_goals) across all WC team matches.
    """
    home_goals, away_goals = [], []
    seen = set()
    for matches in form.values():
        for m in matches:
            # Deduplicate by using a proxy key (goals_for, goals_against, kickoff)
            key = (m["goals_for"], m["goals_against"],
                   str(m["kickoff_utc"]), m["is_home"])
            if key in seen:
                continue
            seen.add(key)
            if m["is_home"]:
                home_goals.append(m["goals_for"])
                away_goals.append(m["goals_against"])
            else:
                away_goals.append(m["goals_for"])
                home_goals.append(m["goals_against"])

    avg_home = sum(home_goals) / len(home_goals) if home_goals else 1.35
    avg_away = sum(away_goals) / len(away_goals) if away_goals else 1.10
    log.info(f"Baseline: avg_home_goals={avg_home:.3f}, avg_away_goals={avg_away:.3f} "
             f"(from {len(home_goals)} home, {len(away_goals)} away observations)")
    return avg_home, avg_away


# ── Step 3-6: Team strength ───────────────────────────────────────────────────

def compute_team_strength(team: dict, matches: list,
                          avg_home: float, avg_away: float) -> dict:
    """
    Returns attack_strength and defense_strength for a team,
    with decay weighting, baseline normalization, confederation
    adjustment, compression, and squad rating blend.
    """
    if not matches:
        poisson_attack = poisson_defense = 1.0
    else:
        weights = [DECAY ** i for i in range(len(matches))]
        total_w = sum(weights)
        w_goals_for     = sum(m["goals_for"]     * w for m, w in zip(matches, weights))
        w_goals_against = sum(m["goals_against"] * w for m, w in zip(matches, weights))
        avg_baseline    = (avg_home + avg_away) / 2
        adj_for         = (w_goals_for  / total_w) / avg_baseline
        adj_against     = (w_goals_against / total_w) / avg_baseline
        poisson_attack  = adj_for
        poisson_defense = adj_against

    # Confederation adjustment
    cc = team.get("country_code", "")
    conf = CONFEDERATION_MAP.get(cc, "UEFA")
    conf_adj = CONF_ADJUSTMENT.get(conf, 1.0)
    poisson_attack  *= conf_adj
    poisson_defense /= conf_adj

    # Compress extremes
    poisson_attack  = 1 + (poisson_attack  - 1) * COMPRESS
    poisson_defense = 1 + (poisson_defense - 1) * COMPRESS

    # Squad rating blend — normalize squad_attack/defense from 1-100 to strength scale
    # Squad rating 50 = average (1.0), 100 = elite (1.5), 1 = poor (0.6)
    squad_attack_raw  = float(team.get("squad_attack", 50.0))
    squad_defense_raw = float(team.get("squad_defense", 50.0))

    # Convert 1-100 scale to strength multiplier (50 → 1.0, 100 → 1.4, 1 → 0.6)
    squad_attack_str  = 0.6 + (squad_attack_raw  - 1) / 99 * 0.8
    squad_defense_str = 0.6 + (squad_defense_raw - 1) / 99 * 0.8
    # Invert defense: higher squad defense rating = lower goals conceded
    squad_defense_str = 2.0 - squad_defense_str

    # Blend: 60% poisson form, 40% squad quality
    SQUAD_BLEND = 0.40
    attack  = (1 - SQUAD_BLEND) * poisson_attack  + SQUAD_BLEND * squad_attack_str
    defense = (1 - SQUAD_BLEND) * poisson_defense + SQUAD_BLEND * squad_defense_str

    # Clamp
    attack  = max(0.40, min(2.50, attack))
    defense = max(0.40, min(2.50, defense))

    return {
        "attack":    attack,
        "defense":   defense,
        "n_matches": len(matches),
        "conf":      conf,
    }


# ── Step 7: Poisson match simulation ─────────────────────────────────────────

def poisson_pmf(lam: float, k: int) -> float:
    return (lam ** k) * math.exp(-lam) / math.factorial(k)


def match_probabilities(home_attack: float, home_defense: float,
                        away_attack: float, away_defense: float,
                        avg_home: float, avg_away: float,
                        max_goals: int = 8) -> tuple[float, float, float]:
    """
    Returns (home_win_prob, draw_prob, away_win_prob) using Poisson distribution.
    """
    home_xg = avg_home * home_attack * away_defense
    away_xg = avg_away * away_attack * home_defense

    home_win = draw = away_win = 0.0
    for h in range(max_goals + 1):
        for a in range(max_goals + 1):
            p = poisson_pmf(home_xg, h) * poisson_pmf(away_xg, a)
            if h > a:
                home_win += p
            elif h == a:
                draw += p
            else:
                away_win += p

    total = home_win + draw + away_win
    return home_win / total, draw / total, away_win / total


def simulate_group(teams_in_group: list[dict],
                   strengths: dict,
                   avg_home: float, avg_away: float,
                   n_sims: int) -> dict[str, float]:
    """
    Simulate group stage N times.
    Returns {team_id: probability_of_finishing_top_2}.
    """
    team_ids = [t["id"] for t in teams_in_group]
    advance_counts = defaultdict(int)

    # Precompute match probabilities for all pairs
    matchups = {}
    for i, t1 in enumerate(team_ids):
        for j, t2 in enumerate(team_ids):
            if i >= j:
                continue
            s1 = strengths[t1]
            s2 = strengths[t2]
            hw, d, aw = match_probabilities(
                s1["attack"], s1["defense"],
                s2["attack"], s2["defense"],
                avg_home, avg_away
            )
            matchups[(t1, t2)] = (hw, d, aw)

    for _ in range(n_sims):
        points = defaultdict(int)
        gd     = defaultdict(int)
        gf     = defaultdict(int)

        for i, t1 in enumerate(team_ids):
            for j, t2 in enumerate(team_ids):
                if i >= j:
                    continue
                hw, d, aw = matchups[(t1, t2)]
                r = random.random()
                if r < hw:
                    points[t1] += 3
                    # Simulate scoreline for GD
                    h_g = max(1, int(random.gauss(1.4, 0.8)))
                    a_g = max(0, int(random.gauss(0.8, 0.7)))
                    gd[t1] += h_g - a_g; gd[t2] += a_g - h_g
                    gf[t1] += h_g; gf[t2] += a_g
                elif r < hw + d:
                    points[t1] += 1; points[t2] += 1
                    g = max(0, int(random.gauss(1.1, 0.7)))
                    gd[t1] += 0; gd[t2] += 0
                    gf[t1] += g; gf[t2] += g
                else:
                    points[t2] += 3
                    h_g = max(0, int(random.gauss(0.8, 0.7)))
                    a_g = max(1, int(random.gauss(1.4, 0.8)))
                    gd[t1] += h_g - a_g; gd[t2] += a_g - h_g
                    gf[t1] += h_g; gf[t2] += a_g

        # Rank by points, then GD, then GF
        ranked = sorted(team_ids,
                        key=lambda t: (points[t], gd[t], gf[t]),
                        reverse=True)
        advance_counts[ranked[0]] += 1
        advance_counts[ranked[1]] += 1

    return {tid: advance_counts[tid] / n_sims for tid in team_ids}


# ── Step 8: Tournament simulation ────────────────────────────────────────────

def simulate_tournament(groups: dict[str, list],
                        strengths: dict,
                        avg_home: float, avg_away: float,
                        n_sims: int) -> dict[str, float]:
    """
    Simulate the full WC tournament N times.
    Returns {team_id: win_probability}.
    """
    win_counts = defaultdict(int)
    group_keys = sorted(groups.keys())

    for _ in range(n_sims):
        # Simulate group stage to get qualifiers
        qualifiers = {}  # group → [1st, 2nd]
        third_place = {}  # group → 3rd place team

        for g, teams in groups.items():
            team_ids = [t["id"] for t in teams]
            pts  = defaultdict(int)
            gd   = defaultdict(int)
            gf_d = defaultdict(int)

            for i, t1 in enumerate(team_ids):
                for j, t2 in enumerate(team_ids):
                    if i >= j:
                        continue
                    s1 = strengths[t1]
                    s2 = strengths[t2]
                    hw, d, aw = match_probabilities(
                        s1["attack"], s1["defense"],
                        s2["attack"], s2["defense"],
                        avg_home, avg_away
                    )
                    r = random.random()
                    if r < hw:
                        pts[t1] += 3
                        hg = max(1, int(random.gauss(1.4, 0.8)))
                        ag = max(0, int(random.gauss(0.8, 0.7)))
                    elif r < hw + d:
                        pts[t1] += 1; pts[t2] += 1
                        hg = ag = max(0, int(random.gauss(1.1, 0.7)))
                    else:
                        pts[t2] += 3
                        hg = max(0, int(random.gauss(0.8, 0.7)))
                        ag = max(1, int(random.gauss(1.4, 0.8)))
                    gd[t1] += hg - ag; gd[t2] += ag - hg
                    gf_d[t1] += hg; gf_d[t2] += ag

            ranked = sorted(team_ids,
                            key=lambda t: (pts[t], gd[t], gf_d[t]),
                            reverse=True)
            qualifiers[g] = ranked[:2]
            third_place[g] = ranked[2]

        # Build Round of 32 bracket (simplified: winners vs runners-up cross-group)
        # WC2026: 12 group winners + 12 runners-up + 8 best 3rd place = 32
        # Simplified bracket: pair groups A-B, C-D, E-F, G-H, I-J, K-L
        bracket = []
        for i in range(0, len(group_keys), 2):
            if i + 1 < len(group_keys):
                g1, g2 = group_keys[i], group_keys[i+1]
                bracket.append((qualifiers[g1][0], qualifiers[g2][1]))
                bracket.append((qualifiers[g2][0], qualifiers[g1][1]))

        # Add 8 best 3rd place teams (simplified: take first 8 alphabetically)
        thirds = [third_place[g] for g in group_keys[:8]]
        # Pair thirds against winners from remaining groups
        for i, t3 in enumerate(thirds):
            g_idx = (i + 8) % len(group_keys)
            if g_idx < len(group_keys):
                opponent = qualifiers[group_keys[g_idx]][0]
                bracket.append((opponent, t3))

        # Simulate knockout rounds
        remaining = set()
        for t1, t2 in bracket:
            remaining.add(t1)
            remaining.add(t2)

        current_round = bracket
        while len(current_round) > 1:
            next_round = []
            winners = []
            for t1, t2 in current_round:
                s1 = strengths.get(t1, {"attack": 1.0, "defense": 1.0})
                s2 = strengths.get(t2, {"attack": 1.0, "defense": 1.0})
                hw, d, aw = match_probabilities(
                    s1["attack"], s1["defense"],
                    s2["attack"], s2["defense"],
                    avg_home, avg_away
                )
                # In knockouts no draw — split draw prob 50/50
                r = random.random()
                winner = t1 if r < hw + d / 2 else t2
                winners.append(winner)

            # Pair winners for next round
            for i in range(0, len(winners), 2):
                if i + 1 < len(winners):
                    next_round.append((winners[i], winners[i+1]))
                else:
                    # Bye
                    win_counts[winners[i]] += 1

            current_round = next_round

        if current_round:
            t1, t2 = current_round[0]
            s1 = strengths.get(t1, {"attack": 1.0, "defense": 1.0})
            s2 = strengths.get(t2, {"attack": 1.0, "defense": 1.0})
            hw, d, aw = match_probabilities(
                s1["attack"], s1["defense"],
                s2["attack"], s2["defense"],
                avg_home, avg_away
            )
            r = random.random()
            winner = t1 if r < hw + d / 2 else t2
            win_counts[winner] += 1

    return {tid: win_counts[tid] / n_sims for tid in win_counts}


# ── Step 9: Fair odds conversion ──────────────────────────────────────────────

def prob_to_american(p: float) -> str:
    if p <= 0:
        return "N/A"
    if p >= 1:
        return "-∞"
    if p >= 0.5:
        odds = -round((p / (1 - p)) * 100)
        return str(odds)
    else:
        odds = round(((1 - p) / p) * 100)
        return f"+{odds}"


def prob_to_decimal(p: float) -> float:
    if p <= 0:
        return 999.0
    return round(1 / p, 2)


# ── Step 10: Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="WC2026 Power Ratings")
    parser.add_argument("--simulations", type=int, default=N_SIMS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = get_conn()

    # Load data
    teams = load_teams(conn)
    form  = load_team_form(conn)
    log.info(f"Loaded {len(teams)} WC teams, form data for {len(form)} teams")

    # Baseline
    avg_home, avg_away = compute_baseline(form)

    # Compute team strengths (Poisson + squad ratings)
    strengths = {}
    for team in teams:
        tid = team["id"]
        matches = form.get(tid, [])
        strengths[tid] = compute_team_strength(team, matches, avg_home, avg_away)
        strengths[tid]["team_name"] = team["name"]

    # Build results — all probabilities straight from market odds as %
    results = []
    for team in teams:
        tid  = team["id"]
        name = team["name"]
        s    = strengths[tid]

        # All tournament probabilities — pure market, converted to %
        win_p     = american_to_prob(MARKET_WIN_ODDS[name])     if name in MARKET_WIN_ODDS     else None
        final_p   = american_to_prob(MARKET_FINAL_ODDS[name])   if name in MARKET_FINAL_ODDS   else None
        semi_p    = american_to_prob(MARKET_SEMI_ODDS[name])    if name in MARKET_SEMI_ODDS    else None
        qf_p      = american_to_prob(MARKET_QF_ODDS[name])      if name in MARKET_QF_ODDS      else None
        advance_p = american_to_prob(MARKET_ADVANCE_ODDS[name]) if name in MARKET_ADVANCE_ODDS else None

        # Power rating 0-100 based on market win probability
        # Spain 18.87% → ~99, Qatar 0.2% → ~50
        # Use log scale so lower teams spread out properly
        import math
        base_score = 50 + (math.log10((win_p or 0.001) * 100 + 1) / math.log10(20)) * 50

        # Small model nudge (±3 pts max)
        attack = s["attack"]
        defense = s["defense"]
        squad_rating = float(team.get("squad_rating", 50.0))
        model_nudge = (
            (attack - 1.0) * 1.5 +
            (1.0 - defense) * 1.5 +
            (squad_rating - 50) * 0.03
        )

        power_rating = round(min(99.9, max(50.0, base_score + model_nudge)), 2)

        # Tier based on market win probability
        tier = 5
        if win_p:
            for threshold, t in TIER_THRESHOLDS:
                if win_p >= threshold:
                    tier = t
                    break

        results.append({
            "id":           tid,
            "name":         name,
            "group_name":   team["group_name"],
            "country_code": team["country_code"],
            "conf":         s.get("conf", "?"),
            "n_matches":    s["n_matches"],
            "attack":       round(attack, 4),
            "defense":      round(defense, 4),
            "squad_rating": round(squad_rating, 1),
            "power_rating": power_rating,
            "tier":         tier,
            # Market probabilities as percentages
            "win_pct":      round(win_p * 100, 2)     if win_p     else None,
            "final_pct":    round(final_p * 100, 2)   if final_p   else None,
            "semi_pct":     round(semi_p * 100, 2)    if semi_p    else None,
            "qf_pct":       round(qf_p * 100, 2)      if qf_p      else None,
            "advance_pct":  round(advance_p * 100, 2) if advance_p else None,
            # Keep raw American odds for display
            "win_american":     f"+{MARKET_WIN_ODDS[name]}"     if name in MARKET_WIN_ODDS     else None,
            "final_american":   f"+{MARKET_FINAL_ODDS[name]}"   if name in MARKET_FINAL_ODDS   else None,
            "semi_american":    f"+{MARKET_SEMI_ODDS[name]}"    if name in MARKET_SEMI_ODDS    else None,
            "qf_american":      f"+{MARKET_QF_ODDS[name]}"      if name in MARKET_QF_ODDS      else None,
            "advance_american": str(MARKET_ADVANCE_ODDS[name])  if name in MARKET_ADVANCE_ODDS else None,
        })

    # Sort by power rating (our model)
    results.sort(key=lambda x: x["power_rating"], reverse=True)
    for i, r in enumerate(results):
        r["power_rank"] = i + 1

    # Print results
    log.info("\n" + "=" * 100)
    log.info(f"{'Rank':<5} {'Team':<25} {'Tier':<5} {'Rating':<8} {'Win%':<8} {'Final%':<8} {'Semi%':<8} {'QF%':<8} {'Adv%':<8} {'ATT':<6} {'DEF':<6} {'Squad'}")
    log.info("-" * 100)
    for r in results:
        log.info(
            f"{r['power_rank']:<5} {r['name']:<25} {r['tier']:<5} "
            f"{r['power_rating']:<8.2f} "
            f"{str(r['win_pct'])+'%':<8} "
            f"{str(r['final_pct'])+'%':<8} "
            f"{str(r['semi_pct'])+'%':<8} "
            f"{str(r['qf_pct'])+'%':<8} "
            f"{str(r['advance_pct'])+'%':<8} "
            f"{r['attack']:<6.3f} {r['defense']:<6.3f} {r['squad_rating']}"
        )

    if args.dry_run:
        log.info("Dry run — not saving to DB")
        conn.close()
        return

    # Ensure power_rankings table exists
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS power_rankings (
                id              SERIAL PRIMARY KEY,
                team_id         TEXT NOT NULL UNIQUE,
                power_rank      INT,
                power_rating    NUMERIC(6,3),
                tier            INT,
                attack          NUMERIC(6,4),
                defense         NUMERIC(6,4),
                squad_rating    NUMERIC(6,2),
                win_pct         NUMERIC(6,3),
                final_pct       NUMERIC(6,3),
                semi_pct        NUMERIC(6,3),
                qf_pct          NUMERIC(6,3),
                advance_pct     NUMERIC(6,3),
                win_american    TEXT,
                final_american  TEXT,
                semi_american   TEXT,
                qf_american     TEXT,
                advance_american TEXT,
                n_matches       INT,
                conf            TEXT,
                computed_at     TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        conn.commit()

    with conn.cursor() as cur:
        for r in results:
            cur.execute("""
                UPDATE teams SET
                    power_rating = %(power_rating)s,
                    power_rank   = %(power_rank)s,
                    tier         = %(tier)s
                WHERE id = %(id)s
            """, r)

            cur.execute("""
                INSERT INTO power_rankings (
                    team_id, power_rank, power_rating, tier,
                    attack, defense, squad_rating,
                    win_pct, final_pct, semi_pct, qf_pct, advance_pct,
                    win_american, final_american, semi_american,
                    qf_american, advance_american,
                    n_matches, conf, computed_at
                ) VALUES (
                    %(id)s, %(power_rank)s, %(power_rating)s, %(tier)s,
                    %(attack)s, %(defense)s, %(squad_rating)s,
                    %(win_pct)s, %(final_pct)s, %(semi_pct)s, %(qf_pct)s, %(advance_pct)s,
                    %(win_american)s, %(final_american)s, %(semi_american)s,
                    %(qf_american)s, %(advance_american)s,
                    %(n_matches)s, %(conf)s, NOW()
                )
                ON CONFLICT (team_id) DO UPDATE SET
                    power_rank      = EXCLUDED.power_rank,
                    power_rating    = EXCLUDED.power_rating,
                    tier            = EXCLUDED.tier,
                    attack          = EXCLUDED.attack,
                    defense         = EXCLUDED.defense,
                    squad_rating    = EXCLUDED.squad_rating,
                    win_pct         = EXCLUDED.win_pct,
                    final_pct       = EXCLUDED.final_pct,
                    semi_pct        = EXCLUDED.semi_pct,
                    qf_pct          = EXCLUDED.qf_pct,
                    advance_pct     = EXCLUDED.advance_pct,
                    win_american    = EXCLUDED.win_american,
                    final_american  = EXCLUDED.final_american,
                    semi_american   = EXCLUDED.semi_american,
                    qf_american     = EXCLUDED.qf_american,
                    advance_american = EXCLUDED.advance_american,
                    n_matches       = EXCLUDED.n_matches,
                    conf            = EXCLUDED.conf,
                    computed_at     = NOW()
            """, r)

        conn.commit()

    log.info(f"✅ Power ratings saved for {len(results)} teams")
    conn.close()


if __name__ == "__main__":
    main()
