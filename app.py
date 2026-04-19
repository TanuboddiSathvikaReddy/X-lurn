from flask import Flask, render_template, request, redirect, url_for, session, jsonify, Response
import heapq
import time
import base64
import os
import sqlite3
import json

app = Flask(__name__)
app.secret_key = "xlurn_secret_2024"

DB_PATH = "xlurn.db"

# ---------------------------
# CONSTANTS
# ---------------------------

PRIORITY_BONUS        = {"Low": 0, "Normal": 5, "Urgent": 15, "Critical": 30}
BEGINNER_SERVICE_LIMIT = 3
BEGINNER_MIN_RATING    = 3.0
LEGITIMACY_WEIGHT      = 0.4
RESET_COOLDOWN_DAYS    = 30
MAX_RESETS             = 2
WEEKLY_ALLOWANCE       = 2
WEEKLY_ALLOWANCE_DAYS  = 7
ALLOWED_EXTENSIONS     = {"png", "jpg", "jpeg", "gif", "webp", "pdf"}
MAX_FILE_BYTES         = 5 * 1024 * 1024

# ---------------------------
# DATABASE SETUP
# ---------------------------

def get_db():
    """Get a database connection with row_factory so rows behave like dicts."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    """Create all tables if they don't exist."""
    conn = get_db()
    c = conn.cursor()

    c.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        id                TEXT PRIMARY KEY,
        name              TEXT NOT NULL,
        password          TEXT NOT NULL,
        role              TEXT DEFAULT 'user',
        credits           INTEGER DEFAULT 10,
        skills            TEXT DEFAULT '[]',
        skill_data        TEXT DEFAULT '{}',
        ratings           TEXT DEFAULT '[]',
        avg_rating        REAL DEFAULT 0.0,
        completed_services INTEGER DEFAULT 0,
        provider_status   TEXT DEFAULT 'Beginner',
        legitimacy_score  INTEGER DEFAULT 0,
        reset_count       INTEGER DEFAULT 0,
        stuck_since       REAL DEFAULT NULL,
        last_allowance    REAL DEFAULT NULL,
        profile_links     TEXT DEFAULT '{}'
    );

    CREATE TABLE IF NOT EXISTS requests (
        id            TEXT PRIMARY KEY,
        skill         TEXT NOT NULL,
        requester_id  TEXT NOT NULL,
        provider_id   TEXT DEFAULT NULL,
        priority      TEXT DEFAULT 'Normal',
        note          TEXT DEFAULT '',
        duration      TEXT DEFAULT '',
        credit_cost   INTEGER DEFAULT 1,
        status        TEXT DEFAULT 'Pending',
        timestamp     REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS connections (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        requester_id  TEXT NOT NULL,
        provider_id   TEXT NOT NULL,
        skill         TEXT NOT NULL,
        request_id    TEXT NOT NULL,
        status        TEXT DEFAULT 'Active'
    );

    CREATE TABLE IF NOT EXISTS escrow (
        request_id            TEXT PRIMARY KEY,
        amount                INTEGER NOT NULL,
        status                TEXT DEFAULT 'held',
        requester_confirmed   INTEGER DEFAULT 0,
        provider_confirmed    INTEGER DEFAULT 0,
        timestamp             REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS chats (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        request_id    TEXT NOT NULL,
        sender        TEXT NOT NULL,
        sender_name   TEXT NOT NULL,
        text          TEXT NOT NULL,
        timestamp     REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS notifications (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        uid       TEXT NOT NULL,
        type      TEXT NOT NULL,
        title     TEXT NOT NULL,
        body      TEXT NOT NULL,
        link      TEXT DEFAULT '',
        read      INTEGER DEFAULT 0,
        timestamp REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS credit_logs (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        uid           TEXT NOT NULL,
        amount        INTEGER NOT NULL,
        direction     TEXT NOT NULL,
        reason        TEXT NOT NULL,
        balance_after INTEGER NOT NULL,
        timestamp     REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS reviews (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        provider_id   TEXT NOT NULL,
        reviewer_id   TEXT NOT NULL,
        reviewer_name TEXT NOT NULL,
        skill         TEXT NOT NULL,
        rating        INTEGER DEFAULT 0,
        text          TEXT DEFAULT '',
        req_id        TEXT DEFAULT NULL,
        timestamp     REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS request_counter (
        id  INTEGER PRIMARY KEY CHECK (id = 1),
        val INTEGER DEFAULT 0
    );
    INSERT OR IGNORE INTO request_counter (id, val) VALUES (1, 0);
    """)

    # Create admin if not exists
    existing = c.execute("SELECT id FROM users WHERE id = 'admin'").fetchone()
    if not existing:
        c.execute("""
            INSERT INTO users (id, name, password, role, credits, skills, skill_data,
                               ratings, avg_rating, completed_services, provider_status,
                               legitimacy_score, reset_count, profile_links)
            VALUES ('admin','Admin','admin123','admin',0,'[]','{}','[]',0.0,0,'Trusted',0,0,'{}')
        """)

    conn.commit()
    conn.close()


# ---------------------------
# USER HELPERS
# ---------------------------

def get_user(uid):
    conn = get_db()
    row = conn.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()
    conn.close()
    if not row:
        return None
    u = dict(row)
    u["skills"]       = json.loads(u["skills"])
    u["skill_data"]   = json.loads(u["skill_data"])
    u["ratings"]      = json.loads(u["ratings"])
    u["profile_links"] = json.loads(u["profile_links"])
    return u


def save_user(u):
    conn = get_db()
    conn.execute("""
        UPDATE users SET
            name=?, password=?, role=?, credits=?, skills=?, skill_data=?,
            ratings=?, avg_rating=?, completed_services=?, provider_status=?,
            legitimacy_score=?, reset_count=?, stuck_since=?, last_allowance=?,
            profile_links=?
        WHERE id=?
    """, (
        u["name"], u["password"], u["role"], u["credits"],
        json.dumps(u["skills"]), json.dumps(u["skill_data"]),
        json.dumps(u["ratings"]), u["avg_rating"], u["completed_services"],
        u["provider_status"], u["legitimacy_score"], u["reset_count"],
        u["stuck_since"], u["last_allowance"],
        json.dumps(u["profile_links"]), u["id"]
    ))
    conn.commit()
    conn.close()


def get_all_users(exclude_admin=True):
    conn = get_db()
    if exclude_admin:
        rows = conn.execute("SELECT * FROM users WHERE role != 'admin'").fetchall()
    else:
        rows = conn.execute("SELECT * FROM users").fetchall()
    conn.close()
    result = []
    for row in rows:
        u = dict(row)
        u["skills"]       = json.loads(u["skills"])
        u["skill_data"]   = json.loads(u["skill_data"])
        u["ratings"]      = json.loads(u["ratings"])
        u["profile_links"] = json.loads(u["profile_links"])
        result.append(u)
    return result


def next_request_id():
    conn = get_db()
    conn.execute("UPDATE request_counter SET val = val + 1 WHERE id = 1")
    val = conn.execute("SELECT val FROM request_counter WHERE id = 1").fetchone()["val"]
    conn.commit()
    conn.close()
    return f"REQ{val:04d}"


# ---------------------------
# NOTIFICATION + CREDIT LOG HELPERS
# ---------------------------

def add_notification(uid, ntype, title, body, link=""):
    conn = get_db()
    conn.execute(
        "INSERT INTO notifications (uid, type, title, body, link, read, timestamp) VALUES (?,?,?,?,?,0,?)",
        (uid, ntype, title, body, link, time.time())
    )
    conn.commit()
    conn.close()


def unread_count(uid):
    conn = get_db()
    count = conn.execute(
        "SELECT COUNT(*) as c FROM notifications WHERE uid=? AND read=0", (uid,)
    ).fetchone()["c"]
    conn.close()
    return count


def log_credit(uid, amount, direction, reason, balance_after):
    conn = get_db()
    conn.execute(
        "INSERT INTO credit_logs (uid, amount, direction, reason, balance_after, timestamp) VALUES (?,?,?,?,?,?)",
        (uid, amount, direction, reason, balance_after, time.time())
    )
    conn.commit()
    conn.close()


# ---------------------------
# LEGITIMACY HELPERS
# ---------------------------

def check_and_verify_skill(uid, skill):
    u = get_user(uid)
    if not u:
        return
    sd = u["skill_data"].setdefault(skill, {
        "verified": False, "proofs": [], "test_score": None,
        "endorsements": [], "bonus_credits": 0
    })
    passed_test   = (sd.get("test_score") or 0) >= 70
    bonus_hit     = sd.get("bonus_credits", 0) >= 20
    file_approved = any(
        p.get("type") == "file" and p.get("approved") is True
        for p in sd.get("proofs", [])
    )
    sd["verified"] = passed_test or bonus_hit or file_approved
    if skill not in u["skills"]:
        u["skills"].append(skill)
    save_user(u)


def calculate_legitimacy_score(uid, skill=None):
    u = get_user(uid)
    if not u:
        return 0
    if skill:
        sd = u.get("skill_data", {}).get(skill, {})
        verified_pts = 20 if sd.get("verified") else 0
        endorse_pts  = min(len(sd.get("endorsements", [])), 10) * 2
    else:
        verified_pts = 20 if any(
            sd.get("verified") for sd in u.get("skill_data", {}).values()
        ) else 0
        all_endorse = sum(
            len(sd.get("endorsements", [])) for sd in u.get("skill_data", {}).values()
        )
        endorse_pts = min(all_endorse, 10) * 2

    rating_pts     = round(u.get("avg_rating", 0) * 8)
    experience_pts = min(u.get("completed_services", 0), 10) * 3
    score = verified_pts + rating_pts + experience_pts + endorse_pts
    if u.get("provider_status") == "Beginner":
        score = max(0, score - 15)
    return score


def update_avg_rating(uid):
    u = get_user(uid)
    if not u or not u["ratings"]:
        return
    u["avg_rating"]       = round(sum(u["ratings"]) / len(u["ratings"]), 2)
    u["legitimacy_score"] = calculate_legitimacy_score(uid)
    save_user(u)


def check_provider_graduation(uid):
    u = get_user(uid)
    if not u or u.get("provider_status") not in ("Beginner", "Stuck"):
        return
    completed = u.get("completed_services", 0)
    avg       = u.get("avg_rating", 0)
    if completed >= BEGINNER_SERVICE_LIMIT and avg >= BEGINNER_MIN_RATING:
        u["provider_status"] = "Trusted"
        u["stuck_since"]     = None
        save_user(u)
        add_notification(uid, "system", "🎉 You are now a Trusted Provider!",
                         "You completed 3 sessions with rating ≥3.0. Graduated from Beginner.", "/dashboard")
        return
    if completed >= BEGINNER_SERVICE_LIMIT and avg < BEGINNER_MIN_RATING:
        if u.get("provider_status") != "Stuck":
            u["provider_status"] = "Stuck"
            u["stuck_since"]     = time.time()
            save_user(u)


def is_eligible_for_reset(uid):
    u = get_user(uid)
    if not u or u.get("provider_status") != "Stuck":
        return False, "Not stuck"
    if u.get("reset_count", 0) >= MAX_RESETS:
        return False, f"Maximum {MAX_RESETS} resets already used"
    stuck_since = u.get("stuck_since")
    if not stuck_since:
        return False, "No stuck timestamp recorded"
    days_stuck = (time.time() - stuck_since) / 86400
    if days_stuck < RESET_COOLDOWN_DAYS:
        days_left = int(RESET_COOLDOWN_DAYS - days_stuck)
        return False, f"{days_left} days remaining before reset is available"
    return True, "Eligible"


def apply_provider_reset(uid):
    u = get_user(uid)
    if not u:
        return False, "User not found"
    eligible, reason = is_eligible_for_reset(uid)
    if not eligible:
        return False, reason
    u["provider_status"]    = "Beginner"
    u["completed_services"] = 0
    u["ratings"]            = []
    u["avg_rating"]         = 0.0
    u["reset_count"]        = u.get("reset_count", 0) + 1
    u["stuck_since"]        = None
    u["legitimacy_score"]   = calculate_legitimacy_score(uid)
    save_user(u)
    return True, f"Reset #{u['reset_count']} applied. You have 3 fresh sessions."


def apply_weekly_allowance(uid):
    u = get_user(uid)
    if not u:
        return False
    if u.get("provider_status") != "Stuck":
        return False
    if u.get("reset_count", 0) < MAX_RESETS:
        return False
    last = u.get("last_allowance")
    if last and (time.time() - last) < (WEEKLY_ALLOWANCE_DAYS * 86400):
        return False
    u["credits"]       += WEEKLY_ALLOWANCE
    u["last_allowance"] = time.time()
    save_user(u)
    log_credit(uid, WEEKLY_ALLOWANCE, "in", "Weekly credit allowance", u["credits"])
    add_notification(uid, "credit", "Weekly credits added",
                     f"+{WEEKLY_ALLOWANCE} credits — weekly allowance.", "/dashboard")
    return True


# ---------------------------
# PRIORITY QUEUE
# ---------------------------

def get_all_requests():
    conn = get_db()
    rows = conn.execute("SELECT * FROM requests").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_request(req_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM requests WHERE id=?", (req_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def save_request(r):
    conn = get_db()
    conn.execute("""
        UPDATE requests SET skill=?, requester_id=?, provider_id=?, priority=?,
        note=?, duration=?, credit_cost=?, status=?, timestamp=? WHERE id=?
    """, (r["skill"], r["requester_id"], r["provider_id"], r["priority"],
          r["note"], r["duration"], r["credit_cost"], r["status"], r["timestamp"], r["id"]))
    conn.commit()
    conn.close()


def calculate_priority(req):
    waiting_factor = (time.time() - req["timestamp"]) / 60
    u       = get_user(req["requester_id"])
    credits = u["credits"] if u else 10
    bonus   = PRIORITY_BONUS.get(req.get("priority", "Normal"), 5)
    return (100 - credits) + waiting_factor + bonus


def get_priority_queue():
    heap = []
    for req in get_all_requests():
        if req["status"] == "Pending":
            heapq.heappush(heap, (-calculate_priority(req), req["id"]))
    return heap


# ---------------------------
# CONNECTIONS
# ---------------------------

def get_all_connections():
    conn = get_db()
    rows = conn.execute("SELECT * FROM connections").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_connection(requester_id, provider_id, skill, request_id):
    conn = get_db()
    conn.execute(
        "INSERT INTO connections (requester_id, provider_id, skill, request_id, status) VALUES (?,?,?,?,'Active')",
        (requester_id, provider_id, skill, request_id)
    )
    conn.commit()
    conn.close()


def update_connection_status(request_id, status):
    conn = get_db()
    conn.execute("UPDATE connections SET status=? WHERE request_id=?", (status, request_id))
    conn.commit()
    conn.close()


# ---------------------------
# PROVIDER FINDING & MATCHING
# ---------------------------

def find_providers(skill):
    connections = get_all_connections()
    busy = set(c["provider_id"] for c in connections if c["status"] == "Active")
    result = []
    for u in get_all_users():
        if u["role"] == "admin" or skill not in u["skills"] or u["id"] in busy:
            continue
        status = u.get("provider_status")
        if status == "Stuck":
            apply_weekly_allowance(u["id"])
            continue
        if status == "Beginner" and u.get("completed_services", 0) >= BEGINNER_SERVICE_LIMIT:
            continue
        result.append(u["id"])
    return sorted(result, key=lambda uid: calculate_legitimacy_score(uid, skill), reverse=True)


def bipartite_match(pending_requests):
    matches = {}
    def dfs(req_id, skill, visited):
        for p in find_providers(skill):
            if p not in visited:
                visited.add(p)
                req = get_request(matches[p]) if p in matches else None
                if p not in matches or dfs(matches[p], req["skill"] if req else skill, visited):
                    matches[p] = req_id
                    return True
        return False
    for _, rid in pending_requests:
        req = get_request(rid)
        if req:
            dfs(rid, req["skill"], set())
    result, used = [], set()
    for p_id, r_id in matches.items():
        if r_id not in used:
            result.append((r_id, p_id))
            used.add(r_id)
    return result


def run_matching():
    matches = bipartite_match(get_priority_queue())
    match_details = []
    for req_id, provider_id in matches:
        req = get_request(req_id)
        if not req:
            continue
        req["status"]      = "Assigned"
        req["provider_id"] = provider_id
        save_request(req)
        add_connection(req["requester_id"], provider_id, req["skill"], req_id)
        lock_credits_in_escrow(req_id)
        add_notification(req["requester_id"], "match", "Provider matched!",
                         f"Matched with {provider_id} for {req['skill']}. Chat is open.", f"/chat/{req_id}")
        add_notification(provider_id, "match", "New session assigned",
                         f"Assigned to teach {req['skill']} to {req['requester_id']}. Chat is open.", f"/chat/{req_id}")
        match_details.append({"requester": req["requester_id"], "provider": provider_id, "skill": req["skill"]})
    return matches, match_details


# ---------------------------
# ESCROW
# ---------------------------

def get_escrow(req_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM escrow WHERE request_id=?", (req_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def lock_credits_in_escrow(req_id):
    req = get_request(req_id)
    if not req:
        return False, "Request not found"
    u   = get_user(req["requester_id"])
    amt = req.get("credit_cost", 1)
    if not u or u["credits"] < amt:
        return False, "Insufficient credits"
    u["credits"] -= amt
    save_user(u)
    conn = get_db()
    conn.execute(
        "INSERT INTO escrow (request_id, amount, status, requester_confirmed, provider_confirmed, timestamp) VALUES (?,?,'held',0,0,?)",
        (req_id, amt, time.time())
    )
    conn.commit()
    conn.close()
    log_credit(req["requester_id"], amt, "hold",
               f"Escrowed for {req['skill']} session ({req_id})", u["credits"])
    return True, "Credits held in escrow"


def confirm_completion(req_id, confirming_uid):
    esc = get_escrow(req_id)
    req = get_request(req_id)
    if not esc or esc["status"] != "held":
        return False, "No active escrow"
    conn = get_db()
    if confirming_uid == req["requester_id"]:
        conn.execute("UPDATE escrow SET requester_confirmed=1 WHERE request_id=?", (req_id,))
    elif confirming_uid == req["provider_id"]:
        conn.execute("UPDATE escrow SET provider_confirmed=1 WHERE request_id=?", (req_id,))
    else:
        conn.close()
        return False, "Not a participant"
    conn.commit()
    conn.close()
    esc = get_escrow(req_id)
    if esc["requester_confirmed"] and esc["provider_confirmed"]:
        release_escrow(req_id)
        return True, "released"
    return True, "partial"


def release_escrow(req_id):
    esc = get_escrow(req_id)
    req = get_request(req_id)
    if not esc or not req:
        return
    provider = get_user(req["provider_id"])
    if provider:
        provider["credits"]            += esc["amount"]
        provider["completed_services"]  = provider.get("completed_services", 0) + 1
        save_user(provider)
        check_provider_graduation(req["provider_id"])
        log_credit(req["provider_id"], esc["amount"], "in",
                   f"Payment for {req['skill']} session ({req_id})", provider["credits"])
        add_notification(req["provider_id"], "credit", "Credits received",
                         f"+{esc['amount']} credits for completing {req['skill']} session", "/dashboard")
    requester = get_user(req["requester_id"])
    if requester:
        add_notification(req["requester_id"], "credit", "Session completed",
                         f"Your {req['skill']} session is complete. Credits released.", "/dashboard")
    conn = get_db()
    conn.execute("UPDATE escrow SET status='released' WHERE request_id=?", (req_id,))
    conn.commit()
    conn.close()
    req["status"] = "Completed"
    save_request(req)
    update_connection_status(req_id, "Completed")


def refund_escrow(req_id):
    esc = get_escrow(req_id)
    req = get_request(req_id)
    if not esc or esc["status"] != "held":
        return False, "Cannot refund"
    requester = get_user(req["requester_id"])
    if requester:
        requester["credits"] += esc["amount"]
        save_user(requester)
        log_credit(req["requester_id"], esc["amount"], "refund",
                   f"Dispute refund for {req['skill']} session ({req_id})", requester["credits"])
        add_notification(req["requester_id"], "credit", "Credits refunded",
                         f"+{esc['amount']} credits refunded after dispute on {req['skill']} session", "/dashboard")
    prov = get_user(req.get("provider_id") or "")
    if prov:
        add_notification(req["provider_id"], "system", "Session disputed",
                         f"The requester raised a dispute for the {req['skill']} session.", "/dashboard")
    conn = get_db()
    conn.execute("UPDATE escrow SET status='refunded' WHERE request_id=?", (req_id,))
    conn.commit()
    conn.close()
    req["status"] = "Disputed"
    save_request(req)
    update_connection_status(req_id, "Disputed")
    return True, "Refunded"


# ---------------------------
# ROUTES — AUTH
# ---------------------------

@app.route("/", methods=["GET", "POST"])
def login():
    if "user_id" in session:
        return redirect(url_for("dashboard"))
    error = None
    if request.method == "POST":
        uid = request.form.get("username")
        pwd = request.form.get("password")
        u   = get_user(uid)
        if u and u.get("password") == pwd:
            session["user_id"] = uid
            return redirect(url_for("dashboard"))
        error = "Invalid credentials. Try again."
    return render_template("index.html", logged_in=False, page="login", error=error)


@app.route("/register", methods=["GET", "POST"])
def register():
    if "user_id" in session:
        return redirect(url_for("dashboard"))
    error = success = None
    if request.method == "POST":
        uid        = request.form.get("username", "").strip()
        name       = request.form.get("name", "").strip()
        pwd        = request.form.get("password", "").strip()
        raw_skills = [s.strip() for s in request.form.get("skills", "").split(",") if s.strip()]
        portfolio  = request.form.get("portfolio", "").strip()
        if not uid or not name or not pwd:
            error = "All fields are required."
        elif get_user(uid):
            error = f"Username '{uid}' is already taken."
        elif len(pwd) < 4:
            error = "Password must be at least 4 characters."
        else:
            skill_data = {}
            for sk in raw_skills:
                skill_data[sk] = {"verified": False, "proofs": [], "test_score": None,
                                   "endorsements": [], "bonus_credits": 0}
            conn = get_db()
            conn.execute("""
                INSERT INTO users (id, name, password, role, credits, skills, skill_data,
                                   ratings, avg_rating, completed_services, provider_status,
                                   legitimacy_score, reset_count, stuck_since, last_allowance, profile_links)
                VALUES (?,?,?,'user',10,?,?,'[]',0.0,0,'Beginner',0,0,NULL,NULL,?)
            """, (uid, name, pwd, json.dumps(raw_skills), json.dumps(skill_data),
                  json.dumps({"portfolio": portfolio} if portfolio else {})))
            conn.commit()
            conn.close()
            success = f"Account created! You can now sign in as '{uid}'."
    return render_template("index.html", logged_in=False, page="register", error=error, success=success)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ---------------------------
# DASHBOARD
# ---------------------------

@app.route("/dashboard")
def dashboard():
    if "user_id" not in session:
        return redirect(url_for("login"))
    uid = session["user_id"]
    u   = get_user(uid)
    if u is None:
        session.clear()
        return redirect(url_for("login"))

    if u["role"] == "admin":
        all_users   = get_all_users()
        all_reqs    = get_all_requests()
        all_conns   = get_all_connections()
        pending_count   = sum(1 for r in all_reqs if r["status"] == "Pending")
        assigned_count  = sum(1 for r in all_reqs if r["status"] == "Assigned")
        completed_count = sum(1 for r in all_reqs if r["status"] == "Completed")

        heap = get_priority_queue()
        pq_items = []
        for neg_score, rid in heap:
            req = get_request(rid)
            pq_items.append({"id": rid, "skill": req["skill"],
                             "requester": req["requester_id"], "priority": round(-neg_score, 1)})

        req_details = []
        for r in all_reqs:
            esc = get_escrow(r["id"]) or {}
            req_details.append({"id": r["id"], "skill": r["skill"],
                                 "requester": r["requester_id"],
                                 "provider": r["provider_id"] or "—",
                                 "status": r["status"],
                                 "escrow": esc.get("status", "none"),
                                 "credits": esc.get("amount", 0)})

        conn_details = [{"requester": c["requester_id"], "provider": c["provider_id"],
                          "skill": c["skill"], "status": c["status"]} for c in all_conns]

        pending_proofs = []
        for usr in get_all_users():
            for sk, sd in usr.get("skill_data", {}).items():
                for p in sd.get("proofs", []):
                    if p.get("type") == "file" and p.get("approved") is None:
                        pending_proofs.append({"uid": usr["id"], "name": usr["name"],
                                               "skill": sk, "filename": p.get("filename", ""),
                                               "mime": p.get("mime", ""), "index": p.get("index", 0)})

        return render_template("index.html", logged_in=True, user=u,
            users_count=len(all_users), pending_count=pending_count,
            assigned_count=assigned_count, completed_count=completed_count,
            connections_count=len(all_conns),
            all_users=all_users, pq_items=pq_items, req_details=req_details,
            conn_details=conn_details, pending_proofs=pending_proofs,
            legitimacy_score=0, unread_notifs=0, my_requests=[], my_connections=[],
            skill_display=[], reset_eligible=False, reset_reason="",
            resets_left=0, days_until_reset=0, my_reviews=[])

    # Regular user
    all_conns = get_all_connections()
    my_requests = []
    conn = get_db()
    req_rows = conn.execute("SELECT * FROM requests WHERE requester_id=?", (uid,)).fetchall()
    conn.close()
    for r in req_rows:
        r = dict(r)
        esc = get_escrow(r["id"]) or {}
        my_requests.append({"id": r["id"], "skill": r["skill"],
                             "provider": r["provider_id"] or "—",
                             "status": r["status"],
                             "escrow": esc.get("status", "none"),
                             "req_confirmed": bool(esc.get("requester_confirmed", 0)),
                             "prov_confirmed": bool(esc.get("provider_confirmed", 0))})

    my_connections = []
    for c in all_conns:
        if c["requester_id"] == uid:
            my_connections.append({"other": c["provider_id"], "skill": c["skill"],
                                    "status": c["status"], "role": "requester",
                                    "request_id": c["request_id"]})
        elif c["provider_id"] == uid:
            my_connections.append({"other": c["requester_id"], "skill": c["skill"],
                                    "status": c["status"], "role": "provider",
                                    "request_id": c["request_id"]})

    skill_display = []
    for sk in u["skills"]:
        sd = u["skill_data"].get(sk, {})
        skill_display.append({"name": sk, "verified": sd.get("verified", False),
                               "proofs": sd.get("proofs", []), "test_score": sd.get("test_score"),
                               "endorsements": sd.get("endorsements", []),
                               "bonus_credits": sd.get("bonus_credits", 0)})

    reset_eligible, reset_reason = is_eligible_for_reset(uid)
    resets_left = MAX_RESETS - u.get("reset_count", 0)
    days_until_reset = 0
    if u.get("provider_status") == "Stuck" and u.get("stuck_since"):
        days_stuck = (time.time() - u["stuck_since"]) / 86400
        days_until_reset = max(0, int(RESET_COOLDOWN_DAYS - days_stuck))

    conn = get_db()
    rev_rows = conn.execute(
        "SELECT * FROM reviews WHERE provider_id=? ORDER BY timestamp DESC", (uid,)
    ).fetchall()
    conn.close()
    my_reviews = [dict(r) for r in rev_rows]

    return render_template("index.html", logged_in=True, user=u,
        my_requests=my_requests, my_connections=my_connections,
        skill_display=skill_display,
        legitimacy_score=calculate_legitimacy_score(uid),
        reset_eligible=reset_eligible, reset_reason=reset_reason,
        resets_left=resets_left, days_until_reset=days_until_reset,
        my_reviews=my_reviews,
        users_count=0, pending_count=0, assigned_count=0,
        completed_count=0, connections_count=0,
        all_users=[], pq_items=[], req_details=[], conn_details=[],
        pending_proofs=[], unread_notifs=unread_count(uid))


# ---------------------------
# API ROUTES
# ---------------------------

@app.route("/request-skill", methods=["POST"])
def request_skill():
    if "user_id" not in session:
        return jsonify({"success": False, "error": "Not logged in"}), 401
    uid = session["user_id"]
    u   = get_user(uid)
    if not u or u["role"] == "admin":
        return jsonify({"success": False, "error": "Not allowed"}), 403
    data     = request.get_json()
    skill    = data.get("skill", "").strip()
    priority = data.get("priority", "Normal")
    note     = data.get("note", "").strip()
    duration = data.get("duration", "").strip()
    cost     = int(data.get("credit_cost", 1))
    if not skill:
        return jsonify({"success": False, "error": "Skill required"}), 400
    if u["credits"] < cost:
        return jsonify({"success": False, "error": "Insufficient credits"}), 400
    req_id = next_request_id()
    conn = get_db()
    conn.execute("""
        INSERT INTO requests (id, skill, requester_id, provider_id, priority, note,
                              duration, credit_cost, status, timestamp)
        VALUES (?,?,?,NULL,?,?,?,?,'Pending',?)
    """, (req_id, skill, uid, priority, note, duration, cost, time.time()))
    conn.commit()
    conn.close()
    return jsonify({"success": True, "id": req_id})


@app.route("/update-skills", methods=["POST"])
def update_skills():
    if "user_id" not in session:
        return jsonify({"success": False, "error": "Not logged in"}), 401
    uid = session["user_id"]
    u   = get_user(uid)

    if request.content_type and "multipart" in request.content_type:
        skills_raw     = [s.strip() for s in request.form.get("skills", "").split(",") if s.strip()]
        portfolio      = request.form.get("portfolio", "").strip()
        skill_for_file = request.form.get("skill_for_file", "").strip()
        uploaded_file  = request.files.get("proof_file")
    else:
        data           = request.get_json() or {}
        skills_raw     = [s.strip() for s in data.get("skills", "").split(",") if s.strip()]
        portfolio      = data.get("portfolio", "").strip()
        skill_for_file = ""
        uploaded_file  = None

    for sk in skills_raw:
        u["skill_data"].setdefault(sk, {"verified": False, "proofs": [], "test_score": None,
                                         "endorsements": [], "bonus_credits": 0})

    if portfolio:
        u.setdefault("profile_links", {})["portfolio"] = portfolio

    if uploaded_file and skill_for_file:
        ext = uploaded_file.filename.rsplit(".", 1)[-1].lower() if "." in uploaded_file.filename else ""
        if ext not in ALLOWED_EXTENSIONS:
            return jsonify({"success": False, "error": f"File type .{ext} not allowed"}), 400
        file_bytes = uploaded_file.read()
        if len(file_bytes) > MAX_FILE_BYTES:
            return jsonify({"success": False, "error": "File too large. Max 5 MB."}), 400
        b64  = base64.b64encode(file_bytes).decode("utf-8")
        mime = "application/pdf" if ext == "pdf" else f"image/{ext}"
        sd   = u["skill_data"].setdefault(skill_for_file, {"verified": False, "proofs": [],
                                                             "test_score": None, "endorsements": [],
                                                             "bonus_credits": 0})
        proof_index = len(sd["proofs"])
        sd["proofs"].append({"type": "file", "filename": uploaded_file.filename, "mime": mime,
                              "data": b64, "index": proof_index, "submitted_at": time.time()})
        if skill_for_file not in u["skills"]:
            u["skills"].append(skill_for_file)

    u["skills"] = skills_raw if skills_raw else u["skills"]
    save_user(u)
    for sk in u["skills"]:
        check_and_verify_skill(uid, sk)
    return jsonify({"success": True, "skills": u["skills"]})


@app.route("/proof-file/<uid>/<skill>/<int:index>")
def proof_file(uid, skill, index):
    u = get_user(uid)
    if not u:
        return "User not found", 404
    sd     = u.get("skill_data", {}).get(skill, {})
    proofs = [p for p in sd.get("proofs", []) if p.get("type") == "file" and p.get("index") == index]
    if not proofs:
        return "File not found", 404
    proof      = proofs[0]
    file_bytes = base64.b64decode(proof["data"])
    return Response(file_bytes, mimetype=proof["mime"],
                    headers={"Content-Disposition": f'inline; filename="{proof["filename"]}"'})


@app.route("/skill-test", methods=["POST"])
def skill_test():
    if "user_id" not in session:
        return jsonify({"success": False, "error": "Not logged in"}), 401
    uid   = session["user_id"]
    u     = get_user(uid)
    data  = request.get_json()
    skill = data.get("skill", "").strip()
    score = float(data.get("score", 0))
    sd    = u["skill_data"].setdefault(skill, {"verified": False, "proofs": [], "test_score": None,
                                                "endorsements": [], "bonus_credits": 0})
    sd["test_score"] = score
    bonus_earned = 0
    if score >= 70:
        bonus_earned = 5 if score >= 90 else 3 if score >= 80 else 1
        sd["bonus_credits"] = sd.get("bonus_credits", 0) + bonus_earned
        u["credits"] += bonus_earned
        log_credit(uid, bonus_earned, "in", f"Skill test bonus — {skill} ({int(score)}%)", u["credits"])
    save_user(u)
    check_and_verify_skill(uid, skill)
    u = get_user(uid)
    verified = u["skill_data"].get(skill, {}).get("verified", False)
    u["legitimacy_score"] = calculate_legitimacy_score(uid)
    save_user(u)
    if verified and score >= 70:
        add_notification(uid, "verified", f"⭐ {skill} verified!",
                         f"You scored {int(score)}% — {skill} is now verified.", "/dashboard")
    return jsonify({"success": True, "score": score, "verified": verified, "bonus": bonus_earned})


@app.route("/endorse", methods=["POST"])
def endorse():
    if "user_id" not in session:
        return jsonify({"success": False, "error": "Not logged in"}), 401
    endorser = session["user_id"]
    data     = request.get_json()
    target   = data.get("provider_id", "")
    skill    = data.get("skill", "")
    u = get_user(target)
    if not u:
        return jsonify({"success": False, "error": "User not found"}), 404
    if target == endorser:
        return jsonify({"success": False, "error": "Cannot endorse yourself"}), 400
    sd = u["skill_data"].setdefault(skill, {"verified": False, "proofs": [], "test_score": None,
                                             "endorsements": [], "bonus_credits": 0})
    if endorser in sd.get("endorsements", []):
        return jsonify({"success": False, "error": "Already endorsed"}), 400
    sd.setdefault("endorsements", []).append(endorser)
    sd["bonus_credits"] = sd.get("bonus_credits", 0) + 2
    save_user(u)
    check_and_verify_skill(target, skill)
    u = get_user(target)
    u["legitimacy_score"] = calculate_legitimacy_score(target)
    save_user(u)
    ename = get_user(endorser)["name"] if get_user(endorser) else endorser

    review_text = (data.get("review_text") or "").strip()
    rating      = data.get("rating")
    if review_text or rating:
        conn = get_db()
        conn.execute("""
            INSERT INTO reviews (provider_id, reviewer_id, reviewer_name, skill, rating, text, req_id, timestamp)
            VALUES (?,?,?,?,?,?,NULL,?)
        """, (target, endorser, ename, skill, int(rating) if rating else 0, review_text, time.time()))
        conn.commit()
        conn.close()
        if review_text:
            add_notification(target, "verified", f"New review from {ename}",
                             f'"{review_text[:60]}{"..." if len(review_text) > 60 else ""}"', "/dashboard")

    add_notification(target, "verified", f"New endorsement for {skill}",
                     f"{ename} endorsed your {skill} skill. ({len(sd['endorsements'])} total)", "/dashboard")
    if sd.get("verified"):
        add_notification(target, "verified", f"⭐ {skill} verified!",
                         f"10 endorsements reached — {skill} is now verified.", "/dashboard")
    return jsonify({"success": True, "endorsements": len(sd["endorsements"])})


@app.route("/confirm-completion", methods=["POST"])
def confirm_completion_route():
    if "user_id" not in session:
        return jsonify({"success": False, "error": "Not logged in"}), 401
    uid    = session["user_id"]
    data   = request.get_json()
    req_id = data.get("request_id", "")
    rating = data.get("rating")
    ok, msg = confirm_completion(req_id, uid)
    if not ok:
        return jsonify({"success": False, "error": msg}), 400
    req = get_request(req_id)
    if rating and req and uid == req["requester_id"]:
        provider = get_user(req["provider_id"])
        if provider:
            provider["ratings"].append(int(rating))
            save_user(provider)
            update_avg_rating(req["provider_id"])
            review_text = (data.get("review_text") or "").strip()
            reviewer    = get_user(uid)
            conn = get_db()
            conn.execute("""
                INSERT INTO reviews (provider_id, reviewer_id, reviewer_name, skill, rating, text, req_id, timestamp)
                VALUES (?,?,?,?,?,?,?,?)
            """, (req["provider_id"], uid, reviewer["name"] if reviewer else uid,
                  req["skill"], int(rating), review_text, req_id, time.time()))
            conn.commit()
            conn.close()
            if review_text:
                stars   = "★" * int(rating)
                preview = review_text[:60] + ("..." if len(review_text) > 60 else "")
                add_notification(req["provider_id"], "verified",
                                 f"New review from {reviewer['name'] if reviewer else uid}",
                                 f"{stars} — \"{preview}\"", "/dashboard")
    return jsonify({"success": True, "status": msg})


@app.route("/dispute", methods=["POST"])
def dispute():
    if "user_id" not in session:
        return jsonify({"success": False, "error": "Not logged in"}), 401
    uid    = session["user_id"]
    data   = request.get_json()
    req_id = data.get("request_id", "")
    req    = get_request(req_id)
    if not req or req["requester_id"] != uid:
        return jsonify({"success": False, "error": "Not allowed"}), 403
    ok, msg = refund_escrow(req_id)
    return jsonify({"success": ok, "message": msg})


@app.route("/request-reset", methods=["POST"])
def request_reset():
    if "user_id" not in session:
        return jsonify({"success": False, "error": "Not logged in"}), 401
    uid = session["user_id"]
    eligible, reason = is_eligible_for_reset(uid)
    if not eligible:
        return jsonify({"success": False, "error": reason}), 400
    ok, msg = apply_provider_reset(uid)
    return jsonify({"success": ok, "message": msg})


@app.route("/approve-proof", methods=["POST"])
def approve_proof():
    if "user_id" not in session:
        return jsonify({"success": False, "error": "Not logged in"}), 401
    admin = get_user(session["user_id"])
    if admin["role"] != "admin":
        return jsonify({"success": False, "error": "Admins only"}), 403
    data       = request.get_json()
    target_uid = data.get("uid", "")
    skill      = data.get("skill", "")
    proof_idx  = data.get("index", -1)
    action     = data.get("action", "approve")
    u = get_user(target_uid)
    if not u:
        return jsonify({"success": False, "error": "User not found"}), 404
    sd = u.get("skill_data", {}).get(skill, {})
    target = next((p for p in sd.get("proofs", []) if p.get("type") == "file"
                   and p.get("index") == proof_idx), None)
    if not target:
        return jsonify({"success": False, "error": "Proof not found"}), 404
    target["approved"]    = (action == "approve")
    target["reviewed_by"] = session["user_id"]
    save_user(u)
    check_and_verify_skill(target_uid, skill)
    u = get_user(target_uid)
    u["legitimacy_score"] = calculate_legitimacy_score(target_uid)
    save_user(u)
    if action == "approve":
        add_notification(target_uid, "verified", f"Proof approved for {skill}",
                         f"Your proof for {skill} was approved by admin.", "/dashboard")
        if u["skill_data"].get(skill, {}).get("verified"):
            add_notification(target_uid, "verified", f"⭐ {skill} verified!",
                             f"{skill} is now verified.", "/dashboard")
    else:
        add_notification(target_uid, "system", f"Proof rejected for {skill}",
                         f"Your proof for {skill} was rejected. Upload a clearer document.", "/dashboard")
    return jsonify({"success": True, "verified": u["skill_data"].get(skill, {}).get("verified", False)})


@app.route("/run-matching", methods=["POST"])
def trigger_matching():
    if "user_id" not in session:
        return jsonify({"error": "Not logged in"}), 401
    u = get_user(session["user_id"])
    if u["role"] != "admin":
        return jsonify({"error": "Admins only"}), 403
    matches, details = run_matching()
    return jsonify({"count": len(matches), "matches": details})


# ---------------------------
# NOTIFICATION ROUTES
# ---------------------------

@app.route("/notifications")
def get_notifications():
    if "user_id" not in session:
        return jsonify({"error": "Not logged in"}), 401
    uid = session["user_id"]
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM notifications WHERE uid=? ORDER BY timestamp DESC", (uid,)
    ).fetchall()
    conn.close()
    notifs = [dict(r) for r in rows]
    return jsonify({"notifications": notifs, "unread": unread_count(uid)})


@app.route("/notifications/read", methods=["POST"])
def mark_notifications_read():
    if "user_id" not in session:
        return jsonify({"error": "Not logged in"}), 401
    uid  = session["user_id"]
    data = request.get_json()
    conn = get_db()
    if data.get("all"):
        conn.execute("UPDATE notifications SET read=1 WHERE uid=?", (uid,))
    elif data.get("id") is not None:
        conn.execute("UPDATE notifications SET read=1 WHERE uid=? AND id=?", (uid, data["id"]))
    conn.commit()
    conn.close()
    return jsonify({"success": True})


@app.route("/notifications/clear", methods=["POST"])
def clear_notifications():
    if "user_id" not in session:
        return jsonify({"error": "Not logged in"}), 401
    conn = get_db()
    conn.execute("DELETE FROM notifications WHERE uid=?", (session["user_id"],))
    conn.commit()
    conn.close()
    return jsonify({"success": True})


@app.route("/notifications/count")
def notifications_count():
    if "user_id" not in session:
        return jsonify({"count": 0})
    return jsonify({"count": unread_count(session["user_id"])})


# ---------------------------
# CREDIT LOG ROUTES
# ---------------------------

@app.route("/credits/history")
def credit_history():
    if "user_id" not in session:
        return jsonify({"error": "Not logged in"}), 401
    uid = session["user_id"]
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM credit_logs WHERE uid=? ORDER BY timestamp DESC", (uid,)
    ).fetchall()
    conn.close()
    logs = [dict(r) for r in rows]
    u    = get_user(uid)
    return jsonify({"logs": logs, "balance": u["credits"]})


# ---------------------------
# GEMINI DEBUG ROUTE
# ---------------------------

@app.route("/test-gemini")
def test_gemini():
    import urllib.request, urllib.error
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        return "NO API KEY SET"
    prompt = "Say hello in one word."
    payload = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}]
    }).encode("utf-8")
    try:
        url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=" + api_key
        req = urllib.request.Request(url, data=payload,
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read().decode("utf-8"))
        return "SUCCESS: " + str(result["candidates"][0]["content"]["parts"][0]["text"])
    except urllib.error.HTTPError as e:
        return "HTTP ERROR " + str(e.code) + ": " + e.read().decode("utf-8")
    except Exception as e:
        return "ERROR: " + str(e)

# ---------------------------
# SKILL TEST QUESTIONS ROUTE
# ---------------------------

@app.route("/generate-questions", methods=["POST"])
def generate_questions():
    if "user_id" not in session:
        return jsonify({"success": False, "error": "Not logged in"}), 401
    import urllib.request
    import urllib.error
    data  = request.get_json()
    skill = (data.get("skill") or "").strip()
    if not skill:
        return jsonify({"success": False, "error": "Skill required"}), 400

    prompt = (
        "Generate exactly 3 multiple choice questions to test knowledge of \"" + skill + "\". "
        "Return ONLY valid JSON with no markdown or explanation. "
        "Format: {\"questions\":[{\"q\":\"question\",\"options\":[\"A\",\"B\",\"C\"],\"answer\":0}]} "
        "where answer is the index (0,1,2) of the correct option. "
        "Make questions practical and specific to " + skill + "."
    )

    payload = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}]
    }).encode("utf-8")

    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        return jsonify({"success": False, "error": "No API key configured"}), 500

    try:
        url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=" + api_key
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read().decode("utf-8"))
        text = result["candidates"][0]["content"]["parts"][0]["text"].strip()
        text = text.replace("```json", "").replace("```", "").strip()
        # Find JSON in response
        start = text.find("{")
        end   = text.rfind("}") + 1
        if start != -1 and end > start:
            text = text[start:end]
        parsed = json.loads(text)
        return jsonify({"success": True, "questions": parsed["questions"]})
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        return jsonify({"success": False, "error": "HTTP " + str(e.code) + ": " + body[:200]}), 500
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ---------------------------
# REVIEWS ROUTE
# ---------------------------

@app.route("/reviews/<uid>")
def get_reviews(uid):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM reviews WHERE provider_id=? ORDER BY timestamp DESC", (uid,)
    ).fetchall()
    conn.close()
    rev_list = [dict(r) for r in rows]
    avg = round(sum(r["rating"] for r in rev_list) / len(rev_list), 1) if rev_list else 0.0
    return jsonify({"reviews": rev_list, "count": len(rev_list), "avg": avg})


# ---------------------------
# CHAT ROUTES
# ---------------------------

@app.route("/chat/<req_id>")
def chat_page(req_id):
    if "user_id" not in session:
        return redirect(url_for("login"))
    uid = session["user_id"]
    req = get_request(req_id)
    if not req:
        return "Request not found", 404
    if uid != req["requester_id"] and uid != req.get("provider_id"):
        return "Access denied", 403
    other_uid    = req["provider_id"] if uid == req["requester_id"] else req["requester_id"]
    other_user   = get_user(other_uid)
    current_user = get_user(uid)
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM chats WHERE request_id=? ORDER BY id ASC", (req_id,)
    ).fetchall()
    conn.close()
    messages = [dict(r) for r in rows]
    return render_template("chat.html", req_id=req_id, req=req,
                           current_user=current_user, other_user=other_user,
                           messages=messages, user=current_user)


@app.route("/chat/send/<req_id>", methods=["POST"])
def chat_send(req_id):
    if "user_id" not in session:
        return jsonify({"success": False, "error": "Not logged in"}), 401
    uid = session["user_id"]
    req = get_request(req_id)
    if not req:
        return jsonify({"success": False, "error": "Not found"}), 404
    if uid != req["requester_id"] and uid != req.get("provider_id"):
        return jsonify({"success": False, "error": "Not a participant"}), 403
    data = request.get_json()
    text = data.get("text", "").strip()
    if not text:
        return jsonify({"success": False, "error": "Empty message"}), 400
    if len(text) > 1000:
        return jsonify({"success": False, "error": "Message too long"}), 400
    sender_user = get_user(uid)
    conn = get_db()
    conn.execute(
        "INSERT INTO chats (request_id, sender, sender_name, text, timestamp) VALUES (?,?,?,?,?)",
        (req_id, uid, sender_user["name"], text, time.time())
    )
    msg_id = conn.execute("SELECT last_insert_rowid() as id").fetchone()["id"]
    conn.commit()
    conn.close()
    # Fetch the inserted message
    conn = get_db()
    row = conn.execute("SELECT * FROM chats WHERE id=?", (msg_id,)).fetchone()
    conn.close()
    msg = dict(row)
    # Use rowid as the polling index
    msg["id"] = msg_id - 1  # 0-based for frontend
    return jsonify({"success": True, "message": msg})


@app.route("/chat/poll/<req_id>")
def chat_poll(req_id):
    if "user_id" not in session:
        return jsonify({"success": False, "error": "Not logged in"}), 401
    uid = session["user_id"]
    req = get_request(req_id)
    if not req:
        return jsonify({"success": False, "error": "Not found"}), 404
    if uid != req["requester_id"] and uid != req.get("provider_id"):
        return jsonify({"success": False, "error": "Not a participant"}), 403
    since = int(request.args.get("since", 0))
    conn  = get_db()
    rows  = conn.execute(
        "SELECT * FROM chats WHERE request_id=? ORDER BY id ASC", (req_id,)
    ).fetchall()
    conn.close()
    all_msgs  = [dict(r) for r in rows]
    total     = len(all_msgs)
    new_msgs  = all_msgs[since:]
    # Add 0-based id for frontend
    for i, m in enumerate(all_msgs):
        m["id"] = i
    for i, m in enumerate(new_msgs):
        m["id"] = since + i
    return jsonify({"success": True, "messages": new_msgs, "total": total})


# ---------------------------
# TEST ROUTES (remove before production)
# ---------------------------

@app.route("/test/force-stuck/<uid>")
def force_stuck(uid):
    u = get_user(uid)
    if u:
        u["provider_status"] = "Stuck"
        u["stuck_since"]     = time.time() - (31 * 86400)
        save_user(u)
    return f"Done — {uid} is now stuck and past cooldown"

@app.route("/test/force-perm-stuck/<uid>")
def force_perm_stuck(uid):
    u = get_user(uid)
    if u:
        u["provider_status"] = "Stuck"
        u["reset_count"]     = 2
        u["last_allowance"]  = time.time() - (8 * 86400)
        save_user(u)
    return f"Done — {uid} is permanently stuck"

@app.route("/test/set-credits/<uid>/<int:amount>")
def set_credits(uid, amount):
    u = get_user(uid)
    if u:
        u["credits"] = amount
        save_user(u)
    return f"Done — {uid} now has {amount} credits"


# ---------------------------
# MAIN
# ---------------------------

if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
