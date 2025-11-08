from flask import Flask, render_template, request, redirect, url_for, abort, Response, jsonify
import sqlite3
import os
from datetime import datetime, timezone, timedelta
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Optional AI providers
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')

# Gemini
try:
    import google.generativeai as genai  # type: ignore
    if GEMINI_API_KEY:
        genai.configure(api_key=GEMINI_API_KEY)
except Exception:
    genai = None

# OpenAI
try:
    from openai import OpenAI  # type: ignore
    openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
except Exception:
    openai_client = None

app = Flask(__name__)

@app.route('/')
def home():
    services = fetch_services()
    return render_template('index.html', services=services)

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/services')
def services():
    return render_template('services.html')

@app.route('/gallery')
def gallery():
    return render_template('gallery.html')

@app.route('/contact', methods=['GET', 'POST'])
def contact():
    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        email = request.form.get('email', '').strip()
        dog = request.form.get('dog', '').strip()
        message = request.form.get('message', '').strip()

        # Basic validation
        if not name or not email:
            return render_template('contact.html', error="Please provide your name and email.", form=request.form), 400

        save_enquiry(name, email, dog, message)
        return redirect(url_for('contact', submitted='1'))

    submitted = request.args.get('submitted') == '1'
    return render_template('contact.html', submitted=submitted)


"""Persistence layer
Chooses Postgres (if DATABASE_URL env var is set) otherwise falls back to local SQLite.
Uses SQLAlchemy Core for portability and minimal overhead.
"""

DB_PATH = os.path.join(os.path.dirname(__file__), 'enquiries.db')

def _client_ip():
    xff = request.headers.get('X-Forwarded-For')
    if xff:
        return xff.split(',')[0].strip()
    return request.remote_addr or ''

def _make_engine():
    url = os.environ.get('DATABASE_URL')
    if url:
        # Render sometimes provides a URL starting with postgres:// (deprecated); normalize to postgresql://
        if url.startswith('postgres://'):
            url = url.replace('postgres://', 'postgresql://', 1)
        try:
            return create_engine(url, pool_pre_ping=True)
        except SQLAlchemyError:
            pass  # fallback to sqlite below
    # SQLite fallback for local dev or if Postgres not configured
    return create_engine(f'sqlite:///{DB_PATH}', pool_pre_ping=True)

engine = _make_engine()

def init_db():
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS enquiries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                dog TEXT,
                message TEXT,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'new',
                ip TEXT,
                sid TEXT,
                visit_summary TEXT
            )
        """))
        # In case the table existed before without the new columns, try to add them.
        try:
            conn.execute(text("ALTER TABLE enquiries ADD COLUMN status TEXT NOT NULL DEFAULT 'new'"))
        except Exception:
            pass
        for col in ["ip TEXT", "sid TEXT", "visit_summary TEXT"]:
            try:
                conn.execute(text(f"ALTER TABLE enquiries ADD COLUMN {col}"))
            except Exception:
                pass

        # Basic events table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sid TEXT,
                path TEXT,
                referrer TEXT,
                event TEXT,
                user_agent TEXT,
                ip TEXT,
                created_at TEXT NOT NULL
            )
        """))
        # Cache table for visitor-level AI insights
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS visitor_insights (
                sid TEXT PRIMARY KEY,
                summary TEXT
            )
        """))
        # Live chat tables
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sid TEXT,
                name TEXT,
                status TEXT NOT NULL DEFAULT 'open',
                created_at TEXT NOT NULL,
                last_activity TEXT NOT NULL,
                ip TEXT
            )
        """))
        # Add ip column if it doesn't exist (for existing databases)
        try:
            conn.execute(text("ALTER TABLE chats ADD COLUMN ip TEXT"))
        except Exception:
            pass  # column already exists
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS chat_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                sender TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """))
        # Content management table for editable website content
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS site_content (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                section TEXT NOT NULL,
                key TEXT NOT NULL,
                title TEXT,
                content TEXT,
                price TEXT,
                sort_order INTEGER DEFAULT 0,
                UNIQUE(section, key)
            )
        """))
        # Seed default services if table is empty
        count = conn.execute(text("SELECT COUNT(*) FROM site_content WHERE section='services'")).scalar()
        if count == 0:
            services = [
                ("group-walks", "Group Walks", "From £14/hour", "Professional small group walks for socialization, stimulation and exercise in safe dog-friendly areas.\n• Matched with compatible temperaments\n• Hydration & rest breaks included\n• GPS route & photo update", 1),
                ("solo-walks", "Solo Walks", "£15–£20", "One-on-one focused walks perfect for anxious, reactive or senior dogs needing tailored pacing.\n• 30–60 minute tailored durations\n• Calmer, personalized routes\n• Behaviour notes & progress tracking", 2),
                ("dog-daycare", "Dog Day Care", "£30/day", "A full adventure day with social play, 2 group walks and supervised downtime. Pickup & drop-off included.*\n• 2 structured group walks\n• Play & social enrichment sessions\n• Collection & delivery (*availability)", 3),
                ("puppy-senior", "Puppy & Senior Care", "£14/visit", "Gentle, age-appropriate visits with toilet breaks, light exercise, socialisation or medication support.\n• Age & health considerate pacing\n• Socialisation & routine building\n• Flexible scheduling options", 4),
            ]
            for key, title, price, content, order in services:
                conn.execute(text(
                    "INSERT INTO site_content (section, key, title, price, content, sort_order) VALUES (:sec, :key, :title, :price, :content, :order)"
                ), {"sec": "services", "key": key, "title": title, "price": price, "content": content, "order": order})

def save_enquiry(name: str, email: str, dog: str, message: str):
    init_db()
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO enquiries (name, email, dog, message, created_at, status, ip, sid) VALUES (:name,:email,:dog,:message,:created_at,:status,:ip,:sid)"),
            {
                'name': name,
                'email': email,
                'dog': dog,
                'message': message,
                'created_at': datetime.utcnow().isoformat(),
                'status': 'new',
                'ip': _client_ip(),
                'sid': request.form.get('sid', '').strip() or None,
            }
        )

def fetch_services():
    """Fetch all services content from database"""
    init_db()
    with engine.begin() as conn:
        result = conn.execute(text("SELECT id, key, title, price, content, sort_order FROM site_content WHERE section='services' ORDER BY sort_order ASC"))
        services = []
        for r in result:
            services.append({
                'id': r.id,
                'key': r.key,
                'title': r.title,
                'price': r.price,
                'content': r.content,
                'sort_order': r.sort_order
            })
        return services

def fetch_enquiries():
    init_db()
    with engine.begin() as conn:
        result = conn.execute(text("SELECT id, name, email, dog, message, created_at, status, ip, sid, visit_summary FROM enquiries ORDER BY id DESC"))
        # Convert to list of dicts for template compatibility
        rows = []
        for r in result:
            rows.append({
                'id': r.id,
                'name': r.name,
                'email': r.email,
                'dog': r.dog,
                'message': r.message,
                'created_at': r.created_at,
                'status': getattr(r, 'status', 'new'),
                'ip': getattr(r, 'ip', ''),
                'sid': getattr(r, 'sid', ''),
                'visit_summary': getattr(r, 'visit_summary', ''),
            })
        return rows


# ---- Formatting helpers ----
def to_uk(dt_iso: str) -> str:
    """Convert an ISO8601 UTC string to UK local time string DD/MM/YYYY HH:MM."""
    if not dt_iso:
        return ''
    try:
        dt = datetime.fromisoformat(dt_iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if ZoneInfo is not None:
            uk = dt.astimezone(ZoneInfo('Europe/London'))
        else:
            # Fallback: assume UTC; no DST awareness
            uk = dt  # better than crashing
        return uk.strftime('%d/%m/%Y %H:%M')
    except Exception:
        return dt_iso

@app.template_filter('uk_datetime')
def uk_datetime_filter(s: str) -> str:
    return to_uk(s)


@app.route('/track', methods=['POST'])
def track():
    payload = request.get_json(silent=True) or {}
    data = {
        'sid': payload.get('sid') or request.cookies.get('sid') or '',
        'path': payload.get('path') or request.path,
        'referrer': payload.get('referrer') or request.headers.get('Referer') or '',
        'event': payload.get('event') or 'view',
        'user_agent': request.headers.get('User-Agent') or '',
        'ip': _client_ip(),
        'created_at': datetime.utcnow().isoformat()
    }
    init_db()
    # Drop admin page events entirely from analytics
    try:
        if (data.get('path') or '').startswith('/admin'):
            return jsonify({'ok': True})
    except Exception:
        pass
    with engine.begin() as conn:
        # If returning after inactivity threshold, insert a 'return' marker event
        try:
            threshold_min = int(os.environ.get('RETURN_THRESHOLD_MINUTES', '30'))
        except Exception:
            threshold_min = 30
        if data['sid']:
            last = conn.execute(text("SELECT created_at FROM events WHERE sid=:sid AND path NOT LIKE '/admin%' ORDER BY id DESC LIMIT 1"), {"sid": data['sid']}).fetchone()
            if last:
                try:
                    last_dt = datetime.fromisoformat(last.created_at)
                except Exception:
                    last_dt = None
                now_dt = datetime.fromisoformat(data['created_at'])
                if last_dt and (now_dt - last_dt) > timedelta(minutes=threshold_min):
                    conn.execute(text("INSERT INTO events (sid, path, referrer, event, user_agent, ip, created_at) VALUES (:sid,:path,:referrer,:event,:user_agent,:ip,:created_at)"), {
                        'sid': data['sid'],
                        'path': '/',
                        'referrer': data['referrer'],
                        'event': 'return',
                        'user_agent': data['user_agent'],
                        'ip': data['ip'],
                        'created_at': data['created_at']
                    })
        conn.execute(text("INSERT INTO events (sid, path, referrer, event, user_agent, ip, created_at) VALUES (:sid,:path,:referrer,:event,:user_agent,:ip,:created_at)"), data)
    return jsonify({'ok': True})

def require_admin():
    # Password (basic) takes precedence over token; token kept for backward compatibility.
    admin_password = os.environ.get('ADMIN_PASSWORD')
    if admin_password:
        auth = request.authorization
        if not auth or auth.username != 'admin' or auth.password != admin_password:
            return Response(
                'Authentication required',
                401,
                {'WWW-Authenticate': 'Basic realm="Admin Area"'}
            )
        return True
    token_cfg = os.environ.get('ADMIN_TOKEN')
    if not token_cfg:
        return False  # unprotected
    token = request.args.get('token') or request.headers.get('X-Admin-Token')
    if token != token_cfg:
        abort(403)
    return True

# ---------- Unified Admin Dashboard ----------
@app.route('/admin')
def admin_dashboard():
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    
    init_db()
    with engine.begin() as conn:
        # Count open chats
        open_chats = conn.execute(text("SELECT COUNT(*) FROM chats WHERE status='open'")).scalar() or 0
        
        # Count new enquiries (status='new')
        new_enquiries = conn.execute(text("SELECT COUNT(*) FROM enquiries WHERE status='new'")).scalar() or 0
        
        # Count recent visitors (last 24 hours)
        from datetime import datetime, timedelta
        cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        active_visitors = conn.execute(text("SELECT COUNT(DISTINCT sid) FROM events WHERE created_at > :cutoff"), {"cutoff": cutoff}).scalar() or 0
    
    return render_template('admin_dashboard.html', 
                         open_chats=open_chats,
                         new_enquiries=new_enquiries, 
                         active_visitors=active_visitors)

@app.route('/admin/status.json')
def admin_status_json():
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    
    init_db()
    with engine.begin() as conn:
        open_chats = conn.execute(text("SELECT COUNT(*) FROM chats WHERE status='open'")).scalar() or 0
        new_enquiries = conn.execute(text("SELECT COUNT(*) FROM enquiries WHERE status='new'")).scalar() or 0
        from datetime import datetime, timedelta
        cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        active_visitors = conn.execute(text("SELECT COUNT(DISTINCT sid) FROM events WHERE created_at > :cutoff"), {"cutoff": cutoff}).scalar() or 0
    
    return jsonify({
        "open_chats": open_chats,
        "new_enquiries": new_enquiries,
        "active_visitors": active_visitors
    })

@app.route('/admin/enquiries')
def admin_enquiries():
    # Enforce admin auth if configured
    _auth = require_admin()
    if isinstance(_auth, Response):
        return _auth  # triggers browser Basic Auth prompt when ADMIN_PASSWORD is set
    protected = bool(_auth)
    rows = fetch_enquiries()
    deleted = request.args.get('deleted') == '1'
    return render_template('admin_enquiries.html', rows=rows, protected=protected, deleted=deleted)

# Alias: some users prefer calling them "bookings".
@app.route('/admin/bookings')
def admin_bookings():
    return admin_enquiries()

@app.route('/admin/enquiries.csv')
def admin_enquiries_csv():
    _auth = require_admin()
    if isinstance(_auth, Response):
        return _auth
    rows = fetch_enquiries()
    # Build a simple CSV
    lines = ["id,name,email,dog,message,status,ip,sid,created_at"]
    for r in rows:
        # naive CSV escaping for commas/quotes
        def esc(x):
            x = (x or "").replace('"', '""')
            if any(c in x for c in [',', '"', '\n']):
                return f'"{x}"'
            return x
        lines.append(
            f"{r['id']},{esc(r['name'])},{esc(r['email'])},{esc(r['dog'])},{esc(r['message'])},{esc(r.get('status') or 'new')},{esc(r.get('ip') or '')},{esc(r.get('sid') or '')},{r['created_at']}"
        )
    csv = "\n".join(lines)
    return Response(csv, mimetype='text/csv', headers={'Content-Disposition': 'attachment; filename=enquiries.csv'})


@app.route('/admin/enquiries/delete/<int:enquiry_id>', methods=['POST'])
def delete_enquiry(enquiry_id: int):
    # Admin auth
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result  # return 401 challenge when using Basic Auth
    init_db()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM enquiries WHERE id = :id"), {"id": enquiry_id})
    # Preserve token in redirect if present so user stays authenticated
    token = request.args.get('token')
    args = {"deleted": "1"}
    if token:
        args["token"] = token
    return redirect(url_for('admin_enquiries', **args))


@app.route('/admin/enquiries/activity/<int:enquiry_id>')
def admin_enquiry_activity(enquiry_id: int):
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    init_db()
    with engine.begin() as conn:
        e = conn.execute(text("SELECT sid FROM enquiries WHERE id=:id"), {"id": enquiry_id}).fetchone()
        if not e:
            abort(404)
    rows = conn.execute(text("SELECT path, referrer, event, created_at FROM events WHERE sid=:sid AND path NOT LIKE '/admin%' ORDER BY id DESC LIMIT 50"), {"sid": e.sid}).fetchall()
    events = [{
        'path': r.path,
        'referrer': r.referrer,
        'event': r.event,
        'created_at': to_uk(r.created_at)
    } for r in rows]
    return jsonify({'events': events})


def _ai_analyze(events: list) -> str | None:
    """Use OpenAI if configured, else Gemini, to summarise events."""
    timeline = "\n".join([f"- [{e['created_at']}] {e['event']} {e['path']} (ref: {e['referrer'] or '-'} )" for e in events])
    prompt = (
        "Summarise in 2-3 short sentences the visitor's interest and likely intent based on these website events. "
        "Be concise and friendly.\n\n" + timeline
    )
    # OpenAI first
    if openai_client:
        try:
            resp = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a helpful analytics assistant."},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=180,
                temperature=0.2,
            )
            txt = resp.choices[0].message.content if resp and resp.choices else None
            return (txt or '').strip() or None
        except Exception:
            pass
    # Gemini fallback
    if GEMINI_API_KEY and genai is not None:
        try:
            model = genai.GenerativeModel('gemini-1.5-flash')
            resp = model.generate_content(prompt)
            txt = getattr(resp, 'text', None)
            if not txt and getattr(resp, 'candidates', None):
                txt = resp.candidates[0].content.parts[0].text
            return (txt or '').strip() or None
        except Exception:
            return None
    return None


def _heuristic_summary(raw_events: list[dict]) -> str | None:
    """Generate a simple rule-based summary if AI providers are unavailable or fail.
    raw_events: list of dicts with keys: path, event, created_at (ISO string), referrer
    """
    if not raw_events:
        return None
    paths = [e['path'] for e in raw_events if e.get('path')]
    unique_paths = list(dict.fromkeys(paths))
    page_count = len(paths)
    # Duration
    from datetime import datetime
    def parse_iso(x):
        try:
            return datetime.fromisoformat(x)
        except Exception:
            return None
    first_dt = parse_iso(raw_events[0]['created_at'])
    last_dt = parse_iso(raw_events[-1]['created_at']) if raw_events else first_dt
    duration_secs = (last_dt - first_dt).total_seconds() if first_dt and last_dt else 0
    has_services = any('/services' in p for p in paths)
    has_contact = any('/contact' in p for p in paths)
    has_about = any('/about' in p for p in paths)

    # Buckets
    if page_count <= 1:
        return "Very quick visit; user viewed a single page and left."
    if page_count == 2 and duration_secs < 30:
        return "Brief glance at a couple of pages; low engagement so far."
    intent_bits = []
    if has_services:
        intent_bits.append('looked at services')
    if has_contact:
        intent_bits.append('checked contact page')
    if has_about:
        intent_bits.append('viewed about page')
    if duration_secs > 600 and page_count >= 5:
        intent_phrase = 'highly engaged session'
    elif duration_secs > 180 and page_count >= 4:
        intent_phrase = 'engaged browsing'
    else:
        intent_phrase = 'moderate interest'
    if intent_bits:
        return f"User {intent_phrase}; {', '.join(intent_bits)} (visited {page_count} pages)."
    return f"User showed {intent_phrase}, visited {page_count} pages across {len(unique_paths)} unique sections."


@app.route('/admin/enquiries/analyze/<int:enquiry_id>', methods=['POST'])
def admin_enquiry_analyze(enquiry_id: int):
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    init_db()
    with engine.begin() as conn:
        e = conn.execute(text("SELECT sid FROM enquiries WHERE id=:id"), {"id": enquiry_id}).fetchone()
        if not e:
            abort(404)
        rows = conn.execute(text("SELECT path, referrer, event, created_at FROM events WHERE sid=:sid AND path NOT LIKE '/admin%' ORDER BY id ASC"), {"sid": e.sid}).fetchall()
        events = [{'path': r.path, 'referrer': r.referrer, 'event': r.event, 'created_at': to_uk(r.created_at)} for r in rows]
        summary = _ai_analyze(events)
        if not summary:
            # Get raw events again for heuristic (without uk formatting)
            raw_rows = conn.execute(text("SELECT path, referrer, event, created_at FROM events WHERE sid=:sid AND path NOT LIKE '/admin%' ORDER BY id ASC"), {"sid": e.sid}).fetchall()
            raw_events = [{'path': r.path, 'referrer': r.referrer, 'event': r.event, 'created_at': r.created_at} for r in raw_rows]
            summary = _heuristic_summary(raw_events)
        if summary:
            conn.execute(text("UPDATE enquiries SET visit_summary=:s WHERE id=:id"), {"s": summary, "id": enquiry_id})
    token = request.args.get('token')
    args = {}
    if token:
        args['token'] = token
    return redirect(url_for('admin_enquiries', **args))


# -------- Visitors (non-enquiry) admin --------
def fetch_visitors():
    init_db()
    with engine.begin() as conn:
        rows = conn.execute(text(
            """
            SELECT e.sid AS sid,
                   MIN(e.created_at) AS first_seen,
                   MAX(e.created_at) AS last_seen,
                   COUNT(*) AS page_count,
                   MAX(e.ip) AS ip,
                   vi.summary AS summary
            FROM events e
            LEFT JOIN visitor_insights vi ON vi.sid = e.sid
            WHERE e.sid IS NOT NULL AND e.sid != '' AND e.path NOT LIKE '/admin%'
            GROUP BY e.sid, vi.summary
            ORDER BY last_seen DESC
            """
        )).fetchall()
    visitors = []
    for r in rows:
        visitors.append({
            'sid': r.sid,
            'ip': r.ip,
            'first_seen': r.first_seen,
            'last_seen': r.last_seen,
            'page_count': r.page_count,
            'summary': r.summary or ''
        })
    return visitors


def fetch_visitor_stats():
    """Compute high-level metrics excluding admin pages."""
    init_db()
    with engine.begin() as conn:
        total = conn.execute(text("SELECT COUNT(DISTINCT sid) AS c FROM events WHERE sid IS NOT NULL AND sid != '' AND path NOT LIKE '/admin%'")).scalar() or 0
        returning = conn.execute(text("SELECT COUNT(DISTINCT sid) AS c FROM events WHERE event='return'"))
        returning = returning.scalar() if returning else 0
        views = conn.execute(text("SELECT COUNT(*) AS c FROM events WHERE event='view' AND path NOT LIKE '/admin%'")).scalar() or 0
    avg_pages = round(views / total, 1) if total else 0
    return {
        'total_visitors': int(total),
        'returning_visitors': int(returning),
        'avg_pages_per_visitor': avg_pages
    }


@app.route('/admin/visitors')
def admin_visitors():
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    rows = fetch_visitors()
    stats = fetch_visitor_stats()
    return render_template('admin_visitors.html', rows=rows, stats=stats)

# ---------- Live Chat Endpoints (user side) ----------
@app.post('/chat/start')
def chat_start():
    init_db()
    with engine.begin() as conn:
        sid = request.cookies.get('sid') or request.form.get('sid') or ''
        ip = request.headers.get('X-Forwarded-For', request.remote_addr or '').split(',')[0].strip()
        # Reuse existing open chat for this sid if present
        row = conn.execute(text("SELECT id FROM chats WHERE sid=:sid AND status='open' ORDER BY id DESC LIMIT 1"), {"sid": sid}).fetchone()
        if row:
            chat_id = row.id
        else:
            now = datetime.utcnow().isoformat()
            res = conn.execute(text("INSERT INTO chats (sid, name, status, created_at, last_activity, ip) VALUES (:sid, :name, 'open', :c, :c, :ip)"), {"sid": sid, "name": request.form.get('name') or '', "c": now, "ip": ip})
            chat_id = res.lastrowid if hasattr(res, 'lastrowid') else conn.execute(text("SELECT last_insert_rowid() as id")).fetchone().id
            # Seed a welcome message from admin
            conn.execute(text("INSERT INTO chat_messages (chat_id, sender, message, created_at) VALUES (:cid, 'admin', :msg, :t)"), {"cid": chat_id, "msg": "Hi! How can I help with your walks today?", "t": now})
        return jsonify({"ok": True, "chat_id": chat_id})

@app.post('/chat/send')
def chat_send():
    init_db()
    chat_id = request.form.get('chat_id') or (request.get_json(silent=True) or {}).get('chat_id')
    message = request.form.get('message') or (request.get_json(silent=True) or {}).get('message')
    sender = (request.form.get('sender') or (request.get_json(silent=True) or {}).get('sender') or 'user').lower()
    if not chat_id or not message:
        abort(400)
    # Only allow 'admin' sender if authenticated
    if sender == 'admin':
        auth_ok = require_admin()
        if isinstance(auth_ok, Response):
            return auth_ok
    else:
        sender = 'user'
    now = datetime.utcnow().isoformat()
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO chat_messages (chat_id, sender, message, created_at) VALUES (:cid, :s, :m, :t)"), {"cid": int(chat_id), "s": sender, "m": message.strip()[:2000], "t": now})
        conn.execute(text("UPDATE chats SET last_activity=:t WHERE id=:cid"), {"cid": int(chat_id), "t": now})
    return jsonify({"ok": True})

@app.get('/chat/poll/<int:chat_id>')
def chat_poll(chat_id: int):
    init_db()
    after = request.args.get('after')
    q = "SELECT id, sender, message, created_at FROM chat_messages WHERE chat_id=:cid"
    params = {"cid": chat_id}
    if after:
        q += " AND id > :after"
        try:
            params['after'] = int(after)
        except Exception:
            params['after'] = 0
    q += " ORDER BY id ASC"
    with engine.begin() as conn:
        msgs = conn.execute(text(q), params).fetchall()
        chat = conn.execute(text("SELECT status FROM chats WHERE id=:cid"), {"cid": chat_id}).fetchone()
    messages = [{"id": m.id, "sender": m.sender, "message": m.message, "created_at": m.created_at} for m in msgs]
    return jsonify({"ok": True, "messages": messages, "status": (chat.status if chat else 'open')})

# ---------- Admin Chat Views ----------
@app.get('/admin/chats')
def admin_chats():
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    init_db()
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT id, sid, name, status, created_at, last_activity FROM chats ORDER BY CASE status WHEN 'open' THEN 0 ELSE 1 END, last_activity DESC"))
        chats = [dict(id=r.id, sid=r.sid, name=r.name or '', status=r.status, created_at=r.created_at, last_activity=r.last_activity) for r in rows]
    return render_template('admin_chats.html', chats=chats)

@app.get('/admin/chats/<int:chat_id>')
def admin_chat_view(chat_id: int):
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    init_db()
    
    # Get current chat info including IP
    with engine.begin() as conn:
        current_chat = conn.execute(text("SELECT id, sid, name, ip, created_at FROM chats WHERE id=:cid"), {"cid": chat_id}).fetchone()
        
        if not current_chat:
            abort(404)
        
        visitor_ip = current_chat.ip
        is_returning = False
        previous_chats = []
        
        # Check if this IP has previous chats
        if visitor_ip:
            prev_rows = conn.execute(text(
                "SELECT id, name, created_at, status FROM chats WHERE ip=:ip AND id!=:cid ORDER BY created_at DESC"
            ), {"ip": visitor_ip, "cid": chat_id}).fetchall()
            
            if prev_rows:
                is_returning = True
                # Get message history for each previous chat
                for prev_chat in prev_rows:
                    messages = conn.execute(text(
                        "SELECT sender, message, created_at FROM chat_messages WHERE chat_id=:cid ORDER BY id ASC"
                    ), {"cid": prev_chat.id}).fetchall()
                    
                    previous_chats.append({
                        'id': prev_chat.id,
                        'name': prev_chat.name or 'Visitor',
                        'created_at': prev_chat.created_at,
                        'status': prev_chat.status,
                        'messages': [{'sender': m.sender, 'message': m.message, 'created_at': m.created_at} for m in messages]
                    })
    
    return render_template('admin_chat.html', 
                         chat_id=chat_id, 
                         is_returning=is_returning, 
                         previous_chats=previous_chats,
                         visitor_ip=visitor_ip)

@app.post('/admin/chats/close/<int:chat_id>')
def admin_chat_close(chat_id: int):
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    init_db()
    with engine.begin() as conn:
        conn.execute(text("UPDATE chats SET status='closed' WHERE id=:cid"), {"cid": chat_id})
    return redirect(url_for('admin_chats'))

# ---------- Admin Content Management ----------
@app.route('/admin/content')
def admin_content():
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    services = fetch_services()
    return render_template('admin_content.html', services=services)

@app.post('/admin/content/service/<int:service_id>')
def admin_content_update(service_id: int):
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    
    title = request.form.get('title', '').strip()
    price = request.form.get('price', '').strip()
    content = request.form.get('content', '').strip()
    
    if not title:
        abort(400, "Title is required")
    
    init_db()
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE site_content SET title=:title, price=:price, content=:content WHERE id=:id"
        ), {"title": title, "price": price, "content": content, "id": service_id})
    
    return redirect(url_for('admin_content'))


@app.route('/admin/visitors/analyze/<sid>', methods=['POST'])
def admin_visitors_analyze(sid: str):
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    init_db()
    with engine.begin() as conn:
        ev = conn.execute(text("SELECT path, referrer, event, created_at FROM events WHERE sid=:sid AND path NOT LIKE '/admin%' ORDER BY id ASC"), {"sid": sid}).fetchall()
        events = [{'path': r.path, 'referrer': r.referrer, 'event': r.event, 'created_at': to_uk(r.created_at)} for r in ev]
    summary = _ai_analyze(events)
    if not summary:
        raw_events = [{'path': r.path, 'referrer': r.referrer, 'event': r.event, 'created_at': r.created_at} for r in ev]
        summary = _heuristic_summary(raw_events)
    if summary:
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO visitor_insights(sid, summary) VALUES (:sid, :s) ON CONFLICT(sid) DO UPDATE SET summary = excluded.summary"), {"sid": sid, "s": summary})
    return redirect(url_for('admin_visitors'))


@app.route('/admin/visitors/analyze/<sid>.json', methods=['POST'])
def admin_visitors_analyze_json(sid: str):
    """Analyze a visitor session and return details for a popup modal (AJAX)."""
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    init_db()
    with engine.begin() as conn:
        ev = conn.execute(text("SELECT path, referrer, event, created_at FROM events WHERE sid=:sid AND path NOT LIKE '/admin%' ORDER BY id ASC"), {"sid": sid}).fetchall()
        events = [{'path': r.path, 'referrer': r.referrer, 'event': r.event, 'created_at': to_uk(r.created_at)} for r in ev]
    # Create timeline like the prompt
    timeline = "\n".join([f"- [{e['created_at']}] {e['event']} {e['path']} (ref: {e['referrer'] or '-'} )" for e in events])
    provider = 'openai' if openai_client else ('gemini' if genai and GEMINI_API_KEY else 'none')
    summary = _ai_analyze(events)
    if not summary:
        raw_events = [{'path': r.path, 'referrer': r.referrer, 'event': r.event, 'created_at': r.created_at} for r in ev]
        summary = _heuristic_summary(raw_events)
    if summary:
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO visitor_insights(sid, summary) VALUES (:sid, :s) ON CONFLICT(sid) DO UPDATE SET summary = excluded.summary"), {"sid": sid, "s": summary})
    return jsonify({
        'ok': bool(summary),
        'provider': provider,
        'events': events,
        'timeline': timeline,
        'summary': summary,
        'error': None if summary else ('No summary generated' if events else 'No events for this session')
    })


@app.route('/admin/visitors/analyze-all', methods=['POST'])
def admin_visitors_analyze_all():
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    init_db()
    with engine.begin() as conn:
        sids = [r.sid for r in conn.execute(text("SELECT DISTINCT sid FROM events WHERE sid IS NOT NULL AND sid != ''"))]
        for sid in sids:
            ev = conn.execute(text("SELECT path, referrer, event, created_at FROM events WHERE sid=:sid AND path NOT LIKE '/admin%' ORDER BY id ASC"), {"sid": sid}).fetchall()
            events = [{'path': r.path, 'referrer': r.referrer, 'event': r.event, 'created_at': to_uk(r.created_at)} for r in ev]
            summary = _ai_analyze(events)
            if summary:
                conn.execute(text("INSERT INTO visitor_insights(sid, summary) VALUES (:sid, :s) ON CONFLICT(sid) DO UPDATE SET summary = excluded.summary"), {"sid": sid, "s": summary})
    return redirect(url_for('admin_visitors'))


@app.route('/admin/visitors/delete/<sid>', methods=['POST'])
def admin_visitors_delete(sid: str):
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    init_db()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM events WHERE sid=:sid"), {"sid": sid})
        conn.execute(text("DELETE FROM visitor_insights WHERE sid=:sid"), {"sid": sid})
    return redirect(url_for('admin_visitors'))


@app.route('/admin/ai_status')
def admin_ai_status():
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    status = {
        'provider': 'openai' if openai_client else ('gemini' if genai and GEMINI_API_KEY else 'none'),
        'openai': {
            'env_present': bool(OPENAI_API_KEY),
            'client_loaded': openai_client is not None,
            'model_ok': False,
            'error': None,
        },
        'gemini': {
            'env_present': bool(GEMINI_API_KEY),
            'module_loaded': genai is not None,
            'model_ok': False,
            'error': None,
        }
    }
    # OpenAI ping
    if openai_client:
        try:
            r = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role":"user","content":"ping"}],
                max_tokens=5
            )
            status['openai']['model_ok'] = bool(r and r.choices)
        except Exception as e:
            status['openai']['error'] = str(e)
    # Gemini ping
    if genai and GEMINI_API_KEY:
        try:
            m = genai.GenerativeModel('gemini-1.5-flash')
            r = m.generate_content("ping")
            status['gemini']['model_ok'] = bool(getattr(r, 'text', '') or getattr(r, 'candidates', None))
        except Exception as e:
            status['gemini']['error'] = str(e)
    return jsonify(status)


@app.route('/admin/enquiries/status/<int:enquiry_id>', methods=['POST'])
def update_enquiry_status(enquiry_id: int):
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    new_status = request.form.get('status', '').strip().lower()
    if new_status not in {'new', 'in-progress', 'replied', 'closed'}:
        abort(400)
    init_db()
    with engine.begin() as conn:
        conn.execute(text("UPDATE enquiries SET status = :s WHERE id = :id"), {"s": new_status, "id": enquiry_id})
    token = request.args.get('token')
    args = {}
    if token:
        args['token'] = token
    return redirect(url_for('admin_enquiries', **args))

if __name__ == '__main__':
    app.run(debug=True)
