from flask import Flask, render_template, request, redirect, url_for, abort, Response
import sqlite3
import os
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

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
                status TEXT NOT NULL DEFAULT 'new'
            )
        """))
        # In case the table existed before without the new columns, try to add them.
        try:
            conn.execute(text("ALTER TABLE enquiries ADD COLUMN status TEXT NOT NULL DEFAULT 'new'"))
        except Exception:
            pass

def save_enquiry(name: str, email: str, dog: str, message: str):
    init_db()
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO enquiries (name, email, dog, message, created_at, status) VALUES (:name,:email,:dog,:message,:created_at,:status)"),
            {
                'name': name,
                'email': email,
                'dog': dog,
                'message': message,
                'created_at': datetime.utcnow().isoformat(),
                'status': 'new'
            }
        )

def fetch_enquiries():
    init_db()
    with engine.begin() as conn:
        result = conn.execute(text("SELECT id, name, email, dog, message, created_at, status FROM enquiries ORDER BY id DESC"))
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
    lines = ["id,name,email,dog,message,status,created_at"]
    for r in rows:
        # naive CSV escaping for commas/quotes
        def esc(x):
            x = (x or "").replace('"', '""')
            if any(c in x for c in [',', '"', '\n']):
                return f'"{x}"'
            return x
        lines.append(
            f"{r['id']},{esc(r['name'])},{esc(r['email'])},{esc(r['dog'])},{esc(r['message'])},{esc(r.get('status') or 'new')},{r['created_at']}"
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
