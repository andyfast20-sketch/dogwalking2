from flask import Flask, render_template, request, redirect, url_for, abort, Response
import sqlite3
import os
from datetime import datetime

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


# ----------------------
# Enquiries persistence
# ----------------------
DB_PATH = os.path.join(os.path.dirname(__file__), 'enquiries.db')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as db:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS enquiries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                dog TEXT,
                message TEXT,
                created_at TEXT NOT NULL
            )
            """
        )

def save_enquiry(name: str, email: str, dog: str, message: str):
    init_db()
    with get_db() as db:
        db.execute(
            "INSERT INTO enquiries (name, email, dog, message, created_at) VALUES (?,?,?,?,?)",
            (name, email, dog, message, datetime.utcnow().isoformat())
        )

def fetch_enquiries():
    init_db()
    with get_db() as db:
        cur = db.execute(
            "SELECT id, name, email, dog, message, created_at FROM enquiries ORDER BY id DESC"
        )
        return cur.fetchall()

def require_admin():
    token_cfg = os.environ.get('ADMIN_TOKEN')
    if not token_cfg:
        # If not configured, allow access (useful for local dev) but mark as unprotected
        return False
    token = request.args.get('token') or request.headers.get('X-Admin-Token')
    if token != token_cfg:
        abort(403)
    return True

@app.route('/admin/enquiries')
def admin_enquiries():
    protected = require_admin()
    rows = fetch_enquiries()
    return render_template('admin_enquiries.html', rows=rows, protected=protected)

@app.route('/admin/enquiries.csv')
def admin_enquiries_csv():
    require_admin()
    rows = fetch_enquiries()
    # Build a simple CSV
    lines = ["id,name,email,dog,message,created_at"]
    for r in rows:
        # naive CSV escaping for commas/quotes
        def esc(x):
            x = (x or "").replace('"', '""')
            if any(c in x for c in [',', '"', '\n']):
                return f'"{x}"'
            return x
        lines.append(
            f"{r['id']},{esc(r['name'])},{esc(r['email'])},{esc(r['dog'])},{esc(r['message'])},{r['created_at']}"
        )
    csv = "\n".join(lines)
    return Response(csv, mimetype='text/csv', headers={'Content-Disposition': 'attachment; filename=enquiries.csv'})

if __name__ == '__main__':
    app.run(debug=True)
