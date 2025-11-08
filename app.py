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

@app.context_processor
def inject_globals():
    """Make maintenance_mode, hero_images and meet_andy available to all templates"""
    return {
        'maintenance_mode': get_maintenance_mode(),
        'hero_images': get_hero_images(),
        'meet_andy': get_meet_andy()
    }

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

@app.route('/book', methods=['GET', 'POST'])
def book():
    if request.method == 'POST':
        slot_id = request.form.get('slot_id', '').strip()
        customer_name = request.form.get('name', '').strip()
        customer_email = request.form.get('email', '').strip()
        customer_phone = request.form.get('phone', '').strip()
        dog_name = request.form.get('dog_name', '').strip()
        dog_info = request.form.get('dog_info', '').strip()
        service_type = request.form.get('service_type', '').strip()
        message = request.form.get('message', '').strip()
        
        # Get IP address
        ip_address = request.headers.get('X-Forwarded-For', request.remote_addr)
        if ip_address and ',' in ip_address:
            ip_address = ip_address.split(',')[0].strip()
        
        # Validation
        if not all([slot_id, customer_name, customer_email, dog_name]):
            slots = fetch_booking_slots()
            return render_template('book.html', slots=slots, error="Please fill in all required fields.", form=request.form, ip=ip_address), 400
        
        # Create booking
        booking_id = create_booking(
            slot_id=int(slot_id),
            customer_name=customer_name,
            customer_email=customer_email,
            customer_phone=customer_phone,
            dog_name=dog_name,
            dog_info=dog_info,
            service_type=service_type,
            message=message,
            ip_address=ip_address
        )
        
        if booking_id:
            return redirect(url_for('book', success='1'))
        else:
            slots = fetch_booking_slots()
            return render_template('book.html', slots=slots, error="Sorry, that time slot is no longer available. Please select another.", form=request.form, ip=ip_address), 400
    
    # GET request
    slots = fetch_booking_slots()
    success = request.args.get('success') == '1'
    
    # Get IP for display
    ip_address = request.headers.get('X-Forwarded-For', request.remote_addr)
    if ip_address and ',' in ip_address:
        ip_address = ip_address.split(',')[0].strip()
    
    return render_template('book.html', slots=slots, success=success, ip=ip_address)


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
        # Seed default "Meet Andy" content if not exists
        count = conn.execute(text("SELECT COUNT(*) FROM site_content WHERE section='about'")).scalar()
        if count == 0:
            about_content = [
                ("heading", "Meet Andy", None, "Meet Andy", 1),
                ("paragraph1", None, None, "Hi, I'm Andy — a local dog walker who's DBS checked, fully insured and first‑aid trained. I keep walks calm, positive and tailored to your dog's pace and personality.", 2),
                ("paragraph2", None, None, "You'll get GPS routes and photo updates after each walk so you always know how it went. I treat every dog like my own and build routines they genuinely enjoy.", 3),
                ("badge1", None, None, "✓ DBS Checked", 4),
                ("badge2", None, None, "✓ Public Liability Insured", 5),
                ("badge3", None, None, "✓ First‑Aid Trained", 6),
                ("badge4", None, None, "✓ GPS & Photo Updates", 7),
            ]
            for key, title, price, content, order in about_content:
                conn.execute(text(
                    "INSERT INTO site_content (section, key, title, price, content, sort_order) VALUES (:sec, :key, :title, :price, :content, :order)"
                ), {"sec": "about", "key": key, "title": title, "price": price, "content": content, "order": order})
        # Site settings table for global configurations
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS site_settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """))
        # Initialize maintenance mode to off if not exists
        try:
            conn.execute(text("INSERT INTO site_settings (key, value) VALUES ('maintenance_mode', 'false')"))
        except Exception:
            pass  # setting already exists
        # Initialize default hero images
        default_hero_images = [
            ("hero_slide_1", "https://image.pollinations.ai/prompt/stocky%20build%20dark-haired%20male%20dog%20walker%20back%20view%20walking%20a%20cute%20small%20dog%2C%20black%20trousers%2C%20no%20suit%2C%20professional%20casual%2C%20no%20face%20visible%2C%20emerald%20and%20amber%20tones%2C%20photorealistic%2C%20crisp%20lighting?width=1100&height=1400&nologo=true"),
            ("hero_slide_2", "https://image.pollinations.ai/prompt/stocky%20dark-haired%20male%20dog%20walker%20mid%20section%20holding%20lead%20with%20cute%20dog%2C%20black%20trousers%2C%20no%20suit%2C%20professional%20casual%2C%20face%20out%20of%20frame%2C%20emerald%20amber%20color%20grading%2C%20photorealistic?width=1100&height=1400&nologo=true"),
            ("hero_strip_1", "https://image.pollinations.ai/prompt/stocky%20dark-haired%20male%20dog%20walker%20rear%20view%20with%20cute%20small%20dog%20on%20lead%2C%20black%20trousers%2C%20no%20suit%2C%20professional%20casual%2C%20no%20face%2C%20emerald%20and%20amber%20tones%2C%20wide%20angle%20street?width=900&height=600&nologo=true"),
            ("hero_strip_2", "https://image.pollinations.ai/prompt/side%20view%20stocky%20male%20dog%20walker%20dark%20hair%20black%20trousers%20casual%20(no%20suit)%20walking%20cute%20dog%2C%20face%20out%20of%20frame%2C%20emerald%20amber%20tones%20professional?width=900&height=600&nologo=true"),
            ("hero_strip_3", "https://image.pollinations.ai/prompt/close%20up%20dog%20looking%20up%20at%20stocky%20walker%20legs%20black%20trousers%20(no%20suit)%20lead%20visible%2C%20emerald%20and%20amber%20tones%2C%20professional%20casual?width=900&height=600&nologo=true"),
            ("hero_strip_4", "https://image.pollinations.ai/prompt/stocky%20male%20dog%20walker%20dark%20hair%20holding%20lead%20hand%20detail%20with%20cute%20dog%2C%20black%20trousers%2C%20no%20suit%2C%20emerald%20amber%20tones%2C%20professional%20macro?width=900&height=600&nologo=true"),
        ]
        for key, url in default_hero_images:
            try:
                conn.execute(text("INSERT INTO site_settings (key, value) VALUES (:key, :val)"), {"key": key, "val": url})
            except Exception:
                pass  # setting already exists
        # Booking slots table for admin-defined available times
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS booking_slots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                time TEXT NOT NULL,
                duration_minutes INTEGER DEFAULT 60,
                capacity INTEGER DEFAULT 1,
                booked_count INTEGER DEFAULT 0,
                is_available INTEGER DEFAULT 1,
                price TEXT,
                notes TEXT,
                created_at TEXT NOT NULL
            )
        """))
        # Add price column if it doesn't exist (for existing databases)
        try:
            conn.execute(text("ALTER TABLE booking_slots ADD COLUMN price TEXT"))
        except Exception:
            pass  # column already exists
        # Bookings table for customer reservations
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bookings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slot_id INTEGER NOT NULL,
                customer_name TEXT NOT NULL,
                customer_email TEXT NOT NULL,
                customer_phone TEXT,
                dog_name TEXT NOT NULL,
                dog_info TEXT,
                service_type TEXT,
                message TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                ip_address TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (slot_id) REFERENCES booking_slots(id)
            )
        """))

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

def get_maintenance_mode():
    """Get current maintenance mode status"""
    init_db()
    with engine.begin() as conn:
        result = conn.execute(text("SELECT value FROM site_settings WHERE key='maintenance_mode'")).fetchone()
        return result.value == 'true' if result else False

def set_maintenance_mode(enabled: bool):
    """Set maintenance mode on or off"""
    init_db()
    value = 'true' if enabled else 'false'
    with engine.begin() as conn:
        conn.execute(text("INSERT OR REPLACE INTO site_settings (key, value) VALUES ('maintenance_mode', :val)"), {"val": value})

def get_hero_images():
    """Get all hero image URLs"""
    init_db()
    with engine.begin() as conn:
        images = {}
        for key in ['hero_slide_1', 'hero_slide_2', 'hero_strip_1', 'hero_strip_2', 'hero_strip_3', 'hero_strip_4']:
            result = conn.execute(text("SELECT value FROM site_settings WHERE key=:key"), {"key": key}).fetchone()
            images[key] = result.value if result else ''
        return images

def set_hero_image(key: str, url: str):
    """Set a hero image URL"""
    init_db()
    with engine.begin() as conn:
        conn.execute(text("INSERT OR REPLACE INTO site_settings (key, value) VALUES (:key, :val)"), {"key": key, "val": url})

def get_meet_andy():
    """Get Meet Andy section content"""
    init_db()
    with engine.begin() as conn:
        result = conn.execute(text("SELECT key, title, content FROM site_content WHERE section='about' ORDER BY sort_order ASC"))
        content = {}
        for row in result:
            content[row.key] = {'title': row.title, 'content': row.content}
        return content

def update_meet_andy(data: dict):
    """Update Meet Andy section content"""
    init_db()
    with engine.begin() as conn:
        for key, value in data.items():
            conn.execute(text(
                "UPDATE site_content SET content=:content WHERE section='about' AND key=:key"
            ), {"content": value, "key": key})

# ---- Booking Management Functions ----
def fetch_booking_slots(include_past=False):
    """Fetch all booking slots, optionally filtering out past dates"""
    init_db()
    with engine.begin() as conn:
        if include_past:
            result = conn.execute(text("SELECT * FROM booking_slots ORDER BY date ASC, time ASC"))
        else:
            # For customer view, only show future slots
            from datetime import datetime
            today = datetime.now().strftime('%d/%m/%Y')
            result = conn.execute(text("SELECT * FROM booking_slots WHERE date >= :today AND is_available = 1 ORDER BY date ASC, time ASC"), {"today": today})
        slots = []
        for r in result:
            slots.append({
                'id': r.id,
                'date': r.date,
                'time': r.time,
                'duration_minutes': r.duration_minutes,
                'capacity': r.capacity,
                'booked_count': r.booked_count,
                'is_available': r.is_available,
                'price': getattr(r, 'price', '') or '',
                'notes': r.notes or '',
                'created_at': r.created_at,
                'spaces_left': r.capacity - r.booked_count
            })
        return slots

def create_booking_slot(date: str, time: str, duration: int = 60, capacity: int = 1, price: str = '', notes: str = ''):
    """Create a new booking slot with overlap and gap validation"""
    init_db()
    from datetime import datetime, timedelta
    
    # Parse the new slot's start time
    day, month, year = map(int, date.split('/'))
    hour, minute = map(int, time.split(':'))
    new_start = datetime(year, month, day, hour, minute)
    new_end = new_start + timedelta(minutes=duration)
    
    # Add 1 hour buffer after the slot ends (time to get to next customer)
    buffer_end = new_end + timedelta(hours=1)
    
    with engine.begin() as conn:
        # Get all existing slots for the same date
        existing_slots = conn.execute(text("""
            SELECT time, duration_minutes 
            FROM booking_slots 
            WHERE date = :date
        """), {"date": date}).fetchall()
        
        # Check for conflicts
        for slot in existing_slots:
            slot_hour, slot_minute = map(int, slot.time.split(':'))
            existing_start = datetime(year, month, day, slot_hour, slot_minute)
            existing_end = existing_start + timedelta(minutes=slot.duration_minutes)
            
            # Add 1 hour buffer after existing slot
            existing_buffer_end = existing_end + timedelta(hours=1)
            
            # Check if new slot overlaps with existing slot OR its buffer
            # OR if new slot's buffer overlaps with existing slot
            if (new_start < existing_buffer_end and buffer_end > existing_start):
                # Conflict detected
                return {
                    'success': False,
                    'error': f'Conflict: This slot overlaps with or is too close to an existing slot at {slot.time}. You need at least 1 hour gap between walks.'
                }
        
        # No conflicts, create the slot
        created_at = datetime.utcnow().isoformat() + 'Z'
        conn.execute(text("""
            INSERT INTO booking_slots (date, time, duration_minutes, capacity, booked_count, is_available, price, notes, created_at)
            VALUES (:date, :time, :duration, :capacity, 0, 1, :price, :notes, :created_at)
        """), {"date": date, "time": time, "duration": duration, "capacity": capacity, "price": price, "notes": notes, "created_at": created_at})
        
        return {'success': True}

def delete_booking_slot(slot_id: int):
    """Delete a booking slot (only if no bookings exist)"""
    init_db()
    with engine.begin() as conn:
        # Check if any bookings exist for this slot
        count = conn.execute(text("SELECT COUNT(*) FROM bookings WHERE slot_id = :sid"), {"sid": slot_id}).scalar()
        if count > 0:
            return False  # Cannot delete slot with existing bookings
        conn.execute(text("DELETE FROM booking_slots WHERE id = :sid"), {"sid": slot_id})
        return True

def fetch_bookings():
    """Fetch all bookings with slot details"""
    init_db()
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT b.*, bs.date, bs.time, bs.duration_minutes
            FROM bookings b
            JOIN booking_slots bs ON b.slot_id = bs.id
            ORDER BY bs.date DESC, bs.time DESC, b.created_at DESC
        """))
        bookings = []
        for r in result:
            bookings.append({
                'id': r.id,
                'slot_id': r.slot_id,
                'customer_name': r.customer_name,
                'customer_email': r.customer_email,
                'customer_phone': r.customer_phone or '',
                'dog_name': r.dog_name,
                'dog_info': r.dog_info or '',
                'service_type': r.service_type or '',
                'message': r.message or '',
                'status': r.status,
                'ip_address': r.ip_address or '',
                'created_at': r.created_at,
                'booking_date': r.date,
                'booking_time': r.time,
                'duration_minutes': r.duration_minutes
            })
        return bookings

def create_booking(slot_id: int, customer_name: str, customer_email: str, customer_phone: str,
                   dog_name: str, dog_info: str, service_type: str, message: str, ip_address: str):
    """Create a new booking"""
    init_db()
    from datetime import datetime
    created_at = datetime.utcnow().isoformat() + 'Z'
    with engine.begin() as conn:
        # Check if slot is available
        slot = conn.execute(text("SELECT capacity, booked_count, is_available FROM booking_slots WHERE id = :sid"), {"sid": slot_id}).fetchone()
        if not slot or not slot.is_available or slot.booked_count >= slot.capacity:
            return None  # Slot not available
        
        # Create booking
        result = conn.execute(text("""
            INSERT INTO bookings (slot_id, customer_name, customer_email, customer_phone, dog_name, dog_info, service_type, message, status, ip_address, created_at)
            VALUES (:slot_id, :name, :email, :phone, :dog_name, :dog_info, :service, :message, 'pending', :ip, :created_at)
        """), {
            "slot_id": slot_id, "name": customer_name, "email": customer_email, "phone": customer_phone,
            "dog_name": dog_name, "dog_info": dog_info, "service": service_type, "message": message,
            "ip": ip_address, "created_at": created_at
        })
        booking_id = result.lastrowid
        
        # Increment booked_count
        new_count = slot.booked_count + 1
        conn.execute(text("UPDATE booking_slots SET booked_count = :count WHERE id = :sid"), {"count": new_count, "sid": slot_id})
        
        # If fully booked, mark as unavailable
        if new_count >= slot.capacity:
            conn.execute(text("UPDATE booking_slots SET is_available = 0 WHERE id = :sid"), {"sid": slot_id})
        
        return booking_id

def update_booking_status(booking_id: int, status: str):
    """Update booking status"""
    init_db()
    with engine.begin() as conn:
        conn.execute(text("UPDATE bookings SET status = :status WHERE id = :bid"), {"status": status, "bid": booking_id})

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
        
        # Count pending bookings
        pending_bookings = conn.execute(text("SELECT COUNT(*) FROM bookings WHERE status='pending'")).scalar() or 0
        
        # Count recent visitors (last 24 hours)
        from datetime import datetime, timedelta
        cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        active_visitors = conn.execute(text("SELECT COUNT(DISTINCT sid) FROM events WHERE created_at > :cutoff"), {"cutoff": cutoff}).scalar() or 0
    
    return render_template('admin_dashboard.html', 
                         open_chats=open_chats,
                         new_enquiries=new_enquiries,
                         pending_bookings=pending_bookings,
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
        pending_bookings = conn.execute(text("SELECT COUNT(*) FROM bookings WHERE status='pending'")).scalar() or 0
        from datetime import datetime, timedelta
        cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        active_visitors = conn.execute(text("SELECT COUNT(DISTINCT sid) FROM events WHERE created_at > :cutoff"), {"cutoff": cutoff}).scalar() or 0
    
    return jsonify({
        "open_chats": open_chats,
        "new_enquiries": new_enquiries,
        "pending_bookings": pending_bookings,
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
    maintenance_mode = get_maintenance_mode()
    hero_imgs = get_hero_images()
    meet_andy = get_meet_andy()
    return render_template('admin_content.html', services=services, maintenance_mode_enabled=maintenance_mode, hero_imgs=hero_imgs, meet_andy=meet_andy)

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

@app.post('/admin/content/hero-images')
def admin_hero_images_update():
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    
    # Update each hero image URL
    for key in ['hero_slide_1', 'hero_slide_2', 'hero_strip_1', 'hero_strip_2', 'hero_strip_3', 'hero_strip_4']:
        url = request.form.get(key, '').strip()
        if url:  # Only update if URL is provided
            set_hero_image(key, url)
    
    return redirect(url_for('admin_content'))

@app.post('/admin/content/meet-andy')
def admin_meet_andy_update():
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    
    # Update Meet Andy content
    data = {
        'heading': request.form.get('heading', '').strip(),
        'paragraph1': request.form.get('paragraph1', '').strip(),
        'paragraph2': request.form.get('paragraph2', '').strip(),
        'badge1': request.form.get('badge1', '').strip(),
        'badge2': request.form.get('badge2', '').strip(),
        'badge3': request.form.get('badge3', '').strip(),
        'badge4': request.form.get('badge4', '').strip(),
    }
    
    update_meet_andy(data)
    
    return redirect(url_for('admin_content'))

@app.post('/admin/content/maintenance-mode')
def admin_maintenance_toggle():
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    
    enabled = request.form.get('enabled') == 'true'
    set_maintenance_mode(enabled)
    
    return redirect(url_for('admin_content'))


# ---------- Admin Booking Management ----------
@app.route('/admin/bookings')
def admin_bookings():
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    slots = fetch_booking_slots(include_past=True)
    bookings = fetch_bookings()
    return render_template('admin_bookings.html', slots=slots, bookings=bookings)

@app.post('/admin/bookings/slot/create')
def admin_create_slot():
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    
    date = request.form.get('date', '').strip()
    time = request.form.get('time', '').strip()
    duration = int(request.form.get('duration', '60'))
    capacity = int(request.form.get('capacity', '1'))
    price = request.form.get('price', '').strip()
    notes = request.form.get('notes', '').strip()
    
    if date and time:
        result = create_booking_slot(date, time, duration, capacity, price, notes)
        if not result['success']:
            # Conflict detected, redirect with error message
            from urllib.parse import quote
            return redirect(url_for('admin_bookings') + '?error=' + quote(result['error']))
    
    return redirect(url_for('admin_bookings'))

@app.post('/admin/bookings/slot/delete/<int:slot_id>')
def admin_delete_slot(slot_id: int):
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    
    success = delete_booking_slot(slot_id)
    if not success:
        # Slot has bookings, cannot delete
        return redirect(url_for('admin_bookings') + '?error=cannot_delete')
    
    return redirect(url_for('admin_bookings'))

@app.post('/admin/bookings/update/<int:booking_id>')
def admin_update_booking(booking_id: int):
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    
    status = request.form.get('status', '').strip()
    if status:
        update_booking_status(booking_id, status)
    
    return redirect(url_for('admin_bookings'))

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
