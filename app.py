def chat_send():
    init_db()
    # Refresh AI clients at the start of handling a send so admin-saved keys are picked up
    try:
        refresh_ai_clients()
    except Exception:
        pass
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
        try:
            conn.execute(text("INSERT INTO chat_messages (chat_id, sender, message, created_at) VALUES (:cid, :s, :m, :t)"), {"cid": int(chat_id), "s": sender, "m": message.strip()[:2000], "t": now})
            conn.execute(text("UPDATE chats SET last_activity=:t WHERE id=:cid"), {"cid": int(chat_id), "t": now})
            autopilot_on = get_autopilot_enabled()
            if sender == 'user' and autopilot_on:
                # Insert a short system typing indicator, then delegate to the centralized HTTP-first autopilot helper.
                conn.execute(text("INSERT INTO chat_messages (chat_id, sender, message, created_at) VALUES (:cid, 'system', :m, :t)"), {"cid": int(chat_id), "m": "[AI is responding...]", "t": datetime.utcnow().isoformat()})
                try:
                    autopilot_entry = _maybe_send_autopilot_reply_db(conn, int(chat_id))
                    if not autopilot_entry:
                        # No AI reply — insert a friendly fallback from admin
                        fallback = "Thanks for your message! I'm Andy's assistant. Could you please share your dog's breed, age, and your preferred walk times?"
                        conn.execute(text("INSERT INTO chat_messages (chat_id, sender, message, created_at) VALUES (:cid, 'admin', :m, :t)"), {"cid": int(chat_id), "m": fallback, "t": datetime.utcnow().isoformat()})
                except Exception as e:
                    conn.execute(text("INSERT INTO chat_messages (chat_id, sender, message, created_at) VALUES (:cid, 'system', :m, :t)"), {"cid": int(chat_id), "m": f"(Autopilot error: {str(e)[:120]})", "t": datetime.utcnow().isoformat()})
            elif sender == 'user' and not autopilot_on:
                # Autopilot is off: do not insert repetitive system messages for every user message.
                # Instead, create a lightweight admin notification so admin pages can alert staff.
                try:
                    # Fetch a bit of chat context for the payload
                    chat_row = conn.execute(text("SELECT sid, name FROM chats WHERE id=:cid"), {"cid": int(chat_id)}).fetchone()
                    payload_obj = {
                        'excerpt': (message or '')[:180],
                        'sid': getattr(chat_row, 'sid', None) if chat_row else None,
                        'name': getattr(chat_row, 'name', None) if chat_row else None,
                    }
                    conn.execute(text("INSERT INTO admin_notifications (type, chat_id, message_id, payload, created_at, seen) VALUES ('new_user_message', :cid, NULL, :payload, :t, 0)"), {"cid": int(chat_id), "payload": json.dumps(payload_obj, default=str), "t": now})
                except Exception:
                    # Swallow notification failures to avoid impacting chat flow
                    pass
        except Exception as e:
            # Log error and guarantee fallback message
            err_msg = f"(Autopilot error: {str(e)[:120]})"
            conn.execute(text("INSERT INTO chat_messages (chat_id, sender, message, created_at) VALUES (:cid, 'system', :m, :t)"), {"cid": chat_id, "m": err_msg, "t": datetime.utcnow().isoformat()})
            fallback = "Sorry, there was a technical issue. I'm Andy's assistant. Could you share your dog's breed, age, and preferred walk times?"
            conn.execute(text("INSERT INTO chat_messages (chat_id, sender, message, created_at) VALUES (:cid, 'admin', :m, :t)"), {"cid": chat_id, "m": fallback, "t": datetime.utcnow().isoformat()})
    return jsonify({"ok": True})
from flask import Flask, render_template, request, redirect, url_for, abort, Response, jsonify
import sqlite3
import os
import json
from datetime import datetime, timezone, timedelta
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import types

# Optional AI providers
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
DEEPSEEK_API_KEY = os.environ.get('DEEPSEEK_API_KEY')

# Gemini
try:
    import google.generativeai as genai  # type: ignore
    if GEMINI_API_KEY:
        genai.configure(api_key=GEMINI_API_KEY)
except Exception:
    genai = None
# DeepSeek (OpenAI-compatible)
try:
    if DEEPSEEK_API_KEY:
        try:
            from openai import OpenAI as NewOpenAI2  # type: ignore
            deepseek_client = NewOpenAI2(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com")
        except TypeError:
            # Some OpenAI package versions pass unexpected kwargs to httpx/client construction
            # Fall back to the older `openai` module if available and configure its base URL
            try:
                import openai as _legacy_openai
                _legacy_openai.api_key = DEEPSEEK_API_KEY
                # legacy client uses api_base
                try:
                    _legacy_openai.api_base = "https://api.deepseek.com"
                except Exception:
                    pass
                deepseek_client = _legacy_openai
            except Exception:
                deepseek_client = None
    else:
        deepseek_client = None
except Exception:
    deepseek_client = None


# OpenAI official / legacy client wrapper
try:
    openai_client = None
    if OPENAI_API_KEY:
        try:
            from openai import OpenAI as OpenAIClient  # type: ignore
            try:
                openai_client = OpenAIClient(api_key=OPENAI_API_KEY)
            except TypeError:
                # Some versions may not accept api_key kwarg
                import openai as _legacy_openai
                _legacy_openai.api_key = OPENAI_API_KEY
                openai_client = _legacy_openai
        except Exception:
            try:
                import openai as _legacy_openai
                if OPENAI_API_KEY:
                    _legacy_openai.api_key = OPENAI_API_KEY
                    openai_client = _legacy_openai
                else:
                    openai_client = None
            except Exception:
                openai_client = None
    else:
        openai_client = None
except Exception:
    openai_client = None


def refresh_ai_clients():
    """Reload AI provider API keys from environment or DB-backed settings.
    This is a lightweight refresh that updates global key variables and configures Gemini if available.
    """
    global OPENAI_API_KEY, GEMINI_API_KEY, DEEPSEEK_API_KEY
    global genai
    global openai_client, deepseek_client
    # Prefer environment variables but allow admin-saved DB values for testing
    try:
        OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY') or get_site_setting('OPENAI_API_KEY')
    except Exception:
        OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
    try:
        GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY') or get_site_setting('GEMINI_API_KEY')
    except Exception:
        GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
    try:
        DEEPSEEK_API_KEY = os.environ.get('DEEPSEEK_API_KEY') or get_site_setting('DEEPSEEK_API_KEY')
    except Exception:
        DEEPSEEK_API_KEY = os.environ.get('DEEPSEEK_API_KEY')

    # Configure Gemini client if module loaded
    try:
        if genai and GEMINI_API_KEY:
            genai.configure(api_key=GEMINI_API_KEY)
    except Exception:
        pass
    # Refresh OpenAI client wrapper
    try:
        if OPENAI_API_KEY:
            try:
                from openai import OpenAI as OpenAIClient  # type: ignore
                try:
                    openai_client = OpenAIClient(api_key=OPENAI_API_KEY)
                except TypeError:
                    import openai as _legacy_openai
                    _legacy_openai.api_key = OPENAI_API_KEY
                    openai_client = _legacy_openai
            except Exception:
                try:
                    import openai as _legacy_openai
                    _legacy_openai.api_key = OPENAI_API_KEY
                    openai_client = _legacy_openai
                except Exception:
                    openai_client = None
        else:
            openai_client = None
    except Exception:
        openai_client = None
    # Refresh DeepSeek client wrapper
    try:
        if DEEPSEEK_API_KEY:
            try:
                from openai import OpenAI as OpenAIClient  # type: ignore
                try:
                    deepseek_client = OpenAIClient(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com")
                except TypeError:
                    import openai as _legacy_openai
                    _legacy_openai.api_key = DEEPSEEK_API_KEY
                    try:
                        _legacy_openai.api_base = "https://api.deepseek.com"
                    except Exception:
                        pass
                    deepseek_client = _legacy_openai
            except Exception:
                deepseek_client = None
        else:
            deepseek_client = None
    except Exception:
        deepseek_client = None


# Attempt initial refresh on startup
try:
    refresh_ai_clients()
except Exception:
    pass

# DeepSeek (uses OpenAI-compatible API)
try:
    from openai import OpenAI  # type: ignore
    try:
        deepseek_client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com") if DEEPSEEK_API_KEY else None
    except TypeError:
        # Fall back to legacy openai module if the modern client constructor signature is incompatible
        try:
            import openai as _legacy_openai
            if DEEPSEEK_API_KEY:
                _legacy_openai.api_key = DEEPSEEK_API_KEY
                try:
                    _legacy_openai.api_base = "https://api.deepseek.com"
                except Exception:
                    pass
                deepseek_client = _legacy_openai
            else:
                deepseek_client = None
        except Exception:
            deepseek_client = None
except Exception:
    deepseek_client = None

app = Flask(__name__)
# Register chat_send route after app is created so decorators don't run before app exists
app.post('/chat/send')(chat_send)

@app.context_processor
def inject_globals():
    """Make maintenance_mode, hero_images and meet_andy available to all templates"""
    return {
        'maintenance_mode': get_maintenance_mode(),
        'hero_images': get_hero_images(),
        'meet_andy': get_meet_andy(),
        # Expose provider presence so templates don't guess
        'has_openai': bool(OPENAI_API_KEY),
        'has_gemini': bool(GEMINI_API_KEY),
        'has_deepseek': bool(DEEPSEEK_API_KEY),
        'autopilot_enabled': get_autopilot_enabled(),
        # Admin notification sound preference
        'admin_notifications_sound': (get_site_setting('admin_notifications_sound') == 'true')
    }

@app.before_request
def track_and_block_ips():
    """Track visitor IPs and block if necessary (except admin routes)"""
    # Skip tracking/blocking for admin routes
    if request.path.startswith('/admin'):
        return None
    
    # Get IP address
    ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    if ip and ',' in ip:
        ip = ip.split(',')[0].strip()
    
    # Track the visit
    user_agent = request.headers.get('User-Agent', '')
    track_ip_visit(ip, user_agent)
    
    # Check if blocked
    if is_ip_blocked(ip):
        return render_template('blocked.html'), 403

@app.route('/')
def home():
    try:
        services = fetch_services()
        service_areas = fetch_service_areas()
        contact_info = get_contact_info()
        homepage_sections = fetch_homepage_sections()
        # Optionally show a highlighted free slot randomly (1-10 roll). If roll==4 and slots exist,
        # pass the earliest available slot as `featured_slot` to the template. If no slots or roll!=4,
        # featured_slot will be None and nothing will display.
        try:
            import random
            slots = fetch_booking_slots()
            featured_slot = None
            if slots:
                roll = random.randint(1, 10)
                if roll == 4:
                    # pick the earliest available slot (first in list)
                    s = slots[0]
                    featured_slot = {
                        'id': s.get('id'),
                        'date': s.get('date'),
                        'time': s.get('time'),
                        'price': s.get('price') or ''
                    }
        except Exception:
            featured_slot = None
        return render_template('index.html', services=services, service_areas=service_areas, contact_info=contact_info, homepage_sections=homepage_sections, featured_slot=featured_slot)
    except Exception as e:
        import traceback
        error_msg = f"Error in home route: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)  # Log to console
        return f"<h1>Error</h1><pre>{error_msg}</pre>", 500

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
        location = request.form.get('location', '').strip()
        num_dogs = request.form.get('num_dogs', '1').strip()
        # Collect breeds: breed_1, breed_2, ... up to num_dogs
        try:
            num_dogs_i = int(num_dogs)
        except Exception:
            num_dogs_i = 1
        breeds = []
        for i in range(1, min(6, max(1, num_dogs_i)) + 1):
            v = request.form.get(f'breed_{i}')
            if v:
                breeds.append(v.strip())
        message = request.form.get('message', '').strip()
        
        # Get IP address
        ip_address = request.headers.get('X-Forwarded-For', request.remote_addr)
        if ip_address and ',' in ip_address:
            ip_address = ip_address.split(',')[0].strip()
        
        # Validation: require slot, name, email, location and breed(s)
        service_areas = fetch_service_areas()
        area_names = [a['name'] for a in service_areas]
        breeds_allowed = fetch_breeds()
        if not slot_id or not customer_name or not customer_email or not location:
            slots = fetch_booking_slots()
            return render_template('book.html', slots=slots, error="Please fill in all required fields.", form=request.form, ip=ip_address, service_areas=service_areas, breeds=breeds_allowed), 400
        if location not in area_names:
            slots = fetch_booking_slots()
            return render_template('book.html', slots=slots, error="Please select a valid location from the list.", form=request.form, ip=ip_address, service_areas=service_areas, breeds=breeds_allowed), 400
        if len(breeds) < 1:
            slots = fetch_booking_slots()
            return render_template('book.html', slots=slots, error="Please select the dog breed(s).", form=request.form, ip=ip_address, service_areas=service_areas, breeds=breeds_allowed), 400
        # Ensure selected breeds are allowed
        for b in breeds:
            if b not in breeds_allowed:
                slots = fetch_booking_slots()
                return render_template('book.html', slots=slots, error="Invalid dog breed selected.", form=request.form, ip=ip_address, service_areas=service_areas, breeds=breeds_allowed), 400
        
        # Create booking: compose dog_name and dog_info from breeds/num
        dog_name_field = breeds[0] if breeds else 'Dog'
        dog_info_field = f"Location: {location}; Breeds: {', '.join(breeds)}; Count: {len(breeds)}"
        booking_id = create_booking(
            slot_id=int(slot_id),
            customer_name=customer_name,
            customer_email=customer_email,
            customer_phone=customer_phone,
            dog_name=dog_name_field,
            dog_info=dog_info_field,
            service_type=request.form.get('service_type', '').strip(),
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
    
    # Provide service areas and breeds for the booking form
    service_areas = fetch_service_areas()
    breeds = fetch_breeds()
    return render_template('book.html', slots=slots, success=success, ip=ip_address, service_areas=service_areas, breeds=breeds)


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
    # Avoid running DDL on every request — perform once per process.
    global _db_initialized
    try:
        if _db_initialized:
            return
    except NameError:
        _db_initialized = False

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
        # Notifications for admin UI (new visitor messages etc.)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS admin_notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL,
                chat_id INTEGER,
                message_id INTEGER,
                payload TEXT,
                created_at TEXT NOT NULL,
                seen INTEGER DEFAULT 0
            )
        """))
        # Service Areas table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS service_areas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                sort_order INTEGER DEFAULT 0
            )
        """))
        # Seed default service areas if table is empty
        count = conn.execute(text("SELECT COUNT(*) FROM service_areas")).scalar()
        if count == 0:
            default_areas = [
                ("Downtown", 1),
                ("Riverside Park District", 2),
                ("Northside Neighborhood", 3),
                ("Central Commons", 4),
                ("West End", 5),
                ("East Hills", 6),
            ]
            for name, order in default_areas:
                conn.execute(text(
                    "INSERT INTO service_areas (name, sort_order) VALUES (:name, :order)"
                ), {"name": name, "order": order})
        # Homepage Sections table for section ordering
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS homepage_sections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                section_key TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                enabled INTEGER DEFAULT 1,
                sort_order INTEGER DEFAULT 0
            )
        """))
        # Seed default homepage sections if table is empty
        count = conn.execute(text("SELECT COUNT(*) FROM homepage_sections")).scalar()
        if count == 0:
            default_sections = [
                ("features", "Key Features", 1, 1),
                ("services", "Services & Pricing", 1, 2),
                ("meet-andy", "Meet Andy", 1, 3),
                ("service-areas", "Service Areas", 1, 4),
                ("how-it-works", "Book a Walk in 3 Simple Steps", 1, 5),
                ("photo-strip", "GPS Tracking & Photo Updates", 1, 6),
                ("enquiry", "Get in Touch", 1, 7),
                ("testimonials", "Testimonials", 1, 8),
                ("gallery", "Gallery", 1, 9),
                ("cta", "Final Call to Action", 1, 10),
            ]
            for key, title, enabled, order in default_sections:
                conn.execute(text(
                    "INSERT INTO homepage_sections (section_key, title, enabled, sort_order) VALUES (:key, :title, :enabled, :order)"
                ), {"key": key, "title": title, "enabled": enabled, "order": order})
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
        # Seed default contact info if not exists
        count = conn.execute(text("SELECT COUNT(*) FROM site_content WHERE section='contact'")).scalar()
        if count == 0:
            contact_content = [
                ("profile_title", None, None, "Friendly, Reliable, Local", 1),
                ("phone", None, None, "07595 289669", 2),
                ("email", None, None, "hello@happypawswalking.com", 3),
            ]
            for key, title, price, content, order in contact_content:
                conn.execute(text(
                    "INSERT INTO site_content (section, key, title, price, content, sort_order) VALUES (:sec, :key, :title, :price, :content, :order)"
                ), {"sec": "contact", "key": key, "title": title, "price": price, "content": content, "order": order})
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
        # Initialize chat autopilot setting (off by default)
        try:
            conn.execute(text("INSERT INTO site_settings (key, value) VALUES ('chat_autopilot', 'false')"))
        except Exception:
            pass
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
        # IP tracking and blocking table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ip_tracking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ip_address TEXT UNIQUE NOT NULL,
                visit_count INTEGER DEFAULT 1,
                is_blocked INTEGER DEFAULT 0,
                country TEXT,
                city TEXT,
                first_visit TEXT NOT NULL,
                last_visit TEXT NOT NULL,
                user_agent TEXT
            )
        """))
        # Mark DB init complete so subsequent requests skip DDL work
        _db_initialized = True

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
    try:
        init_db()
        with engine.begin() as conn:
            result = conn.execute(text("SELECT id, key, title, price, content, sort_order FROM site_content WHERE section='services' ORDER BY sort_order ASC"))
            services = []
            for r in result:
                services.append({
                    'id': r.id,
                    'key': r.key,
                    'title': r.title,
                    'price': r.price or '',
                    'content': r.content or '',
                    'sort_order': r.sort_order
                })
            return services
    except Exception as e:
        print(f"Error fetching services: {e}")
        return []  # Return empty list on error

def fetch_service_areas():
    """Fetch all service areas from database"""
    try:
        init_db()
        with engine.begin() as conn:
            result = conn.execute(text("SELECT id, name, sort_order FROM service_areas ORDER BY sort_order ASC"))
            areas = []
            for r in result:
                areas.append({
                    'id': r.id,
                    'name': r.name,
                    'sort_order': r.sort_order
                })
            return areas
    except Exception as e:
        print(f"Error fetching service areas: {e}")
        return []  # Return empty list on error


def fetch_breeds():
    """Fetch configured dog breeds from site_content section 'breeds'. Returns list of names."""
    try:
        init_db()
        with engine.begin() as conn:
            result = conn.execute(text("SELECT key, title, content FROM site_content WHERE section='breeds' ORDER BY sort_order ASC"))
            breeds = []
            for r in result:
                # Use title if present otherwise content/key
                name = (r.title or r.content or r.key or '').strip()
                if name:
                    breeds.append(name)
            return breeds
    except Exception:
        return []

def fetch_homepage_sections():
    """Fetch all homepage sections in display order"""
    try:
        init_db()
        with engine.begin() as conn:
            result = conn.execute(text("SELECT id, section_key, title, enabled, sort_order FROM homepage_sections ORDER BY sort_order ASC"))
            sections = []
            for r in result:
                sections.append({
                    'id': r.id,
                    'section_key': r.section_key,
                    'title': r.title,
                    'enabled': bool(r.enabled) if r.enabled is not None else True,  # Convert to boolean
                    'sort_order': r.sort_order
                })
            return sections
    except Exception as e:
        print(f"Error fetching homepage sections: {e}")
        return []  # Return empty list on error

def move_section_up(section_id: int):
    """Move a section up in the order"""
    init_db()
    with engine.begin() as conn:
        # Get current section
        current = conn.execute(text("SELECT id, sort_order FROM homepage_sections WHERE id=:id"), {"id": section_id}).fetchone()
        if not current:
            return
        
        # Find the section above it
        above = conn.execute(text(
            "SELECT id, sort_order FROM homepage_sections WHERE sort_order < :order ORDER BY sort_order DESC LIMIT 1"
        ), {"order": current.sort_order}).fetchone()
        
        if above:
            # Swap sort orders
            conn.execute(text("UPDATE homepage_sections SET sort_order=:order WHERE id=:id"), 
                        {"order": above.sort_order, "id": current.id})
            conn.execute(text("UPDATE homepage_sections SET sort_order=:order WHERE id=:id"), 
                        {"order": current.sort_order, "id": above.id})

def move_section_down(section_id: int):
    """Move a section down in the order"""
    init_db()
    with engine.begin() as conn:
        # Get current section
        current = conn.execute(text("SELECT id, sort_order FROM homepage_sections WHERE id=:id"), {"id": section_id}).fetchone()
        if not current:
            return
        
        # Find the section below it
        below = conn.execute(text(
            "SELECT id, sort_order FROM homepage_sections WHERE sort_order > :order ORDER BY sort_order ASC LIMIT 1"
        ), {"order": current.sort_order}).fetchone()
        
        if below:
            # Swap sort orders
            conn.execute(text("UPDATE homepage_sections SET sort_order=:order WHERE id=:id"), 
                        {"order": below.sort_order, "id": current.id})
            conn.execute(text("UPDATE homepage_sections SET sort_order=:order WHERE id=:id"), 
                        {"order": current.sort_order, "id": below.id})

def toggle_section_visibility(section_id: int):
    """Toggle whether a section is visible on the homepage"""
    init_db()
    with engine.begin() as conn:
        current = conn.execute(text("SELECT enabled FROM homepage_sections WHERE id=:id"), {"id": section_id}).fetchone()
        if current:
            new_value = 0 if current.enabled else 1
            conn.execute(text("UPDATE homepage_sections SET enabled=:enabled WHERE id=:id"), 
                        {"enabled": new_value, "id": section_id})

def get_maintenance_mode():
    """Get current maintenance mode status"""
    init_db()
    with engine.begin() as conn:
        result = conn.execute(text("SELECT value FROM site_settings WHERE key='maintenance_mode'")).fetchone()
        return result.value == 'true' if result else False


def get_business_description() -> str:
    """Return the admin-provided business description used to seed the autopilot context."""
    init_db()
    with engine.begin() as conn:
        row = conn.execute(text("SELECT value FROM site_settings WHERE key='business_description'")).fetchone()
        return row.value if row and row.value else ''


def set_business_description(text_value: str):
    """Save the admin-provided business description into site_settings."""
    init_db()
    with engine.begin() as conn:
        try:
            conn.execute(text("INSERT INTO site_settings (key, value) VALUES ('business_description', :v) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"), {"v": text_value})
        except Exception:
            # SQLite fallback
            conn.execute(text("INSERT OR REPLACE INTO site_settings (key, value) VALUES ('business_description', :v)"), {"v": text_value})


def get_site_setting(key: str) -> str:
    """Return a site_settings value for the given key (or empty string)."""
    init_db()
    with engine.begin() as conn:
        row = conn.execute(text("SELECT value FROM site_settings WHERE key=:k"), {"k": key}).fetchone()
        return row.value if row and row.value is not None else ''


def set_site_setting(key: str, value: str):
    """Insert or replace a site_settings key/value pair."""
    init_db()
    with engine.begin() as conn:
        try:
            # Postgres UPSERT
            conn.execute(text("INSERT INTO site_settings (key, value) VALUES (:k, :v) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"), {"k": key, "v": value})
        except Exception:
            conn.execute(text("INSERT OR REPLACE INTO site_settings (key, value) VALUES (:k, :v)"), {"k": key, "v": value})

def get_autopilot_enabled():
    """Return True if chat autopilot is enabled."""
    init_db()
    with engine.begin() as conn:
        row = conn.execute(text("SELECT value FROM site_settings WHERE key='chat_autopilot'")) .fetchone()
        return row.value == 'true' if row else False

def set_autopilot_enabled(enabled: bool):
    """Toggle chat autopilot setting."""
    init_db()
    val = 'true' if enabled else 'false'
    with engine.begin() as conn:
        # Try Postgres-style UPSERT first
        try:
            conn.execute(text("""
                INSERT INTO site_settings (key, value) 
                VALUES ('chat_autopilot', :v)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """), {"v": val})
        except Exception:
            # Fallback for SQLite
            conn.execute(text("INSERT OR REPLACE INTO site_settings (key, value) VALUES ('chat_autopilot', :v)"), {"v": val})


def set_maintenance_mode(enabled: bool):
    """Set maintenance mode on or off"""
    init_db()
    value = 'true' if enabled else 'false'
    with engine.begin() as conn:
        conn.execute(text("INSERT OR REPLACE INTO site_settings (key, value) VALUES ('maintenance_mode', :val)"), {"val": value})


# ---- Autopilot helpers (HTTP-first, modeled after the working booking_app) ----
DEFAULT_AUTOPILOT_MODEL = "gpt-3.5-turbo"
DEFAULT_AUTOPILOT_TEMPERATURE = 0.3
AUTOPILOT_HISTORY_LIMIT = 12


def _build_autopilot_messages(conversation, *, business_profile: str) -> list:
    """Build a list of chat messages suitable for the OpenAI chat/completions API.
    conversation is a list of dicts with at least 'sender' and 'text' keys.
    """
    instructions = "You are Andy's Dog Walking assistant. Keep replies friendly, concise (<80 words), and actionable. If pricing or availability is unclear, invite them to share their dog's needs."
    business_profile = (business_profile or '').strip()
    messages = [{"role": "system", "content": f"{instructions}\n\nBusiness knowledge:\n{business_profile}"}]

    history = list(conversation or [])[-AUTOPILOT_HISTORY_LIMIT:]
    for entry in history:
        if not isinstance(entry, dict):
            continue
        text = str(entry.get("text") or entry.get("message") or "").strip()
        if not text:
            continue
        # Skip invites or internal system rows
        if str(entry.get("type") or entry.get("message_type") or "") == "invite":
            continue
        sender = (entry.get("sender") or "").lower()
        role = "assistant" if sender in {"admin", "autopilot"} else "user"
        messages.append({"role": role, "content": text})

    return messages


def _request_autopilot_reply(messages, *, model: str, temperature: float, api_key: str) -> str:
    """Make a single HTTP POST to the OpenAI chat completions endpoint and return assistant text or empty string on failure."""
    try:
        import requests
    except Exception:
        return ""

    if not api_key or not messages:
        return ""

    payload = {
        "model": model or DEFAULT_AUTOPILOT_MODEL,
        "messages": messages,
        "temperature": float(temperature or DEFAULT_AUTOPILOT_TEMPERATURE),
        "max_tokens": 350,
    }

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
    try:
        r = requests.post("https://api.openai.com/v1/chat/completions", json=payload, headers=headers, timeout=15)
    except Exception:
        return ""

    try:
        if r.status_code != 200:
            return ""
        j = r.json()
    except Exception:
        return ""

    choices = j.get("choices") if isinstance(j, dict) else None
    if not isinstance(choices, list) or not choices:
        return ""

    message_payload = choices[0].get("message") if isinstance(choices[0], dict) else {}
    if not isinstance(message_payload, dict):
        return ""

    content = message_payload.get("content")
    if isinstance(content, str):
        return content.strip()

    # Fallback for older OpenAI shapes
    text_choice = choices[0].get("text") if isinstance(choices[0], dict) else None
    if isinstance(text_choice, str):
        return text_choice.strip()

    return ""


def _maybe_send_autopilot_reply_db(conn, chat_id: int, conversation=None):
    """If autopilot is enabled and an API key is available, request an AI reply and insert it into DB using the provided connection.
    Returns the inserted row dict on success or None.
    """
    try:
        if not get_autopilot_enabled():
            return None
    except Exception:
        return None

    # Determine which provider to use (admin-selectable)
    provider = (get_site_setting('AUTOPILOT_PROVIDER') or 'auto').lower()
    openai_key = get_site_setting('OPENAI_API_KEY') or os.environ.get('OPENAI_API_KEY') or globals().get('OPENAI_API_KEY') or ''
    ds_key = get_site_setting('DEEPSEEK_API_KEY') or globals().get('DEEPSEEK_API_KEY') or ''
    gemini_key = get_site_setting('GEMINI_API_KEY') or globals().get('GEMINI_API_KEY') or ''

    # Auto-select provider if requested
    if provider == 'auto' or not provider:
        if ds_key:
            provider = 'deepseek'
        elif openai_key:
            provider = 'openai'
        elif gemini_key:
            provider = 'gemini'
        else:
            return None

    # If conversation not provided, load from DB
    if conversation is None:
        rows = conn.execute(text("SELECT sender, message as text, created_at, id FROM chat_messages WHERE chat_id=:cid ORDER BY id ASC"), {"cid": chat_id}).fetchall()
        conversation = [dict(r) for r in rows]

    business_profile = get_business_description()
    messages = _build_autopilot_messages(conversation, business_profile=business_profile)
    if len(messages) <= 1:
        return None
    model = get_site_setting('DEEPSEEK_MODEL') or get_site_setting('OPENAI_MODEL') or DEFAULT_AUTOPILOT_MODEL
    try:
        temperature = float(get_site_setting('OPENAI_TEMPERATURE') or DEFAULT_AUTOPILOT_TEMPERATURE)
    except Exception:
        temperature = DEFAULT_AUTOPILOT_TEMPERATURE

    # Try providers in an order that respects admin preference but falls back.
    def try_openai(key):
        if not key:
            return None
        return _request_autopilot_reply(messages, model=model, temperature=temperature, api_key=key)

    def try_deepseek(key):
        if not key:
            return None
        try:
            import requests
            endpoints = [
                'https://api.deepseek.com/v1/chat/completions',
                'https://api.deepseek.com/v1/responses'
            ]
            headers = {'Authorization': f'Bearer {key}', 'Content-Type': 'application/json'}
            default_models = ['deepseek-chat', 'deepseek-reasoner', 'gpt-4o', 'gpt-3.5-turbo', 'gpt-3.5']
            models = [m for m in ([model] + default_models) if m]
            for url in endpoints:
                for m in models:
                    payload = {'model': m, 'messages': messages, 'max_tokens': 350, 'temperature': temperature}
                    try:
                        r = requests.post(url, json=payload, headers=headers, timeout=15)
                    except Exception:
                        continue
                    if r.status_code != 200:
                        continue
                    try:
                        j = r.json()
                    except Exception:
                        return (r.text or '').strip()
                    # Extract content from common shapes
                    got = None
                    if isinstance(j, dict):
                        if j.get('choices'):
                            try:
                                got = j['choices'][0]['message']['content']
                            except Exception:
                                got = j['choices'][0].get('text')
                        if not got:
                            got = j.get('output') or j.get('output_text') or None
                        if not got and j.get('data'):
                            try:
                                d0 = j['data'][0]
                                if isinstance(d0, dict):
                                    got = d0.get('text') or d0.get('content') or None
                            except Exception:
                                pass
                    if got:
                        return got.strip()
            return None
        except Exception:
            return None

    def try_gemini(key):
        if not key or not globals().get('genai'):
            return None
        try:
            model_obj = genai.GenerativeModel('gemini-1.5-mini')
            # Build a simple prompt from the last visitor message
            last_user = None
            for m in reversed(messages):
                if m.get('role') == 'user':
                    last_user = m.get('content')
                    break
            r = model_obj.generate_content(last_user or '')
            result = getattr(r, 'text', None)
            if not result and getattr(r, 'candidates', None):
                try:
                    result = r.candidates[0].content.parts[0].text
                except Exception:
                    result = None
            return result
        except Exception:
            return None

    diagnostics = []
    # Determine ordered list based on admin preference
    attempts = []
    p = provider
    # If DeepSeek key is present, prefer it (worked in Quick AI Test); this makes autopilot more likely to succeed
    if ds_key:
        attempts = [('deepseek', ds_key), ('openai', openai_key), ('gemini', gemini_key)]
    else:
        if p == 'openai':
            attempts = [('openai', openai_key), ('deepseek', ds_key), ('gemini', gemini_key)]
        elif p == 'deepseek':
            attempts = [('deepseek', ds_key), ('openai', openai_key), ('gemini', gemini_key)]
        elif p == 'gemini':
            attempts = [('gemini', gemini_key), ('deepseek', ds_key), ('openai', openai_key)]
        else:
            # default auto: prefer OpenAI if no DeepSeek present
            attempts = [('openai', openai_key), ('deepseek', ds_key), ('gemini', gemini_key)]

    reply_text = None
    for name, key in attempts:
        if not key:
            continue
        if name == 'openai':
            try:
                reply_text = try_openai(key)
            except Exception as e:
                diagnostics.append(f"openai_error:{str(e)[:180]}")
        elif name == 'deepseek':
            try:
                reply_text = try_deepseek(key)
            except Exception as e:
                diagnostics.append(f"deepseek_error:{str(e)[:180]}")
        elif name == 'gemini':
            try:
                reply_text = try_gemini(key)
            except Exception as e:
                diagnostics.append(f"gemini_error:{str(e)[:180]}")
        if reply_text:
            break

    # If no reply, store diagnostics for visibility
    if not reply_text:
        diagnostics_text = ' | '.join(diagnostics[-6:]) if diagnostics else ''
        try:
            dbg = f"Autopilot attempts: {[(n) for n,_ in attempts]}; diagnostics: {diagnostics_text}"
            conn.execute(text("INSERT INTO chat_messages (chat_id, sender, message, created_at) VALUES (:cid, 'system', :m, :t)"), {"cid": chat_id, "m": dbg, "t": datetime.utcnow().isoformat()})
        except Exception:
            pass
    clean_reply = (reply_text or "").strip()
    if not clean_reply:
        # Insert a short debug/system message so admins can see why autopilot didn't reply
        try:
            debug_info = f"Autopilot debug: provider={provider}, has_openai={bool(openai_key)}, has_deepseek={bool(ds_key)}, has_gemini={bool(gemini_key)}, messages={len(messages)}"
            conn.execute(text("INSERT INTO chat_messages (chat_id, sender, message, created_at) VALUES (:cid, 'system', :m, :t)"), {"cid": chat_id, "m": debug_info, "t": datetime.utcnow().isoformat()})
        except Exception:
            pass
        return None

    now = datetime.utcnow().isoformat()
    try:
        conn.execute(text("INSERT INTO chat_messages (chat_id, sender, message, created_at) VALUES (:cid, 'admin', :m, :t)"), {"cid": chat_id, "m": clean_reply[:2000], "t": now})
        return {"chat_id": chat_id, "sender": "admin", "message": clean_reply, "created_at": now}
    except Exception:
        return None


def get_hero_images():
    """Get all hero image URLs"""
    init_db()
    default_images = {
        'hero_slide_1': 'https://image.pollinations.ai/prompt/stocky%20build%20dark-haired%20male%20dog%20walker%20back%20view%20walking%20a%20cute%20small%20dog%2C%20black%20trousers%2C%20no%20suit%2C%20professional%20casual%2C%20no%20face%20visible%2C%20emerald%20and%20amber%20tones%2C%20photorealistic%2C%20crisp%20lighting?width=1100&height=1400&nologo=true',
        'hero_slide_2': 'https://image.pollinations.ai/prompt/stocky%20dark-haired%20male%20dog%20walker%20mid%20section%20holding%20lead%20with%20cute%20dog%2C%20black%20trousers%2C%20no%20suit%2C%20professional%20casual%2C%20face%20out%20of%20frame%2C%20emerald%20amber%20color%20grading%2C%20photorealistic?width=1100&height=1400&nologo=true',
        'hero_strip_1': '',
        'hero_strip_2': '',
        'hero_strip_3': '',
        'hero_strip_4': ''
    }
    try:
        with engine.begin() as conn:
            images = {}
            for key in ['hero_slide_1', 'hero_slide_2', 'hero_strip_1', 'hero_strip_2', 'hero_strip_3', 'hero_strip_4']:
                result = conn.execute(text("SELECT value FROM site_settings WHERE key=:key"), {"key": key}).fetchone()
                images[key] = result.value if result and result.value else default_images.get(key, '')
            return images
    except Exception:
        # If database error, return defaults
        return default_images

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

def get_contact_info():
    """Get contact info section content"""
    try:
        init_db()
        with engine.begin() as conn:
            result = conn.execute(text("SELECT key, title, content FROM site_content WHERE section='contact' ORDER BY sort_order ASC"))
            content = {}
            for row in result:
                content[row.key] = {'title': row.title or '', 'content': row.content or ''}
            # Ensure required keys exist with defaults
            if 'profile_title' not in content:
                content['profile_title'] = {'title': '', 'content': 'Friendly, Reliable, Local'}
            if 'phone' not in content:
                content['phone'] = {'title': '', 'content': '07595 289669'}
            if 'email' not in content:
                content['email'] = {'title': '', 'content': 'hello@happypawswalking.com'}
            return content
    except Exception as e:
        print(f"Error fetching contact info: {e}")
        return {
            'profile_title': {'title': '', 'content': 'Friendly, Reliable, Local'},
            'phone': {'title': '', 'content': '07595 289669'},
            'email': {'title': '', 'content': 'hello@happypawswalking.com'}
        }

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

def track_ip_visit(ip_address: str, user_agent: str = ''):
    """Track or update IP visit"""
    init_db()
    from datetime import datetime
    now = datetime.utcnow().isoformat() + 'Z'
    
    with engine.begin() as conn:
        # Check if IP exists
        existing = conn.execute(text("SELECT id, visit_count FROM ip_tracking WHERE ip_address = :ip"), {"ip": ip_address}).fetchone()
        
        if existing:
            # Update visit count and last visit
            new_count = existing.visit_count + 1
            conn.execute(text("""
                UPDATE ip_tracking 
                SET visit_count = :count, last_visit = :last, user_agent = :ua
                WHERE ip_address = :ip
            """), {"count": new_count, "last": now, "ua": user_agent, "ip": ip_address})
        else:
            # Insert new IP
            conn.execute(text("""
                INSERT INTO ip_tracking (ip_address, visit_count, is_blocked, first_visit, last_visit, user_agent)
                VALUES (:ip, 1, 0, :first, :last, :ua)
            """), {"ip": ip_address, "first": now, "last": now, "ua": user_agent})

def is_ip_blocked(ip_address: str) -> bool:
    """Check if an IP is blocked"""
    init_db()
    with engine.begin() as conn:
        result = conn.execute(text("SELECT is_blocked FROM ip_tracking WHERE ip_address = :ip"), {"ip": ip_address}).fetchone()
        return result.is_blocked == 1 if result else False

def fetch_all_ips():
    """Fetch all tracked IPs with stats"""
    init_db()
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT ip_address, visit_count, is_blocked, country, city, first_visit, last_visit, user_agent
            FROM ip_tracking
            ORDER BY last_visit DESC, visit_count DESC
        """))
        ips = []
        for r in result:
            ips.append({
                'ip_address': r.ip_address,
                'visit_count': r.visit_count,
                'is_blocked': r.is_blocked == 1,
                'country': r.country or 'Unknown',
                'city': r.city or 'Unknown',
                'first_visit': r.first_visit,
                'last_visit': r.last_visit,
                'user_agent': r.user_agent or ''
            })
        return ips

def toggle_ip_block(ip_address: str, block: bool):
    """Block or unblock an IP address"""
    init_db()
    with engine.begin() as conn:
        conn.execute(text("UPDATE ip_tracking SET is_blocked = :blocked WHERE ip_address = :ip"), 
                    {"blocked": 1 if block else 0, "ip": ip_address})

def delete_ip(ip_address: str):
    """Delete an IP address from tracking"""
    init_db()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM ip_tracking WHERE ip_address = :ip"), {"ip": ip_address})

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


@app.route('/admin/notifications/next')
def admin_notifications_next():
    """Return next unseen admin notifications (and optionally mark them seen).
    GET params: mark_seen=1 will mark returned notifications as seen.
    """
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result

    init_db()
    mark = request.args.get('mark_seen') == '1'
    results = []
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT id, type, chat_id, message_id, payload, created_at FROM admin_notifications WHERE seen=0 ORDER BY id ASC LIMIT 20")).fetchall()
        for r in rows:
            results.append({
                'id': r.id,
                'type': r.type,
                'chat_id': r.chat_id,
                'message_id': getattr(r, 'message_id', None),
                'payload': (r.payload or ''),
                'created_at': r.created_at,
            })
        if mark and results:
            ids = [str(r['id']) for r in results]
            try:
                conn.execute(text(f"UPDATE admin_notifications SET seen=1 WHERE id IN ({','.join(ids)})"))
            except Exception:
                pass

    return jsonify({'ok': True, 'notifications': results})

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
    # Render a simple visitors page showing recent visitor sessions and stats
    visitors = fetch_visitors()
    stats = fetch_visitor_stats()
    return render_template('admin_visitors.html', visitors=visitors, stats=stats)

@app.post('/chat/start')
def chat_start():
    """Create a new chat session and return the chat_id"""
    init_db()
    from datetime import datetime
    now = datetime.utcnow().isoformat()
    ip = _client_ip()
    sid = request.cookies.get('sid') or request.form.get('sid') or ''
    
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO chats (sid, name, status, created_at, last_activity, ip)
            VALUES (:sid, NULL, 'open', :created, :created, :ip)
        """), {"sid": sid or None, "created": now, "ip": ip})
        chat_id = result.lastrowid
        # Notify admins of a new visitor chat (new_chat)
        try:
            payload_obj = {'excerpt': 'New visitor started a chat', 'sid': sid or ''}
            conn.execute(text("INSERT INTO admin_notifications (type, chat_id, message_id, payload, created_at, seen) VALUES ('new_chat', :cid, NULL, :payload, :t, 0)"), {"cid": int(chat_id), "payload": json.dumps(payload_obj, default=str), "t": now})
        except Exception:
            pass
    
    return jsonify({"ok": True, "chat_id": chat_id})


# New endpoint: AI-only reply using selected provider/settings. Returns JSON {reply: str} or {error: str}
@app.post('/chat/ai-reply')
def chat_ai_reply():
    """Accepts form or JSON with 'message' and returns a single AI-crafted reply using the selected provider."""
    init_db()
    try:
        refresh_ai_clients()
    except Exception:
        pass
    data = request.form or (request.get_json(silent=True) or {})
    message = (data.get('message') or '').strip()
    if not message:
        return jsonify({"error": "No message provided"}), 400

    # Respect global autopilot toggle: if admin has turned autopilot OFF, refuse to answer
    try:
        if not get_autopilot_enabled():
            return jsonify({"error": "Autopilot is currently OFF"}), 403
    except Exception:
        # If the check fails for any reason, proceed conservatively and refuse
        return jsonify({"error": "Autopilot unavailable"}), 503

    # Build a minimal conversation: system + user message, including business description
    business_profile = get_business_description()
    conv = [{'sender': 'user', 'message': message}]
    messages = _build_autopilot_messages(conv, business_profile=business_profile)
    if len(messages) <= 1:
        return jsonify({"error": "Invalid conversation"}), 400

    model = get_site_setting('DEEPSEEK_MODEL') or get_site_setting('OPENAI_MODEL') or DEFAULT_AUTOPILOT_MODEL
    try:
        temperature = float(get_site_setting('OPENAI_TEMPERATURE') or DEFAULT_AUTOPILOT_TEMPERATURE)
    except Exception:
        temperature = DEFAULT_AUTOPILOT_TEMPERATURE

    # Choose provider preference similarly to autopilot helper
    provider = (get_site_setting('AUTOPILOT_PROVIDER') or 'auto').lower()
    openai_key = get_site_setting('OPENAI_API_KEY') or os.environ.get('OPENAI_API_KEY') or globals().get('OPENAI_API_KEY') or ''
    ds_key = get_site_setting('DEEPSEEK_API_KEY') or globals().get('DEEPSEEK_API_KEY') or ''
    gemini_key = get_site_setting('GEMINI_API_KEY') or globals().get('GEMINI_API_KEY') or ''

    # Build attempts order
    attempts = []
    if ds_key:
        attempts = [('deepseek', ds_key), ('openai', openai_key), ('gemini', gemini_key)]
    else:
        if provider == 'openai':
            attempts = [('openai', openai_key), ('deepseek', ds_key), ('gemini', gemini_key)]
        elif provider == 'deepseek':
            attempts = [('deepseek', ds_key), ('openai', openai_key), ('gemini', gemini_key)]
        elif provider == 'gemini':
            attempts = [('gemini', gemini_key), ('deepseek', ds_key), ('openai', openai_key)]
        else:
            attempts = [('openai', openai_key), ('deepseek', ds_key), ('gemini', gemini_key)]

    reply_text = None
    # Reuse helper functions from _maybe_send_autopilot_reply_db scope by re-implementing small tryers here
    def try_openai_local(key):
        if not key:
            return None
        return _request_autopilot_reply(messages, model=model, temperature=temperature, api_key=key)

    def try_deepseek_local(key):
        if not key:
            return None
        try:
            import requests
            endpoints = [
                'https://api.deepseek.com/v1/chat/completions',
                'https://api.deepseek.com/v1/responses'
            ]
            headers = {'Authorization': f'Bearer {key}', 'Content-Type': 'application/json'}
            default_models = ['deepseek-chat', 'deepseek-reasoner', 'gpt-4o', 'gpt-3.5-turbo', 'gpt-3.5']
            models = [m for m in ([model] + default_models) if m]
            for url in endpoints:
                for m in models:
                    payload = {'model': m, 'messages': messages, 'max_tokens': 350, 'temperature': temperature}
                    try:
                        r = requests.post(url, json=payload, headers=headers, timeout=15)
                    except Exception:
                        continue
                    if r.status_code != 200:
                        continue
                    try:
                        j = r.json()
                    except Exception:
                        return (r.text or '').strip()
                    got = None
                    if isinstance(j, dict):
                        if j.get('choices'):
                            try:
                                got = j['choices'][0]['message']['content']
                            except Exception:
                                got = j['choices'][0].get('text')
                        if not got:
                            got = j.get('output') or j.get('output_text') or None
                        if not got and j.get('data'):
                            try:
                                d0 = j['data'][0]
                                if isinstance(d0, dict):
                                    got = d0.get('text') or d0.get('content') or None
                            except Exception:
                                pass
                    if got:
                        return got.strip()
            return None
        except Exception:
            return None

    def try_gemini_local(key):
        if not key or not globals().get('genai'):
            return None
        try:
            model_obj = genai.GenerativeModel('gemini-1.5-mini')
            last_user = message
            r = model_obj.generate_content(last_user or '')
            result = getattr(r, 'text', None)
            if not result and getattr(r, 'candidates', None):
                try:
                    result = r.candidates[0].content.parts[0].text
                except Exception:
                    result = None
            return result
        except Exception:
            return None

    for name, key in attempts:
        if not key:
            continue
        if name == 'openai':
            try:
                reply_text = try_openai_local(key)
            except Exception:
                reply_text = None
        elif name == 'deepseek':
            try:
                reply_text = try_deepseek_local(key)
            except Exception:
                reply_text = None
        elif name == 'gemini':
            try:
                reply_text = try_gemini_local(key)
            except Exception:
                reply_text = None
        if reply_text:
            break

    clean_reply = (reply_text or '').strip()
    if not clean_reply:
        return jsonify({"error": "No AI reply available"}), 502
    return jsonify({"ok": True, "reply": clean_reply})

@app.get('/chat/autopilot-status')
def chat_autopilot_status():
    """Return the current autopilot status"""
    return jsonify({"autopilot": get_autopilot_enabled()})

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
    from datetime import datetime, timezone
    STALE_MINUTES = 60
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT id, sid, name, status, created_at, last_activity FROM chats ORDER BY CASE status WHEN 'open' THEN 0 ELSE 1 END, last_activity DESC"))
        chats = []
        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        for r in rows:
            last = r.last_activity
            stale = False
            age_minutes = None
            if last:
                try:
                    dt = datetime.fromisoformat(last)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    age_minutes = (now - dt).total_seconds() / 60.0
                    if age_minutes > STALE_MINUTES and r.status == 'open':
                        stale = True
                except Exception:
                    age_minutes = None
            display_status = r.status
            if r.status == 'open' and stale:
                display_status = 'ended'
            chats.append(dict(id=r.id, sid=r.sid, name=r.name or '', status=r.status, display_status=display_status, stale=stale, age_minutes=age_minutes, created_at=r.created_at, last_activity=r.last_activity))
    return render_template('admin_chats.html', chats=chats)


@app.get('/admin/chats/list.json')
def admin_chats_list_json():
    """Lightweight JSON list of chats for admin UI auto-refresh.
    Returns minimal per-chat fields to render the grid on the client.
    """
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    init_db()
    from datetime import datetime, timezone
    STALE_MINUTES = 60
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT id, sid, name, status, created_at, last_activity FROM chats ORDER BY CASE status WHEN 'open' THEN 0 ELSE 1 END, last_activity DESC"))
        chats = []
        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        for r in rows:
            last = r.last_activity
            stale = False
            age_minutes = None
            if last:
                try:
                    dt = datetime.fromisoformat(last)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    age_minutes = (now - dt).total_seconds() / 60.0
                    if age_minutes > STALE_MINUTES and r.status == 'open':
                        stale = True
                except Exception:
                    age_minutes = None
            display_status = r.status
            if r.status == 'open' and stale:
                display_status = 'ended'
            chats.append(dict(id=r.id, sid=r.sid, name=r.name or '', status=r.status, display_status=display_status, stale=stale, age_minutes=age_minutes, created_at=r.created_at, last_activity=r.last_activity))
    return jsonify({'ok': True, 'chats': chats})

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


@app.post('/admin/chats/delete/<int:chat_id>')
def admin_chat_delete(chat_id: int):
    """Delete a closed chat and its messages. Admin-only.
    Accepts optional form field 'force'=='true' to delete regardless of status.
    """
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    force = request.form.get('force') == 'true'
    init_db()
    with engine.begin() as conn:
        row = conn.execute(text("SELECT status FROM chats WHERE id=:cid"), {"cid": chat_id}).fetchone()
        if not row:
            abort(404)
        if row.status != 'closed' and not force:
            # Only allow deletion of closed chats by default
            abort(400, "Can only delete closed chats. Use force=true to override.")
        # Delete messages then chat row
        conn.execute(text("DELETE FROM chat_messages WHERE chat_id=:cid"), {"cid": chat_id})
        conn.execute(text("DELETE FROM chats WHERE id=:cid"), {"cid": chat_id})
    # Redirect back to chat list with a flag
    return redirect(url_for('admin_chats', deleted='1'))


@app.post('/admin/chats/close_all')
def admin_chats_close_all():
    """Close all non-closed chats (mark status='closed')."""
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    init_db()
    from datetime import datetime
    now = datetime.utcnow().isoformat()
    with engine.begin() as conn:
        conn.execute(text("UPDATE chats SET status='closed', last_activity=:t WHERE status != 'closed'"), {"t": now})
    return redirect(url_for('admin_chats'))

# ---------- Admin Content Management ----------
@app.route('/admin/content')
def admin_content():
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    services = fetch_services()
    maintenance_mode = get_maintenance_mode()
    autopilot = get_autopilot_enabled()
    hero_imgs = get_hero_images()
    meet_andy = get_meet_andy()
    contact_info = get_contact_info()
    service_areas = fetch_service_areas()
    homepage_sections = fetch_homepage_sections()
    business_desc = get_business_description()
    ai_keys = {
        'OPENAI_API_KEY': get_site_setting('OPENAI_API_KEY'),
        'GEMINI_API_KEY': get_site_setting('GEMINI_API_KEY'),
        'DEEPSEEK_API_KEY': get_site_setting('DEEPSEEK_API_KEY'),
        'DEEPSEEK_MODEL': get_site_setting('DEEPSEEK_MODEL')
    }
    autopilot_provider = get_site_setting('AUTOPILOT_PROVIDER') or 'auto'
    # Last AI test result (for quick visibility)
    ai_test_result = get_site_setting('ai_test_result')
    notif_sound = (get_site_setting('admin_notifications_sound') == 'true')
    return render_template('admin_content.html', services=services, maintenance_mode_enabled=maintenance_mode, autopilot_enabled=autopilot, hero_imgs=hero_imgs, meet_andy=meet_andy, contact_info=contact_info, service_areas=service_areas, homepage_sections=homepage_sections, business_description=business_desc, ai_keys=ai_keys, ai_test_result=ai_test_result, autopilot_provider=autopilot_provider, admin_notifications_sound=notif_sound)

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

@app.post('/admin/content/contact')
def admin_contact_update():
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    
    # Get form data
    profile_title = request.form.get('profile_title', '').strip()
    phone = request.form.get('phone', '').strip()
    email = request.form.get('email', '').strip()
    
    if not profile_title or not phone or not email:
        abort(400, "All contact fields are required")
    
    # Update contact info in database
    init_db()
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE site_content SET content=:content WHERE section='contact' AND key='profile_title'"
        ), {"content": profile_title})
        
        conn.execute(text(
            "UPDATE site_content SET content=:content WHERE section='contact' AND key='phone'"
        ), {"content": phone})
        
        conn.execute(text(
            "UPDATE site_content SET content=:content WHERE section='contact' AND key='email'"
        ), {"content": email})
    
    return redirect(url_for('admin_content'))

@app.post('/admin/content/maintenance-mode')
def admin_maintenance_toggle():
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    
    enabled = request.form.get('enabled') == 'true'
    set_maintenance_mode(enabled)
    
    return redirect(url_for('admin_content'))

@app.post('/admin/content/chat-autopilot')
def admin_chat_autopilot_toggle():
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    enabled = request.form.get('enabled') == 'true'
    set_autopilot_enabled(enabled)
    return redirect(url_for('admin_content'))


@app.post('/admin/content/business-description')
def admin_business_description_update():
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    desc = (request.form.get('business_description') or '').strip()
    set_business_description(desc)
    return redirect(url_for('admin_content'))


@app.post('/admin/content/api-keys')
def admin_api_keys_update():
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    openai = (request.form.get('OPENAI_API_KEY') or '').strip()
    gemini = (request.form.get('GEMINI_API_KEY') or '').strip()
    deepseek = (request.form.get('DEEPSEEK_API_KEY') or '').strip()
    deepseek_model = (request.form.get('DEEPSEEK_MODEL') or '').strip()
    # Save into site_settings for testing environments (admin-only)
    if openai:
        set_site_setting('OPENAI_API_KEY', openai)
    else:
        set_site_setting('OPENAI_API_KEY', '')
    if gemini:
        set_site_setting('GEMINI_API_KEY', gemini)
    else:
        set_site_setting('GEMINI_API_KEY', '')
    if deepseek:
        set_site_setting('DEEPSEEK_API_KEY', deepseek)
    else:
        set_site_setting('DEEPSEEK_API_KEY', '')
    # Persist optional DeepSeek model string so admin can test specific models
    if deepseek_model:
        set_site_setting('DEEPSEEK_MODEL', deepseek_model)
    else:
        set_site_setting('DEEPSEEK_MODEL', '')

    # Save chosen autopilot provider (optional)
    autopilot_provider = (request.form.get('AUTOPILOT_PROVIDER') or '').strip().lower()
    if autopilot_provider:
        set_site_setting('AUTOPILOT_PROVIDER', autopilot_provider)
    else:
        # Clear if empty
        set_site_setting('AUTOPILOT_PROVIDER', '')

    # Preserve previous page
    return redirect(url_for('admin_content'))


@app.post('/admin/content/notifications-sound')
def admin_notifications_sound_toggle():
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    enabled = request.form.get('enabled') == 'true'
    set_site_setting('admin_notifications_sound', 'true' if enabled else 'false')
    return redirect(url_for('admin_content'))

    # Refresh runtime clients so changes take effect immediately
    try:
        refresh_ai_clients()
    except Exception:
        pass

    return redirect(url_for('admin_content'))


@app.post('/admin/content/api-test')
def admin_api_test():
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result

    provider = (request.form.get('provider') or '').lower()
    prompt = (request.form.get('prompt') or '').strip()
    if not prompt:
        set_site_setting('ai_test_result', 'No prompt provided.')
        return redirect(url_for('admin_content'))

    result = None
    diagnostics = []
    try:
        # OpenAI / Legacy adapter
        if provider == 'openai':
            if openai_client:
                try:
                    resp = openai_client.chat.completions.create(
                        model="gpt-3.5-turbo",
                        messages=[{"role": "system", "content": "You are a helpful assistant."}, {"role": "user", "content": prompt}],
                        max_tokens=120,
                        temperature=0.2,
                    )
                    result = None
                    if resp and getattr(resp, 'choices', None):
                        # Compatible with modern and legacy wrappers
                        try:
                            result = resp.choices[0].message.content
                        except Exception:
                            result = getattr(resp.choices[0], 'text', str(resp))
                except Exception as e:
                    import traceback
                    tb = traceback.format_exc()
                    diagnostics.append(f"OpenAI SDK error: {str(e)}")
                    diagnostics.append(tb)
                    # Fallback: try direct HTTP to OpenAI REST API
                    try:
                        import requests
                        api_key = get_site_setting('OPENAI_API_KEY') or OPENAI_API_KEY or getattr(openai_client, 'api_key', None)
                        headers = {'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'}
                        payload = {'model': 'gpt-3.5-turbo', 'messages': [{"role":"system","content":"You are a helpful assistant."},{"role":"user","content":prompt}], 'max_tokens': 120, 'temperature': 0.2}
                        r = requests.post('https://api.openai.com/v1/chat/completions', json=payload, headers=headers, timeout=15)
                        diagnostics.append(f"OpenAI HTTP status: {r.status_code}")
                        if r.status_code == 200:
                            try:
                                j = r.json()
                                try:
                                    result = j['choices'][0]['message']['content']
                                except Exception:
                                    result = str(j)
                            except Exception:
                                diagnostics.append(f"OpenAI HTTP non-JSON body: {r.text[:1000]}")
                                result = f"OpenAI HTTP 200 but non-JSON body: {r.text[:1000]}"
                        else:
                            try:
                                diagnostics.append(f"OpenAI HTTP error body: {r.text[:1000]}")
                            except Exception:
                                pass
                            result = f"OpenAI HTTP error: {r.status_code}"
                    except Exception as e2:
                        diagnostics.append(f"OpenAI HTTP fallback failed: {str(e2)}")
                        result = None
            else:
                result = "No OpenAI client configured."

        # DeepSeek (OpenAI-compatible) - use direct HTTP fallback to avoid SDK incompatibilities
        elif provider == 'deepseek':
            ds_key = DEEPSEEK_API_KEY or get_site_setting('DEEPSEEK_API_KEY')
            if ds_key:
                try:
                    import requests
                    # Try both common OpenAI-compatible endpoints for DeepSeek
                    endpoints = [
                        'https://api.deepseek.com/v1/chat/completions',
                        'https://api.deepseek.com/v1/responses'
                    ]
                    headers = {
                        'Authorization': f'Bearer {ds_key}',
                        'Content-Type': 'application/json'
                    }
                    ds_model = get_site_setting('DEEPSEEK_MODEL') or ''
                    default_models = ['deepseek-chat', 'deepseek-reasoner', 'gpt-4o', 'gpt-3.5-turbo', 'gpt-3.5']
                    models = [m for m in ([ds_model] + default_models) if m]
                    result = None
                    last_err = None
                    for url in endpoints:
                        for m in models:
                            payload = {
                                'model': m,
                                'messages': [
                                    {"role": "system", "content": "You are a helpful assistant."},
                                    {"role": "user", "content": prompt}
                                ],
                                'max_tokens': 120,
                                'temperature': 0.2
                            }
                            try:
                                r = requests.post(url, json=payload, headers=headers, timeout=12)
                            except Exception as e:
                                diagnostics.append(f"DeepSeek network error for model {m} at {url}: {str(e)}")
                                last_err = str(e)
                                continue
                            diagnostics.append(f"DeepSeek HTTP {r.status_code} for model {m} at {url}")
                            if r.status_code == 200:
                                try:
                                    j = r.json()
                                except Exception:
                                    diagnostics.append(f"DeepSeek 200 non-JSON body: {r.text[:1000]}")
                                    result = f"DeepSeek 200 non-JSON body: {r.text[:1000]}"
                                    break
                                # Try several paths that DeepSeek/OpenAI-compatible endpoints may use
                                got = None
                                if isinstance(j, dict):
                                    # OpenAI-like: choices -> message -> content
                                    if j.get('choices'):
                                        try:
                                            got = j['choices'][0]['message']['content']
                                        except Exception:
                                            got = j['choices'][0].get('text') or None
                                    # Response-style: output_text or output
                                    if not got:
                                        got = j.get('output') or j.get('output_text') or None
                                    # DeepSeek specific shapes
                                    if not got and j.get('data'):
                                        try:
                                            # e.g. data -> 0 -> content -> text
                                            d0 = j['data'][0]
                                            if isinstance(d0, dict):
                                                got = d0.get('text') or d0.get('content') or None
                                        except Exception:
                                            pass
                                if got:
                                    result = got
                                    break
                                else:
                                    diagnostics.append(f"DeepSeek 200 but no content in JSON for model {m} at {url}: {str(j)[:800]}")
                                    # try next model/url
                                    last_err = f"No content in JSON for model {m} at {url}"
                                    continue
                            else:
                                try:
                                    body = r.text[:800]
                                    diagnostics.append(f"DeepSeek error body for model {m} at {url}: {body}")
                                except Exception:
                                    diagnostics.append(f"DeepSeek HTTP {r.status_code} with unreadable body for {m} at {url}")
                                # If the error mentions model missing, keep trying other models; otherwise record and stop trying this endpoint/model
                                try:
                                    errj = r.json()
                                    msg = str(errj)
                                except Exception:
                                    msg = r.text[:500]
                                last_err = f"HTTP {r.status_code}: {msg}"
                                # continue trying other models
                                continue
                        if result:
                            break
                    if not result and last_err:
                        result = f"DeepSeek attempts failed: {last_err}. Diagnostics: {' | '.join(diagnostics[-6:])}"
                    elif not result:
                        result = f"DeepSeek attempts produced no content. Diagnostics: {' | '.join(diagnostics[-6:])}"
                except Exception as e:
                    import traceback
                    diagnostics.append(traceback.format_exc())
                    result = f"DeepSeek error: {str(e)}; diagnostics: {' | '.join(diagnostics[-6:])}"
            else:
                result = "No DEEPSEEK_API_KEY configured."

        # Gemini
        elif provider == 'gemini' and genai:
            try:
                model = genai.GenerativeModel('gemini-1.5-mini')
                r = model.generate_content(prompt)
                result = getattr(r, 'text', None)
                if not result and getattr(r, 'candidates', None):
                    result = r.candidates[0].content.parts[0].text
            except Exception as e:
                result = f"Gemini error: {str(e)}"
        else:
            result = f"No client available for provider '{provider}'."
    except Exception as e:
        result = f"Error: {str(e)}"

    if not result:
        result = "(no response)"
    # Truncate to a reasonable length
    if len(result) > 1500:
        result = result[:1500] + '...'

    # Persist the last test result for visibility
    set_site_setting('ai_test_result', result)
    return redirect(url_for('admin_content'))

@app.post('/admin/service-areas/add')
def admin_service_area_add():
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    
    name = request.form.get('name', '').strip()
    if not name:
        abort(400, "Area name is required")
    
    init_db()
    with engine.begin() as conn:
        # Get max sort_order and add 1
        max_order = conn.execute(text("SELECT MAX(sort_order) FROM service_areas")).scalar() or 0
        conn.execute(text(
            "INSERT INTO service_areas (name, sort_order) VALUES (:name, :order)"
        ), {"name": name, "order": max_order + 1})
    
    return redirect(url_for('admin_content'))

@app.post('/admin/service-areas/update/<int:area_id>')
def admin_service_area_update(area_id: int):
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    
    name = request.form.get('name', '').strip()
    if not name:
        abort(400, "Area name is required")
    
    init_db()
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE service_areas SET name=:name WHERE id=:id"
        ), {"name": name, "id": area_id})
    
    return redirect(url_for('admin_content'))

@app.post('/admin/service-areas/delete/<int:area_id>')
def admin_service_area_delete(area_id: int):
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    
    init_db()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM service_areas WHERE id=:id"), {"id": area_id})
    
    return redirect(url_for('admin_content'))

@app.post('/admin/sections/move-up/<int:section_id>')
def admin_section_move_up(section_id: int):
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    
    move_section_up(section_id)
    return redirect(url_for('admin_content'))

@app.post('/admin/sections/move-down/<int:section_id>')
def admin_section_move_down(section_id: int):
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    
    move_section_down(section_id)
    return redirect(url_for('admin_content'))

@app.post('/admin/sections/toggle/<int:section_id>')
def admin_section_toggle(section_id: int):
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    
    toggle_section_visibility(section_id)
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


@app.route('/admin/ip-management')
def admin_ip_management():
    """IP tracking and blocking management"""
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    
    ips = fetch_all_ips()
    return render_template('admin_ip_management.html', ips=ips)


@app.route('/admin/block-ip/<path:ip_address>', methods=['POST'])
def admin_block_ip(ip_address: str):
    """Block an IP address"""
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    
    try:
        toggle_ip_block(ip_address, True)
        return jsonify({'success': True, 'message': 'IP blocked successfully'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/admin/unblock-ip/<path:ip_address>', methods=['POST'])
def admin_unblock_ip(ip_address: str):
    """Unblock an IP address"""
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    
    try:
        toggle_ip_block(ip_address, False)
        return jsonify({'success': True, 'message': 'IP unblocked successfully'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/admin/delete-ip/<path:ip_address>', methods=['POST'])
def admin_delete_ip(ip_address: str):
    """Delete an IP address from tracking"""
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    
    try:
        delete_ip(ip_address)
        return jsonify({'success': True, 'message': 'IP deleted successfully'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


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

@app.route('/admin/ai_env')
def admin_ai_env():
    """Diagnostics: show which AI-related env flags are visible and autopilot state.
    Protect with admin auth to avoid leaking environment hints.
    """
    auth_result = require_admin()
    if isinstance(auth_result, Response):
        return auth_result
    return jsonify({
        'has_openai': bool(OPENAI_API_KEY),
        'has_gemini': bool(GEMINI_API_KEY),
        'autopilot_enabled': get_autopilot_enabled()
    })


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
