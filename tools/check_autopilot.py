import os
import sys
# Import the app module (makes sure path resolves)
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import app

print('ENV KEYS:')
print('OPENAI_API_KEY set:', bool(os.environ.get('OPENAI_API_KEY')))
print('GEMINI_API_KEY set:', bool(os.environ.get('GEMINI_API_KEY')))
print('DEEPSEEK_API_KEY set:', bool(os.environ.get('DEEPSEEK_API_KEY')))

print('\nModule-level clients:')
print('app.openai_client is None?:', app.openai_client is None)
print('app.genai is None?:', app.genai is None)
print('app.deepseek_client is None?:', app.deepseek_client is None)

print('\nStored site settings (chat_autopilot):')
try:
    print('autopilot enabled:', app.get_autopilot_enabled())
except Exception as e:
    print('Error reading autopilot flag:', e)

print('\nBusiness description (truncated 500 chars):')
try:
    bd = app.get_business_description() or ''
    print(repr(bd[:500]))
except Exception as e:
    print('Error reading business_description:', e)

print('\nQuick DB check: last 5 chat_messages')
try:
    app.init_db()
    with app.engine.begin() as conn:
        rows = conn.execute("SELECT id, chat_id, sender, message, created_at FROM chat_messages ORDER BY id DESC LIMIT 5").fetchall()
        for r in rows[::-1]:
            print(r)
except Exception as e:
    print('Error querying chat_messages:', e)
