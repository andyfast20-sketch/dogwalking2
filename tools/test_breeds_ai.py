from app import app

with app.test_client() as c:
    # POST without admin auth (require_admin may allow if ADMIN_PASSWORD not set)
    data = {'prompt': 'add generally small dog breeds'}
    resp = c.post('/admin/breeds/ai_update', data=data)
    print('STATUS:', resp.status_code)
    try:
        print('JSON:', resp.get_json())
    except Exception as e:
        print('RAW:', resp.data[:1000])
