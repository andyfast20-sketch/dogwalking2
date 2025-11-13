import traceback

try:
    from app import app
    with app.test_request_context('/admin'):
        try:
            res = app.view_functions['admin_content']()
            print('OK', type(res))
            if hasattr(res, 'status_code'):
                print('Response status', res.status_code)
            else:
                print('Rendered length', len(res))
        except Exception:
            traceback.print_exc()
except Exception:
    traceback.print_exc()
