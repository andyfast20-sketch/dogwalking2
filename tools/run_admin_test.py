import sys, os, traceback
# Ensure repo root is on sys.path
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

try:
    from app import app
    with app.test_request_context('/admin'):
        try:
            res = app.view_functions['admin_content']()
            print('OK', type(res))
            if hasattr(res, 'status_code'):
                print('Response status', res.status_code)
            else:
                # Try to measure length of rendered string
                try:
                    print('Rendered length', len(res))
                except Exception:
                    print('Rendered repr:', repr(res)[:400])
        except Exception:
            traceback.print_exc()
except Exception:
    traceback.print_exc()
