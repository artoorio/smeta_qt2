from pathlib import Path
from web_app.backend import app
import traceback
out = Path('openapi_probe.txt')
try:
    schema = app.openapi()
    out.write_text(f"OK {len(schema.get('paths', {}))}\n", encoding='utf-8')
except Exception:
    out.write_text(traceback.format_exc(), encoding='utf-8')
