"""Create local demo configuration without overwriting an existing .env."""

import secrets
from pathlib import Path

root = Path(__file__).resolve().parents[1]
path = root / ".env"
if path.exists():
    print("Existing .env preserved")
else:
    text = (root / ".env.example").read_text()
    text = text.replace("CUSTOMER_TOKEN_KEY=\n", f"CUSTOMER_TOKEN_KEY={secrets.token_hex(32)}\n")
    text = text.replace(
        "AIRFLOW_WEBSERVER_SECRET=\n", f"AIRFLOW_WEBSERVER_SECRET={secrets.token_hex(32)}\n"
    )
    path.write_text(text)
    path.chmod(0o600)
    print("Created .env (local demo login: admin / admin)")
