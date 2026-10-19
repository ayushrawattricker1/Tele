
import os
import re
import time
import asyncio
import secrets
import threading
from pathlib import Path
from datetime import datetime
from functools import wraps

from flask import (
    Flask, request, redirect, url_for, render_template_string,
    flash, jsonify
)
from telethon import TelegramClient
from telethon.tl.functions.auth import ExportLoginTokenRequest
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PasswordHashInvalidError
)

# ==========================================================
# CONFIGURATION
# Set TELEGRAM_API_ID and TELEGRAM_API_HASH in environment.
# ==========================================================

app = Flask(__name__)
app.secret_key = os.environ.get(
    "FLASK_SECRET_KEY", secrets.token_hex(32)
)

# Telegram API credentials
# API ID is included here; keep the API HASH in Render Environment Variables.
API_ID = os.environ.get("TELEGRAM_API_ID", "37618132").strip()
# Keep API HASH in Render Environment Variables; never commit the secret to GitHub.
API_HASH = os.environ.get("TELEGRAM_API_HASH", "").strip()

BASE_DIR = Path(__file__).resolve().parent
if os.environ.get("TELEGRAM_DATA_DIR"):
    SESSION_DIR = Path(os.environ["TELEGRAM_DATA_DIR"]).expanduser()
else:
    SESSION_DIR = BASE_DIR / "telegram_sessions"

UPLOAD_DIR = BASE_DIR / "message_files"

SESSION_DIR.mkdir(parents=True, exist_ok=True)
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

MAX_FILE_BYTES = 1_000_000
MIN_INTERVAL = 1
MAX_INTERVAL = 3600

# Task state
tasks = {}
task_lock = threading.Lock()

# Pending QR logins: login_id -> {client, expires, status}
pending_qr_logins = {}
login_lock = threading.Lock()


def run_async(coro):
    """Run a Telethon coroutine in the current request/thread."""
    return asyncio.run(coro)


def check_api_config():
    if not API_ID:
        raise ValueError("Telegram API ID is missing.")
    if not API_HASH:
        raise ValueError(
            "Telegram API HASH is missing. Set TELEGRAM_API_HASH "
            "in your Render Environment Variables."
        )
    try:
        return int(API_ID)
    except ValueError:
        raise ValueError("TELEGRAM_API_ID must be a number.")


def clean_phone(phone):
    phone = phone.strip()
    if not re.fullmatch(r"\+?[0-9]{7,16}", phone):
        raise ValueError(
            "Enter a valid phone number with country code, e.g. +91..."
        )
    return phone


def session_name(phone):
    """Never use raw phone numbers as filesystem paths."""
    import hashlib
    digest = hashlib.sha256(phone.encode()).hexdigest()[:20]
    return str(SESSION_DIR / digest)


def available_accounts():
    result = []
    for path in SESSION_DIR.glob("*.session"):
        if path.name.endswith(".session-journal"):
            continue
        result.append({
            "id": path.stem,
            "label": "Account " + path.stem[:8]
        })
    return sorted(result, key=lambda x: x["id"])


def add_log(task_id, message):
    with task_lock:
        task = tasks.get(task_id)
        if task:
            stamp = datetime.now().strftime("%H:%M:%S")
            task["logs"].append(f"[{stamp}] {message}")
            task["logs"] = task["logs"][-300:]


async def create_qr_login():
    """Create a Telegram QR login session without asking for a phone number."""
    api_id = check_api_config()
    login_id = secrets.token_urlsafe(12)
    client = TelegramClient(session_name("qr_" + login_id), api_id, API_HASH)
    await client.connect()

    if await client.is_user_authorized():
        me = await client.get_me()
        await client.disconnect()
        return {
            "already_logged_in": True,
            "username": me.username or "",
            "login_id": login_id,
        }

    login = await client.qr_login()
    with login_lock:
        pending_qr_logins[login_id] = {
            "client": client,
            "login": login,
            "created": time.time(),
            "status": "waiting",
        }

    return {
        "already_logged_in": False,
        "login_id": login_id,
        "url": login.url,
        "expires": login.expires,
    }


async def finish_qr_login(login_id):
    with login_lock:
        item = pending_qr_logins.get(login_id)

    if not item:
        raise ValueError("QR login session not found or expired.")

    client = item["client"]
    login = item["login"]

    try:
        await login.wait()

        me = await client.get_me()
        username = me.username or "account"

        with login_lock:
            pending_qr_logins.pop(login_id, None)

        return {
            "ok": True,
            "message": "Login successful: @" + username,
        }
    except Exception:
        with login_lock:
            pending_qr_logins.pop(login_id, None)
        raise
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


def qr_status(login_id):
    with login_lock:
        item = pending_qr_logins.get(login_id)

    if not item:
        return {"status": "not_found"}

    age = time.time() - item["created"]
    if age > 180:
        return {"status": "expired"}

    return {
        "status": "waiting",
        "login_id": login_id,
    }


async def send_task_messages(task_id, account_ids, recipient,
                             prefix, interval, messages):
    clients = []
    try:
        add_log(task_id, "Connecting selected account sessions...")

        for account_id in account_ids:
            session_path = SESSION_DIR / account_id
            if not session_path.exists() and not Path(
                str(session_path) + ".session"
            ).exists():
                add_log(task_id, f"Skipped unavailable session: {account_id}")
                continue

            client = TelegramClient(
                str(SESSION_DIR / account_id), check_api_config(), API_HASH
            )
            await client.connect()

            if not await client.is_user_authorized():
                add_log(task_id, f"Session is not authorized: {account_id}")
                await client.disconnect()
                continue

            clients.append((account_id, client))

        if not clients:
            raise RuntimeError("No authorized account sessions selected.")

        # Resolve recipient separately for each account. Each TXT line
        # is sent once; there is no endless repeat loop.
        for account_id, client in clients:
            if tasks[task_id]["stop"].is_set():
                break

            try:
                entity = await client.get_entity(recipient)
            except Exception as exc:
                add_log(
                    task_id,
                    f"{account_id}: cannot resolve recipient ({exc})"
                )
                continue

            add_log(task_id, f"{account_id}: recipient resolved.")

            for index, line in enumerate(messages, start=1):
                if tasks[task_id]["stop"].is_set():
                    break

                line = line.strip()
                if not line:
                    continue

                text = f"{prefix} {line}".strip() if prefix else line

                try:
                    await client.send_message(entity, text)
                    with task_lock:
                        tasks[task_id]["sent"] += 1
                    add_log(task_id, f"{account_id}: message {index} sent.")
                except Exception as exc:
                    with task_lock:
                        tasks[task_id]["failed"] += 1
                    add_log(
                        task_id,
                        f"{account_id}: message {index} failed ({exc})"
                    )

                # Don't sleep after the final line.
                if index < len(messages):
                    if tasks[task_id]["stop"].wait(interval):
                        break

        add_log(task_id, "Task stopped by user." if
                tasks[task_id]["stop"].is_set() else "Task completed.")

    except Exception as exc:
        add_log(task_id, f"Task error: {exc}")

    finally:
        for _, client in clients:
            try:
                await client.disconnect()
            except Exception:
                pass

        with task_lock:
            if task_id in tasks:
                tasks[task_id]["status"] = (
                    "stopped" if tasks[task_id]["stop"].is_set()
                    else "completed"
                )


def task_worker(task_id, account_ids, recipient, prefix,
                interval, messages):
    try:
        asyncio.run(send_task_messages(
            task_id, account_ids, recipient, prefix, interval, messages
        ))
    finally:
        with task_lock:
            if task_id in tasks and tasks[task_id]["status"] == "running":
                tasks[task_id]["status"] = "completed"


PAGE = r"""
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Telegram Message Panel</title>
<style>
:root {
  color-scheme: dark;
  --bg:#101522; --panel:#192235; --line:#34435c;
  --text:#f3f6ff; --muted:#a8b5ca; --blue:#438cff;
  --green:#24c78b; --red:#ff6175;
}
* { box-sizing:border-box }
body {
  margin:0; color:var(--text);
  font-family:system-ui,-apple-system,Segoe UI,sans-serif;
  background:radial-gradient(ellipse at top,#253b61 0%,var(--bg) 62%);
  min-height:100vh;
}
header { padding:28px 18px 12px;text-align:center }
header h1 { margin:0;font-size:clamp(25px,5vw,38px) }
header p { color:var(--muted);margin:9px 0 0 }
main { width:min(960px,94%);margin:20px auto 40px }
.grid { display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,340px),1fr));gap:18px }
.panel {
  background:rgba(25,34,53,.96);border:1px solid var(--line);
  border-radius:18px;padding:22px;margin-bottom:18px;
  box-shadow:0 12px 32px #0003;
}
h2 { font-size:19px;margin:0 0 18px }
label { display:block;font-size:14px;color:#d5e0f2;margin:14px 0 7px }
input,select,textarea {
  width:100%;padding:12px 13px;border:1px solid #42516b;
  border-radius:10px;background:#111a2a;color:white;font:inherit;
}
input[type=file] { padding:9px }
input[type=checkbox] { width:auto;accent-color:var(--blue) }
textarea { min-height:95px;resize:vertical }
button,.button {
  display:inline-block;border:0;border-radius:10px;padding:12px 17px;
  font:600 14px system-ui;cursor:pointer;text-decoration:none;
  background:var(--blue);color:white;margin-top:13px;
}
button:hover,.button:hover { filter:brightness(1.12) }
.danger { background:#a93249 }
.success { background:#147c60 }
.muted { color:var(--muted);font-size:13px;line-height:1.6 }
.account { padding:11px 0;border-bottom:1px solid #34435c }
.account:last-child { border-bottom:0 }
.row { display:flex;align-items:center;gap:9px;flex-wrap:wrap }
.row button { flex:1 }
.flash { padding:12px;border-radius:9px;background:#243754;margin-bottom:10px }
pre {
  white-space:pre-wrap;overflow-wrap:anywhere;background:#0b111d;
  border:1px solid #34435c;border-radius:12px;padding:14px;
  min-height:110px;max-height:360px;overflow:auto;color:#d3e4ff;
}
.task { border:1px solid var(--line);padding:14px;border-radius:12px;margin:10px 0 }
footer { text-align:center;padding:10px 20px 35px;color:var(--muted);font-size:13px }
</style>
</head>
<body>
<header>
  <h1>Telegram Message Panel</h1>
  <p>Account sessions · TXT messages · Task status</p>
</header>
<main>
{% with messages = get_flashed_messages() %}
  {% for message in messages %}<div class="flash">{{ message }}</div>{% endfor %}
{% endwith %}

<div class="grid">
<section class="panel">
<h2>1. Telegram QR Login</h2>
<p class="muted">
No phone number or OTP is entered here. Generate a QR code and scan it
from Telegram on the account you want to authorize.
</p>
<form method="post" action="/login/qr">
<button type="submit">Generate Telegram QR</button>
</form>

{% if qr_login %}
<div class="task" style="margin-top:16px">
<strong>Scan this QR with Telegram</strong>
<p class="muted">Telegram → Settings → Devices → Link Desktop Device</p>
<div id="qrBox" style="text-align:center;margin:15px 0">
<img
  alt="Telegram QR Login"
  style="max-width:280px;width:100%;background:white;padding:12px;border-radius:12px"
  src="https://api.qrserver.com/v1/create-qr-code/?size=280x280&data={{ qr_login.url|urlencode }}"
>
</div>
<p class="muted" id="qrStatus">Waiting for QR scan...</p>
<script>
(async function pollQR() {
  const box = document.getElementById("qrStatus");
  try {
    const r = await fetch("/login/qr/status/{{ qr_login.login_id }}");
    const d = await r.json();
    if (d.status === "success") {
      box.textContent = "Login successful. Reloading...";
      setTimeout(() => location.reload(), 800);
      return;
    }
    if (d.status === "expired" || d.status === "not_found") {
      box.textContent = "QR expired. Generate a new QR code.";
      return;
    }
    box.textContent = "Waiting for QR scan...";
    setTimeout(pollQR, 2000);
  } catch (e) {
    box.textContent = "Checking login status...";
    setTimeout(pollQR, 3000);
  }
})();
</script>
</div>
{% endif %}
</section>

<section class="panel">
<h2>2. Saved Accounts</h2>
{% if accounts %}
{% for account in accounts %}
<div class="account">
<strong>{{ account.label }}</strong>
<div class="muted">Session file: {{ account.id }}.session</div>
</div>
{% endfor %}
{% else %}
<p class="muted">No saved account sessions found. Add an account above.</p>
{% endif %}
<p class="muted">Session files grant account access. Keep the
telegram_sessions folder private and never upload it publicly.</p>
</section>
</div>

<section class="panel">
<h2>3. Start Message Task</h2>
<form method="post" action="/task/start" enctype="multipart/form-data">
<label>Choose account(s)</label>
{% if accounts %}
{% for account in accounts %}
<div class="account">
<label class="row">
<input type="checkbox" name="accounts" value="{{ account.id }}">
<span>{{ account.label }}</span>
</label>
</div>
{% endfor %}
{% else %}
<p class="muted">Log in to an account before starting a task.</p>
{% endif %}

<label>Recipient username or chat ID</label>
<input name="recipient" placeholder="@username or numeric chat ID" required>

<label>Optional message prefix</label>
<input name="prefix" placeholder="Leave blank for no prefix">

<label>Interval between messages (seconds)</label>
<input name="interval" type="number" min="1" max="3600" value="5" required>

<label>Upload TXT message file</label>
<input name="message_file" type="file" accept=".txt,text/plain" required>
<p class="muted">Each non-empty line is treated as one message.
The file is processed once per selected account.</p>
<button type="submit">Run Task</button>
</form>
</section>

<section class="panel">
<h2>4. Tasks and Live Logs</h2>
<div id="tasks">
{% for task in task_list %}
<div class="task">
<strong>Task ID:</strong> {{ task.id }}<br>
<strong>Status:</strong> {{ task.status }}<br>
<strong>Sent:</strong> {{ task.sent }} &nbsp;
<strong>Failed:</strong> {{ task.failed }}
<div class="row">
<button type="button" onclick="loadLogs('{{ task.id }}')">View Logs</button>
<form method="post" action="/task/stop">
<input type="hidden" name="task_id" value="{{ task.id }}">
<button class="danger" type="submit">Stop Task</button>
</form>
</div>
</div>
{% else %}
<p class="muted">No tasks have been started yet.</p>
{% endfor %}
</div>
<label>Selected task logs</label>
<pre id="logs">Choose “View Logs” to see task activity.</pre>
</section>
</main>
<footer>Keep API credentials, login codes and session files private.</footer>
<script>
async function loadLogs(id) {
  const box = document.getElementById('logs');
  box.textContent = 'Loading...';
  try {
    const response = await fetch('/task/' + encodeURIComponent(id) + '/status');
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'Request failed');
    box.textContent = data.logs.join('\\n') +
      '\\n\\nStatus: ' + data.status +
      '\\nSent: ' + data.sent + ' | Failed: ' + data.failed;
  } catch (err) { box.textContent = 'Error: ' + err.message; }
}
</script>
</body>
</html>
"""


@app.route("/", methods=["GET"])
def home():
    with task_lock:
        task_list = [
            {
                "id": task_id,
                "status": item["status"],
                "sent": item["sent"],
                "failed": item["failed"]
            }
            for task_id, item in reversed(list(tasks.items()))
        ]
    qr_login = None
    qr_id = request.args.get("qr_login", "").strip()
    if qr_id:
        with login_lock:
            item = pending_qr_logins.get(qr_id)
        if item:
            try:
                qr_login = {
                    "login_id": qr_id,
                    "url": item["login"].url,
                }
            except Exception:
                qr_login = None

    return render_template_string(
        PAGE,
        accounts=available_accounts(),
        task_list=task_list,
        qr_login=qr_login,
    )


@app.route("/login/code", methods=["POST"])
def login_code():
    try:
        phone = clean_phone(request.form.get("phone", ""))
        result = run_async(request_login_code(phone))

        if result.get("already_logged_in"):
            flash("This account already has an active saved session.")
            return redirect(url_for("home"))

        with login_lock:
            pending_logins[phone] = result["phone_code_hash"]

        flash("Telegram login code requested. Enter it in the verification form.")
    except Exception as exc:
        flash(f"Could not request login code: {exc}")
    return redirect(url_for("home"))


@app.route("/login/verify", methods=["POST"])
def login_verify():
    try:
        phone = clean_phone(request.form.get("phone", ""))
        code = request.form.get("code", "").strip()
        password = request.form.get("password", "")

        with login_lock:
            code_hash = pending_logins.get(phone)

        if not code_hash and not password:
            raise ValueError("Request a login code first.")

        try:
            result = run_async(complete_login(
                phone, code, code_hash, password or None
            ))
        except SessionPasswordNeededError:
            flash("Telegram requires your 2-Step Verification password.")
            return redirect(url_for("home"))
        except (PhoneCodeInvalidError, PasswordHashInvalidError):
            flash("The code or 2-Step Verification password is invalid.")
            return redirect(url_for("home"))

        if result.get("ok"):
            with login_lock:
                pending_logins.pop(phone, None)
            flash(result["message"])
        else:
            flash("Login was not completed.")
    except Exception as exc:
        flash(f"Login error: {exc}")
    return redirect(url_for("home"))


@app.route("/login/qr", methods=["POST"])
def login_qr():
    try:
        result = run_async(create_qr_login())
        if result.get("already_logged_in"):
            flash("This account is already logged in.")
            return redirect(url_for("home"))

        return redirect(
            url_for("home", qr_login=result["login_id"])
        )
    except Exception as exc:
        flash(f"Could not create QR login: {exc}")
        return redirect(url_for("home"))


@app.route("/login/qr/status/<login_id>", methods=["GET"])
def login_qr_status(login_id):
    status = qr_status(login_id)

    if status["status"] == "waiting":
        try:
            result = run_async(finish_qr_login(login_id))
            return jsonify({"status": "success", **result})
        except Exception as exc:
            message = str(exc)
            if "wait" in message.lower() or "timeout" in message.lower():
                return jsonify({"status": "waiting"})
            return jsonify({"status": "error", "error": message}), 400

    return jsonify(status)


@app.route("/task/start", methods=["POST"])
def task_start():
    try:
        account_ids = request.form.getlist("accounts")
        recipient = request.form.get("recipient", "").strip()
        prefix = request.form.get("prefix", "").strip()
        interval = int(request.form.get("interval", "5"))
        uploaded = request.files.get("message_file")

        if not account_ids:
            raise ValueError("Select at least one saved account.")
        known = {a["id"] for a in available_accounts()}
        if any(a not in known for a in account_ids):
            raise ValueError("An account selection is invalid.")
        if not recipient:
            raise ValueError("Enter a recipient username or chat ID.")
        if not MIN_INTERVAL <= interval <= MAX_INTERVAL:
            raise ValueError("Interval must be between 1 and 3600 seconds.")
        if not uploaded or not uploaded.filename:
            raise ValueError("Choose a TXT message file.")
        if not uploaded.filename.lower().endswith(".txt"):
            raise ValueError("Only .txt files are accepted.")

        raw = uploaded.read(MAX_FILE_BYTES + 1)
        if len(raw) > MAX_FILE_BYTES:
            raise ValueError("The TXT file must be 1 MB or smaller.")

        messages = raw.decode("utf-8-sig").splitlines()
        messages = [line.strip() for line in messages if line.strip()]
        if not messages:
            raise ValueError("The TXT file contains no messages.")

        task_id = secrets.token_urlsafe(10)

        with task_lock:
            tasks[task_id] = {
                "status": "running",
                "stop": threading.Event(),
                "logs": [],
                "sent": 0,
                "failed": 0,
                "created": time.time()
            }

        worker = threading.Thread(
            target=task_worker,
            args=(
                task_id, account_ids, recipient, prefix,
                interval, messages
            ),
            daemon=True
        )
        worker.start()

        flash(f"Task started. Task ID: {task_id}")
    except Exception as exc:
        flash(f"Could not start task: {exc}")

    return redirect(url_for("home"))


@app.route("/task/stop", methods=["POST"])
def task_stop():
    task_id = request.form.get("task_id", "").strip()
    with task_lock:
        task = tasks.get(task_id)
        if task and task["status"] == "running":
            task["stop"].set()
            task["status"] = "stopping"
            task["logs"].append(
                f"[{datetime.now():%H:%M:%S}] Stop requested."
            )
            flash(f"Stop requested for task {task_id}.")
        else:
            flash("Task not found or already finished.")
    return redirect(url_for("home"))


@app.route("/task/<task_id>/status", methods=["GET"])
def task_status(task_id):
    with task_lock:
        task = tasks.get(task_id)
        if not task:
            return jsonify(error="Task not found"), 404
        return jsonify({
            "status": task["status"],
            "sent": task["sent"],
            "failed": task["failed"],
            "logs": list(task["logs"])
        })


if __name__ == "__main__":
    # Local development only. Use a production WSGI server for deployment.
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5040")),
            debug=False)
