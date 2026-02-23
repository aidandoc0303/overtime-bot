import os
import io
import re
import zipfile
from typing import Optional, Tuple, List, Dict, Any

import pandas as pd
import httpx
from fastapi import FastAPI, Request
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

# Optional AI (hybrid): only used when a message starts with "Kenobi "
try:
    from openai import OpenAI
except Exception:
    OpenAI = None


# ====== ENV VARS ======
BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")

# Optional: site pull (Path A)
REPORT_URL = os.getenv("REPORT_URL")  # direct download URL to report (csv/xls/xlsx)
REPORT_BEARER_TOKEN = os.getenv("REPORT_BEARER_TOKEN")  # optional
REPORT_USER = os.getenv("REPORT_USER")  # optional
REPORT_PASS = os.getenv("REPORT_PASS")  # optional
REPORT_COOKIE = os.getenv("REPORT_COOKIE")  # optional (raw Cookie header string)

# Optional: AI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Option B (persistent names): Upstash Redis REST
UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

# SNAP COPY/PASTE (Venmo handle)
VENMO_HANDLE = os.getenv("VENMO_HANDLE", "@aidandoc3")


# ====== APP STATE ======
app = FastAPI()
tg_app: Optional[Application] = None

LATEST_FILE_BYTES: Optional[bytes] = None
LATEST_FILE_NAME: Optional[str] = None
CURRENT_GROUP: List[str] = []


# ====== PLAYER NAME MAP (fallback) ======
PLAYER_NAMES: Dict[str, str] = {
    "AIDN001": "Austin",
    "AIDN002": "Anthony Cancro",
    "AIDN003": "Blake",
    "AIDN004": "Charlie",
    "AIDN005": "Chris",
    "AIDN006": "Colby",
    "AIDN007": "Colin",
    "AIDN008": "Jonny",
    "AIDN009": "Miles",
    "AIDN010": "Liam",
    "AIDN011": "Nick",
    "AIDN012": "Ryan",
    "AIDN013": "Oliver",
    "AIDN014": "John",
    "AIDN015": "Nooch",
    "AIDN016": "Spencer",
    "AIDN017": "Ian",
    "AIDN018": "Beckett",
    "AIDN019": "Aston",
    "AIDN020": "Liam",
    "AIDN021": "Mason",
    "AIDN022": "Jake",
    "AIDN023": "Blake",
    "AIDN024": "Anthony",
    "AIDN025": "Max",
    "AIDN026": "Ben",
    "AIDN027": "Jake",
    "AIDN028": "Coby",
    "AIDN029": "Jake",
    "AIDN030": "Mark",
    "AIDN031": "Nick",
    "AIDN032": "Owen",
    "AIDN033": "Carter",
    "AIDN034": "Beckett",
    "AIDN035": "Mason",
    "AIDN036": "Ian",
    "AIDN037": "Andrew",
    "AIDN038": "Robbie",
    "AIDN039": "Mason",
    "AIDN040": "Brody",
    "AIDN041": "Luke",
    "AIDN042": "Casey Gilford",
    "AIDN043": "Reid",
    "AIDN044": "Anthony Labin",
    "AIDN045": "Jack Bradford",
    "AIDN046": "Jon Boyd",
    "AIDN047": "Blake Peterson",
    "AIDN048": "Seamus Pinkerton",
    "AIDN049": "Gavin Feulner",
    "AIDN050": "Andrew Byrne",
    "AIDN051": "Mason Diltz",
    "AIDN052": "Jason Coury",
    "AIDN053": "Diego Yanez",
    "AIDN054": "Brian Christy",
    "AIDN055": "Luke Angelo",
    "AIDN056": "Kevin Mcdonald",
    "AIDN057": "Alex Komorowski",
    "AIDN058": "Enzo Ferrero",
    "AIDN059": "Corey Gilford",
    "AIDN060": "Alex Senyk",
    "AIDN061": "Jamie Petrone",
    "AIDN062": "Davis Ozerkis",
    "AIDN063": "Jackson Folvik",
    "AIDN064": "Jackson Skawinski",
}

# Upstash keys prefix (so you can share Redis with other stuff)
NAME_KEY_PREFIX = "kenobi:name:"


# ====== AI CLIENT ======
oa_client = OpenAI(api_key=OPENAI_API_KEY) if (OpenAI and OPENAI_API_KEY) else None


# ====== HELPERS ======
def say(text: str) -> str:
    return f"Yes sir — {text}"


def normalize_id(x: str) -> str:
    return str(x or "").strip().upper()


def extract_ids(text: str) -> List[str]:
    return re.findall(r"\b[A-Z]{2,}\d{2,}\b", (text or "").upper())


def parse_money(x) -> Optional[float]:
    # Handles numbers already parsed from Excel + strings like "$1,234.56"
    if x is None:
        return None
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass

    if isinstance(x, (int, float)):
        return float(x)

    s = str(x).strip()
    if not s:
        return None
    s = s.replace("$", "").replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        return None


def normalize_col_name(x) -> str:
    return re.sub(r"\s+", " ", str(x or "")).strip().lower()


def find_header_row(df_raw: pd.DataFrame, required_headers: List[str], search_rows: int = 30) -> Optional[int]:
    req = [h.lower() for h in required_headers]
    max_r = min(search_rows, len(df_raw))

    for r in range(max_r):
        row_vals = [normalize_col_name(v) for v in df_raw.iloc[r].tolist()]
        if all(any(h == cell for cell in row_vals) for h in req):
            return r

    # fallback: allow partial match (CustomerId alone)
    for r in range(max_r):
        row_vals = [normalize_col_name(v) for v in df_raw.iloc[r].tolist()]
        if any("customerid" == cell for cell in row_vals):
            return r

    return None


def _looks_like_html(file_bytes: bytes) -> bool:
    head = (file_bytes[:256] or b"").lstrip().lower()
    return head.startswith(b"<!doctype") or head.startswith(b"<html") or b"<html" in head[:80]


def _read_excel_any_engine(file_bytes: bytes, header, filename: str) -> pd.DataFrame:
    """
    Robust Excel reader:
      - .xlsx -> openpyxl
      - .xls  -> xlrd (if installed), else fallback to default engine
    """
    low = filename.lower()

    if low.endswith(".xlsx"):
        return pd.read_excel(io.BytesIO(file_bytes), header=header, engine="openpyxl")

    if low.endswith(".xls"):
        # Old binary Excel needs xlrd (pandas removed built-in support unless xlrd installed)
        try:
            return pd.read_excel(io.BytesIO(file_bytes), header=header, engine="xlrd")
        except Exception:
            # fallback: let pandas try whatever is installed
            return pd.read_excel(io.BytesIO(file_bytes), header=header)

    # unknown extension fallback
    return pd.read_excel(io.BytesIO(file_bytes), header=header)


def load_report_table(file_bytes: bytes, filename: str) -> pd.DataFrame:
    low = filename.lower()

    if low.endswith(".csv"):
        return pd.read_csv(io.BytesIO(file_bytes))

    if _looks_like_html(file_bytes):
        raise ValueError("Report download looks like an HTML page (login/redirect). Check REPORT_URL/cookies/auth.")

    # Excel: read with header=None first so we can locate the true header row
    try:
        df_raw = _read_excel_any_engine(file_bytes, header=None, filename=filename)
    except zipfile.BadZipFile:
        # This is the classic "File is not a zip file" when trying to read non-xlsx with openpyxl,
        # or when the content isn't actually an Excel file.
        raise ValueError("Excel read failed (not a valid .xlsx zip). If this is .xls, install xlrd, or export as .xlsx/.csv.")
    except Exception as e:
        raise ValueError(f"Excel read failed: {e}")

    header_row = find_header_row(df_raw, required_headers=["CustomerId", "C. Balance"])
    if header_row is None:
        # last resort: try pandas normal header parse
        try:
            return _read_excel_any_engine(file_bytes, header=0, filename=filename)
        except Exception as e:
            raise ValueError(f"Couldn’t find headers and default read failed: {e}")

    headers = df_raw.iloc[header_row].tolist()
    df = df_raw.iloc[header_row + 1 :].copy()
    df.columns = headers
    df = df.reset_index(drop=True)
    df = df.dropna(how="all")  # Drop completely empty rows
    return df


# ====== UPSTASH NAME STORAGE (OPTION B) ======
def upstash_enabled() -> bool:
    return bool(UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN)


async def upstash_call(path: str, method: str = "GET", body: Optional[str] = None, params: Optional[Dict[str, str]] = None) -> Any:
    """
    Upstash REST API: command + args are path segments, like:
      GET  /GET/key
      POST /SET/key   with body = value
      GET  /DEL/key
      GET  /SCAN/0/MATCH/pattern/COUNT/100
    """
    if not upstash_enabled():
        raise RuntimeError("Upstash is not configured (missing UPSTASH_REDIS_REST_URL/TOKEN)")

    url = UPSTASH_REDIS_REST_URL.rstrip("/") + "/" + path.lstrip("/")
    headers = {"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"}

    async with httpx.AsyncClient(timeout=20) as client:
        if method.upper() == "POST":
            r = await client.post(url, headers=headers, content=(body or ""), params=params)
        else:
            r = await client.get(url, headers=headers, params=params)

        r.raise_for_status()
        return r.json()


async def name_get(uid: str) -> Optional[str]:
    uid = normalize_id(uid)
    # Upstash
    if upstash_enabled():
        try:
            data = await upstash_call(f"GET/{NAME_KEY_PREFIX}{uid}", method="GET")
            val = data.get("result")
            if isinstance(val, str) and val.strip():
                return val.strip()
        except Exception:
            pass

    # Fallback
    return PLAYER_NAMES.get(uid)


async def name_set(uid: str, name: str) -> None:
    uid = normalize_id(uid)
    nm = str(name or "").strip()
    if not nm:
        raise ValueError("Name is empty")

    PLAYER_NAMES[uid] = nm

    if upstash_enabled():
        await upstash_call(f"SET/{NAME_KEY_PREFIX}{uid}", method="POST", body=nm)


async def name_del(uid: str) -> None:
    uid = normalize_id(uid)
    if uid in PLAYER_NAMES:
        del PLAYER_NAMES[uid]

    if upstash_enabled():
        await upstash_call(f"DEL/{NAME_KEY_PREFIX}{uid}", method="GET")


async def names_list(limit: int = 500) -> Dict[str, str]:
    if not upstash_enabled():
        return dict(sorted(PLAYER_NAMES.items()))

    cursor = "0"
    found: Dict[str, str] = {}

    for _ in range(20):
        data = await upstash_call(f"SCAN/{cursor}/MATCH/{NAME_KEY_PREFIX}*/COUNT/200", method="GET")
        res = data.get("result")

        if isinstance(res, list) and len(res) == 2:
            cursor = str(res[0])
            keys = res[1] if isinstance(res[1], list) else []
        else:
            break

        for k in keys:
            if not isinstance(k, str):
                continue
            uid = k.replace(NAME_KEY_PREFIX, "").strip()
            nm = await name_get(uid)
            if nm:
                found[normalize_id(uid)] = nm
            if len(found) >= limit:
                return dict(sorted(found.items()))

        if cursor == "0":
            break

    return dict(sorted(found.items()))


async def format_player(user_id: str) -> str:
    uid = normalize_id(user_id)
    nm = await name_get(uid)
    return f"{uid} ({nm})" if nm else uid


def build_snap_message(display_name: str, amount: float) -> str:
    # chill + firm, starts with "Yo", encourages sending today (NO "if you can")
    return f"{display_name} — Yo you’re down ${amount:,.2f} this week. Please send today. Venmo {VENMO_HANDLE}"


# ====== CORE COMPUTE ======
def compute(file_bytes: bytes, filename: str, current_group: List[str]):
    """
    Returns:
      group_total: float
      per_player: Dict[uid, balance]  (selected players only)
      freeplay_total: int
      breakdown: List[(uid, balance, freeplay)]
      err: Optional[str]
    """
    df = load_report_table(file_bytes, filename)

    col_map = {normalize_col_name(c): c for c in df.columns}

    user_col = col_map.get("customerid") or col_map.get("customer id")
    bal_col = (
        col_map.get("c. balance")
        or col_map.get("c.balance")
        or col_map.get("c balance")
        or col_map.get("balance")
    )

    if not user_col or not bal_col:
        return None, None, None, None, "Missing CustomerId or C. Balance column"

    df["_u"] = df[user_col].astype(str).str.strip().str.upper()
    df["_b"] = df[bal_col].apply(parse_money)

    # ---- Selected players (sum by player in case duplicates) ----
    selected = [normalize_id(u) for u in current_group]
    sub = df[df["_u"].isin(selected)].copy()
    sub["_b"] = sub["_b"].fillna(0)

    per_player = sub.groupby("_u")["_b"].sum().to_dict()

    # ensure all selected appear (even if missing)
    for uid in selected:
        per_player.setdefault(uid, 0.0)

    group_total = float(sum(per_player.values()))

    # ---- Free play (ALL players) ----
    freeplay_total = 0
    breakdown = []
    for _, r in df.iterrows():
        bal = r["_b"]
        if bal is not None and bal <= -100:
            fp = int(round(abs(bal) * 0.20))
            freeplay_total += fp
            breakdown.append((str(r[user_col]).strip(), float(bal), fp))

    return group_total, per_player, freeplay_total, breakdown, None


def compute_settle_lists(file_bytes: bytes, filename: str):
    df = load_report_table(file_bytes, filename)
    col_map = {normalize_col_name(c): c for c in df.columns}
    user_col = col_map.get("customerid") or col_map.get("customer id")
    bal_col = (
        col_map.get("c. balance")
        or col_map.get("c.balance")
        or col_map.get("c balance")
        or col_map.get("balance")
    )
    if not user_col or not bal_col:
        return None, None, "Missing CustomerId or C. Balance column"

    df["_u"] = df[user_col].astype(str).str.strip().str.upper()
    df["_b"] = df[bal_col].apply(parse_money).fillna(0)

    winners = []
    losers = []
    for _, r in df.iterrows():
        uid = str(r[user_col]).strip().upper()
        bal = float(r["_b"])
        if bal > 0:
            winners.append((uid, bal))
        elif bal < 0:
            losers.append((uid, bal))

    winners.sort(key=lambda x: x[1], reverse=True)
    losers.sort(key=lambda x: x[1])  # most negative first

    total_winners = sum(x[1] for x in winners)
    total_losers_abs = sum(abs(x[1]) for x in losers)
    net = total_winners - total_losers_abs

    return {
        "winners": winners,
        "losers": losers,
        "total_winners": total_winners,
        "total_losers_abs": total_losers_abs,
        "net": net,
    }, None, None


async def fetch_report_bytes() -> Tuple[bytes, str]:
    if not REPORT_URL:
        raise RuntimeError("REPORT_URL is not set")

    headers = {}
    if REPORT_BEARER_TOKEN:
        headers["Authorization"] = f"Bearer {REPORT_BEARER_TOKEN}"
    if REPORT_COOKIE:
        headers["Cookie"] = REPORT_COOKIE

    auth = None
    if REPORT_USER and REPORT_PASS:
        auth = (REPORT_USER, REPORT_PASS)

    async with httpx.AsyncClient(follow_redirects=True, timeout=60) as client:
        r = await client.get(REPORT_URL, headers=headers, auth=auth)
        r.raise_for_status()

        filename = "weekly_report.xlsx"
        cd = r.headers.get("content-disposition", "") or ""
        if "filename=" in cd.lower():
            part = cd.split("filename=")[-1].strip()
            filename = part.strip('"').strip("'")
        else:
            url_low = REPORT_URL.lower()
            if ".csv" in url_low:
                filename = "weekly_report.csv"
            elif ".xlsx" in url_low:
                filename = "weekly_report.xlsx"
            elif ".xls" in url_low:
                filename = "weekly_report.xls"

        return r.content, filename


# ====== TELEGRAM HANDLERS ======
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        say(
            "How to use:\n"
            "1) /players AIDN003 AIDN014\n"
            "2) Upload report (CSV/XLS/XLSX) OR run /weekly if REPORT_URL is set\n"
            "3) Say 'weekly report' anytime\n\n"
            "Names:\n"
            "  /name AIDN003 Blake\n"
            "  /name_get AIDN003\n"
            "  /name_del AIDN003\n"
            "  /names\n\n"
            "Settles:\n"
            "  /settle (splits winners vs losers)\n"
            "  /snap (copy/paste Snap messages for LOSERS)\n\n"
            "AI only triggers if you type: Kenobi <message>"
        )
    )


async def players(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global CURRENT_GROUP
    if not context.args:
        await update.message.reply_text(say("Usage: /players AIDN015 AIDN042"))
        return

    CURRENT_GROUP = [normalize_id(a) for a in context.args if str(a).strip()]
    lines = [await format_player(pid) for pid in CURRENT_GROUP]
    await update.message.reply_text(say("Players saved:\n" + "\n".join(lines)))


async def weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global LATEST_FILE_BYTES, LATEST_FILE_NAME

    if not CURRENT_GROUP:
        await update.message.reply_text(say("Set players first: /players AIDN015 AIDN042"))
        return

    if (not LATEST_FILE_BYTES or not LATEST_FILE_NAME) and REPORT_URL:
        try:
            LATEST_FILE_BYTES, LATEST_FILE_NAME = await fetch_report_bytes()
        except Exception as e:
            await update.message.reply_text(say(f"Couldn’t pull the report from the site: {e}"))
            return

    if not LATEST_FILE_BYTES or not LATEST_FILE_NAME:
        await update.message.reply_text(say("Send the report file first (CSV/XLS/XLSX), or set REPORT_URL on Render."))
        return

    try:
        total, per_player, freeplay, breakdown, err = compute(LATEST_FILE_BYTES, LATEST_FILE_NAME, CURRENT_GROUP)
    except Exception as e:
        await update.message.reply_text(say(f"Couldn’t read that report: {e}"))
        return

    if err:
        await update.message.reply_text(say(err))
        return

    msg = "Weekly Report\n\n"
    msg += "Selected Players (C. Balance):\n"
    for uid in CURRENT_GROUP:
        bal = float((per_player or {}).get(normalize_id(uid), 0.0))
        msg += f"{await format_player(uid)}: {bal:,.2f}\n"

    msg += "\n"
    msg += f"Group Total: {total:,.2f}\n"
    msg += f"20% Free Play (balances ≤ -100): ${freeplay}\n\n"

    if breakdown:
        msg += "Free Play Owed:\n"
        for u, b, fp in breakdown:
            msg += f"{await format_player(u)}: {b:,.2f} → ${fp}\n"
    else:
        msg += "No accounts qualify for free play (≤ -100)."

    await update.message.reply_text(say("\n" + msg))


async def settle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global LATEST_FILE_BYTES, LATEST_FILE_NAME

    if (not LATEST_FILE_BYTES or not LATEST_FILE_NAME) and REPORT_URL:
        try:
            LATEST_FILE_BYTES, LATEST_FILE_NAME = await fetch_report_bytes()
        except Exception as e:
            await update.message.reply_text(say(f"Couldn’t pull the report from the site: {e}"))
            return

    if not LATEST_FILE_BYTES or not LATEST_FILE_NAME:
        await update.message.reply_text(say("Send the report file first (CSV/XLS/XLSX), or set REPORT_URL on Render."))
        return

    try:
        data, err, _ = compute_settle_lists(LATEST_FILE_BYTES, LATEST_FILE_NAME)
    except Exception as e:
        await update.message.reply_text(say(f"Couldn’t read that report: {e}"))
        return

    if err:
        await update.message.reply_text(say(err))
        return

    winners = data["winners"]
    losers = data["losers"]

    msg = "Settle\n\n"
    msg += f"Total Winners: {data['total_winners']:,.2f}\n"
    msg += f"Total Losers: {data['total_losers_abs']:,.2f}\n"
    msg += f"Net: {data['net']:,.2f}\n\n"

    msg += "Winners (owed to you):\n"
    if winners:
        for uid, bal in winners:
            msg += f"{await format_player(uid)}: +{bal:,.2f}\n"
    else:
        msg += "None\n"

    msg += "\nLosers (they owe you):\n"
    if losers:
        for uid, bal in losers:
            msg += f"{await format_player(uid)}: {bal:,.2f}\n"
    else:
        msg += "None\n"

    await update.message.reply_text(say("\n" + msg))


async def snap(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /snap -> outputs copy/paste Snap messages for LOSERS only
    """
    global LATEST_FILE_BYTES, LATEST_FILE_NAME

    if (not LATEST_FILE_BYTES or not LATEST_FILE_NAME) and REPORT_URL:
        try:
            LATEST_FILE_BYTES, LATEST_FILE_NAME = await fetch_report_bytes()
        except Exception as e:
            await update.message.reply_text(say(f"Couldn’t pull the report from the site: {e}"))
            return

    if not LATEST_FILE_BYTES or not LATEST_FILE_NAME:
        await update.message.reply_text(say("Send the report file first (CSV/XLS/XLSX), or set REPORT_URL on Render."))
        return

    try:
        data, err, _ = compute_settle_lists(LATEST_FILE_BYTES, LATEST_FILE_NAME)
    except Exception as e:
        await update.message.reply_text(say(f"Couldn’t read that report: {e}"))
        return

    if err:
        await update.message.reply_text(say(err))
        return

    losers = data["losers"]
    if not losers:
        await update.message.reply_text(say("No losers this week (no balances < 0)."))
        return

    msg = "Snap Messages (copy & paste)\n\n"
    for uid, bal in losers:
        display = await format_player(uid)
        amt = abs(float(bal))
        msg += build_snap_message(display, amt) + "\n\n"

    await update.message.reply_text(say(msg))


# ---- Name commands ----
async def cmd_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /name AIDN003 Blake Peterson
    """
    if not context.args or len(context.args) < 2:
        await update.message.reply_text(say("Usage: /name AIDN003 Blake (or full name)"))
        return

    uid = normalize_id(context.args[0])
    nm = " ".join(context.args[1:]).strip()

    try:
        await name_set(uid, nm)
        await update.message.reply_text(say(f"Saved: {uid} ({nm})"))
    except Exception as e:
        await update.message.reply_text(say(f"Couldn’t save name: {e}"))


async def cmd_name_get(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(say("Usage: /name_get AIDN003"))
        return
    uid = normalize_id(context.args[0])
    nm = await name_get(uid)
    if nm:
        await update.message.reply_text(say(f"{uid} = {nm}"))
    else:
        await update.message.reply_text(say(f"No name saved for {uid}"))


async def cmd_name_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(say("Usage: /name_del AIDN003"))
        return
    uid = normalize_id(context.args[0])
    try:
        await name_del(uid)
        await update.message.reply_text(say(f"Deleted name for {uid}"))
    except Exception as e:
        await update.message.reply_text(say(f"Couldn’t delete name: {e}"))


async def cmd_names(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        mp = await names_list(limit=500)
    except Exception as e:
        await update.message.reply_text(say(f"Couldn’t list names: {e}"))
        return

    if not mp:
        await update.message.reply_text(say("No names saved."))
        return

    lines = [f"{k} = {v}" for k, v in mp.items()]
    chunk = []
    size = 0
    for line in lines:
        if size + len(line) + 1 > 3500:
            await update.message.reply_text(say("Names:\n" + "\n".join(chunk)))
            chunk = []
            size = 0
        chunk.append(line)
        size += len(line) + 1
    if chunk:
        await update.message.reply_text(say("Names:\n" + "\n".join(chunk)))


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global LATEST_FILE_BYTES, LATEST_FILE_NAME

    if not update.message or not update.message.document:
        await update.message.reply_text(say("Attach a report file (CSV/XLS/XLSX)."))
        return

    filename = (update.message.document.file_name or "").strip()
    low = filename.lower()

    if not (low.endswith(".csv") or low.endswith(".xlsx") or low.endswith(".xls")):
        await update.message.reply_text(say("Please send a CSV or Excel file (.csv, .xlsx, .xls)."))
        return

    f = await update.message.document.get_file()
    data = await f.download_as_bytearray()

    LATEST_FILE_BYTES = bytes(data)
    LATEST_FILE_NAME = filename

    await update.message.reply_text(say("Report received. Running weekly report now..."))
    await weekly(update, context)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text_raw = (update.message.text or "").strip()
    text = text_raw.lower()

    # AI trigger: MUST be "kenobi " (with a space)
    if text.startswith("kenobi "):
        if not oa_client:
            await update.message.reply_text(say("AI isn’t configured yet. Add OPENAI_API_KEY on Render."))
            return

        prompt = text_raw[len("kenobi "):].strip()
        if not prompt:
            await update.message.reply_text(say("Example: Kenobi summarize this week"))
            return

        try:
            resp = oa_client.responses.create(
                model="gpt-5-mini",
                input=[
                    {"role": "system", "content": "You are Kenobi. Always address the user as 'sir'. Keep replies concise."},
                    {"role": "user", "content": prompt},
                ],
            )
            await update.message.reply_text(resp.output_text)
        except Exception as e:
            await update.message.reply_text(say(f"AI error: {e}"))
        return

    # Weekly intents
    if any(p in text for p in ["weekly report", "send weekly report", "run weekly", "weekly totals", "/weekly"]):
        return await weekly(update, context)

    if any(p in text for p in ["settle", "settlement", "/settle"]):
        return await settle(update, context)

    if any(p in text for p in ["snap", "/snap", "snap messages", "snapchat"]):
        return await snap(update, context)

    if text.startswith("players") or text.startswith("set players"):
        ids = extract_ids(text_raw)
        if not ids:
            await update.message.reply_text(say("Tell me IDs like: players AIDN015 AIDN042"))
            return
        context.args = ids
        return await players(update, context)

    if any(p in text for p in ["help", "instructions", "how do i use"]):
        return await start(update, context)

    await update.message.reply_text(say("Try: ‘weekly report’, ‘settle’, ‘snap’, or ‘/players AIDN015 AIDN042’."))


# ====== FASTAPI LIFECYCLE + WEBHOOK ======
@app.on_event("startup")
async def startup():
    global tg_app
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is missing")
    if not WEBHOOK_SECRET:
        raise RuntimeError("WEBHOOK_SECRET is missing")

    tg_app = Application.builder().token(BOT_TOKEN).build()

    tg_app.add_handler(CommandHandler("start", start))
    tg_app.add_handler(CommandHandler("players", players))
    tg_app.add_handler(CommandHandler("weekly", weekly))
    tg_app.add_handler(CommandHandler("settle", settle))
    tg_app.add_handler(CommandHandler("snap", snap))

    tg_app.add_handler(CommandHandler("name", cmd_name))
    tg_app.add_handler(CommandHandler("name_get", cmd_name_get))
    tg_app.add_handler(CommandHandler("name_del", cmd_name_del))
    tg_app.add_handler(CommandHandler("names", cmd_names))

    tg_app.add_handler(MessageHandler(filters.Document.ALL, handle_file))
    tg_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    await tg_app.initialize()
    await tg_app.start()


@app.post(f"/webhook/{WEBHOOK_SECRET}")
async def webhook(req: Request):
    if not tg_app:
        return {"ok": False, "error": "tg_app not ready"}
    data = await req.json()
    update = Update.de_json(data, tg_app.bot)
    await tg_app.process_update(update)
    return {"ok": True}


@app.get("/setwebhook")
async def set_webhook(request: Request):
    assert tg_app is not None
    url = str(request.base_url).rstrip("/") + f"/webhook/{WEBHOOK_SECRET}"
    await tg_app.bot.set_webhook(url)
    return {"webhook_set": True, "url": url}


@app.get("/")
async def root():
    return {"status": "running"}
