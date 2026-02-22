import os
import io
import re
from typing import Optional, Tuple, List

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


# ====== APP STATE ======
app = FastAPI()
tg_app: Optional[Application] = None

LATEST_FILE_BYTES: Optional[bytes] = None
LATEST_FILE_NAME: Optional[str] = None
CURRENT_GROUP: List[str] = []


# ====== AI CLIENT ======
oa_client = OpenAI(api_key=OPENAI_API_KEY) if (OpenAI and OPENAI_API_KEY) else None


# ====== HELPERS ======
def say(text: str) -> str:
    return f"Yes sir — {text}"


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


def extract_ids(text: str) -> List[str]:
    return re.findall(r"\b[A-Z]{2,}\d{2,}\b", (text or "").upper())


def normalize_col_name(x) -> str:
    return re.sub(r"\s+", " ", str(x or "")).strip().lower()


def find_header_row(df_raw: pd.DataFrame, required_headers: List[str], search_rows: int = 30) -> Optional[int]:
    """
    df_raw is header=None dataframe.
    Finds a row index where required header(s) appear (case-insensitive).
    """
    req = [h.lower() for h in required_headers]
    max_r = min(search_rows, len(df_raw))
    for r in range(max_r):
        row_vals = [normalize_col_name(v) for v in df_raw.iloc[r].tolist()]
        # if any required header is in that row
        if all(any(h == cell for cell in row_vals) for h in req):
            return r
    # fallback: allow partial match (CustomerId alone)
    for r in range(max_r):
        row_vals = [normalize_col_name(v) for v in df_raw.iloc[r].tolist()]
        if any("customerid" == cell for cell in row_vals):
            return r
    return None


def load_report_table(file_bytes: bytes, filename: str) -> pd.DataFrame:
    low = filename.lower()

    if low.endswith(".csv"):
        # CSV usually already has headers, but we still guard
        df = pd.read_csv(io.BytesIO(file_bytes))
        return df

    # Excel: read with header=None first so we can locate the true header row
    df_raw = pd.read_excel(io.BytesIO(file_bytes), header=None, engine="openpyxl")

    header_row = find_header_row(df_raw, required_headers=["CustomerId", "C. Balance"])
    if header_row is None:
        # If we can't find it, try the first row as header as a last resort
        df = pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl")
        return df

    headers = df_raw.iloc[header_row].tolist()
    df = df_raw.iloc[header_row + 1 :].copy()
    df.columns = headers
    df = df.reset_index(drop=True)

    # Drop completely empty rows
    df = df.dropna(how="all")
    return df


def compute(file_bytes: bytes, filename: str, current_group: List[str]):
    df = load_report_table(file_bytes, filename)

    # normalize column names lookup
    col_map = {normalize_col_name(c): c for c in df.columns}

    # expected columns in YOUR file
    user_col = col_map.get("customerid") or col_map.get("customer id")
    bal_col = col_map.get("c. balance") or col_map.get("c.balance") or col_map.get("balance") or col_map.get("c balance")

    if not user_col or not bal_col:
        return None, None, None, "Missing CustomerId or C. Balance column"

    df["_u"] = df[user_col].astype(str).str.strip().str.lower()
    df["_b"] = df[bal_col].apply(parse_money)

    # ---- Group total (selected players only) ----
    selected = [u.strip().lower() for u in current_group]
    sub = df[df["_u"].isin(selected)]
    group_total = float(sub["_b"].fillna(0).sum())

    # ---- Free play (ALL players) ----
    freeplay_total = 0
    breakdown = []
    for _, r in df.iterrows():
        bal = r["_b"]
        if bal is not None and bal <= -100:
            fp = int(round(abs(bal) * 0.20))
            freeplay_total += fp
            breakdown.append((str(r[user_col]).strip(), float(bal), fp))

    return group_total, freeplay_total, breakdown, None


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
            "3) Say 'weekly report' anytime to run it\n\n"
            "AI only triggers if you type: Kenobi <message>"
        )
    )


async def players(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global CURRENT_GROUP
    if not context.args:
        await update.message.reply_text(say("Usage: /players AIDN015 AIDN042"))
        return

    CURRENT_GROUP = context.args
    await update.message.reply_text(say("Players saved:\n" + "\n".join(CURRENT_GROUP)))


async def weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global LATEST_FILE_BYTES, LATEST_FILE_NAME

    if not CURRENT_GROUP:
        await update.message.reply_text(say("Set players first: /players AIDN015 AIDN042"))
        return

    # If no file uploaded yet, try pulling from site
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
        total, freeplay, breakdown, err = compute(LATEST_FILE_BYTES, LATEST_FILE_NAME, CURRENT_GROUP)
    except Exception as e:
        await update.message.reply_text(say(f"Couldn’t read that report: {e}"))
        return

    if err:
        await update.message.reply_text(say(err))
        return

    msg = "Weekly Report\n\n"
    msg += f"Group Total: {total:,.2f}\n"
    msg += f"20% Free Play (balances ≤ -100): ${freeplay}\n\n"

    if breakdown:
        msg += "Free Play Owed:\n"
        for u, b, fp in breakdown:
            msg += f"{u.upper()}: {b:,.2f} → ${fp}\n"
    else:
        msg += "No accounts qualify for free play (≤ -100)."

    await update.message.reply_text(say("\n" + msg))


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

    if text.startswith("players") or text.startswith("set players"):
        ids = extract_ids(text_raw)
        if not ids:
            await update.message.reply_text(say("Tell me IDs like: players AIDN015 AIDN042"))
            return
        context.args = ids
        return await players(update, context)

    if any(p in text for p in ["help", "instructions", "how do i use"]):
        return await start(update, context)

    await update.message.reply_text(say("Try: ‘weekly report’ or ‘players AIDN015 AIDN042’."))


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
