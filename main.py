import os
import io
import re
from typing import Optional, Tuple, List

import pandas as pd
import httpx
from fastapi import FastAPI, Request
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

# Optional AI (hybrid): only used when a message starts with "Kenobi ..."
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
    # Kenobi’s voice
    return f"Yes sir — {text}"


def parse_money(x) -> Optional[float]:
    s = str(x).replace("$", "").replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        return None


def extract_ids(text: str) -> List[str]:
    # Matches AIDN015 style (letters+digits)
    return re.findall(r"\b[A-Z]{2,}\d{2,}\b", (text or "").upper())


def pick_col(cols, options):
    for c in cols:
        if c.strip().lower() in [o.lower() for o in options]:
            return c
    return None


def compute(file_bytes, filename, current_group):
    # Read file
    if filename.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(file_bytes))
    else:
        df = pd.read_excel(io.BytesIO(file_bytes))

    # Detect columns
    user_col = pick_col(df.columns, ["CustomerId", "Customer", "Username"])
    bal_col  = pick_col(df.columns, ["C. Balance", "Balance"])

    if user_col is None or bal_col is None:
        return None, None, None, "Missing CustomerId or Balance column"

    df["_u"] = df[user_col].astype(str).str.strip().str.lower()
    df["_b"] = df[bal_col].apply(parse_money)

    # -------- Group total (selected players only) --------
    selected = [u.lower() for u in current_group]
    sub = df[df["_u"].isin(selected)]
    group_total = float(sub["_b"].fillna(0).sum())

    # -------- Free play (ALL players) --------
    freeplay_total = 0
    breakdown = []

    for _, r in df.iterrows():
        bal = r["_b"]
        if bal is not None and bal <= -100:
            fp = round(abs(bal) * 0.20)
            freeplay_total += fp
            breakdown.append((r[user_col], bal, fp))

    return group_total, freeplay_total, breakdown, None


async def fetch_report_bytes() -> Tuple[bytes, str]:
    """
    Pulls report from REPORT_URL (Path A).
    Supports optional bearer token, basic auth, or cookie header.
    """
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

        # Try infer filename
        filename = "weekly_report.xlsx"
        cd = r.headers.get("content-disposition", "") or ""
        if "filename=" in cd.lower():
            # crude parse, works for most
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
            "Here’s how to use me:\n"
            "1) Set players: /players AIDN015 AIDN042\n"
            "2) Either upload the report file (CSV/XLS/XLSX) OR just run /weekly if REPORT_URL is configured\n"
            "3) /weekly returns group total + 20% free play owed list"
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

    # Preferred: pull from site if REPORT_URL is set
    if REPORT_URL:
        try:
            LATEST_FILE_BYTES, LATEST_FILE_NAME = await fetch_report_bytes()
        except Exception as e:
            await update.message.reply_text(say(f"Couldn’t pull the report from the site: {e}"))
            return

    # Fallback: use last uploaded
    if not LATEST_FILE_BYTES or not LATEST_FILE_NAME:
        await update.message.reply_text(say("Send the report file first (CSV/XLS/XLSX), or set REPORT_URL on Render."))
        return

    try:
        total, freeplay, breakdown = compute(LATEST_FILE_BYTES, LATEST_FILE_NAME)
    except Exception as e:
        await update.message.reply_text(say(f"Couldn’t read that report: {e}"))
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

    # Nice UX: auto-run weekly right after upload
    await update.message.reply_text(say("Report received. Running weekly report now..."))
    await weekly(update, context)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text_raw = (update.message.text or "").strip()
    text = text_raw.lower()

    # Paid AI trigger (hybrid): must start with "Kenobi"
    if text.startswith("kenobi"):
        if not oa_client:
            await update.message.reply_text(say("AI isn’t configured yet. Add OPENAI_API_KEY on Render."))
            return

        prompt = text_raw[len("kenobi"):].strip()
        if not prompt:
            await update.message.reply_text(say("What should I help with? Example: Kenobi summarize this week"))
            return

        try:
            resp = oa_client.responses.create(
                model="gpt-5-mini",
                input=[
                    {
                        "role": "system",
                        "content": (
                            "You are Kenobi, a calm and practical assistant. "
                            "Always address the user as 'sir'. Keep replies concise."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
            )
            await update.message.reply_text(resp.output_text)
        except Exception as e:
            await update.message.reply_text(say(f"AI error: {e}"))
        return

    # Free English-ish intents
    if any(p in text for p in ["weekly report", "send weekly report", "run weekly", "weekly totals", "/weekly"]):
        return await weekly(update, context)

    if text.startswith("players") or text.startswith("set players"):
        ids = extract_ids(text_raw)
        if not ids:
            await update.message.reply_text(say("Tell me IDs like: players AIDN015 AIDN042"))
            return
        context.args = ids
        return await players(update, context)

    if any(p in text for p in ["help", "instructions", "what do i do", "how do i use"]):
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
    # call once after deploy (or anytime after) to set Telegram -> Render webhook
    assert tg_app is not None
    url = str(request.base_url).rstrip("/") + f"/webhook/{WEBHOOK_SECRET}"
    await tg_app.bot.set_webhook(url)
    return {"webhook_set": True, "url": url}


@app.get("/")
async def root():
    return {"status": "running"}
