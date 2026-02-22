import os
import io
import re
from typing import Optional, Tuple, List

import pandas as pd
import httpx
from fastapi import FastAPI, Request
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

# Optional AI (hybrid)
try:
    from openai import OpenAI
except Exception:
    OpenAI = None


# ====== ENV ======
BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")

REPORT_URL = os.getenv("REPORT_URL")
REPORT_BEARER_TOKEN = os.getenv("REPORT_BEARER_TOKEN")
REPORT_USER = os.getenv("REPORT_USER")
REPORT_PASS = os.getenv("REPORT_PASS")
REPORT_COOKIE = os.getenv("REPORT_COOKIE")

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
    s = str(x).replace("$", "").replace(",", "").strip()
    try:
        return float(s)
    except:
        return None


def extract_ids(text: str) -> List[str]:
    return re.findall(r"\b[A-Z]{2,}\d{2,}\b", text.upper())


def pick_col(cols, options):
    for c in cols:
        if c.strip().lower() in [o.lower() for o in options]:
            return c
    return None


# ====== CORE LOGIC ======
def compute(file_bytes, filename, current_group):
    if filename.lower().endswith(".csv"):
        df = pd.read_csv(io.BytesIO(file_bytes))
    else:
        df = pd.read_excel(io.BytesIO(file_bytes))

    user_col = pick_col(df.columns, ["CustomerId", "Customer", "Username"])
    bal_col = pick_col(df.columns, ["C. Balance", "Balance"])

    if user_col is None or bal_col is None:
        return None, None, None, "Missing CustomerId or Balance column"

    df["_u"] = df[user_col].astype(str).str.strip().str.lower()
    df["_b"] = df[bal_col].apply(parse_money)

    # ===== GROUP TOTAL (selected only) =====
    selected = [u.lower() for u in current_group]
    sub = df[df["_u"].isin(selected)]
    group_total = float(sub["_b"].fillna(0).sum())

    # ===== FREE PLAY (all players) =====
    freeplay_total = 0
    breakdown = []

    for _, r in df.iterrows():
        bal = r["_b"]
        if bal is not None and bal <= -100:
            fp = round(abs(bal) * 0.20)
            freeplay_total += fp
            breakdown.append((r[user_col], bal, fp))

    return group_total, freeplay_total, breakdown, None


# ====== REPORT PULL (optional) ======
async def fetch_report_bytes() -> Tuple[bytes, str]:
    if not REPORT_URL:
        raise RuntimeError("REPORT_URL not set")

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
        if ".csv" in REPORT_URL.lower():
            filename = "weekly_report.csv"

        return r.content, filename


# ====== TELEGRAM ======
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        say(
            "How to use:\n"
            "1) /players AIDN003 AIDN014\n"
            "2) Upload report OR say weekly report\n"
            "I’ll return totals and free play."
        )
    )


async def players(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global CURRENT_GROUP

    if not context.args:
        await update.message.reply_text(say("Usage: /players AIDN003 AIDN014"))
        return

    CURRENT_GROUP = context.args
    await update.message.reply_text(
        say("Players saved:\n" + "\n".join(CURRENT_GROUP))
    )


async def weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global LATEST_FILE_BYTES, LATEST_FILE_NAME

    if not CURRENT_GROUP:
        await update.message.reply_text(say("Set players first"))
        return

    # Try site pull if no upload yet
    if (not LATEST_FILE_BYTES or not LATEST_FILE_NAME) and REPORT_URL:
        try:
            LATEST_FILE_BYTES, LATEST_FILE_NAME = await fetch_report_bytes()
        except Exception as e:
            await update.message.reply_text(say(f"Site pull failed: {e}"))
            return

    if not LATEST_FILE_BYTES:
        await update.message.reply_text(say("Send the report file first"))
        return

    total, freeplay, breakdown, err = compute(
        LATEST_FILE_BYTES,
        LATEST_FILE_NAME,
        CURRENT_GROUP
    )

    if err:
        await update.message.reply_text(say(err))
        return

    msg = "Weekly Report\n\n"
    msg += f"Group Total: {total:,.2f}\n"
    msg += f"20% Free Play Total: ${freeplay}\n\n"

    if breakdown:
        msg += "Free Play Owed:\n"
        for u, b, fp in breakdown:
            msg += f"{str(u).upper()}: {b:,.2f} → ${fp}\n"
    else:
        msg += "No players ≤ -100"

    await update.message.reply_text(say(msg))


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global LATEST_FILE_BYTES, LATEST_FILE_NAME

    filename = (update.message.document.file_name or "").lower()

    if not filename.endswith((".csv", ".xlsx", ".xls")):
        await update.message.reply_text(say("Send CSV or Excel"))
        return

    f = await update.message.document.get_file()
    data = await f.download_as_bytearray()

    LATEST_FILE_BYTES = bytes(data)
    LATEST_FILE_NAME = filename

    await update.message.reply_text(say("Report received"))
    await weekly(update, context)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip().lower()

    # Paid AI
    if text.startswith("kenobi"):
        if not oa_client:
            await update.message.reply_text(say("AI not configured"))
            return

        prompt = text[6:].strip()

        resp = oa_client.responses.create(
            model="gpt-5-mini",
            input=prompt
        )

        await update.message.reply_text(resp.output_text)
        return

    # Free commands
    if "weekly" in text:
        return await weekly(update, context)

    if "players" in text:
        ids = extract_ids(text)
        context.args = ids
        return await players(update, context)

    await update.message.reply_text(say("Try: weekly report"))


# ====== STARTUP ======
@app.on_event("startup")
async def startup():
    global tg_app

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
    data = await req.json()
    update = Update.de_json(data, tg_app.bot)
    await tg_app.process_update(update)
    return {"ok": True}


@app.get("/setwebhook")
async def set_webhook(request: Request):
    url = str(request.base_url).rstrip("/") + f"/webhook/{WEBHOOK_SECRET}"
    await tg_app.bot.set_webhook(url)
    return {"webhook_set": True}


@app.get("/")
async def root():
    return {"status": "running"}
