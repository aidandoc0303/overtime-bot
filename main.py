import os
import io
import pandas as pd
from fastapi import FastAPI, Request
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")

app = FastAPI()
tg_app = None

LATEST_CSV = None
CURRENT_GROUP = []


def parse_money(x):
    s = str(x).replace("$", "").replace(",", "").strip()
    try:
        return float(s)
    except:
        return None


def compute(csv_bytes):
    df = pd.read_csv(io.BytesIO(csv_bytes))
    username_col = df.columns[0]
    balance_col = df.columns[-1]

    df["_u"] = df[username_col].astype(str).str.lower()
    df["_b"] = df[balance_col].apply(parse_money)

    sub = df[df["_u"].isin([u.lower() for u in CURRENT_GROUP])]

    total = sub["_b"].sum()

    freeplay = 0
    breakdown = []

    for _, r in sub.iterrows():
        bal = r["_b"]
        if bal is not None and bal <= -100:
            fp = round(abs(bal) * 0.20)
            freeplay += fp
            breakdown.append((r["_u"], bal, fp))

    return total, freeplay, breakdown


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Weekly steps:\n"
        "1) /players AIDN015 AIDN042\n"
        "2) Send CSV\n"
        "Bot returns totals and free play"
    )


async def players(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global CURRENT_GROUP
    if not context.args:
        await update.message.reply_text("Usage: /players AIDN015 AIDN042")
        return

    CURRENT_GROUP = context.args
    await update.message.reply_text("Players saved:\n" + "\n".join(CURRENT_GROUP))


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global LATEST_CSV

    if not update.message.document.file_name.lower().endswith(".csv"):
        await update.message.reply_text("Please send a CSV file.")
        return

    file = await update.message.document.get_file()
    data = await file.download_as_bytearray()
    LATEST_CSV = bytes(data)

    total, freeplay, breakdown = compute(LATEST_CSV)

    msg = f"Group Total: {total:,.2f}\n"
    msg += f"20% Free Play (≤ -100): ${freeplay}\n\n"

    if breakdown:
        msg += "Free Play Breakdown:\n"
        for u, b, fp in breakdown:
            msg += f"{u}: {b:,.2f} → ${fp}\n"
    else:
        msg += "No players below -100"

    await update.message.reply_text(msg)


@app.on_event("startup")
async def startup():
    global tg_app
    tg_app = Application.builder().token(BOT_TOKEN).build()
    tg_app.add_handler(CommandHandler("start", start))
    tg_app.add_handler(CommandHandler("players", players))
    tg_app.add_handler(MessageHandler(filters.Document.ALL, handle_file))
    await tg_app.initialize()
    await tg_app.start()


@app.post(f"/webhook/{WEBHOOK_SECRET}")
async def webhook(req: Request):
    data = await req.json()
    update = Update.de_json(data, tg_app.bot)
    await tg_app.process_update(update)
    return {"ok": True}


@app.get("/setwebhook")
async def set_webhook(req: Request):
    url = str(req.base_url).rstrip("/") + f"/webhook/{WEBHOOK_SECRET}"
    await tg_app.bot.set_webhook(url)
    return {"webhook_set": True}
@app.get("/")
async def root():
    return {"status": "running"}
