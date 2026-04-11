import asyncio
import os
import signal
import sqlite3
from contextlib import closing
from typing import Optional

from telegram import ReplyKeyboardMarkup, Update
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


BOT_TOKEN = "8627437268:AAGB6kG_HDEdYE4Y0CYmn0GT3GSr8y3oA0o"
if not BOT_TOKEN:
    raise RuntimeError("Set BOT_TOKEN in environment before running.")

DB_PATH = os.getenv("DB_PATH", "anon_chat.db")
REPORT_BAN_THRESHOLD = int(os.getenv("REPORT_BAN_THRESHOLD", "3"))
MAX_WARNINGS = int(os.getenv("MAX_WARNINGS", "3"))
BAD_WORDS = [w.strip().lower() for w in os.getenv("BAD_WORDS", "abuse,slur,badword").split(",") if w.strip()]

KEYBOARD = ReplyKeyboardMarkup(
    [["Find Stranger", "Next"], ["Stop", "Report"]],
    resize_keyboard=True,
)

waiting_queue: list[int] = []
state_lock = asyncio.Lock()


def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with closing(db_conn()) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'idle',
                partner_id INTEGER NULL,
                reports_received INTEGER NOT NULL DEFAULT 0,
                reports_sent INTEGER NOT NULL DEFAULT 0,
                warnings INTEGER NOT NULL DEFAULT 0,
                is_banned INTEGER NOT NULL DEFAULT 0,
                ban_reason TEXT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()


def ensure_user(user_id: int) -> sqlite3.Row:
    with closing(db_conn()) as conn:
        row = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
        if row:
            return row
        conn.execute("INSERT INTO users (user_id) VALUES (?)", (user_id,))
        conn.commit()
        return conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()


def get_user(user_id: int) -> Optional[sqlite3.Row]:
    with closing(db_conn()) as conn:
        return conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()


def update_user(user_id: int, **fields) -> None:
    if not fields:
        return
    keys = ", ".join(f"{k} = ?" for k in fields.keys())
    values = list(fields.values()) + [user_id]
    with closing(db_conn()) as conn:
        conn.execute(
            f"UPDATE users SET {keys}, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?",
            values,
        )
        conn.commit()


def remove_from_queue(user_id: int) -> None:
    try:
        waiting_queue.remove(user_id)
    except ValueError:
        pass


def pop_valid_partner(exclude_user_id: int) -> Optional[int]:
    while waiting_queue:
        candidate_id = waiting_queue.pop(0)
        if candidate_id == exclude_user_id:
            continue
        candidate = get_user(candidate_id)
        if not candidate:
            continue
        if (
            candidate["status"] == "searching"
            and candidate["partner_id"] is None
            and candidate["is_banned"] == 0
        ):
            return candidate_id
    return None


def contains_bad_word(text: str) -> bool:
    normalized = text.lower()
    return any(word in normalized for word in BAD_WORDS)


async def safe_send(application: Application, user_id: int, text: str) -> None:
    try:
        await application.bot.send_message(chat_id=user_id, text=text, reply_markup=KEYBOARD)
    except TelegramError:
        pass


async def start_find_flow(application: Application, user_id: int) -> None:
    async with state_lock:
        user = ensure_user(user_id)

        if user["is_banned"] == 1 or user["status"] == "banned":
            await safe_send(application, user_id, "You are banned from this bot.")
            return

        if user["status"] == "chatting" and user["partner_id"] is not None:
            await safe_send(application, user_id, "You are already chatting. Use /next or /stop.")
            return

        remove_from_queue(user_id)
        update_user(user_id, status="searching", partner_id=None)

        partner_id = pop_valid_partner(user_id)
        if partner_id is None:
            waiting_queue.append(user_id)
            await safe_send(application, user_id, "Searching for a stranger...")
            return

        update_user(user_id, status="chatting", partner_id=partner_id)
        update_user(partner_id, status="chatting", partner_id=user_id)

    await safe_send(application, user_id, "Connected with a stranger. Say hi!")
    await safe_send(application, partner_id, "Connected with a stranger. Say hi!")


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    ensure_user(user_id)
    await safe_send(context.application, user_id, "Welcome. Use /find to chat anonymously.")


async def find_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await start_find_flow(context.application, update.effective_user.id)


async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    partner_id = None

    async with state_lock:
        user = ensure_user(user_id)
        partner_id = user["partner_id"]
        remove_from_queue(user_id)
        update_user(user_id, status="idle", partner_id=None)

        if partner_id is not None:
            partner = get_user(partner_id)
            if partner and partner["partner_id"] == user_id:
                update_user(partner_id, status="idle", partner_id=None)

    await safe_send(context.application, user_id, "Chat stopped. Use /find to search again.")
    if partner_id is not None:
        await safe_send(context.application, partner_id, "Stranger left the chat.")


async def next_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    previous_partner_id = None
    new_partner_id = None

    async with state_lock:
        user = ensure_user(user_id)
        previous_partner_id = user["partner_id"]

        remove_from_queue(user_id)
        if previous_partner_id is not None:
            partner = get_user(previous_partner_id)
            if partner and partner["partner_id"] == user_id:
                update_user(previous_partner_id, status="idle", partner_id=None)

        update_user(user_id, status="searching", partner_id=None)
        new_partner_id = pop_valid_partner(user_id)
        if new_partner_id is None:
            waiting_queue.append(user_id)

        if new_partner_id is not None:
            update_user(user_id, status="chatting", partner_id=new_partner_id)
            update_user(new_partner_id, status="chatting", partner_id=user_id)

    if previous_partner_id is not None:
        await safe_send(context.application, previous_partner_id, "Stranger skipped the chat.")

    if new_partner_id is None:
        await safe_send(context.application, user_id, "Searching for a new stranger...")
        return

    await safe_send(context.application, user_id, "Connected with a new stranger.")
    await safe_send(context.application, new_partner_id, "Connected with a stranger. Say hi!")


async def report_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    partner_id = None
    banned_now = False
    reports_received = 0

    async with state_lock:
        reporter = ensure_user(user_id)
        partner_id = reporter["partner_id"]
        if reporter["status"] != "chatting" or partner_id is None:
            await safe_send(context.application, user_id, "No active partner to report.")
            return

        partner = get_user(partner_id)
        if not partner:
            update_user(user_id, status="idle", partner_id=None)
            await safe_send(context.application, user_id, "No active partner to report.")
            return

        reports_received = int(partner["reports_received"]) + 1
        update_user(user_id, reports_sent=int(reporter["reports_sent"]) + 1)

        if reports_received >= REPORT_BAN_THRESHOLD:
            remove_from_queue(partner_id)
            update_user(
                partner_id,
                reports_received=reports_received,
                status="banned",
                partner_id=None,
                is_banned=1,
                ban_reason="Too many reports",
            )
            banned_now = True
        else:
            update_user(
                partner_id,
                reports_received=reports_received,
                status="idle",
                partner_id=None,
            )

        update_user(user_id, status="idle", partner_id=None)

    if banned_now:
        await safe_send(
            context.application,
            user_id,
            f"Report submitted. User banned at {reports_received} reports.",
        )
        await safe_send(
            context.application,
            partner_id,
            "You have been banned due to repeated reports.",
        )
    else:
        await safe_send(
            context.application,
            user_id,
            f"Report submitted. Chat ended. User now has {reports_received} report(s).",
        )
        await safe_send(context.application, partner_id, "You were reported. Chat ended.")


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    text = (update.message.text or "").strip()
    if not text:
        return

    if text == "Find Stranger":
        await start_find_flow(context.application, user_id)
        return
    if text == "Next":
        await next_cmd(update, context)
        return
    if text == "Stop":
        await stop_cmd(update, context)
        return
    if text == "Report":
        await report_cmd(update, context)
        return

    partner_id = None
    moderation_block = False
    banned_now = False
    warnings_left = 0

    async with state_lock:
        user = ensure_user(user_id)
        if user["is_banned"] == 1 or user["status"] == "banned":
            await safe_send(context.application, user_id, "You are banned from this bot.")
            return

        if user["status"] != "chatting" or user["partner_id"] is None:
            await safe_send(context.application, user_id, "You are not in a chat. Use /find.")
            return

        partner_id = int(user["partner_id"])
        partner = get_user(partner_id)
        if not partner or partner["status"] != "chatting" or partner["partner_id"] != user_id:
            update_user(user_id, status="idle", partner_id=None)
            await safe_send(context.application, user_id, "Previous chat expired. Use /find again.")
            return

        if contains_bad_word(text):
            moderation_block = True
            warnings = int(user["warnings"]) + 1
            warnings_left = max(MAX_WARNINGS - warnings, 0)
            if warnings >= MAX_WARNINGS:
                remove_from_queue(user_id)
                update_user(
                    user_id,
                    warnings=warnings,
                    status="banned",
                    partner_id=None,
                    is_banned=1,
                    ban_reason="Bad language",
                )
                update_user(partner_id, status="idle", partner_id=None)
                banned_now = True
            else:
                update_user(user_id, warnings=warnings)

    if moderation_block and not banned_now:
        await safe_send(
            context.application,
            user_id,
            f"Message blocked by moderation. Warnings left: {warnings_left}.",
        )
        return

    if banned_now:
        await safe_send(context.application, user_id, "You are banned for repeated policy violations.")
        await safe_send(context.application, partner_id, "Stranger was removed by moderation.")
        return

    await safe_send(context.application, partner_id, f"Stranger: {text}")


def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("find", find_cmd))
    app.add_handler(CommandHandler("stop", stop_cmd))
    app.add_handler(CommandHandler("next", next_cmd))
    app.add_handler(CommandHandler("report", report_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    return app


def main() -> None:
    init_db()
    app = build_app()
    print("Anonymous one-file bot is running...")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        stop_signals=[signal.SIGINT, signal.SIGTERM],
    )


if __name__ == "__main__":
    main()

