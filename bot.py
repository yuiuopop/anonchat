import asyncio
import logging
import os
import re
import signal
import sqlite3
import time
from threading import Lock
from typing import Optional

from telegram import ReplyKeyboardMarkup, Update
from telegram.constants import ChatAction
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
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "").strip()
ADMIN_ID = int(ADMIN_ID_RAW) if ADMIN_ID_RAW.isdigit() else None
COMMAND_COOLDOWN_SECONDS = float(os.getenv("COMMAND_COOLDOWN_SECONDS", "3"))

USER_KEYBOARD = ReplyKeyboardMarkup(
    [["🔎 Find Stranger", "⏭️ Next"], ["🛑 Stop", "🚩 Report"]],
    resize_keyboard=True,
)

ADMIN_DASHBOARD_BUTTON = "📊 Admin Dashboard"
ADMIN_KEYBOARD = ReplyKeyboardMarkup(
    [["🔎 Find Stranger", "⏭️ Next"], ["🛑 Stop", "🚩 Report"], [ADMIN_DASHBOARD_BUTTON]],
    resize_keyboard=True,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

state_lock = asyncio.Lock()
db_lock = Lock()
_db_conn: Optional[sqlite3.Connection] = None
user_cooldowns: dict[tuple[int, str], float] = {}


def db_conn() -> sqlite3.Connection:
    global _db_conn
    if _db_conn is None:
        _db_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _db_conn.row_factory = sqlite3.Row
        _db_conn.execute("PRAGMA journal_mode=WAL")
        _db_conn.execute("PRAGMA synchronous=NORMAL")
        _db_conn.commit()
    return _db_conn


def db_close() -> None:
    global _db_conn
    if _db_conn is not None:
        _db_conn.close()
        _db_conn = None


def db_execute(
    query: str,
    params: tuple = (),
    *,
    fetchone: bool = False,
    fetchall: bool = False,
) -> Optional[sqlite3.Row | list[sqlite3.Row]]:
    with db_lock:
        conn = db_conn()
        cursor = conn.execute(query, params)
        if fetchone:
            return cursor.fetchone()
        if fetchall:
            return cursor.fetchall()
        conn.commit()
        return None


def init_db() -> None:
    db_execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'idle',
            partner_id INTEGER NULL,
            waiting_since INTEGER NULL,
            reports_received INTEGER NOT NULL DEFAULT 0,
            reports_sent INTEGER NOT NULL DEFAULT 0,
            warnings INTEGER NOT NULL DEFAULT 0,
            is_banned INTEGER NOT NULL DEFAULT 0,
            ban_reason TEXT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    columns = db_execute("PRAGMA table_info(users)", fetchall=True) or []
    column_names = {row["name"] for row in columns}
    if "waiting_since" not in column_names:
        db_execute("ALTER TABLE users ADD COLUMN waiting_since INTEGER NULL")


def ensure_user(user_id: int) -> sqlite3.Row:
    db_execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
    row = db_execute("SELECT * FROM users WHERE user_id = ?", (user_id,), fetchone=True)
    return row


def get_user(user_id: int) -> Optional[sqlite3.Row]:
    return db_execute("SELECT * FROM users WHERE user_id = ?", (user_id,), fetchone=True)


def update_user(user_id: int, **fields) -> None:
    if not fields:
        return
    keys = ", ".join(f"{k} = ?" for k in fields.keys())
    values = list(fields.values()) + [user_id]
    db_execute(f"UPDATE users SET {keys}, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?", tuple(values))


def pop_valid_partner(exclude_user_id: int) -> Optional[int]:
    row = db_execute(
        """
        SELECT user_id
        FROM users
        WHERE user_id != ?
          AND status = 'searching'
          AND partner_id IS NULL
          AND waiting_since IS NOT NULL
          AND is_banned = 0
        ORDER BY waiting_since ASC
        LIMIT 1
        """,
        (exclude_user_id,),
        fetchone=True,
    )
    if not row:
        return None
    return int(row["user_id"])


def contains_bad_word(text: str) -> bool:
    for word in BAD_WORDS:
        pattern = rf"\b{re.escape(word)}\b"
        if re.search(pattern, text, re.IGNORECASE):
            return True
    return False


def on_cooldown(user_id: int, action: str) -> tuple[bool, float]:
    now = time.monotonic()
    key = (user_id, action)
    last = user_cooldowns.get(key, 0.0)
    delta = now - last
    if delta < COMMAND_COOLDOWN_SECONDS:
        return True, COMMAND_COOLDOWN_SECONDS - delta
    user_cooldowns[key] = now
    return False, 0.0


async def safe_send(application: Application, user_id: int, text: str, *, with_keyboard: bool = True) -> None:
    try:
        keyboard = ADMIN_KEYBOARD if ADMIN_ID is not None and user_id == ADMIN_ID else USER_KEYBOARD
        kwargs = {"reply_markup": keyboard} if with_keyboard else {}
        await application.bot.send_message(chat_id=user_id, text=text, **kwargs)
    except TelegramError:
        logging.warning("Failed to send message to %s", user_id)


async def notify_admin(application: Application, text: str) -> None:
    if ADMIN_ID is None:
        return
    await safe_send(application, ADMIN_ID, text, with_keyboard=False)


async def start_find_flow(application: Application, user_id: int) -> None:
    async with state_lock:
        user = ensure_user(user_id)

        if user["is_banned"] == 1 or user["status"] == "banned":
            await safe_send(application, user_id, "You are banned from this bot.")
            return

        if user["status"] == "chatting" and user["partner_id"] is not None:
            await safe_send(application, user_id, "You are already chatting. Use /next or /stop.")
            return

        update_user(user_id, status="searching", partner_id=None, waiting_since=int(time.time()))

        partner_id = pop_valid_partner(user_id)
        if partner_id is None:
            await safe_send(application, user_id, "Searching for a stranger...")
            return

        update_user(user_id, status="chatting", partner_id=partner_id, waiting_since=None)
        update_user(partner_id, status="chatting", partner_id=user_id, waiting_since=None)

    await safe_send(application, user_id, "Connected with a stranger. Say hi!")
    await safe_send(application, partner_id, "Connected with a stranger. Say hi!")


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    ensure_user(user_id)
    if ADMIN_ID is not None and user_id == ADMIN_ID:
        await safe_send(
            context.application,
            user_id,
            "✅ Admin access granted\nUse 📊 Admin Dashboard to view live bot stats.",
        )
        return
    await safe_send(context.application, user_id, "👋 Welcome. Use /find to chat anonymously.")


async def find_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    blocked, wait_left = on_cooldown(user_id, "find")
    if blocked:
        await safe_send(context.application, user_id, f"Please wait {wait_left:.1f}s before /find again.")
        return
    await start_find_flow(context.application, user_id)


async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    partner_id = None

    async with state_lock:
        user = ensure_user(user_id)
        partner_id = user["partner_id"]
        update_user(user_id, status="idle", partner_id=None, waiting_since=None)

        if partner_id is not None:
            partner = get_user(partner_id)
            if partner and partner["partner_id"] == user_id:
                update_user(partner_id, status="idle", partner_id=None, waiting_since=None)

    await safe_send(context.application, user_id, "Chat stopped. Use /find to search again.")
    if partner_id is not None:
        await safe_send(context.application, partner_id, "Stranger left the chat.")


async def next_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    blocked, wait_left = on_cooldown(user_id, "next")
    if blocked:
        await safe_send(context.application, user_id, f"Please wait {wait_left:.1f}s before /next again.")
        return

    previous_partner_id = None
    new_partner_id = None

    async with state_lock:
        user = ensure_user(user_id)
        previous_partner_id = user["partner_id"]

        if previous_partner_id is not None:
            partner = get_user(previous_partner_id)
            if partner and partner["partner_id"] == user_id:
                update_user(previous_partner_id, status="idle", partner_id=None, waiting_since=None)

        update_user(user_id, status="searching", partner_id=None, waiting_since=int(time.time()))
        new_partner_id = pop_valid_partner(user_id)
        if new_partner_id is not None:
            update_user(user_id, status="chatting", partner_id=new_partner_id, waiting_since=None)
            update_user(new_partner_id, status="chatting", partner_id=user_id, waiting_since=None)

    if previous_partner_id is not None:
        await safe_send(context.application, previous_partner_id, "Stranger skipped the chat.")

    if new_partner_id is None:
        await safe_send(context.application, user_id, "Searching for a new stranger...")
        return

    await safe_send(context.application, user_id, "Connected with a new stranger.")
    await safe_send(context.application, new_partner_id, "Connected with a stranger. Say hi!")


async def report_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    blocked, wait_left = on_cooldown(user_id, "report")
    if blocked:
        await safe_send(context.application, user_id, f"Please wait {wait_left:.1f}s before /report again.")
        return

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
            update_user(
                partner_id,
                reports_received=reports_received,
                status="banned",
                partner_id=None,
                waiting_since=None,
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
                waiting_since=None,
            )

        update_user(user_id, status="idle", partner_id=None, waiting_since=None)

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
        await notify_admin(
            context.application,
            f"User {partner_id} was banned by reports ({reports_received}). Reporter: {user_id}",
        )
    else:
        await safe_send(
            context.application,
            user_id,
            f"Report submitted. Chat ended. User now has {reports_received} report(s).",
        )
        await safe_send(context.application, partner_id, "You were reported. Chat ended.")


async def admin_stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if ADMIN_ID is None or user_id != ADMIN_ID:
        await safe_send(context.application, user_id, "Unauthorized.")
        return

    chatting = db_execute("SELECT COUNT(*) AS c FROM users WHERE status = 'chatting'", fetchone=True)["c"]
    searching = db_execute("SELECT COUNT(*) AS c FROM users WHERE status = 'searching'", fetchone=True)["c"]
    banned = db_execute("SELECT COUNT(*) AS c FROM users WHERE is_banned = 1", fetchone=True)["c"]
    total = db_execute("SELECT COUNT(*) AS c FROM users", fetchone=True)["c"]

    await safe_send(
        context.application,
        user_id,
        (
            "📊 Admin Dashboard\n"
            f"👥 Total users: {total}\n"
            f"💬 Chatting users: {chatting}\n"
            f"🔎 Searching users: {searching}\n"
            f"⛔ Banned users: {banned}"
        ),
    )


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    user_id = update.effective_user.id
    text = (update.message.text or "").strip()
    if not text:
        return

    if text == "🔎 Find Stranger" or text == "Find Stranger":
        await start_find_flow(context.application, user_id)
        return
    if text == "⏭️ Next" or text == "Next":
        await next_cmd(update, context)
        return
    if text == "🛑 Stop" or text == "Stop":
        await stop_cmd(update, context)
        return
    if text == "🚩 Report" or text == "Report":
        await report_cmd(update, context)
        return
    if text == ADMIN_DASHBOARD_BUTTON:
        await admin_stats_cmd(update, context)
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
            update_user(user_id, status="idle", partner_id=None, waiting_since=None)
            await safe_send(context.application, user_id, "Previous chat expired. Use /find again.")
            return

        if contains_bad_word(text):
            moderation_block = True
            warnings = int(user["warnings"]) + 1
            warnings_left = max(MAX_WARNINGS - warnings, 0)
            if warnings >= MAX_WARNINGS:
                update_user(
                    user_id,
                    warnings=warnings,
                    status="banned",
                    partner_id=None,
                    waiting_since=None,
                    is_banned=1,
                    ban_reason="Bad language",
                )
                update_user(partner_id, status="idle", partner_id=None, waiting_since=None)
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
        await notify_admin(
            context.application,
            f"User {user_id} was auto-banned by moderation. Partner at event: {partner_id}",
        )
        return

    try:
        await context.bot.send_chat_action(chat_id=partner_id, action=ChatAction.TYPING)
    except TelegramError:
        pass
    await safe_send(context.application, partner_id, f"Stranger: {text}", with_keyboard=False)


async def media_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    user_id = update.effective_user.id
    partner_id = None

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
            update_user(user_id, status="idle", partner_id=None, waiting_since=None)
            await safe_send(context.application, user_id, "Previous chat expired. Use /find again.")
            return

    try:
        await context.bot.send_chat_action(chat_id=partner_id, action=ChatAction.TYPING)
    except TelegramError:
        pass

    try:
        await context.bot.copy_message(
            chat_id=partner_id,
            from_chat_id=user_id,
            message_id=update.message.message_id,
        )
    except TelegramError:
        await safe_send(context.application, user_id, "Could not forward that media type.")


def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("find", find_cmd))
    app.add_handler(CommandHandler("stop", stop_cmd))
    app.add_handler(CommandHandler("next", next_cmd))
    app.add_handler(CommandHandler("report", report_cmd))
    app.add_handler(CommandHandler("admin_stats", admin_stats_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_handler(MessageHandler(filters.ALL & ~filters.TEXT & ~filters.COMMAND, media_handler))

    return app


def main() -> None:
    init_db()
    app = build_app()
    logging.info("Anonymous one-file bot is running...")
    try:
        app.run_polling(
            allowed_updates=Update.ALL_TYPES,
            stop_signals=[signal.SIGINT, signal.SIGTERM],
        )
    finally:
        db_close()


if __name__ == "__main__":
    main()
