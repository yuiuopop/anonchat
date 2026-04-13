import asyncio
import logging
import os
import re
import signal
import sqlite3
import time
from threading import Lock
from typing import Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update
from telegram.constants import ChatAction
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatJoinRequestHandler,
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
COMMAND_COOLDOWN_SECONDS = float(os.getenv("COMMAND_COOLDOWN_SECONDS", "3"))
BAD_WORDS = [w.strip().lower() for w in os.getenv("BAD_WORDS", "abuse,slur,badword").split(",") if w.strip()]
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "").strip()
ADMIN_ID = int(ADMIN_ID_RAW) if ADMIN_ID_RAW.isdigit() else None

BTN_FIND = "🔎 Find Stranger"
BTN_NEXT = "⏭️ Next"
BTN_STOP = "🛑 Stop"
BTN_REPORT = "🚩 Report"
BTN_ADMIN_PANEL = "🛠️ Admin Panel"
BTN_SET_START = "✏️ Set Start Message"
BTN_TOGGLE_FIREWALL = "🧱 Toggle Firewall"
BTN_SET_GROUP_LINK = "🔗 Link"
BTN_SET_FW_MSG = "💬 Set Firewall Message"
BTN_ADMIN_CANCEL = "❌ Cancel Admin Edit"
BTN_SEX_MALE = "♂️ Male"
BTN_SEX_FEMALE = "♀️ Female"

USER_KEYBOARD = ReplyKeyboardMarkup([[BTN_FIND, BTN_NEXT], [BTN_STOP, BTN_REPORT]], resize_keyboard=True)
SEXUALITY_KEYBOARD = ReplyKeyboardMarkup([[BTN_SEX_MALE, BTN_SEX_FEMALE]], resize_keyboard=True)
ADMIN_KEYBOARD = ReplyKeyboardMarkup(
    [
        [BTN_ADMIN_PANEL],
        [BTN_FIND, BTN_NEXT],
        [BTN_STOP, BTN_REPORT],
    ],
    resize_keyboard=True,
)

SETTING_START_MESSAGE = "start_message"
SETTING_FIREWALL_ENABLED = "firewall_enabled"
SETTING_FIREWALL_GROUP = "firewall_group"
SETTING_FIREWALL_GROUP_LINK = "firewall_group_link"
SETTING_FIREWALL_MESSAGE = "firewall_message"

DEFAULT_START_MESSAGE = "👋 Welcome to Anonymous Chat Bot.\nUse /find to connect with a stranger."
DEFAULT_FIREWALL_MESSAGE = "🧱 Access locked. Join {group} and try again."

CB_ADMIN_STATS = "admin:stats"
CB_ADMIN_SET_START = "admin:set_start"
CB_ADMIN_TOGGLE_FW = "admin:toggle_fw"
CB_ADMIN_SET_LINK = "admin:set_link"
CB_ADMIN_SET_FW_MSG = "admin:set_fw_msg"
CB_ADMIN_CANCEL = "admin:cancel"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

state_lock = asyncio.Lock()
db_lock = Lock()
_db_conn: Optional[sqlite3.Connection] = None
admin_pending_action: Optional[str] = None
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
            sexuality TEXT NULL,
            reports_received INTEGER NOT NULL DEFAULT 0,
            reports_sent INTEGER NOT NULL DEFAULT 0,
            warnings INTEGER NOT NULL DEFAULT 0,
            is_banned INTEGER NOT NULL DEFAULT 0,
            ban_reason TEXT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    db_execute(
        """
        CREATE TABLE IF NOT EXISTS bot_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    db_execute(
        """
        CREATE TABLE IF NOT EXISTS join_requests (
            user_id INTEGER NOT NULL,
            group_id TEXT NOT NULL,
            requested_at INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            PRIMARY KEY (user_id, group_id)
        )
        """
    )

    columns = db_execute("PRAGMA table_info(users)", fetchall=True) or []
    column_names = {row["name"] for row in columns}
    if "waiting_since" not in column_names:
        db_execute("ALTER TABLE users ADD COLUMN waiting_since INTEGER NULL")
    if "sexuality" not in column_names:
        db_execute("ALTER TABLE users ADD COLUMN sexuality TEXT NULL")

    set_setting_if_missing(SETTING_START_MESSAGE, DEFAULT_START_MESSAGE)
    set_setting_if_missing(SETTING_FIREWALL_ENABLED, "0")
    set_setting_if_missing(SETTING_FIREWALL_GROUP, "")
    set_setting_if_missing(SETTING_FIREWALL_GROUP_LINK, "")
    set_setting_if_missing(SETTING_FIREWALL_MESSAGE, DEFAULT_FIREWALL_MESSAGE)


def set_setting_if_missing(key: str, value: str) -> None:
    db_execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES (?, ?)", (key, value))


def get_setting(key: str, default: str = "") -> str:
    row = db_execute("SELECT value FROM bot_settings WHERE key = ?", (key,), fetchone=True)
    if not row:
        return default
    return str(row["value"])


def set_setting(key: str, value: str) -> None:
    db_execute(
        """
        INSERT INTO bot_settings (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )


def ensure_user(user_id: int) -> sqlite3.Row:
    db_execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
    return db_execute("SELECT * FROM users WHERE user_id = ?", (user_id,), fetchone=True)


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
    if now - last < COMMAND_COOLDOWN_SECONDS:
        return True, COMMAND_COOLDOWN_SECONDS - (now - last)
    user_cooldowns[key] = now
    return False, 0.0


def is_admin(user_id: int) -> bool:
    return ADMIN_ID is not None and user_id == ADMIN_ID


def keyboard_for_user(user_id: int, require_sexuality: bool = False) -> ReplyKeyboardMarkup:
    if require_sexuality:
        return SEXUALITY_KEYBOARD
    if is_admin(user_id):
        return ADMIN_KEYBOARD
    return USER_KEYBOARD


def admin_panel_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📊 Refresh Stats", callback_data=CB_ADMIN_STATS)],
            [
                InlineKeyboardButton(BTN_SET_START, callback_data=CB_ADMIN_SET_START),
                InlineKeyboardButton(BTN_TOGGLE_FIREWALL, callback_data=CB_ADMIN_TOGGLE_FW),
            ],
            [
                InlineKeyboardButton(BTN_SET_GROUP_LINK, callback_data=CB_ADMIN_SET_LINK),
                InlineKeyboardButton(BTN_SET_FW_MSG, callback_data=CB_ADMIN_SET_FW_MSG),
            ],
            [InlineKeyboardButton(BTN_ADMIN_CANCEL, callback_data=CB_ADMIN_CANCEL)],
        ]
    )


async def safe_send(
    application: Application,
    user_id: int,
    text: str,
    *,
    with_keyboard: bool = True,
    require_sexuality: bool = False,
    inline_markup: Optional[InlineKeyboardMarkup] = None,
) -> None:
    try:
        kwargs = {}
        if with_keyboard:
            kwargs["reply_markup"] = keyboard_for_user(user_id, require_sexuality=require_sexuality)
        if inline_markup is not None:
            kwargs["reply_markup"] = inline_markup
        await application.bot.send_message(chat_id=user_id, text=text, **kwargs)
    except TelegramError:
        logging.warning("Failed to send message to %s", user_id)


async def notify_admin(application: Application, text: str) -> None:
    if ADMIN_ID is None:
        return
    await safe_send(application, ADMIN_ID, text, with_keyboard=False)


async def check_firewall_access(application: Application, user_id: int) -> bool:
    if is_admin(user_id):
        return True

    if get_setting(SETTING_FIREWALL_ENABLED, "0") != "1":
        return True

    group_id = get_setting(SETTING_FIREWALL_GROUP, "").strip()
    if not group_id:
        await safe_send(application, user_id, "🧱 Firewall is enabled but group is not configured.")
        return False

    is_member = False
    try:
        member = await application.bot.get_chat_member(chat_id=group_id, user_id=user_id)
        if member.status in {"creator", "administrator", "member", "restricted"}:
            is_member = True
    except TelegramError:
        is_member = False

    if is_member:
        db_execute(
            "UPDATE join_requests SET status = 'approved' WHERE user_id = ? AND group_id = ?",
            (user_id, group_id),
        )
        return True

    pending = db_execute(
        """
        SELECT 1 FROM join_requests
        WHERE user_id = ? AND group_id = ? AND status = 'pending'
        LIMIT 1
        """,
        (user_id, group_id),
        fetchone=True,
    )
    if pending:
        return True

    firewall_msg = get_setting(SETTING_FIREWALL_MESSAGE, DEFAULT_FIREWALL_MESSAGE)
    final_msg = firewall_msg.replace("{group}", group_id)
    group_link = get_setting(SETTING_FIREWALL_GROUP_LINK, "").strip()
    if group_link:
        await safe_send(
            application,
            user_id,
            final_msg,
            inline_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔗 Join Firewall Group", url=group_link)]]
            ),
            with_keyboard=False,
        )
    else:
        await safe_send(application, user_id, final_msg)
    return False


def admin_stats_text() -> str:
    total = db_execute("SELECT COUNT(*) AS c FROM users", fetchone=True)["c"]
    active = db_execute(
        "SELECT COUNT(*) AS c FROM users WHERE status IN ('searching', 'chatting')",
        fetchone=True,
    )["c"]
    inactive = total - active
    male = db_execute(
        "SELECT COUNT(*) AS c FROM users WHERE lower(COALESCE(sexuality, '')) = 'male'",
        fetchone=True,
    )["c"]
    female = db_execute(
        "SELECT COUNT(*) AS c FROM users WHERE lower(COALESCE(sexuality, '')) = 'female'",
        fetchone=True,
    )["c"]
    fw_enabled = "ON" if get_setting(SETTING_FIREWALL_ENABLED, "0") == "1" else "OFF"
    fw_group = get_setting(SETTING_FIREWALL_GROUP, "") or "Not set"
    fw_group_link = get_setting(SETTING_FIREWALL_GROUP_LINK, "") or "Not set"

    return (
        "🛠️ Admin Dashboard\n"
        f"👥 Total users: {total}\n"
        f"🟢 Active users: {active}\n"
        f"⚪ Inactive users: {inactive}\n"
        f"♂️ Male users: {male}\n"
        f"♀️ Female users: {female}\n"
        f"🧱 Firewall: {fw_enabled}\n"
        f"🎯 Firewall Group: {fw_group}\n"
        f"🔗 Firewall Link: {fw_group_link}"
    )


async def send_admin_panel(application: Application, user_id: int, heading: Optional[str] = None) -> None:
    text = admin_stats_text() if not heading else f"{heading}\n\n{admin_stats_text()}"
    await safe_send(
        application,
        user_id,
        text,
        with_keyboard=False,
        inline_markup=admin_panel_markup(),
    )


async def start_find_flow(application: Application, user_id: int) -> None:
    async with state_lock:
        user = ensure_user(user_id)
        if user["is_banned"] == 1 or user["status"] == "banned":
            await safe_send(application, user_id, "⛔ You are banned from this bot.")
            return

        if user["status"] == "chatting" and user["partner_id"] is not None:
            await safe_send(application, user_id, "You are already chatting. Use /next or /stop.")
            return

        update_user(user_id, status="searching", partner_id=None, waiting_since=int(time.time()))

        partner_id = pop_valid_partner(user_id)
        if partner_id is None:
            await safe_send(application, user_id, "🔎 Searching for a stranger...")
            return

        update_user(user_id, status="chatting", partner_id=partner_id, waiting_since=None)
        update_user(partner_id, status="chatting", partner_id=user_id, waiting_since=None)

    await safe_send(application, user_id, "✅ Connected with a stranger. Say hi!")
    await safe_send(application, partner_id, "✅ Connected with a stranger. Say hi!")


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    user = ensure_user(user_id)

    if not await check_firewall_access(context.application, user_id):
        return

    if is_admin(user_id):
        await safe_send(
            context.application,
            user_id,
            "✅ Admin access granted\nTap 🛠️ Admin Panel to open dashboard.",
        )
        return

    if not user["sexuality"]:
        await safe_send(
            context.application,
            user_id,
            "Please select your sexuality to continue:",
            require_sexuality=True,
        )
        return

    await safe_send(context.application, user_id, get_setting(SETTING_START_MESSAGE, DEFAULT_START_MESSAGE))


async def find_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    blocked, wait_left = on_cooldown(user_id, "find")
    if blocked:
        await safe_send(context.application, user_id, f"Please wait {wait_left:.1f}s before /find again.")
        return

    user = ensure_user(user_id)
    if not await check_firewall_access(context.application, user_id):
        return
    if not user["sexuality"]:
        await safe_send(context.application, user_id, "Please select sexuality first:", require_sexuality=True)
        return
    await start_find_flow(context.application, user_id)


async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not await check_firewall_access(context.application, user_id):
        return

    partner_id = None
    async with state_lock:
        user = ensure_user(user_id)
        partner_id = user["partner_id"]
        update_user(user_id, status="idle", partner_id=None, waiting_since=None)

        if partner_id is not None:
            partner = get_user(partner_id)
            if partner and partner["partner_id"] == user_id:
                update_user(partner_id, status="idle", partner_id=None, waiting_since=None)

    await safe_send(context.application, user_id, "🛑 Chat stopped. Use /find to search again.")
    if partner_id is not None:
        await safe_send(context.application, partner_id, "Stranger left the chat.")


async def next_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    blocked, wait_left = on_cooldown(user_id, "next")
    if blocked:
        await safe_send(context.application, user_id, f"Please wait {wait_left:.1f}s before /next again.")
        return
    if not await check_firewall_access(context.application, user_id):
        return

    user = ensure_user(user_id)
    if not user["sexuality"]:
        await safe_send(context.application, user_id, "Please select sexuality first:", require_sexuality=True)
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
        await safe_send(context.application, previous_partner_id, "⏭️ Stranger skipped the chat.")

    if new_partner_id is None:
        await safe_send(context.application, user_id, "🔎 Searching for a new stranger...")
        return

    await safe_send(context.application, user_id, "✅ Connected with a new stranger.")
    await safe_send(context.application, new_partner_id, "✅ Connected with a stranger. Say hi!")


async def report_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    blocked, wait_left = on_cooldown(user_id, "report")
    if blocked:
        await safe_send(context.application, user_id, f"Please wait {wait_left:.1f}s before /report again.")
        return
    if not await check_firewall_access(context.application, user_id):
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
            update_user(user_id, status="idle", partner_id=None, waiting_since=None)
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
        await safe_send(context.application, user_id, f"Report submitted. User banned at {reports_received} reports.")
        await safe_send(context.application, partner_id, "You have been banned due to repeated reports.")
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
    if not is_admin(user_id):
        await safe_send(context.application, user_id, "Unauthorized.")
        return
    await send_admin_panel(context.application, user_id)


async def chat_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.chat_join_request:
        return
    request = update.chat_join_request
    db_execute(
        """
        INSERT INTO join_requests (user_id, group_id, requested_at, status)
        VALUES (?, ?, ?, 'pending')
        ON CONFLICT(user_id, group_id)
        DO UPDATE SET requested_at = excluded.requested_at, status = 'pending'
        """,
        (request.from_user.id, str(request.chat.id), int(time.time())),
    )


async def handle_admin_pending_text(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
) -> bool:
    global admin_pending_action
    user_id = update.effective_user.id
    if not is_admin(user_id) or not admin_pending_action:
        return False

    if text == BTN_ADMIN_CANCEL:
        admin_pending_action = None
        await safe_send(context.application, user_id, "Admin edit cancelled.")
        return True

    if text in {
        BTN_ADMIN_PANEL,
        BTN_SET_START,
        BTN_TOGGLE_FIREWALL,
        BTN_SET_GROUP_LINK,
        BTN_SET_FW_MSG,
    }:
        return False

    if admin_pending_action == "set_start_message":
        set_setting(SETTING_START_MESSAGE, text)
        admin_pending_action = None
        await safe_send(context.application, user_id, "✅ Start message updated.")
        return True

    if admin_pending_action == "set_firewall_group":
        raw = text.strip()
        group_for_check = raw
        group_link = ""

        if raw.startswith("https://t.me/") or raw.startswith("http://t.me/"):
            username = raw.split("t.me/", 1)[1].strip().strip("/")
            if username:
                group_for_check = f"@{username}"
                group_link = f"https://t.me/{username}"
        elif raw.startswith("t.me/"):
            username = raw.split("t.me/", 1)[1].strip().strip("/")
            if username:
                group_for_check = f"@{username}"
                group_link = f"https://t.me/{username}"
        elif raw.startswith("@"):
            username = raw[1:].strip()
            if username:
                group_for_check = f"@{username}"
                group_link = f"https://t.me/{username}"

        set_setting(SETTING_FIREWALL_GROUP, group_for_check)
        set_setting(SETTING_FIREWALL_GROUP_LINK, group_link)
        admin_pending_action = None
        link_text = group_link if group_link else "Not available (set @username or t.me link for button)"
        await safe_send(
            context.application,
            user_id,
            f"✅ Firewall group updated to: {group_for_check}\n🔗 Button link: {link_text}",
        )
        return True

    if admin_pending_action == "set_firewall_message":
        set_setting(SETTING_FIREWALL_MESSAGE, text)
        admin_pending_action = None
        await safe_send(context.application, user_id, "✅ Firewall message updated.")
        return True

    return False


async def admin_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global admin_pending_action

    query = update.callback_query
    if not query:
        return
    await query.answer()

    user_id = query.from_user.id
    if not is_admin(user_id):
        return

    data = query.data or ""

    if data == CB_ADMIN_STATS:
        await query.edit_message_text(admin_stats_text(), reply_markup=admin_panel_markup())
        return

    if data == CB_ADMIN_SET_START:
        admin_pending_action = "set_start_message"
        await safe_send(context.application, user_id, "Send new start message text now:")
        return

    if data == CB_ADMIN_TOGGLE_FW:
        enabled = get_setting(SETTING_FIREWALL_ENABLED, "0") == "1"
        set_setting(SETTING_FIREWALL_ENABLED, "0" if enabled else "1")
        await query.edit_message_text(
            f"✅ Firewall is now {'OFF' if enabled else 'ON'}.\n\n{admin_stats_text()}",
            reply_markup=admin_panel_markup(),
        )
        return

    if data == CB_ADMIN_SET_LINK:
        admin_pending_action = "set_firewall_group"
        await safe_send(
            context.application,
            user_id,
            "Send firewall group link now (example: https://t.me/yourgroup or t.me/yourgroup).",
        )
        return

    if data == CB_ADMIN_SET_FW_MSG:
        admin_pending_action = "set_firewall_message"
        await safe_send(
            context.application,
            user_id,
            "Send new firewall message now. Use {group} where group should appear.",
        )
        return

    if data == CB_ADMIN_CANCEL:
        admin_pending_action = None
        await safe_send(context.application, user_id, "Admin edit cancelled.")
        return


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global admin_pending_action

    if not update.message:
        return

    user_id = update.effective_user.id
    text = (update.message.text or "").strip()
    if not text:
        return

    ensure_user(user_id)

    if await handle_admin_pending_text(update, context, text):
        return

    if text in {BTN_SEX_MALE, BTN_SEX_FEMALE}:
        sexuality_value = "male" if text == BTN_SEX_MALE else "female"
        update_user(user_id, sexuality=sexuality_value)
        await safe_send(context.application, user_id, f"✅ Saved sexuality: {sexuality_value.title()}")
        return

    if is_admin(user_id):
        if text == BTN_ADMIN_PANEL:
            await send_admin_panel(
                context.application,
                user_id,
                heading="✅ Admin access granted\nOpening admin dashboard...",
            )
            return

    if text == BTN_FIND or text == "Find Stranger":
        await find_cmd(update, context)
        return
    if text == BTN_NEXT or text == "Next":
        await next_cmd(update, context)
        return
    if text == BTN_STOP or text == "Stop":
        await stop_cmd(update, context)
        return
    if text == BTN_REPORT or text == "Report":
        await report_cmd(update, context)
        return

    if not await check_firewall_access(context.application, user_id):
        return

    user = ensure_user(user_id)
    if not user["sexuality"]:
        await safe_send(context.application, user_id, "Please select sexuality first:", require_sexuality=True)
        return

    partner_id = None
    moderation_block = False
    banned_now = False
    warnings_left = 0

    async with state_lock:
        user = ensure_user(user_id)
        if user["is_banned"] == 1 or user["status"] == "banned":
            await safe_send(context.application, user_id, "⛔ You are banned from this bot.")
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
        await safe_send(context.application, user_id, f"Message blocked by moderation. Warnings left: {warnings_left}.")
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
    ensure_user(user_id)

    if not await check_firewall_access(context.application, user_id):
        return

    user = ensure_user(user_id)
    if not user["sexuality"]:
        await safe_send(context.application, user_id, "Please select sexuality first:", require_sexuality=True)
        return

    partner_id = None

    async with state_lock:
        user = ensure_user(user_id)
        if user["is_banned"] == 1 or user["status"] == "banned":
            await safe_send(context.application, user_id, "⛔ You are banned from this bot.")
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
    app.add_handler(CallbackQueryHandler(admin_callback_handler, pattern=r"^admin:"))
    app.add_handler(ChatJoinRequestHandler(chat_join_request))
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
