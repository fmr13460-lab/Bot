"""
بوت وساطة (إسكرو) لبيع وشراء حسابات الألعاب عبر تيليجرام
-----------------------------------------------------------
هذا البوت لا يعالج الدفع الفعلي، فقط يتابع حالة الصفقة خطوة بخطوة.
الدفع الحقيقي (PayPal / تحويل / عملة رقمية) يتم خارج البوت مباشرة مع الأدمن.

واجهة المستخدم كلها تعمل ضمن رسالة واحدة تتحدّث في مكانها (بدل إرسال
رسائل جديدة في كل مرة)، مع زر "رجوع" للتنقل بين الشاشات.

الإعداد:
1. pip install "python-telegram-bot[job-queue]" requests python-dotenv --upgrade
2. احصل على توكن من @BotFather
3. انسخ .env.example إلى .env واملأ فيه: BOT_TOKEN, OWNER_ID, WALLET_ADDRESS
   (ADMIN_IDS اختياري — قائمة أدمن ابتدائية عند أول تشغيل فقط؛ بعد ذلك
   استخدم أوامر /addadmin و /removeadmin داخل البوت لإدارة الأدمن ديناميكياً
   بدون تعديل أي كود أو إعادة تشغيل).
   اختياري أيضاً: ADMIN_GROUP_ID, FEE_FLAT (افتراضي $0.10)، FEE_PERCENT (افتراضي 0)، DB_PATH، PAYMENT_TIMEOUT_HOURS (افتراضي 24، 0 لتعطيل الإلغاء التلقائي)،
   BSC_RPC_URL (اختياري، افتراضياً https://bsc-dataseed.binance.org/؛ عقدة RPC عامة مجانية بالكامل — لا تحتاج مفتاح أو اشتراك — يستخدمها زر "فحص المحفظة" لعرض تحويلات USDT الواردة على الأدمن؛ فحص مساعد فقط، لا يؤكد الدفع تلقائياً)
4. شغّل: python bot.py

إدارة الأدمن:
- /addadmin <user_id أو @username> — يمنح صلاحية أدمن فورية (المالك فقط).
- /removeadmin <user_id أو @username> — يسحب الصلاحية فوراً (المالك فقط).
- /admins — يعرض قائمة الأدمن الحاليين (لأي أدمن).
- OWNER_ID دائماً أدمن تلقائياً ولا يمكن إزالته بهذه الأوامر.
"""

import os
import re
import sys
import html
import sqlite3
import contextlib
import logging
import logging.handlers
import asyncio
import time
from datetime import datetime, timezone

import requests


def _ensure_env_file():
    """ينشئ ملف .env تلقائياً بجانب bot.py إن لم يكن موجوداً بعد، ويطبع
    تعليمات واضحة في الطرفية بدل ترك المستخدم يبحث بنفسه عن الحل أو ينشئه
    يدوياً. يوقف البرنامج بعد الإنشاء حتى يعبّي المستخدم القيم الحقيقية أولاً."""
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        return  # الملف موجود أصلاً — لا شيء نفعله هنا، الفحص التفصيلي في main()

    template = (
        "# تم إنشاء هذا الملف تلقائياً بواسطة bot.py — عبّئ القيم الحقيقية بالأسفل\n"
        "# ثم شغّل البوت مرة أخرى. لا تشارك هذا الملف مع أحد ولا ترفعه لأي مكان عام.\n"
        "\n"
        "BOT_TOKEN=\n"
        "\n"
        "# رقمك على تيليجرام (من @userinfobot) — أنت مالك البوت، والوحيد المسموح\n"
        "# له باستخدام /addadmin و /removeadmin لاحقاً داخل البوت.\n"
        "OWNER_ID=\n"
        "\n"
        "# أدمن ابتدائي (bootstrap) — اجعله نفس رقم OWNER_ID عادةً. يُستخدم مرة\n"
        "# واحدة فقط عند أول تشغيل؛ بعدها إدارة الأدمن تتم بالكامل عبر\n"
        "# /addadmin و /removeadmin داخل البوت.\n"
        "ADMIN_IDS=\n"
        "\n"
        "# اختياري — اتركه فارغاً أو 0 إن كنت لا تستخدم مجموعة إدارة منفصلة\n"
        "ADMIN_GROUP_ID=0\n"
        "\n"
        "# عنوان محفظتك (BEP20) التي يستقبل عليها المشترون مبلغ الدفع\n"
        "WALLET_ADDRESS=\n"
        "\n"
        "# اختياري — قيم افتراضية معقولة، عدّلها إن أردت\n"
        "FEE_FLAT=0.10\n"
        "FEE_PERCENT=0.0\n"
        "PAYMENT_TIMEOUT_HOURS=24\n"
        "BSC_RPC_URL=https://bsc-dataseed.binance.org/\n"
        "DB_PATH=escrow.db\n"
        "PERSISTENCE_PATH=bot_persistence.pkl\n"
    )

    try:
        with open(env_path, "w", encoding="utf-8") as f:
            f.write(template)
    except OSError as e:
        # لو تعذّر الإنشاء (مثلاً مجلد للقراءة فقط)، نكمل عادي — الفحص في
        # main() سيطبع تعليمات الإنشاء اليدوي كما كان سابقاً.
        print(f"⚠️ تعذر إنشاء ملف .env تلقائياً ({e}). أنشئه يدوياً بجانب bot.py.")
        return

    print("=" * 62)
    print("📄 لم يكن هناك ملف .env، فتم إنشاء واحد جديد تلقائياً بجانب bot.py.")
    print("   افتحه وعبّئ القيم التالية على الأقل قبل تشغيل البوت مرة أخرى:")
    print()
    print("   BOT_TOKEN       → التوكن من @BotFather")
    print("   OWNER_ID        → رقم حسابك على تيليجرام (من @userinfobot)")
    print("   ADMIN_IDS       → نفس رقمك عادةً (أو عدة أرقام مفصولة بفاصلة)")
    print("   WALLET_ADDRESS  → عنوان محفظتك (BEP20) لاستقبال مدفوعات المشترين")
    print()
    print(f"   عدّله بالأمر:  nano {env_path}")
    print("   ثم شغّله مرة أخرى:  python bot.py")
    print("=" * 62)
    sys.exit(0)


_ensure_env_file()

try:
    from dotenv import load_dotenv
    load_dotenv()  # يقرأ ملف .env المجاور لهذا الملف تلقائياً إن وُجد
except ImportError:
    pass  # يعمل البوت طبيعياً حتى بدون python-dotenv، طالما ضبطت المتغيرات بطريقة أخرى

from telegram import (
    Update,
    BotCommand,
    BotCommandScopeChat,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    CallbackQueryHandler,
    TypeHandler,
    ContextTypes,
    PicklePersistence,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.handlers.RotatingFileHandler(
            os.environ.get("LOG_PATH", "bot.log"),
            maxBytes=5 * 1024 * 1024,  # 5MB لكل ملف
            backupCount=3,              # يحتفظ بـ 3 ملفات قديمة قبل الحذف
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)
# نقلل من ضجيج مكتبة httpx (تستخدمها python-telegram-bot داخلياً) في اللوج.
logging.getLogger("httpx").setLevel(logging.WARNING)

# ---------- الإعدادات ----------

# ============================================================
# ⚙️ إعدادات البوت — كل القيم الآن تُقرأ من ملف .env (لا تضع أسراراً هنا)
# ============================================================
def _parse_admin_ids_env(raw: str):
    ids = set()
    for part in (raw or "").split(","):
        part = part.strip()
        if part.isdigit() or (part.startswith("-") and part[1:].isdigit()):
            ids.add(int(part))
    return ids


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "")
    return int(raw) if raw.strip().lstrip("-").isdigit() else default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "")
    try:
        return float(raw) if raw.strip() else default
    except ValueError:
        return default


# 1) توكن البوت من @BotFather
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

# 2) قائمة أدمن ابتدائية (bootstrap) — تُستخدم مرة واحدة فقط عند أول تشغيل
#    لتعبئة جدول admins في قاعدة البيانات. بعد ذلك، إدارة الأدمن تتم بالكامل
#    عبر أوامر /addadmin و /removeadmin (المالك فقط) — وليس من هنا.
_BOOTSTRAP_ADMIN_IDS = _parse_admin_ids_env(os.environ.get("ADMIN_IDS", ""))

# 2.1) آيدي المالك (Owner) — الشخص الوحيد المسموح له بإضافة/إزالة أدمن.
#      يُفضّل أن يكون نفس رقمك أنت (من @userinfobot). اتركه 0 لتعطيل الأمر تماماً.
OWNER_ID = _env_int("OWNER_ID", 0)

# 3) آيدي مجموعة الإدارة (0 إذا لا تستخدم مجموعة)
ADMIN_GROUP_ID = _env_int("ADMIN_GROUP_ID", 0)

# 4) عنوان المحفظة الذي يستقبل الدفع
WALLET_ADDRESS = os.environ.get("WALLET_ADDRESS", "")

# 4.1) عقدة RPC عامة على BNB Smart Chain — مجانية بالكامل ولا تحتاج مفتاح
#      API أو اشتراك. تُستخدم لفحص معاملات USDT (BEP20) الواردة للمحفظة
#      أعلاه مباشرة من البلوكتشين. يمكن تغييرها لعقدة عامة أخرى إذا واجهت
#      بطئاً أو حدود استخدام (rate limit).
BSC_RPC_URL = os.environ.get("BSC_RPC_URL", "https://bsc-dataseed.binance.org/")

# عقد USDT الرسمي على BNB Smart Chain (BEP20) — لا حاجة لتعديله عادة.
USDT_BEP20_CONTRACT = "0x55d398326f99059fF775485246999027B3197955"
USDT_BEP20_DECIMALS = 18

BEP20_ADDRESS_PATTERN = re.compile(r"^0x[a-fA-F0-9]{40}$")


def is_valid_bep20_address(address: str) -> bool:
    """تحقق أساسي من شكل عنوان محفظة BNB Smart Chain (BEP20): 0x متبوعاً
    بـ40 خانة hex. لا يتحقق من صحة الـchecksum ولا من كون العنوان مسجّلاً
    فعلياً على البلوكتشين — هذا فحص شكلي فقط لمنع الأخطاء الواضحة."""
    return bool(BEP20_ADDRESS_PATTERN.match((address or "").strip()))

# 5) رسوم الوسيط: مبلغ ثابت بالدولار + نسبة مئوية اختيارية (تُجمع الاثنتان).
#    اترك FEE_PERCENT على 0 إذا كنت تريد رسم ثابت فقط دون أي نسبة إضافية.
FEE_FLAT = _env_float("FEE_FLAT", 0.10)
FEE_PERCENT = _env_float("FEE_PERCENT", 0.0)

# 6) مهلة الدفع بالساعات
PAYMENT_TIMEOUT_HOURS = _env_float("PAYMENT_TIMEOUT_HOURS", 24.0)

# 7) اسم/مسار ملف قاعدة البيانات — غالبًا لا تحتاج لتغييره
DB_PATH = os.environ.get("DB_PATH", "escrow.db")

# 8) ملف حفظ حالة الشاشات (أي رسالة هي "الشاشة الحالية" لكل مستخدم) بحيث
#    لا تُفقد عند إعادة تشغيل البوت — هذا يمنع ظهور رسائل شاشة مكرّرة/عالقة
#    بعد كل إعادة تشغيل. غالبًا لا تحتاج لتغييره.
PERSISTENCE_PATH = os.environ.get("PERSISTENCE_PATH", "bot_persistence.pkl")
# ============================================================
# ⚠️ لا تعدّل أي شيء تحت هذا السطر إلا إذا كنت تعرف البرمجة.
# ============================================================

GAME, LEVEL, PRICE, DESC, VIDEO, CREDS, WALLET, EDITPRICE = range(8)

SUPPORTED_GAMES = ["Clash of Clans/كلاش", "PUBG/ببجي", "Brawl Stars/براول", "Roblox/روبلوكس", "Bounty Rush/باونتي", "Pes/بيس"]

LINK_PATTERN = re.compile(
    r"(https?://|www\.|t\.me/|telegram\.me/|\.com|\.net|\.org|\.io|\.ru|\.xyz|\.gg\b)",
    re.IGNORECASE,
)


def contains_link(text: str) -> bool:
    return bool(LINK_PATTERN.search(text))


import base64
# الكلمات المسيئة مخفية عن القراءة المباشرة داخل الملف.
# هذه ليست تشفيراً أمنياً؛ الهدف إخفاؤها عن العرض العادي مع بقائها قابلة للتعديل هنا.
_BAD_WORDS_B64 = "ZnVjawpmdWNraW5nCnNoaXQKYnVsbHNoaXQKYml0Y2gKYml0Y2hlcwphc3Nob2xlCmFyc2Vob2xlCmJhc3RhcmQKZGljawpkaWNraGVhZApjdW50Cndob3JlCnNsdXQKbW90aGVyZnVja2VyCmZhZ2dvdApyZXRhcmQKbmlnZ2VyCnBvcm4KcG9ybm8KbnVkZQpudWRlcwpuYWtlZApuc2Z3Cnh4eApjb2NrCnB1c3N5CmJvb2JzCnBlbmlzCnZhZ2luYQphbmFsCm9yZ2FzbQptYXN0dXJiYXQKY3VtCmhvcm55CnNleApzZXh5CtmD2YTYqArYrdmK2YjYp9mGCtiu2LHYpwrYrtmG2LLZitixCtmC2K3YqNipCti02LHZhdmI2LfYqQrYstio2YoK2YXZhtmK2YMK2LnYsdi1CtmF2KrZhtin2YMK2YPYs9mF2YMK2KfYqNmGINin2YTZg9mE2KgK2K3ZgtmK2LEK2KrYp9mB2YcK2KzZhtizCtiz2YPYswrYudin2LHZigrYudin2YfYsdipCtiy2KgK2YPYswrYt9mK2LIK2YbZitmDCtmE2K3YswrZhdi1CtmF2KrYrtmE2YEK2YjYs9iuCtmC2LDYsQrYtNix2YXZiNi3Ctiv2YrZiNirCtiu2YjZhArZhdmG2YrZiNmDCtmF2YXYrdmI2YYK2KjYstin2LIK2YXYpNiu2LHYqQrYp9i62KrYtdin2KgK2KfYutiq2LXYqA=="
BAD_WORDS = _BAD_WORDS_B64
BAD_WORDS = base64.b64decode(BAD_WORDS).decode("utf-8").splitlines()

# تطبيع بسيط للتحايل بالمسافات/الرموز الشائعة.
def normalize_moderation_text(value: str) -> str:
    value = value.casefold()
    replacements = str.maketrans({
        "0": "o", "1": "i", "3": "e", "4": "a", "5": "s", "7": "t",
        "@": "a", "$": "s"
    })
    value = value.translate(replacements)
    return re.sub(r"[^\w\u0600-\u06ff]+", "", value, flags=re.UNICODE)

BAD_WORD_PATTERN = re.compile(
    "|".join(re.escape(w) for w in BAD_WORDS),
    re.IGNORECASE | re.UNICODE,
)


def contains_insult(text: str) -> bool:
    if not text:
        return False
    # الفحص الطبيعي
    if BAD_WORD_PATTERN.search(text):
        return True
    # فحص نسخة مطبّعة لمحاولات التحايل بالرموز والمسافات.
    normalized = normalize_moderation_text(text)
    return any(normalize_moderation_text(w) in normalized for w in BAD_WORDS)


BTN_LISTINGS = "📋 العروض"
BTN_SELL = "🆕 بيع"
BTN_BUY = "🛒 شراء"
BTN_MYDEALS = "📂 صفقاتي"
BTN_MYLISTINGS = "📦 عروضي"
BTN_WALLET = "💳 محفظتي"
BTN_HELP = "❓ مساعدة"
BTN_PAY = "💰 تأكيد دفع"
BTN_RELEASE = "✅ تحويل المبلغ"
BTN_BACK = "🔙 رجوع"
BTN_HOME = "🏠 القائمة الرئيسية"
BTN_MENU = "🏠 فتح القائمة"

BASE_COMMANDS = [
    BotCommand("start", "بدء التعامل مع البوت"),
    BotCommand("help", "شرح كامل لطريقة العمل"),
    BotCommand("sell", "عرض حساب للبيع"),
    BotCommand("list", "تصفح العروض المتاحة (أو /list <اسم اللعبة> للتصفية)"),
    BotCommand("buy", "بدء صفقة شراء"),
    BotCommand("ho", "تأكيد تسليم الحساب (بائع)"),
    BotCommand("deal", "حالة صفقة معينة"),
    BotCommand("my", "صفقاتي"),
    BotCommand("cancel", "إلغاء صفقة"),
]
ADMIN_COMMANDS = BASE_COMMANDS + [
    BotCommand("pay", "تأكيد استلام الدفع (أدمن)"),
    BotCommand("rel", "تحويل المبلغ وإغلاق الصفقة (أدمن)"),
    BotCommand("stats", "إحصائيات البوت (أدمن)"),
    BotCommand("admins", "عرض قائمة أدمن البوت"),
]
OWNER_COMMANDS = ADMIN_COMMANDS + [
    BotCommand("addadmin", "إضافة أدمن جديد للبوت (المالك فقط)"),
    BotCommand("removeadmin", "إزالة أدمن من البوت (المالك فقط)"),
    BotCommand("makeadmin", "ترقية مستخدم لأدمن بالمجموعة (المالك فقط)"),
]


# ---------- قاعدة البيانات ----------
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    # WAL يسمح بقراءات متزامنة أثناء الكتابة، ويقلل كثيراً من أخطاء
    # "database is locked" التي تحدث مع SQLite عند تعدد الطلبات في نفس اللحظة
    # (شائعة جداً في بوتات تيليجرام لأن كل تحديث/ضغطة زر قد يصل بالتوازي).
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


@contextlib.contextmanager
def db_session():
    """Context manager آمن لقاعدة البيانات: يضمن commit عند النجاح،
    rollback عند أي استثناء، و close دائماً — حتى لو حدث خطأ في المنتصف.
    يُفضَّل استخدامه في أي كود جديد بدل get_db()/conn.close() اليدوي."""
    conn = get_db()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    conn = get_db()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            seller_id INTEGER NOT NULL,
            seller_username TEXT,
            price REAL NOT NULL,
            niche TEXT,
            followers TEXT,
            description TEXT,
            video_file_id TEXT,
            status TEXT DEFAULT 'awaiting_approval',
            created_at TEXT,
            approved_at TEXT,
            approved_by INTEGER,
            rejection_reason TEXT
        );

        CREATE TABLE IF NOT EXISTS deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id INTEGER NOT NULL,
            buyer_id INTEGER NOT NULL,
            buyer_username TEXT,
            seller_id INTEGER NOT NULL,
            price REAL NOT NULL,
            state TEXT DEFAULT 'pending_payment',
            created_at TEXT,
            updated_at TEXT,
            buyer_confirmed INTEGER DEFAULT 0,
            disputed INTEGER DEFAULT 0,
            dispute_reason TEXT,
            payment_reminded INTEGER DEFAULT 0,
            FOREIGN KEY(listing_id) REFERENCES listings(id)
        );

        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            updated_at TEXT,
            referred_by INTEGER
        );

        CREATE TABLE IF NOT EXISTS ratings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deal_id INTEGER NOT NULL,
            rater_id INTEGER NOT NULL,
            rated_id INTEGER NOT NULL,
            stars INTEGER NOT NULL,
            created_at TEXT,
            UNIQUE(deal_id, rater_id)
        );

        CREATE TABLE IF NOT EXISTS blacklist (
            user_id INTEGER PRIMARY KEY,
            reason TEXT,
            added_by INTEGER,
            created_at TEXT
        );

        CREATE TABLE IF NOT EXISTS admins (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            added_by INTEGER,
            added_at TEXT
        );
        """
    )

    # تعبئة أولية (bootstrap) لجدول الأدمن من متغير البيئة ADMIN_IDS — تحدث
    # مرة واحدة فقط، فقط إذا كان الجدول فارغاً بالكامل (أول تشغيل للبوت).
    # بعد ذلك يصبح جدول admins هو المصدر الوحيد للحقيقة، ولا علاقة له
    # بمتغير البيئة بعد أول تشغيل — حتى لا يعيد إضافة أدمن أُزيل سابقاً.
    admin_count = conn.execute("SELECT COUNT(*) c FROM admins").fetchone()["c"]
    if admin_count == 0 and _BOOTSTRAP_ADMIN_IDS:
        now = datetime.utcnow().isoformat()
        for admin_id in _BOOTSTRAP_ADMIN_IDS:
            conn.execute(
                "INSERT OR IGNORE INTO admins (user_id, added_by, added_at) VALUES (?, ?, ?)",
                (admin_id, OWNER_ID or admin_id, now),
            )
    # ترقية قواعد البيانات القديمة بدون حذف البيانات.
    existing_columns = {row["name"] for row in conn.execute("PRAGMA table_info(listings)").fetchall()}
    for column, definition in [
        ("video_file_id", "TEXT"),
        ("approved_at", "TEXT"),
        ("approved_by", "INTEGER"),
        ("rejection_reason", "TEXT"),
        ("flagged_low", "INTEGER DEFAULT 0"),
        ("credentials", "TEXT"),
        ("seller_wallet", "TEXT"),
    ]:
        if column not in existing_columns:
            conn.execute(f"ALTER TABLE listings ADD COLUMN {column} {definition}")

    existing_deal_columns = {row["name"] for row in conn.execute("PRAGMA table_info(deals)").fetchall()}
    for column, definition in [
        ("buyer_confirmed", "INTEGER DEFAULT 0"),
        ("disputed", "INTEGER DEFAULT 0"),
        ("dispute_reason", "TEXT"),
        ("payment_reminded", "INTEGER DEFAULT 0"),
        ("claimed_by", "INTEGER"),
        ("claimed_at", "TEXT"),
    ]:
        if column not in existing_deal_columns:
            conn.execute(f"ALTER TABLE deals ADD COLUMN {column} {definition}")

    existing_user_columns = {row["name"] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
    if "referred_by" not in existing_user_columns:
        conn.execute("ALTER TABLE users ADD COLUMN referred_by INTEGER")
    if "wallet_address" not in existing_user_columns:
        conn.execute("ALTER TABLE users ADD COLUMN wallet_address TEXT")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_listings_status ON listings(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_listings_niche_status ON listings(niche, status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_deals_state ON deals(state)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_deals_buyer ON deals(buyer_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_deals_seller ON deals(seller_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")
    conn.commit()
    conn.close()


def remember_user(user):
    """يخزّن/يحدّث آيدي واسم مستخدم كل من يتفاعل مع البوت، حتى يمكن لاحقاً
    إيجاد آيدي المستخدم انطلاقاً من يوزره (مطلوب لأمر ترقية أدمن مثلاً،
    لأن Telegram Bot API يحتاج user_id رقمي وليس username)."""
    if not user or not getattr(user, "id", None):
        return
    try:
        conn = get_db()
        conn.execute(
            "INSERT INTO users (user_id, username, first_name, updated_at) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, "
            "first_name=excluded.first_name, updated_at=excluded.updated_at",
            (user.id, user.username, user.first_name, datetime.utcnow().isoformat()),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"تعذر حفظ بيانات المستخدم {user.id}: {e}")


def get_user_wallet(user_id: int):
    """يرجع عنوان محفظة المستخدم المحفوظ (BEP20/USDT)، أو None إن لم يحفظه بعد."""
    conn = get_db()
    row = conn.execute("SELECT wallet_address FROM users WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return row["wallet_address"] if row and row["wallet_address"] else None


def set_user_wallet(user_id: int, address: str):
    """يحفظ/يحدّث عنوان محفظة المستخدم. يُنشئ صفاً في users إن لم يكن موجوداً
    (نادراً ما يحدث، لأن remember_user يُشغَّل على كل تحديث أولاً)."""
    conn = get_db()
    conn.execute(
        "INSERT INTO users (user_id, wallet_address, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET wallet_address=excluded.wallet_address, updated_at=excluded.updated_at",
        (user_id, address, datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()


def lookup_user_id_by_username(username: str):
    """يبحث عن آيدي مستخدم اعتماداً على يوزره من بين من تفاعلوا مع البوت سابقاً."""
    username = username.lstrip("@").strip().lower()
    if not username:
        return None
    conn = get_db()
    row = conn.execute(
        "SELECT user_id FROM users WHERE LOWER(username)=? ORDER BY updated_at DESC LIMIT 1",
        (username,),
    ).fetchone()
    conn.close()
    return row["user_id"] if row else None


# ---------- أدوات مساعدة ----------
async def notify(context: ContextTypes.DEFAULT_TYPE, user_id: int, text: str):
    try:
        await context.bot.send_message(chat_id=user_id, text=text)
    except Exception as e:
        logger.warning(f"تعذر إرسال إشعار لـ {user_id}: {e}")


async def notify_admin(context: ContextTypes.DEFAULT_TYPE, text: str):
    """يرسل إشعارات الأدمن إلى مجموعة الإدارة إن كانت معرّفة، وإلا لكل الأدمنية مباشرة."""
    if ADMIN_GROUP_ID:
        await notify(context, ADMIN_GROUP_ID, text)
        return
    for admin_id in get_admin_ids():
        await notify(context, admin_id, text)


def get_admin_ids() -> set:
    """يرجع مجموعة آيدي كل الأدمن الحاليين (من قاعدة البيانات) + المالك دائماً.
    هذا هو مصدر الحقيقة الوحيد بعد أول تشغيل — تُدار الإضافة/الإزالة عبر
    /addadmin و /removeadmin فقط، وليس بتعديل الكود."""
    conn = get_db()
    rows = conn.execute("SELECT user_id FROM admins").fetchall()
    conn.close()
    ids = {row["user_id"] for row in rows}
    if OWNER_ID:
        ids.add(OWNER_ID)
    return ids


def is_admin(user_id: int) -> bool:
    if OWNER_ID and user_id == OWNER_ID:
        return True
    conn = get_db()
    row = conn.execute("SELECT 1 FROM admins WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return bool(row)


def is_owner(user_id: int) -> bool:
    return bool(OWNER_ID) and user_id == OWNER_ID


def calc_fee(price: float) -> float:
    """يحسب إجمالي رسوم الوسيط لصفقة بسعر معيّن: مبلغ ثابت (FEE_FLAT) +
    نسبة مئوية اختيارية (FEE_PERCENT، تكون 0 افتراضياً فلا تُضيف شيئاً ما
    لم تُغيَّر). لا تتجاوز الرسوم سعر الصفقة نفسه."""
    fee = FEE_FLAT + (price * FEE_PERCENT / 100)
    return round(min(fee, price), 2)


# ---------- التقييمات وشارة "موثّق" ----------
VERIFIED_DEALS_THRESHOLD = 3  # عدد الصفقات المكتملة كبائع لنيل شارة ✅ موثّق


def get_user_rating(user_id: int):
    """يرجع (average, count) لتقييمات مستخدم معيّن. average=None إذا لا تقييمات."""
    conn = get_db()
    row = conn.execute(
        "SELECT AVG(stars) avg_s, COUNT(*) c FROM ratings WHERE rated_id=?", (user_id,)
    ).fetchone()
    conn.close()
    if not row or not row["c"]:
        return None, 0
    return round(row["avg_s"], 1), row["c"]


def get_completed_deals_count(user_id: int, role: str = "seller") -> int:
    col = "seller_id" if role == "seller" else "buyer_id"
    conn = get_db()
    row = conn.execute(
        f"SELECT COUNT(*) c FROM deals WHERE {col}=? AND state='completed'", (user_id,)
    ).fetchone()
    conn.close()
    return row["c"] if row else 0


def is_verified_seller(user_id: int) -> bool:
    return get_completed_deals_count(user_id, "seller") >= VERIFIED_DEALS_THRESHOLD


def rating_badge(user_id: int) -> str:
    """نص صغير جاهز للعرض بجانب اسم المستخدم: ⭐تقييم (عدد) + ✅موثّق إن انطبق."""
    avg, count = get_user_rating(user_id)
    parts = []
    if count:
        parts.append(f"⭐{avg} ({count})")
    if is_verified_seller(user_id):
        parts.append("✅ موثّق")
    return (" " + " ".join(parts)) if parts else ""


def get_total_completed_deals() -> int:
    conn = get_db()
    row = conn.execute("SELECT COUNT(*) c FROM deals WHERE state='completed'").fetchone()
    conn.close()
    return row["c"] if row else 0


def check_low_price_flag(niche: str, price: float) -> bool:
    """يقارن سعر عرض جديد بمتوسط أسعار العروض المفتوحة لنفس اللعبة، ويعتبره
    مشبوهاً إن كان أقل من 40% من المتوسط (وتوجد عيّنة كافية للمقارنة)، أو
    قريباً جداً من الحد الأدنى المسموح ($2) بغض النظر عن المتوسط."""
    if price <= 2.5:
        return True
    conn = get_db()
    row = conn.execute(
        "SELECT AVG(price) avg_p, COUNT(*) c FROM listings WHERE niche=? AND status='open'",
        (niche,),
    ).fetchone()
    conn.close()
    if row and row["c"] and row["c"] >= 3 and row["avg_p"]:
        return price < 0.4 * row["avg_p"]
    return False


# ---------- القائمة السوداء ----------
def is_blacklisted(user_id: int) -> bool:
    conn = get_db()
    row = conn.execute("SELECT 1 FROM blacklist WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return bool(row)


# ---------- نظام "مهمة واحدة" للأدمن ----------
CLAIMABLE_STATES = ("pending_payment", "payment_received", "handed_over")


def get_admin_active_deal(admin_id: int):
    """يرجع رقم الصفقة التي يعمل عليها أدمن معيّن حالياً، أو None إن لم تكن لديه مهمة نشطة."""
    conn = get_db()
    row = conn.execute(
        "SELECT id FROM deals WHERE claimed_by=? AND state IN (?,?,?) AND disputed=0",
        (admin_id,) + CLAIMABLE_STATES,
    ).fetchone()
    conn.close()
    return row["id"] if row else None


# ---------- فحص محفظة BSC (USDT BEP20) عبر عقدة RPC عامة (مجاني بالكامل) ----------
_ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
_RPC_BLOCK_CHUNK_START = 2000    # نبدأ بنطاق متحفّظ (بعض العقد العامة تمنع أكثر من هذا لكل طلب)
_RPC_BLOCK_CHUNK_MIN = 200       # أصغر نطاق نقبل به قبل التوقف
_RPC_MAX_REQUESTS = 12           # حد أقصى لعدد طلبات eth_getLogs الإجمالية
_RPC_RETRIES = 2                 # عدد محاولات إعادة الطلب نفسه عند رفضه بسبب حد المعدّل (rate limit)
_RPC_RETRY_BASE_DELAY = 0.7      # ثوانٍ، تتضاعف مع كل محاولة (تراجع أسّي)
_RPC_INTER_REQUEST_DELAY = 0.2   # ثوانٍ بين كل طلب eth_getLogs وآخر، لتجنّب حد المعدّل من الأساس
_RPC_TIME_BUDGET_SECONDS = 14    # الحد الأقصى للوقت الإجمالي للفحص — بعده نتوقف ونعرض ما جُمع حتى الآن
_RPC_TIMEOUT_SECONDS = 8         # مهلة كل طلب HTTP منفرد


def _rpc_call(method: str, params: list):
    resp = requests.post(
        BSC_RPC_URL,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=_RPC_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(data["error"].get("message", "خطأ من عقدة RPC"))
    return data.get("result")


def _rpc_call_with_retry(method: str, params: list, retries: int = _RPC_RETRIES, deadline: float = None):
    """يعيد محاولة الطلب مع تراجع أسّي (exponential backoff) عند أخطاء تبدو
    كحدّ معدّل (rate limit) من العقدة العامة، بدل الفشل من أول مرة.
    يتوقف فوراً إن تجاوزنا الموعد النهائي (deadline) حتى لا يتأخر الرد كثيراً."""
    last_exc = None
    for attempt in range(retries):
        if deadline is not None and time.time() >= deadline:
            raise last_exc or RuntimeError("انتهى الوقت المتاح للفحص")
        try:
            return _rpc_call(method, params)
        except Exception as e:
            last_exc = e
            if attempt < retries - 1 and _is_retryable_error(e):
                remaining = None if deadline is None else deadline - time.time()
                delay = _RPC_RETRY_BASE_DELAY * (2 ** attempt)
                if remaining is not None:
                    delay = min(delay, max(0, remaining))
                if delay > 0:
                    time.sleep(delay)
                continue
            raise
    raise last_exc


def _address_to_topic(address: str) -> str:
    """يحوّل عنوان 0x... إلى صيغة topic بطول 32 بايت (padded) لاستخدامه في فلترة eth_getLogs."""
    addr = address.lower().replace("0x", "")
    return "0x" + addr.rjust(64, "0")


def _is_range_limit_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(kw in msg for kw in ("block range", "query returned more than"))


def _is_retryable_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(kw in msg for kw in ("limit exceeded", "rate limit", "too many requests", "429"))


def _fetch_usdt_transfers_sync(limit: int = 15):
    """يجلب (مزامن) آخر تحويلات USDT (BEP20) الواردة للمحفظة مباشرة من
    البلوكتشين عبر عقدة RPC عامة — لا يحتاج أي مفتاح API أو اشتراك.
    محدود بميزانية زمنية إجمالية (_RPC_TIME_BUDGET_SECONDS) حتى لا يتأخر
    الرد على الأدمن كثيراً — إن انتهى الوقت يعيد ما جُمع حتى تلك اللحظة
    بدل الاستمرار للأبد. يُستدعى فقط داخل asyncio.to_thread."""
    deadline = time.time() + _RPC_TIME_BUDGET_SECONDS
    latest_hex = _rpc_call_with_retry("eth_blockNumber", [], deadline=deadline)
    latest_block = int(latest_hex, 16)
    to_topic = _address_to_topic(WALLET_ADDRESS)

    transfers = []
    start = latest_block
    chunk = _RPC_BLOCK_CHUNK_START
    requests_made = 0

    while (
        start >= 0
        and len(transfers) < limit
        and requests_made < _RPC_MAX_REQUESTS
        and time.time() < deadline
    ):
        end = start
        begin = max(0, start - chunk)
        try:
            logs = _rpc_call_with_retry("eth_getLogs", [{
                "fromBlock": hex(begin),
                "toBlock": hex(end),
                "address": USDT_BEP20_CONTRACT,
                "topics": [_ERC20_TRANSFER_TOPIC, None, to_topic],
            }], deadline=deadline)
            requests_made += 1
        except Exception as e:
            requests_made += 1
            if _is_range_limit_error(e) and chunk > _RPC_BLOCK_CHUNK_MIN:
                chunk = max(_RPC_BLOCK_CHUNK_MIN, chunk // 2)
                continue  # أعد المحاولة بنفس نقطة البداية لكن بنطاق أصغر
            # استُنفدت المحاولات أو انتهى الوقت أو أي خطأ آخر — نتوقف ونعيد
            # ما جُمع حتى الآن بدل رمي الخطأ، أفضل من فشل كامل بلا نتيجة.
            break

        for log in logs or []:
            try:
                raw_value = int(log["data"], 16)
                amount = raw_value / (10 ** USDT_BEP20_DECIMALS)
                from_addr = "0x" + log["topics"][1][-40:]
                block_num = int(log["blockNumber"], 16)
            except (KeyError, ValueError, TypeError, IndexError):
                continue
            transfers.append({
                "hash": log.get("transactionHash", ""),
                "from": from_addr,
                "amount": amount,
                "block": block_num,
            })
        start = begin - 1
        if time.time() < deadline:
            time.sleep(_RPC_INTER_REQUEST_DELAY)

    transfers.sort(key=lambda t: t["block"], reverse=True)
    return transfers[:limit]



async def check_wallet_usdt(limit: int = 15):
    """نسخة async آمنة لجلب معاملات USDT الواردة للمحفظة عبر RPC عامة (مجاني).
    ترجع (ok: bool, transfers_or_error)."""
    try:
        transfers = await asyncio.to_thread(_fetch_usdt_transfers_sync, limit)
        return True, transfers
    except Exception as e:
        logger.warning(f"تعذر فحص المحفظة عبر RPC: {e}")
        return False, str(e)


def format_wallet_check(transfers, expected_amount: float = None, tolerance: float = 0.01) -> str:
    """يبني نصاً يعرض آخر التحويلات الواردة، ويميّز أي معاملة تطابق المبلغ
    المتوقع (إن أُعطي) بعلامة ✅. لا يوجد أي تأكيد تلقائي هنا — الأدمن هو
    من يقرر ويضغط زر التأكيد يدوياً بعد المراجعة."""
    if not transfers:
        return ("🔍 لم يتم العثور على أي تحويلات USDT (BEP20) واردة حديثاً لهذه المحفظة "
                "(ضمن نطاق الفحص الأخير من الكتل).")
    lines = ["🔍 <b>آخر تحويلات USDT (BEP20) الواردة للمحفظة:</b>\n"]
    for tx in transfers[:10]:
        match = ""
        if expected_amount is not None and abs(tx["amount"] - expected_amount) <= tolerance:
            match = " ✅ يطابق المبلغ المطلوب"
        short_hash = tx["hash"][:10] + "…" + tx["hash"][-6:] if tx["hash"] else "؟"
        short_from = tx["from"][:8] + "…" + tx["from"][-6:] if tx["from"] else "؟"
        lines.append(
            f"• {tx['amount']:.2f} USDT{match}\n"
            f"  من: <code>{short_from}</code>\n"
            f"  الهاش: <code>{short_hash}</code>\n"
            f"  رقم الكتلة: {tx['block']}"
        )
    lines.append(
        "\n⚠️ هذا فحص مساعد فقط — تحقق من تطابق المبلغ والهاش بنفسك قبل "
        "الضغط على زر تأكيد الدفع. لم يتم تأكيد أي شيء تلقائياً."
    )
    return "\n".join(lines)


def display_name(user) -> str:
    """يعرض @username إن وجد، وإلا الاسم الأول بدل المعرف الرقمي."""
    if getattr(user, "username", None):
        return f"@{user.username}"
    return user.first_name or "مستخدم"


def get_claimed_by_note(admin_id: int) -> str:
    """اسم مختصر لعرضه في شاشة الصفقة عندما تكون مستلَمة من أدمن آخر."""
    conn = get_db()
    row = conn.execute("SELECT username, first_name FROM users WHERE user_id=?", (admin_id,)).fetchone()
    conn.close()
    if row and row["username"]:
        return f"@{row['username']}"
    if row and row["first_name"]:
        return row["first_name"]
    return f"أدمن ({admin_id})"


def _parse_single_id(update, context):
    args = context.args
    if len(args) != 1:
        return None
    try:
        return int(args[0])
    except ValueError:
        return None


async def safe_delete(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


STATE_NAMES_AR = {
    "pending_payment": "بانتظار الدفع",
    "payment_received": "تم تأكيد الدفع",
    "handed_over": "تم تسليم الحساب",
    "completed": "مكتملة",
    "cancelled": "ملغاة",
}

LISTING_STATE_NAMES_AR = {
    "awaiting_approval": "⏳ بانتظار مراجعة الأدمن",
    "open": "🟢 معروض للبيع",
    "pending": "🔒 قيد صفقة بيع حالياً",
    "sold": "✅ تم بيعه",
    "rejected": "❌ مرفوض من الأدمن",
}


# ---------- نظام الشاشة الواحدة (رسالة واحدة تتحدّث + رجوع) ----------
def _plain_screen_text(text: str) -> str:
    """Fallback for malformed Telegram HTML: keep readable text without tags."""
    import html as _html
    return _html.unescape(re.sub(r"<[^>]*>", "", text or ""))


async def _send_screen_text(context, chat_id, text, keyboard):
    try:
        return await context.bot.send_message(
            chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, reply_markup=keyboard
        )
    except Exception as e:
        if "can't parse entities" not in str(e).lower():
            raise
        logger.warning("Invalid HTML in screen text; retrying without HTML: %s", e)
        return await context.bot.send_message(
            chat_id=chat_id, text=_plain_screen_text(text), reply_markup=keyboard
        )


async def set_screen(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str, keyboard=None, video_file_id: str = None):
    """Edit/create the single screen message, with a safe fallback for malformed HTML."""
    msg_id = context.user_data.get("screen_msg_id")
    was_video = context.user_data.get("screen_is_video", False)

    if video_file_id:
        if msg_id:
            await safe_delete(context, chat_id, msg_id)
        try:
            msg = await context.bot.send_video(
                chat_id=chat_id, video=video_file_id, caption=text,
                parse_mode=ParseMode.HTML, reply_markup=keyboard,
            )
            context.user_data["screen_msg_id"] = msg.message_id
            context.user_data["screen_is_video"] = True
            return
        except Exception as e:
            logger.warning("تعذر عرض الفيديو ضمن الشاشة، سيتم العرض كنص فقط: %s", e)
            msg_id = None

    if msg_id and not was_video:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=msg_id, text=text,
                parse_mode=ParseMode.HTML, reply_markup=keyboard,
            )
            return
        except Exception as e:
            if "not modified" not in str(e).lower():
                await safe_delete(context, chat_id, msg_id)
            # If HTML parsing failed, send a plain-text replacement below.
            if "can't parse entities" in str(e).lower():
                text = _plain_screen_text(text)
    elif msg_id and was_video:
        await safe_delete(context, chat_id, msg_id)

    msg = await _send_screen_text(context, chat_id, text, keyboard)
    context.user_data["screen_msg_id"] = msg.message_id
    context.user_data["screen_is_video"] = False


def back_row(show_back: bool) -> list:
    if not show_back:
        return []
    return [
        InlineKeyboardButton(BTN_BACK, callback_data="back"),
        InlineKeyboardButton(BTN_HOME, callback_data="go:home"),
    ]


async def render_screen(context, user_id, name, param=None):
    """يبني (النص، لوحة الأزرار) لشاشة معينة."""
    if name == "home":
        total_completed = get_total_completed_deals()
        counter_line = f"✅ {total_completed} صفقة مكتملة بنجاح حتى الآن\n\n" if total_completed else ""
        text = (
            "👋 أهلاً بك في بوت الوساطة لبيع وشراء حسابات الألعاب\n\n"
            + counter_line +
            "🎮 الألعاب المدعومة حالياً: " + "، ".join(SUPPORTED_GAMES) + " (والمزيد قريباً)\n\n"
            f"💰 عمولة الوسيط: ${FEE_FLAT:.2f} ثابتة لكل صفقة"
            + (f" + {FEE_PERCENT:g}% من السعر" if FEE_PERCENT else "")
            + "، تُضاف على المشتري فوق سعر العرض عند الدفع.\n\n"
            "💳 عنوان محفظة USDT (BEP20) للدفع:\n"
            f"<code>{WALLET_ADDRESS}</code>\n"
            "(اضغط على العنوان لنسخه تلقائياً)\n"
            "💠 نقبل أيضاً BNB أو BUSD (BEP20) على نفس العنوان — أخبر الأدمن بالعملة "
            "التي أرسلتها لأن الفحص التلقائي للمحفظة يدعم USDT فقط حالياً.\n\n"
            "اختر من الأزرار بالأسفل:"
        )
        rows = [
            [InlineKeyboardButton(BTN_LISTINGS, callback_data="go:list"), InlineKeyboardButton(BTN_SELL, callback_data="startsell")],
            [InlineKeyboardButton(BTN_BUY, callback_data="go:buypick"), InlineKeyboardButton(BTN_MYDEALS, callback_data="go:my")],
            [InlineKeyboardButton(BTN_MYLISTINGS, callback_data="go:mylistings"), InlineKeyboardButton(BTN_WALLET, callback_data="go:wallet")],
            [InlineKeyboardButton(BTN_HELP, callback_data="go:help")],
        ]
        if is_admin(user_id):
            rows.append([InlineKeyboardButton(BTN_PAY, callback_data="go:pay"), InlineKeyboardButton(BTN_RELEASE, callback_data="go:release")])
        return text, InlineKeyboardMarkup(rows)

    if name == "help":
        text = (
            "📖 كيف يعمل هذا البوت — دليل كامل\n\n"
            "هذا البوت ليس نظام دفع، هو فقط يتابع حالة الصفقة خطوة بخطوة. "
            "الدفع الفعلي (PayPal أو تحويل أو عملة رقمية) يتم خارج البوت مباشرة مع الأدمن.\n\n"
            "───────────────\n"
            "🟢 إذا كنت بائعاً:\n"
            "1️⃣ اضغط 🆕 بيع وسيسألك البوت خطوة بخطوة:\n"
            "   - اختيار اللعبة من الأزرار (" + "، ".join(SUPPORTED_GAMES) + ")\n"
            "   - المستوى (اختياري، اضغط تخطي إذا لم يوجد)\n"
            "   - السعر (بين 2$ و 500$)\n"
            "   - وصف الحساب\n"
            "⚠️ لا يُسمح بإرسال روابط أو كلمات مسيئة، ويجب إرفاق فيديو واضح للحساب.\n⏳ بعد الإرسال، يراجع الأدمن العرض قبل أن يظهر للمشترين.\n\n"
            "2️⃣ عندما يشتري أحدهم عرضك، ستصلك رسالة بوجود مشتري. "
            "لا تسلّم الحساب أبداً قبل أن يؤكد الأدمن استلام الدفعة.\n\n"
            "3️⃣ بعد تأكيد الأدمن استلام الدفع، سلّم بيانات الحساب للمشتري ثم أكّد بالأمر:\n"
            "/ho &lt;رقم_الصفقة&gt;\n\n"
            "4️⃣ بعد أن يتحقق المشتري من الحساب، يحوّل الأدمن المبلغ لك بالكامل (رسوم الوساطة كانت مضافة على المشتري وليست منك).\n\n"
            "───────────────\n"
            "🔵 إذا كنت مشترياً:\n"
            "1️⃣ اضغط 🛒 شراء واختر عرضاً من الأزرار، ثم أكّد الشراء.\n"
            "2️⃣ أرسل المبلغ المطلوب (سعر العرض + رسوم الوساطة) عن طريق USDT (BEP20) إلى عنوان المحفظة:\n"
            f"<code>{WALLET_ADDRESS}</code>\n"
            "(اضغط على العنوان لنسخه تلقائياً)\n"
            "💠 BNB أو BUSD (BEP20) مقبولة أيضاً على نفس العنوان — أخبر الأدمن بالعملة المُرسَلة.\n"
            "3️⃣ عند تأكيد الأدمن استلام الدفع، انتظر تسليم الحساب من البائع.\n"
            "4️⃣ تحقق جيداً من الحساب قبل أن يحوّل الأدمن المبلغ للبائع.\n\n"
            "───────────────\n"
            "⚠️ بعض المنصات تمنع رسمياً بيع الحسابات، لذلك هناك دائماً احتمال حظر "
            "الحساب حتى لو تمت الصفقة بشكل صحيح."
        )
        return text, InlineKeyboardMarkup([back_row(True)])

    if name == "list":
        # param قد يكون: None (بدون فلترة)، فهرس رقمي للعبة (من أزرار الفلترة)،
        # أو نص حر (من أمر /list <اسم> المكتوب يدوياً).
        niche_filter = None
        selected_idx = None
        if isinstance(param, int) and 0 <= param < len(SUPPORTED_GAMES):
            niche_filter = SUPPORTED_GAMES[param]
            selected_idx = param
        elif isinstance(param, str) and param:
            niche_filter = param

        conn = get_db()
        if niche_filter:
            rows_db = conn.execute(
                "SELECT * FROM listings WHERE status='open' AND niche LIKE ? ORDER BY id DESC LIMIT 20",
                (f"%{niche_filter}%",),
            ).fetchall()
        else:
            rows_db = conn.execute("SELECT * FROM listings WHERE status='open' ORDER BY id DESC LIMIT 20").fetchall()
        conn.close()

        filter_btns = [
            InlineKeyboardButton(
                ("✅ " if selected_idx == i else "") + g.split("/")[0],
                callback_data=f"go:list:{i}",
            )
            for i, g in enumerate(SUPPORTED_GAMES)
        ]
        filter_rows = [filter_btns[i:i + 3] for i in range(0, len(filter_btns), 3)]
        if niche_filter:
            filter_rows.append([InlineKeyboardButton("🔄 عرض كل الألعاب", callback_data="go:list")])

        if not rows_db:
            text = "📋 لا توجد عروض متاحة حالياً." if not niche_filter else f"📋 لا توجد عروض متاحة للعبة «{html.escape(niche_filter)}»."
            return text, InlineKeyboardMarkup(filter_rows + [back_row(True)])

        parts = ["📋 <b>العروض المتاحة</b> — اضغط على زر الشراء تحت أي عرض\n"]
        item_rows = []
        for r in rows_db:
            level_line = f"⭐ المستوى: {html.escape(r['followers'])}\n" if r["followers"] else ""
            seller_name = html.escape(r["seller_username"] or str(r["seller_id"]))
            badge = rating_badge(r["seller_id"])
            parts.append(
                f"━━━━━━━━━━━━━━\n"
                f"🆔 #{r['id']}   🎮 <b>{html.escape(r['niche'])}</b>\n"
                f"{level_line}"
                f"💰 السعر: ${r['price']}\n"
                f"📝 {html.escape(r['description'])}\n"
                f"👤 البائع: @{seller_name}{badge}\n"
            )
            item_rows.append([InlineKeyboardButton(f"🛒 شراء #{r['id']} — ${r['price']}", callback_data=f"go:buyconfirm:{r['id']}")])
        text = "\n".join(parts)
        rows = filter_rows + item_rows
        rows.append(back_row(True))
        return text, InlineKeyboardMarkup(rows)

    if name == "my":
        conn = get_db()
        rows_db = conn.execute(
            "SELECT * FROM deals WHERE buyer_id=? OR seller_id=? ORDER BY id DESC LIMIT 20",
            (user_id, user_id),
        ).fetchall()
        conn.close()
        if not rows_db:
            text = "📂 لا توجد لديك صفقات حتى الآن."
            return text, InlineKeyboardMarkup([back_row(True)])
        parts = ["📂 <b>صفقاتك</b> — اضغط على أي صفقة لعرض تفاصيلها\n"]
        item_rows = []
        for r in rows_db:
            role = "مشترٍ" if r["buyer_id"] == user_id else "بائع"
            state_ar = STATE_NAMES_AR.get(r["state"], r["state"])
            parts.append(f"#{r['id']} | ${r['price']} | {state_ar} | أنت {role}")
            item_rows.append([InlineKeyboardButton(f"📄 تفاصيل #{r['id']}", callback_data=f"go:dealstatus:{r['id']}")])
        text = "\n".join(parts)
        rows = item_rows + [back_row(True)]
        return text, InlineKeyboardMarkup(rows)

    if name == "wallet":
        wallet = get_user_wallet(user_id)
        reason_banner = ""
        if context.user_data.get("wallet_prompt_reason") == "sell":
            reason_banner = "⚠️ <b>لازم تحفظ عنوان محفظتك أولاً قبل ما تقدر تبيع.</b>\n\n"

        if wallet:
            text = (
                f"{reason_banner}"
                "💳 <b>محفظتي</b>\n\n"
                "هذا هو عنوان المحفظة (شبكة BEP20) الذي يستلم عليه الأدمن مدفوعاتك "
                "كبائع بعد كل عملية بيع ناجحة.\n\n"
                f"العنوان الحالي:\n<code>{html.escape(wallet)}</code>"
            )
            btn_label = "✏️ تعديل العنوان"
        else:
            text = (
                f"{reason_banner}"
                "💳 <b>محفظتي</b>\n\n"
                "لم تحفظ عنوان محفظة بعد. احفظه الآن حتى يتمكن الأدمن من تحويل "
                "أرباحك إليه مباشرة بعد أي عملية بيع تتم بنجاح.\n\n"
                "🔒 هذا العنوان يظهر للأدمن فقط، ولا يُشارك مع أي مستخدم آخر."
            )
            btn_label = "➕ إضافة عنوان محفظة"

        rows = [
            [InlineKeyboardButton(btn_label, callback_data="editwallet")],
            back_row(True),
        ]
        return text, InlineKeyboardMarkup(rows)

    if name == "mylistings":
        conn = get_db()
        rows_db = conn.execute(
            "SELECT * FROM listings WHERE seller_id=? ORDER BY id DESC LIMIT 30",
            (user_id,),
        ).fetchall()
        conn.close()
        if not rows_db:
            text = "📦 لا توجد لديك عروض حتى الآن. اضغط 🆕 بيع لإضافة أول عرض."
            return text, InlineKeyboardMarkup([back_row(True)])

        editable_statuses = ("open", "awaiting_approval")
        deletable_statuses = ("open", "awaiting_approval", "rejected")

        parts = ["📦 <b>عروضي</b>\n"]
        item_rows = []
        for r in rows_db:
            state_ar = LISTING_STATE_NAMES_AR.get(r["status"], r["status"])
            parts.append(
                f"━━━━━━━━━━━━━━\n"
                f"🆔 #{r['id']}   🎮 {html.escape(r['niche'])}\n"
                f"💰 ${r['price']}   |   {state_ar}"
            )
            if r["status"] == "rejected" and r["rejection_reason"]:
                parts.append(f"سبب الرفض: {html.escape(r['rejection_reason'])}")

            action_row = []
            if r["status"] in editable_statuses:
                action_row.append(InlineKeyboardButton(f"✏️ تعديل السعر #{r['id']}", callback_data=f"editprice:{r['id']}"))
            if r["status"] in deletable_statuses:
                action_row.append(InlineKeyboardButton(f"🗑 حذف #{r['id']}", callback_data=f"delask:{r['id']}"))
            if action_row:
                item_rows.append(action_row)

        text = "\n".join(parts)
        rows = item_rows + [back_row(True)]
        return text, InlineKeyboardMarkup(rows)

    if name == "buypick":
        conn = get_db()
        rows_db = conn.execute("SELECT * FROM listings WHERE status='open' ORDER BY id DESC LIMIT 20").fetchall()
        conn.close()
        if not rows_db:
            text = "لا توجد عروض متاحة حالياً."
            return text, InlineKeyboardMarkup([back_row(True)])
        text = "🛒 اختر العرض الذي تريد شراءه:"
        rows = [
            [InlineKeyboardButton(f"#{r['id']} — {r['niche']} — ${r['price']}", callback_data=f"go:buyconfirm:{r['id']}")]
            for r in rows_db
        ]
        rows.append(back_row(True))
        return text, InlineKeyboardMarkup(rows)

    if name == "buyconfirm":
        listing_id = param
        conn = get_db()
        listing = conn.execute("SELECT * FROM listings WHERE id=? AND status='open'", (listing_id,)).fetchone()
        conn.close()
        if not listing:
            return "هذا العرض لم يعد متاحاً.", InlineKeyboardMarkup([back_row(True)])
        level_line = f"⭐ المستوى: {html.escape(listing['followers'])}\n" if listing["followers"] else ""
        text = (
            f"🎮 <b>{html.escape(listing['niche'])}</b>\n"
            f"{level_line}"
            f"💰 السعر: ${listing['price']}\n"
            f"📝 {html.escape(listing['description'])}\n\n"
            f"هل تريد شراء هذا العرض؟"
        )
        rows = [
            [InlineKeyboardButton("✅ نعم، أشتري", callback_data=f"doconfirm:{listing_id}")],
            back_row(True),
        ]
        return text, InlineKeyboardMarkup(rows)

    if name == "pay":
        conn = get_db()
        rows_db = conn.execute("SELECT * FROM deals WHERE state='pending_payment' ORDER BY id DESC LIMIT 20").fetchall()
        conn.close()
        if not rows_db:
            text = "لا توجد صفقات بانتظار الدفع حالياً."
            return text, InlineKeyboardMarkup([back_row(True)])
        parts = ["💰 <b>الصفقات بانتظار الدفع</b> — اضغط لفتح الصفقة والتأكيد\n"]
        item_rows = []
        for r in rows_db:
            claimed_note = " 🙋" if r["claimed_by"] else ""
            parts.append(f"#{r['id']} | ${r['price']}{claimed_note}")
            item_rows.append([InlineKeyboardButton(f"📄 فتح #{r['id']}", callback_data=f"go:dealstatus:{r['id']}")])
        text = "\n".join(parts)
        return text, InlineKeyboardMarkup(item_rows + [back_row(True)])

    if name == "release":
        conn = get_db()
        rows_db = conn.execute("SELECT * FROM deals WHERE state='handed_over' ORDER BY id DESC LIMIT 20").fetchall()
        conn.close()
        if not rows_db:
            text = "لا توجد صفقات جاهزة لتحويل المبلغ حالياً."
            return text, InlineKeyboardMarkup([back_row(True)])
        parts = ["✅ <b>الصفقات الجاهزة لتحويل المبلغ</b> — اضغط لفتح الصفقة\n"]
        item_rows = []
        for r in rows_db:
            confirm_note = " ✅" if r["buyer_confirmed"] else " ⏳"
            parts.append(f"#{r['id']} | ${r['price']}{confirm_note}")
            item_rows.append([InlineKeyboardButton(f"📄 فتح #{r['id']}", callback_data=f"go:dealstatus:{r['id']}")])
        text = "\n".join(parts)
        return text, InlineKeyboardMarkup(item_rows + [back_row(True)])

    if name == "dealstatus":
        deal_id = param
        conn = get_db()
        deal = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()
        conn.close()
        if not deal:
            text = "الصفقة غير موجودة."
            return text, InlineKeyboardMarkup([back_row(True)])

        state_ar = STATE_NAMES_AR.get(deal["state"], deal["state"])
        dispute_line = "\n🔴 <b>هذه الصفقة عليها نزاع مفتوح حالياً — بانتظار مراجعة الأدمن.</b>" if deal["disputed"] else ""
        confirm_line = ""
        if deal["state"] == "handed_over":
            confirm_line = (
                "\n✅ المشتري أكّد استلام الحساب." if deal["buyer_confirmed"]
                else "\n⏳ بانتظار تأكيد المشتري لاستلام الحساب."
            )

        # عنوان محفظة البائع (BEP20) — يظهر للأدمن فقط، ويصير مهماً تحديداً
        # عند مرحلة تحويل المبلغ، حتى لا يضطر للبحث عنه في رسالة المراجعة
        # القديمة للعرض.
        wallet_line = ""
        if is_admin(user_id):
            conn = get_db()
            listing_row = conn.execute(
                "SELECT seller_wallet FROM listings WHERE id=?", (deal["listing_id"],)
            ).fetchone()
            conn.close()
            if listing_row and listing_row["seller_wallet"]:
                wallet_line = f"\n💳 محفظة البائع (BEP20): <code>{html.escape(listing_row['seller_wallet'])}</code>"

        text = (
            f"📄 <b>الصفقة #{deal['id']}</b>\n"
            f"العرض: #{deal['listing_id']}\n"
            f"السعر: ${deal['price']}\n"
            f"الحالة: {state_ar}"
            f"{confirm_line}"
            f"{dispute_line}"
            f"{wallet_line}\n"
            f"آخر تحديث: {format_dt(deal['updated_at'])}"
        )

        rows = []
        is_party = user_id in (deal["buyer_id"], deal["seller_id"])
        if is_party and not deal["disputed"] and deal["state"] not in ("completed", "cancelled"):
            rows.append([InlineKeyboardButton("⚠️ فتح نزاع", callback_data=f"dispute:{deal_id}")])
        if user_id == deal["buyer_id"] and deal["state"] == "handed_over" and not deal["buyer_confirmed"] and not deal["disputed"]:
            rows.append([InlineKeyboardButton("✅ استلمت الحساب وتحققت منه", callback_data=f"buyerconfirm:{deal_id}")])

        # أزرار الأدمن — بدل إجبار الأدمن على تذكّر وكتابة /pay أو /rel يدوياً،
        # هذه الشاشة نفسها الآن تعرض له الزر الصحيح مباشرة حسب حالة الصفقة.
        if is_admin(user_id) and not deal["disputed"] and deal["state"] in ("pending_payment", "handed_over"):
            if not deal["claimed_by"]:
                rows.append([InlineKeyboardButton("🙋 استلام المهمة", callback_data=f"claim:{deal_id}")])
            elif deal["claimed_by"] == user_id:
                if deal["state"] == "pending_payment":
                    rows.append([InlineKeyboardButton("✅ استلمت الدفع — تأكيد", callback_data=f"admpay:{deal_id}")])
                    rows.append([InlineKeyboardButton("🔍 فحص المحفظة (USDT)", callback_data=f"checkwallet:{deal_id}")])
                elif deal["state"] == "handed_over" and deal["buyer_confirmed"]:
                    rows.append([InlineKeyboardButton("✅ تحويل المبلغ للبائع", callback_data=f"admrel:{deal_id}")])
                rows.append([InlineKeyboardButton("🏁 إنهاء المهمة", callback_data=f"unclaim:{deal_id}")])
            else:
                admin_note = get_claimed_by_note(deal["claimed_by"])
                text += f"\n\n🙋 مستلَمة حالياً من: {admin_note}"

        rows.append(back_row(True))
        return text, InlineKeyboardMarkup(rows)

    if name == "msg":
        udata = context.application.user_data[user_id]
        text = udata.pop("pending_msg", "...")
        return text, InlineKeyboardMarkup([back_row(True)])

    # fallback
    return await render_screen(context, user_id, "home")


async def show_current(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int):
    stack = context.user_data.get("nav_stack") or [("home", None)]
    name, param = stack[-1]
    text, kb = await render_screen(context, user_id, name, param)

    video_file_id = None
    if name == "buyconfirm" and param:
        conn = get_db()
        row = conn.execute(
            "SELECT video_file_id FROM listings WHERE id=? AND status='open'", (param,)
        ).fetchone()
        conn.close()
        if row and row["video_file_id"]:
            video_file_id = row["video_file_id"]

    await set_screen(context, chat_id, text, kb, video_file_id=video_file_id)


async def bump_user_screen(context: ContextTypes.DEFAULT_TYPE, target_user_id: int):
    """يعيد إرسال القائمة الرئيسية في أسفل المحادثة لمستخدم آخر بعد إشعاره،
    حتى لا يحتاج لكتابة /start لإحضار الأزرار مجدداً."""
    udata = context.application.user_data[target_user_id]
    if "screen_msg_id" not in udata and "nav_stack" not in udata:
        return  # المستخدم لم يبدأ التعامل مع البوت من قبل

    chat_id = target_user_id
    old_id = udata.get("screen_msg_id")
    if old_id:
        await safe_delete(context, chat_id, old_id)

    udata["nav_stack"] = [("home", None)]
    try:
        text, kb = await render_screen(context, target_user_id, "home", None)
        msg = await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, reply_markup=kb)
        udata["screen_msg_id"] = msg.message_id
        udata["screen_is_video"] = False
    except Exception as e:
        logger.warning(f"تعذر تحديث شاشة المستخدم {target_user_id}: {e}")


def stash_message(context: ContextTypes.DEFAULT_TYPE, user_id: int, text: str):
    """يخزن نصاً مؤقتاً ليعرضه شاشة 'msg' لمستخدم معيّن."""
    context.application.user_data[user_id]["pending_msg"] = text


async def goto(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int, name: str, param=None, reset: bool = False):
    if reset or "nav_stack" not in context.user_data:
        context.user_data["nav_stack"] = [("home", None)]
    if name != "home":
        context.user_data["nav_stack"].append((name, param))
    else:
        context.user_data["nav_stack"] = [("home", None)]
    await show_current(context, chat_id, user_id)


async def go_back(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int):
    stack = context.user_data.setdefault("nav_stack", [("home", None)])
    if len(stack) > 1:
        stack.pop()
    await show_current(context, chat_id, user_id)


# ---------- أوامر الدخول (تفتح الشاشة الموحّدة) ----------
async def ensure_persistent_menu(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """يثبّت زر القائمة في لوحة المفاتيح السفلية مرة واحدة فقط لكل مستخدم،
    عبر إرسال رسالة صغيرة تحمل الزر ثم حذفها فوراً (الزر يبقى ظاهراً)."""
    if context.user_data.get("menu_kb_set"):
        return
    keyboard = ReplyKeyboardMarkup([[BTN_MENU]], resize_keyboard=True)
    msg = await context.bot.send_message(chat_id, "⌨️", reply_markup=keyboard)
    await safe_delete(context, chat_id, msg.message_id)
    context.user_data["menu_kb_set"] = True


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if update.message:
        await safe_delete(context, chat_id, update.message.message_id)

    # تتبّع الإحالة (referral): رابط بصيغة t.me/<bot>?start=ref_<user_id>.
    # يُسجَّل فقط لمستخدم جديد لم يُسجَّل من قبل، ولا يُسمح بإحالة الشخص لنفسه.
    if context.args:
        arg = context.args[0]
        if arg.startswith("ref_"):
            try:
                referrer_id = int(arg[4:])
            except ValueError:
                referrer_id = None
            if referrer_id and referrer_id != user_id:
                conn = get_db()
                existing = conn.execute("SELECT referred_by FROM users WHERE user_id=?", (user_id,)).fetchone()
                if not existing or existing["referred_by"] is None:
                    conn.execute(
                        "INSERT INTO users (user_id, username, first_name, updated_at, referred_by) VALUES (?, ?, ?, ?, ?) "
                        "ON CONFLICT(user_id) DO UPDATE SET referred_by=COALESCE(users.referred_by, excluded.referred_by)",
                        (user_id, update.effective_user.username, update.effective_user.first_name,
                         datetime.utcnow().isoformat(), referrer_id),
                    )
                    conn.commit()
                conn.close()

    await context.bot.send_chat_action(chat_id=chat_id, action="typing")
    await ensure_persistent_menu(context, chat_id)
    context.user_data["nav_stack"] = [("home", None)]
    await show_current(context, chat_id, user_id)


async def referral_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if update.message:
        await safe_delete(context, chat_id, update.message.message_id)
    me = await context.bot.get_me()
    link = f"https://t.me/{me.username}?start=ref_{user_id}"
    conn = get_db()
    count = conn.execute("SELECT COUNT(*) c FROM users WHERE referred_by=?", (user_id,)).fetchone()["c"]
    conn.close()
    stash_message(
        context, user_id,
        f"🔗 <b>رابط دعوتك:</b>\n<code>{link}</code>\n\n"
        f"👥 عدد من انضم عبر رابطك: {count}",
    )
    await goto(context, chat_id, user_id, "msg")


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if update.message:
        await safe_delete(context, chat_id, update.message.message_id)
    await goto(context, chat_id, user_id, "help")


async def listings_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if update.message:
        await safe_delete(context, chat_id, update.message.message_id)
    game_filter = " ".join(context.args).strip() if context.args else None
    await goto(context, chat_id, user_id, "list", game_filter)


async def my_deals_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if update.message:
        await safe_delete(context, chat_id, update.message.message_id)
    await goto(context, chat_id, user_id, "my")


# ---------- التنقل عبر الأزرار (نظام الشاشة الواحدة) ----------
async def nav_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

    if data == "back":
        await go_back(context, chat_id, user_id)
        return

    # go:screen  أو  go:screen:param
    parts = data.split(":")
    screen = parts[1]
    param = int(parts[2]) if len(parts) > 2 else None
    await goto(context, chat_id, user_id, screen, param)


# ---------- معالج /sell التفاعلي (خطوة بخطوة، يعيد استخدام نفس الرسالة) ----------
def summary_so_far(data: dict) -> str:
    lines = []
    if data.get("game"):
        lines.append(f"🎮 اللعبة: {html.escape(data['game'])}")
    if data.get("level"):
        lines.append(f"⭐ المستوى: {html.escape(data['level'])}")
    if data.get("price") is not None:
        lines.append(f"💰 السعر: ${data['price']}")
    return "\n".join(lines)


async def sell_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if update.message:
        await safe_delete(context, chat_id, update.message.message_id)
    if update.callback_query:
        await update.callback_query.answer()

    if is_blacklisted(user_id):
        stash_message(context, user_id, "🚫 حسابك محظور من استخدام هذا البوت.")
        await goto(context, chat_id, user_id, "msg")
        return ConversationHandler.END

    context.user_data["game"] = None
    context.user_data["level"] = None
    context.user_data["price"] = None

    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton(g, callback_data=f"g:{i}")] for i, g in enumerate(SUPPORTED_GAMES)]
        + [back_row(True)]
    )
    await set_screen(context, chat_id, "🎮 اختر اللعبة:", keyboard)
    return GAME


async def sell_game(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    idx = int(query.data.split(":")[1])
    game = SUPPORTED_GAMES[idx]
    context.user_data["game"] = game

    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("⏭ تخطي", callback_data="skiplevel")], back_row(True)])
    text = f"{summary_so_far(context.user_data)}\n\nما هو المستوى؟"
    await set_screen(context, update.effective_chat.id, text, keyboard)
    return LEVEL


async def sell_level(update: Update, context: ContextTypes.DEFAULT_TYPE):
    level = update.message.text.strip()
    chat_id = update.effective_chat.id
    await safe_delete(context, chat_id, update.message.message_id)

    if contains_link(level) or contains_insult(level):
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("⏭ تخطي", callback_data="skiplevel")], back_row(True)])
        text = f"{summary_so_far(context.user_data)}\n\n⚠️ نص غير مسموح. اكتب المستوى فقط:"
        await set_screen(context, chat_id, text, keyboard)
        return LEVEL

    context.user_data["level"] = level
    text = f"{summary_so_far(context.user_data)}\n\n💵 كم السعر؟ (بين 2$ و 500$)"
    await set_screen(context, chat_id, text, InlineKeyboardMarkup([back_row(True)]))
    return PRICE


async def sell_level_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["level"] = ""
    text = f"{summary_so_far(context.user_data)}\n\n💵 كم السعر؟ (بين 2$ و 500$)"
    await set_screen(context, update.effective_chat.id, text, InlineKeyboardMarkup([back_row(True)]))
    return PRICE


async def sell_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text_in = update.message.text.strip()
    chat_id = update.effective_chat.id
    await safe_delete(context, chat_id, update.message.message_id)

    try:
        price = float(text_in)
    except ValueError:
        text = f"{summary_so_far(context.user_data)}\n\n⚠️ الرجاء إدخال رقم فقط. كم السعر؟ (بين 2$ و 500$)"
        await set_screen(context, chat_id, text, InlineKeyboardMarkup([back_row(True)]))
        return PRICE

    import math
    if not math.isfinite(price) or price < 2 or price > 500:
        text = f"{summary_so_far(context.user_data)}\n\n⚠️ السعر يجب أن يكون بين 2$ و 500$. حاول مرة أخرى:"
        await set_screen(context, chat_id, text, InlineKeyboardMarkup([back_row(True)]))
        return PRICE

    context.user_data["price"] = price
    text = f"{summary_so_far(context.user_data)}\n\n📝 اكتب وصفاً مختصراً للحساب:"
    await set_screen(context, chat_id, text, InlineKeyboardMarkup([back_row(True)]))
    return DESC


async def sell_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    description = update.message.text.strip()
    chat_id = update.effective_chat.id
    await safe_delete(context, chat_id, update.message.message_id)

    if contains_link(description) or contains_insult(description):
        text = f"{summary_so_far(context.user_data)}\n\n⚠️ نص غير مسموح. اكتب وصفاً آخر:"
        await set_screen(context, chat_id, text, InlineKeyboardMarkup([back_row(True)]))
        return DESC

    # بعد الوصف نطلب فيديو واضح للحساب.
    context.user_data["pending_description"] = description
    await set_screen(
        context,
        chat_id,
        f"{summary_so_far(context.user_data)}\n\n"
        "🎥 أرسل الآن <b>فيديو للحساب</b> يظهر فيه الحساب بوضوح.\n"
        "سيتم إرسال الفيديو إلى الأدمن للمراجعة، ولن يظهر العرض للمشترين قبل الموافقة.",
        InlineKeyboardMarkup([back_row(True)]),
    )
    return VIDEO


async def sell_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if update.message and update.message.video:
        video = update.message.video
        context.user_data["video_file_id"] = video.file_id
        description = context.user_data.pop("pending_description", "")
        context.user_data["description"] = description

        await safe_delete(context, chat_id, update.message.message_id)
        await set_screen(
            context,
            chat_id,
            f"{summary_so_far(context.user_data)}\n\n"
            "🔐 أرسل الآن <b>بيانات الدخول للحساب</b> (البريد الإلكتروني أو اليوزر + كلمة المرور).\n"
            "سيتحقق الأدمن منها قبل الموافقة على العرض، ولن تُشارك مع أي مشترٍ قبل إتمام الدفع بالكامل.",
            InlineKeyboardMarkup([back_row(True)]),
        )
        return CREDS

    # إذا أرسل صورة/نص/ملف غير فيديو
    if update.message:
        await safe_delete(context, chat_id, update.message.message_id)
    await set_screen(
        context,
        chat_id,
        "⚠️ أرسل <b>فيديو</b> للحساب فقط، وليس صورة.\n"
        "يجب أن يكون الفيديو واضحاً لإتمام مراجعة الأدمن.",
        InlineKeyboardMarkup([back_row(True)]),
    )
    return VIDEO


async def sell_creds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    credentials = (update.message.text or "").strip()
    await safe_delete(context, chat_id, update.message.message_id)

    if not credentials or len(credentials) < 4:
        await set_screen(
            context,
            chat_id,
            f"{summary_so_far(context.user_data)}\n\n"
            "⚠️ الرجاء إرسال بيانات دخول فعلية (البريد/اليوزر + كلمة المرور):",
            InlineKeyboardMarkup([back_row(True)]),
        )
        return CREDS

    # عنوان محفظة الاستلام يُقرأ من صفحة "محفظتي" المحفوظة مسبقاً، بدل سؤال
    # البائع عنه في كل مرة يبيع فيها. إن لم يحفظه بعد، نوقف هنا فوراً بدل
    # إضاعة كل خطوات النموذج السابقة، ونوجّهه لصفحة المحفظة لحفظه أولاً.
    wallet = get_user_wallet(user_id)
    if not wallet:
        for k in ("game", "level", "price", "video_file_id", "description", "pending_description", "credentials"):
            context.user_data.pop(k, None)
        context.user_data["wallet_prompt_reason"] = "sell"
        await goto(context, chat_id, user_id, "wallet", reset=True)
        return ConversationHandler.END

    context.user_data["credentials"] = credentials
    data = context.user_data
    user = update.effective_user
    description = data.get("description", "")
    video_file_id = data.get("video_file_id")

    flagged_low = check_low_price_flag(data["game"], data["price"])

    conn = get_db()
    cur = conn.execute(
        "INSERT INTO listings "
        "(seller_id, seller_username, price, niche, followers, description, video_file_id, status, created_at, flagged_low, credentials, seller_wallet) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, 'awaiting_approval', ?, ?, ?, ?)",
        (
            user.id,
            user.username or user.first_name,
            data["price"],
            data["game"],
            data.get("level", ""),
            description,
            video_file_id,
            datetime.utcnow().isoformat(),
            1 if flagged_low else 0,
            credentials,
            wallet,
        ),
    )
    conn.commit()
    listing_id = cur.lastrowid
    conn.close()

    await set_screen(
        context,
        chat_id,
        f"⏳ <b>تم إرسال العرض #{listing_id} للمراجعة.</b>\n"
        "لن يظهر في العروض ولن يستطيع أحد شراءه حتى يوافق الأدمن.",
        InlineKeyboardMarkup([[InlineKeyboardButton(BTN_HOME, callback_data="go:home")]]),
    )

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ موافقة", callback_data=f"approve:{listing_id}"),
        InlineKeyboardButton("❌ رفض", callback_data=f"reject:{listing_id}"),
    ]])
    low_price_warning = (
        "\n⚠️ <b>سعر منخفض بشكل غير معتاد — راجع العرض جيداً قبل الموافقة (احتمال احتيال).</b>"
        if flagged_low else ""
    )
    caption = (
        f"🆕 <b>عرض جديد يحتاج موافقة</b> #{listing_id}\n"
        f"البائع: {display_name(user)}\n"
        f"اللعبة: {html.escape(data['game'])}\n"
        f"السعر: ${data['price']}\n"
        f"المستوى: {html.escape(data.get('level') or 'غير محدد')}\n"
        f"الوصف: {html.escape(description)}\n"
        f"💳 محفظة البائع (BEP20): <code>{html.escape(wallet)}</code>"
        f"{low_price_warning}"
    )
    if video_file_id:
        # الفيديو + معلومات العرض + أزرار الموافقة/الرفض في رسالة واحدة.
        await notify_admin_listing_with_video(context, listing_id, video_file_id, caption, keyboard)
    else:
        await notify_admin_with_keyboard(context, caption, keyboard)

    # بيانات الدخول تُرسل في رسالة منفصلة للأدمن للتحقق منها فقط — لا تُدمج
    # في رسالة المراجعة العامة تفادياً لتسريبها بالخطأ.
    await notify_admin(
        context,
        f"🔐 <b>بيانات دخول العرض #{listing_id}</b> (للتحقق فقط — لا تُشارك):\n"
        f"<code>{html.escape(credentials)}</code>",
    )

    for k in ("game", "level", "price", "video_file_id", "description", "pending_description", "credentials"):
        context.user_data.pop(k, None)
    return ConversationHandler.END


async def sell_video_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    for k in ("game", "level", "price", "video_file_id", "description", "pending_description", "credentials"):
        context.user_data.pop(k, None)
    context.user_data["nav_stack"] = [("home", None)]
    await show_current(context, update.effective_chat.id, update.effective_user.id)
    return ConversationHandler.END


async def sell_creds_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    for k in ("game", "level", "price", "video_file_id", "description", "pending_description", "credentials"):
        context.user_data.pop(k, None)
    context.user_data["nav_stack"] = [("home", None)]
    await show_current(context, update.effective_chat.id, update.effective_user.id)
    return ConversationHandler.END


async def sell_wallet_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """زر الرجوع أثناء تعديل عنوان المحفظة (شاشة 'محفظتي')."""
    query = update.callback_query
    await query.answer()
    context.user_data.pop("wallet_prompt_reason", None)
    context.user_data["nav_stack"] = [("home", None)]
    await show_current(context, update.effective_chat.id, update.effective_user.id)
    return ConversationHandler.END


async def wallet_edit_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """يبدأ محادثة قصيرة (خطوة واحدة) لحفظ/تعديل عنوان محفظة المستخدم."""
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id
    current = get_user_wallet(update.effective_user.id)
    prompt = (
        "✏️ أرسل عنوان محفظتك الجديد على شبكة BNB Smart Chain (BEP20):"
        if current else
        "➕ أرسل عنوان محفظتك على شبكة BNB Smart Chain (BEP20) الذي تريد استلام أرباحك عليه بعملة USDT:"
    )
    await set_screen(
        context,
        chat_id,
        f"{prompt}\n\n"
        "⚠️ تأكد أن العنوان يبدأ بـ <code>0x</code> ومن نفس الشبكة (BEP20) — "
        "أي خطأ في العنوان قد يؤدي لضياع الأرباح نهائياً ولا يمكن استرجاعها.",
        InlineKeyboardMarkup([back_row(True)]),
    )
    return WALLET


async def wallet_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    wallet = (update.message.text or "").strip()
    await safe_delete(context, chat_id, update.message.message_id)

    if not is_valid_bep20_address(wallet):
        await set_screen(
            context,
            chat_id,
            "⚠️ هذا لا يبدو عنوان محفظة BEP20 صحيحاً. يجب أن يبدأ بـ <code>0x</code> "
            "ويتكوّن من 42 حرفاً بالإجمالي. أرسل العنوان مرة أخرى:",
            InlineKeyboardMarkup([back_row(True)]),
        )
        return WALLET

    set_user_wallet(user_id, wallet)
    came_from_sell = context.user_data.pop("wallet_prompt_reason", None) == "sell"

    context.user_data["nav_stack"] = [("home", None), ("wallet", None)]
    await show_current(context, chat_id, user_id)
    if came_from_sell:
        await context.bot.send_message(
            chat_id,
            "✅ تم حفظ عنوان محفظتك. تقدر الآن تبدأ البيع مرة أخرى بالضغط على 🆕 بيع أو أمر /sell.",
        )
    return ConversationHandler.END


# ---------- إدارة عروضي (تعديل السعر / الحذف) ----------
LISTING_EDITABLE_STATUSES = ("open", "awaiting_approval")
LISTING_DELETABLE_STATUSES = ("open", "awaiting_approval", "rejected")


async def delask_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """يعرض تأكيداً قبل حذف عرض — الحذف نهائي فلا نسمح بضغطة واحدة فقط."""
    query = update.callback_query
    user_id = query.from_user.id
    listing_id = int(query.data.split(":")[1])

    conn = get_db()
    listing = conn.execute("SELECT * FROM listings WHERE id=?", (listing_id,)).fetchone()
    conn.close()
    if not listing:
        await query.answer("العرض غير موجود.", show_alert=True)
        return
    if listing["seller_id"] != user_id:
        await query.answer("هذا العرض ليس لك.", show_alert=True)
        return
    if listing["status"] not in LISTING_DELETABLE_STATUSES:
        await query.answer("لا يمكن حذف هذا العرض في حالته الحالية.", show_alert=True)
        return

    await query.answer()
    await set_screen(
        context,
        update.effective_chat.id,
        f"🗑 هل أنت متأكد من حذف العرض #{listing_id} نهائياً؟ لا يمكن التراجع عن هذا.",
        InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ نعم، احذف", callback_data=f"delconfirm:{listing_id}"),
            InlineKeyboardButton("❌ إلغاء", callback_data=f"delcancel:{listing_id}"),
        ]]),
    )


async def delconfirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    listing_id = int(query.data.split(":")[1])

    conn = get_db()
    placeholders = ",".join("?" for _ in LISTING_DELETABLE_STATUSES)
    cur = conn.execute(
        f"DELETE FROM listings WHERE id=? AND seller_id=? AND status IN ({placeholders})",
        (listing_id, user_id, *LISTING_DELETABLE_STATUSES),
    )
    conn.commit()
    conn.close()

    if cur.rowcount != 1:
        await query.answer("تعذر حذف العرض — ربما تغيرت حالته للتو (مثلاً بدأ أحدهم صفقة شراء).", show_alert=True)
    else:
        await query.answer("🗑 تم حذف العرض.")

    await goto(context, update.effective_chat.id, user_id, "mylistings", reset=True)


async def delcancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("تم الإلغاء.")
    await goto(context, update.effective_chat.id, query.from_user.id, "mylistings", reset=True)


async def editprice_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    listing_id = int(query.data.split(":")[1])

    conn = get_db()
    listing = conn.execute("SELECT * FROM listings WHERE id=?", (listing_id,)).fetchone()
    conn.close()
    if not listing:
        await query.answer("العرض غير موجود.", show_alert=True)
        return ConversationHandler.END
    if listing["seller_id"] != user_id:
        await query.answer("هذا العرض ليس لك.", show_alert=True)
        return ConversationHandler.END
    if listing["status"] not in LISTING_EDITABLE_STATUSES:
        await query.answer("لا يمكن تعديل سعر هذا العرض في حالته الحالية.", show_alert=True)
        return ConversationHandler.END

    await query.answer()
    context.user_data["editprice_listing_id"] = listing_id
    await set_screen(
        context,
        update.effective_chat.id,
        f"✏️ العرض #{listing_id} — السعر الحالي: ${listing['price']}\n\n"
        "أرسل السعر الجديد (رقم بين 2$ و500$):",
        InlineKeyboardMarkup([back_row(True)]),
    )
    return EDITPRICE


async def editprice_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    listing_id = context.user_data.get("editprice_listing_id")
    text_in = (update.message.text or "").strip()
    await safe_delete(context, chat_id, update.message.message_id)

    if not listing_id:
        context.user_data["nav_stack"] = [("home", None), ("mylistings", None)]
        await show_current(context, chat_id, user_id)
        return ConversationHandler.END

    try:
        price = float(text_in)
    except ValueError:
        await set_screen(
            context, chat_id,
            "⚠️ الرجاء إدخال رقم فقط. كم السعر الجديد؟ (بين 2$ و500$)",
            InlineKeyboardMarkup([back_row(True)]),
        )
        return EDITPRICE

    import math
    if not math.isfinite(price) or price < 2 or price > 500:
        await set_screen(
            context, chat_id,
            "⚠️ السعر يجب أن يكون بين 2$ و500$. حاول مرة أخرى:",
            InlineKeyboardMarkup([back_row(True)]),
        )
        return EDITPRICE

    conn = get_db()
    placeholders = ",".join("?" for _ in LISTING_EDITABLE_STATUSES)
    cur = conn.execute(
        f"UPDATE listings SET price=? WHERE id=? AND seller_id=? AND status IN ({placeholders})",
        (price, listing_id, user_id, *LISTING_EDITABLE_STATUSES),
    )
    conn.commit()
    conn.close()

    context.user_data.pop("editprice_listing_id", None)
    context.user_data["nav_stack"] = [("home", None), ("mylistings", None)]
    await show_current(context, chat_id, user_id)

    if cur.rowcount == 1:
        await context.bot.send_message(chat_id, f"✅ تم تحديث سعر العرض #{listing_id} إلى ${price}.")
    else:
        await context.bot.send_message(chat_id, "⚠️ تعذر تحديث السعر — ربما تغيرت حالة العرض للتو.")
    return ConversationHandler.END


async def editprice_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data.pop("editprice_listing_id", None)
    context.user_data["nav_stack"] = [("home", None), ("mylistings", None)]
    await show_current(context, update.effective_chat.id, update.effective_user.id)
    return ConversationHandler.END


async def sell_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """زر رجوع أثناء معالج البيع — يلغي المعالج ويعود للرئيسية."""
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    for k in ("game", "level", "price", "video_file_id", "description", "pending_description", "credentials"):
        context.user_data.pop(k, None)
    context.user_data["nav_stack"] = [("home", None)]
    await show_current(context, chat_id, user_id)
    return ConversationHandler.END


async def sell_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if update.message:
        await safe_delete(context, chat_id, update.message.message_id)
    for k in ("game", "level", "price", "video_file_id", "description", "pending_description", "credentials"):
        context.user_data.pop(k, None)
    context.user_data["nav_stack"] = [("home", None)]
    await show_current(context, chat_id, user_id)
    return ConversationHandler.END


# ---------- الشراء: اختيار → تأكيد → تنفيذ ----------
async def create_deal(context: ContextTypes.DEFAULT_TYPE, buyer, listing_id: int):
    conn = get_db()
    listing = conn.execute(
        "SELECT * FROM listings WHERE id=? AND status='open'",
        (listing_id,),
    ).fetchone()
    if not listing:
        conn.close()
        return None, "العرض غير موجود."
    if buyer.id == listing["seller_id"]:
        conn.close()
        return None, "لا يمكنك شراء عرضك الخاص."

    # نغلق العرض أولاً بشرط أن يكون لا يزال 'open' — عملية ذرية تمنع شراء نفس
    # العرض مرتين في نفس اللحظة من مستخدمين مختلفين (race condition).
    cur = conn.execute("UPDATE listings SET status='pending' WHERE id=? AND status='open'", (listing_id,))
    if cur.rowcount == 0:
        conn.close()
        return None, "هذا العرض لم يعد متاحاً (تم بيعه أو حجزه للتو)."

    cur = conn.execute(
        "INSERT INTO deals (listing_id, buyer_id, buyer_username, seller_id, price, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (listing_id, buyer.id, buyer.username or buyer.first_name, listing["seller_id"], listing["price"],
         datetime.utcnow().isoformat(), datetime.utcnow().isoformat()),
    )
    conn.commit()
    deal_id = cur.lastrowid
    conn.close()
    return deal_id, listing


async def notify_admin_with_keyboard(context: ContextTypes.DEFAULT_TYPE, text: str, keyboard):
    targets = [ADMIN_GROUP_ID] if ADMIN_GROUP_ID else list(get_admin_ids())
    for target in targets:
        try:
            await context.bot.send_message(chat_id=target, text=text, reply_markup=keyboard)
        except Exception as e:
            logger.warning(f"تعذر إرسال إشعار الأدمن ({target}): {e}")


async def notify_admin_listing_with_video(
    context: ContextTypes.DEFAULT_TYPE,
    listing_id: int,
    video_file_id: str,
    caption: str,
    keyboard: InlineKeyboardMarkup,
):
    """يرسل فيديو العرض مرفقاً برسالة المراجعة نفسها (نفس الرسالة تحمل
    الفيديو + المعلومات + أزرار الموافقة/الرفض)، مع نسخة احتياطية نصية
    فقط في حال فشل إرسال الفيديو (حجم كبير جداً، مشكلة شبكة...) حتى لا
    تضيع الأزرار على الأدمن."""
    targets = [ADMIN_GROUP_ID] if ADMIN_GROUP_ID else list(get_admin_ids())
    for target in targets:
        try:
            await context.bot.send_video(
                chat_id=target,
                video=video_file_id,
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
        except Exception as e:
            logger.warning(f"تعذر إرسال فيديو العرض #{listing_id} للأدمن {target}: {e}")
            try:
                await context.bot.send_message(
                    chat_id=target,
                    text=caption + f"\n\n⚠️ تعذر إرفاق الفيديو ({html.escape(str(e))}).\n"
                                     "غالباً السبب: البوت ليس أدمن في هذه المجموعة، أو صلاحيات "
                                     "المجموعة تمنع الأعضاء العاديين من إرسال الوسائط (فيديو/صور). "
                                     "اجعل البوت أدمن أو فعّل صلاحية إرسال الوسائط له.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=keyboard,
                )
            except Exception as e2:
                logger.warning(f"فشل الإرسال الاحتياطي أيضاً للأدمن {target}: {e2}")


async def announce_new_deal(context, deal_id, listing_id, listing, buyer):
    await notify(context, listing["seller_id"],
                 f"الصفقة #{deal_id}: يوجد مشتري لعرضك #{listing_id} ({display_name(buyer)}). "
                 f"لا تسلّم الحساب قبل تأكيد الأدمن استلام الدفع.")
    await bump_user_screen(context, listing["seller_id"])

    fee = calc_fee(listing["price"])
    total_to_pay = round(listing["price"] + fee, 2)
    # الصفقة تبدأ بدون أدمن مسؤول عنها — يجب على أحد الأدمنية استلامها أولاً
    # (نظام "مهمة واحدة في كل مرة") قبل ظهور أزرار تأكيد الدفع/التحويل.
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🙋 استلام المهمة", callback_data=f"claim:{deal_id}")]])
    await notify_admin_with_keyboard(
        context,
        f"🆕 صفقة جديدة #{deal_id}\n"
        f"المشتري: {display_name(buyer)}\n"
        f"العرض: #{listing_id} — {listing['niche']}\n"
        f"السعر: ${listing['price']:.2f} + رسوم ${fee:.2f} = المطلوب استلامه: ${total_to_pay:.2f}\n\n"
        f"بانتظار الدفع. اضغط 🙋 استلام المهمة لتتولى متابعة هذه الصفقة (أزرار التأكيد تظهر بعدها).",
        keyboard,
    )


async def buy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    args = context.args

    if len(args) == 0:
        if update.message:
            await safe_delete(context, chat_id, update.message.message_id)
        await goto(context, chat_id, user_id, "buypick")
        return

    if len(args) != 1:
        await update.message.reply_text("الاستخدام: /buy <رقم_العرض>")
        return
    try:
        listing_id = int(args[0])
    except ValueError:
        await update.message.reply_text("رقم العرض يجب أن يكون رقماً.")
        return

    buyer = update.effective_user
    deal_id, result = await create_deal(context, buyer, listing_id)
    if deal_id is None:
        await update.message.reply_text(result)
        return
    listing = result
    fee = calc_fee(listing["price"])
    total_to_pay = round(listing["price"] + fee, 2)
    await update.message.reply_text(
        f"✅ بدأت الصفقة #{deal_id} للعرض #{listing_id} — المطلوب دفعه: ${total_to_pay:.2f} "
        f"(السعر ${listing['price']:.2f} + رسوم الوساطة ${fee:.2f}).\n\n"
        f"الخطوة التالية: أرسل الدفعة عن طريق USDT (BEP20) إلى العنوان:\n"
        f"<code>{WALLET_ADDRESS}</code>\n"
        f"(اضغط على العنوان لنسخه تلقائياً)\n"
        f"💠 BNB أو BUSD (BEP20) مقبولة أيضاً — أخبر الأدمن بالعملة المُرسَلة.\n\n"
        f"ثم انتظر تأكيد الأدمن.\n"
        f"تابع الحالة عبر: /deal {deal_id}",
        parse_mode=ParseMode.HTML,
    )
    await announce_new_deal(context, deal_id, listing_id, listing, buyer)


async def doconfirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    listing_id = int(query.data.split(":")[1])
    buyer = update.effective_user
    chat_id = update.effective_chat.id

    if is_blacklisted(buyer.id):
        await set_screen(
            context, chat_id, "🚫 حسابك محظور من استخدام هذا البوت.",
            InlineKeyboardMarkup([[InlineKeyboardButton(BTN_HOME, callback_data="go:home")]]),
        )
        return

    deal_id, result = await create_deal(context, buyer, listing_id)
    if deal_id is None:
        text = result
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(BTN_HOME, callback_data="go:home")]])
        await set_screen(context, chat_id, text, keyboard)
        return
    listing = result

    fee = calc_fee(listing["price"])
    total_to_pay = round(listing["price"] + fee, 2)
    text = (
        f"✅ بدأت الصفقة #{deal_id} للعرض #{listing_id} — المطلوب دفعه: ${total_to_pay:.2f} "
        f"(السعر ${listing['price']:.2f} + رسوم الوساطة ${fee:.2f}).\n\n"
        f"الخطوة التالية: أرسل الدفعة عن طريق USDT (BEP20) إلى العنوان:\n"
        f"<code>{WALLET_ADDRESS}</code>\n"
        f"(اضغط على العنوان لنسخه تلقائياً)\n"
        f"💠 BNB أو BUSD (BEP20) مقبولة أيضاً — أخبر الأدمن بالعملة المُرسَلة.\n\n"
        f"ثم انتظر تأكيد الأدمن.\n"
        f"تابع الحالة عبر: /deal {deal_id}"
    )
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(BTN_HOME, callback_data="go:home")]])
    await set_screen(context, chat_id, text, keyboard)
    await announce_new_deal(context, deal_id, listing_id, listing, buyer)


# ---------- أوامر الصفقات (تبقى كأوامر عادية لأنها ترتبط برقم صفقة) ----------
async def mark_paid(context: ContextTypes.DEFAULT_TYPE, deal_id: int):
    """ينفذ تأكيد استلام الدفع. يعيد (ok: bool, message: str)."""
    conn = get_db()
    deal = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()
    if not deal or deal["state"] != "pending_payment":
        conn.close()
        return False, "الصفقة غير موجودة أو ليست بانتظار الدفع."
    if deal["disputed"]:
        conn.close()
        return False, f"⚠️ الصفقة #{deal_id} عليها نزاع مفتوح. استخدم /resolve {deal_id} بعد حله أولاً."

    # تأكيد الدفع يجب أن يكون انتقالاً ذرياً من pending_payment فقط.
    # هذا يمنع تأكيد صفقة مرتين أو تأكيد صفقة بعد إلغائها.
    cur = conn.execute(
        "UPDATE deals SET state='payment_received', updated_at=? "
        "WHERE id=? AND state='pending_payment'",
        (datetime.utcnow().isoformat(), deal_id),
    )
    if cur.rowcount != 1:
        conn.close()
        return False, "الصفقة غير موجودة أو تغيرت حالتها بالفعل."

    conn.commit()
    conn.close()

    await notify(context, deal["seller_id"],
                 f"الصفقة #{deal_id}: تم تأكيد الدفع. يمكنك الآن تسليم الحساب للمشتري، "
                 f"ثم أكّد بالأمر /ho {deal_id}.")
    await bump_user_screen(context, deal["seller_id"])
    await notify(context, deal["buyer_id"], f"الصفقة #{deal_id}: تم تأكيد دفعتك. بانتظار تسليم الحساب من البائع.")
    await bump_user_screen(context, deal["buyer_id"])
    return True, f"تم تأكيد استلام الدفع للصفقة #{deal_id}."


async def paid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("هذا الأمر للأدمن فقط.")
        return
    await safe_delete(context, chat_id, update.message.message_id)
    deal_id = _parse_single_id(update, context)
    if deal_id is None:
        stash_message(context, user_id, "الاستخدام: /pay <رقم_الصفقة>")
        await goto(context, chat_id, user_id, "msg")
        return
    ok, msg = await mark_paid(context, deal_id)
    if ok:
        await goto(context, chat_id, user_id, "dealstatus", deal_id)
    else:
        stash_message(context, user_id, f"⚠️ {msg}")
        await goto(context, chat_id, user_id, "msg")


async def admpay_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not is_admin(query.from_user.id):
        await query.answer("هذا الزر للأدمن فقط.", show_alert=True)
        return
    deal_id = int(query.data.split(":")[1])
    conn = get_db()
    deal = conn.execute("SELECT claimed_by FROM deals WHERE id=?", (deal_id,)).fetchone()
    conn.close()
    if not deal or deal["claimed_by"] != query.from_user.id:
        await query.answer("يجب استلام هذه المهمة أولاً (🙋 استلام المهمة).", show_alert=True)
        return
    await query.answer()
    ok, msg = await mark_paid(context, deal_id)
    await query.edit_message_text(msg if ok else f"⚠️ {msg}")


async def checkwallet_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """زر مساعد للأدمن: يجلب آخر تحويلات USDT (BEP20) الواردة للمحفظة من
    BscScan ويعرضها، مع تمييز أي تحويل يطابق مبلغ الصفقة المطلوب. لا يقوم
    بأي تأكيد تلقائي — الأدمن يبقى من يضغط زر "استلمت الدفع" يدوياً بعد
    التحقق بنفسه من المبلغ والهاش."""
    query = update.callback_query
    if not is_admin(query.from_user.id):
        await query.answer("هذا الزر للأدمن فقط.", show_alert=True)
        return
    await query.answer("جاري الفحص...")
    deal_id = int(query.data.split(":")[1])

    conn = get_db()
    deal = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()
    conn.close()
    expected_amount = None
    if deal:
        expected_amount = round(deal["price"] + calc_fee(deal["price"]), 2)

    ok, result = await check_wallet_usdt()
    if not ok:
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=f"⚠️ تعذر فحص المحفظة: {html.escape(result)}",
            parse_mode=ParseMode.HTML,
        )
        return

    text = format_wallet_check(result, expected_amount=expected_amount)
    if expected_amount is not None:
        text = f"الصفقة #{deal_id} — المبلغ المطلوب: {expected_amount:.2f} USDT\n\n" + text
    await context.bot.send_message(chat_id=query.message.chat_id, text=text, parse_mode=ParseMode.HTML)


async def handover(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    await safe_delete(context, chat_id, update.message.message_id)

    deal_id = _parse_single_id(update, context)
    if deal_id is None:
        stash_message(context, user_id, "الاستخدام: /ho <رقم_الصفقة>")
        await goto(context, chat_id, user_id, "msg")
        return

    conn = get_db()
    deal = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()
    if not deal:
        conn.close()
        stash_message(context, user_id, "الصفقة غير موجودة.")
        await goto(context, chat_id, user_id, "msg")
        return
    if user_id != deal["seller_id"]:
        conn.close()
        stash_message(context, user_id, "فقط بائع هذه الصفقة يمكنه تأكيد التسليم.")
        await goto(context, chat_id, user_id, "msg")
        return
    if deal["state"] != "payment_received":
        conn.close()
        stash_message(context, user_id, "لم يتم تأكيد الدفع بعد، لا يمكن التسليم.")
        await goto(context, chat_id, user_id, "msg")
        return
    if deal["disputed"]:
        conn.close()
        stash_message(context, user_id, f"⚠️ الصفقة #{deal_id} عليها نزاع مفتوح، بانتظار مراجعة الأدمن.")
        await goto(context, chat_id, user_id, "msg")
        return

    # انتقال ذري لمنع تنفيذ التسليم مرتين إذا وصلت الطلبات متقاربة.
    cur = conn.execute(
        "UPDATE deals SET state='handed_over', updated_at=? "
        "WHERE id=? AND state='payment_received'",
        (datetime.utcnow().isoformat(), deal_id),
    )
    if cur.rowcount != 1:
        conn.close()
        stash_message(context, user_id, "⚠️ تغيرت حالة الصفقة للتو، أعد المحاولة.")
        await goto(context, chat_id, user_id, "msg")
        return

    conn.commit()
    listing = conn.execute("SELECT credentials FROM listings WHERE id=?", (deal["listing_id"],)).fetchone()
    conn.close()

    await goto(context, chat_id, user_id, "dealstatus", deal_id)

    credentials = listing["credentials"] if listing else None
    if credentials:
        try:
            await context.bot.send_message(
                chat_id=deal["buyer_id"],
                text=(
                    f"📦 الصفقة #{deal_id}: البائع أكّد التسليم. هذه بيانات الدخول للحساب "
                    f"(تم التحقق منها من قبل الأدمن قبل الموافقة على العرض):\n"
                    f"<code>{html.escape(credentials)}</code>\n\n"
                    f"تحقق من الوصول (تسجيل الدخول، معلومات الاسترجاع)، ثم افتح /deal {deal_id} واضغط "
                    f"زر التأكيد قبل أن يحوّل الأدمن المبلغ."
                ),
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.warning(f"تعذر إرسال بيانات الحساب للمشتري في الصفقة {deal_id}: {e}")
    else:
        # عروض قديمة أُنشئت قبل ميزة بيانات الدخول — لا بيانات مخزّنة، التسليم يبقى يدوياً كما كان.
        await notify(context, deal["buyer_id"],
                     f"الصفقة #{deal_id}: يقول البائع إن الحساب تم تسليمه (لا توجد بيانات دخول مخزّنة لهذا "
                     f"العرض، تواصل مع البائع مباشرة للحصول عليها). تحقق من الوصول، ثم افتح /deal {deal_id} "
                     f"واضغط زر التأكيد قبل أن يحوّل الأدمن المبلغ.")
    await bump_user_screen(context, deal["buyer_id"])
    await notify_admin(
        context,
        f"📦 الصفقة #{deal_id}: تم تسليم الحساب من البائع.\n"
        f"بانتظار تأكيد المشتري للاستلام قبل أن يظهر زر تحويل المبلغ.",
    )


async def do_release(context: ContextTypes.DEFAULT_TYPE, deal_id: int):
    """ينفذ تحويل المبلغ وإغلاق الصفقة. يعيد (ok: bool, message: str)."""
    conn = get_db()
    deal = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()
    if not deal or deal["state"] != "handed_over":
        conn.close()
        return False, "الصفقة غير موجودة أو غير جاهزة للتحويل (يجب أن تكون تم تسليمها)."
    if deal["disputed"]:
        conn.close()
        return False, f"⚠️ الصفقة #{deal_id} عليها نزاع مفتوح. استخدم /resolve {deal_id} بعد حله أولاً."
    if not deal["buyer_confirmed"]:
        conn.close()
        return False, "⚠️ لم يؤكد المشتري استلام الحساب بعد. لا يمكن تحويل المبلغ قبل ذلك."

    # الرسوم يدفعها المشتري فوق سعر العرض (السعر + الرسوم عند الدفع)، لذلك
    # يستلم البائع سعر العرض بالكامل هنا دون أي خصم.
    fee = calc_fee(deal["price"])
    payout = deal["price"]

    # إغلاق الصفقة يجب أن يحدث مرة واحدة فقط من حالة handed_over.
    cur = conn.execute(
        "UPDATE deals SET state='completed', updated_at=? "
        "WHERE id=? AND state='handed_over'",
        (datetime.utcnow().isoformat(), deal_id),
    )
    if cur.rowcount != 1:
        conn.close()
        return False, "تعذر إغلاق الصفقة؛ ربما تم تحويل المبلغ أو تغيير الحالة مسبقاً."

    listing_cur = conn.execute(
        "UPDATE listings SET status='sold' WHERE id=? AND status='pending'",
        (deal["listing_id"],),
    )
    if listing_cur.rowcount != 1:
        conn.rollback()
        conn.close()
        return False, "تعذر إغلاق الصفقة لأن حالة العرض غير متوقعة."

    seller_wallet_row = conn.execute(
        "SELECT seller_wallet FROM listings WHERE id=?", (deal["listing_id"],)
    ).fetchone()
    seller_wallet = seller_wallet_row["seller_wallet"] if seller_wallet_row else None

    conn.commit()
    conn.close()

    await notify(context, deal["seller_id"], f"الصفقة #{deal_id} اكتملت. سيتم تحويل ${payout} لك بالكامل (الرسوم كانت على المشتري ${fee}).")
    await bump_user_screen(context, deal["seller_id"])
    await notify(context, deal["buyer_id"], f"الصفقة #{deal_id} اكتملت. شكراً لاستخدامك الخدمة.")
    await bump_user_screen(context, deal["buyer_id"])
    await prompt_rating(context, deal_id, deal["buyer_id"], deal["seller_id"])
    await prompt_rating(context, deal_id, deal["seller_id"], deal["buyer_id"])
    wallet_note = f"\n💳 حوّل إلى: {seller_wallet}" if seller_wallet else "\n⚠️ لا يوجد عنوان محفظة مسجّل لهذا البائع — تواصل معه مباشرة."
    return True, f"تم إغلاق الصفقة #{deal_id}. المبلغ المستحق للبائع: ${payout} (رسوم الوساطة من المشتري: ${fee}).{wallet_note}"


async def prompt_rating(context: ContextTypes.DEFAULT_TYPE, deal_id: int, rater_id: int, rated_id: int):
    """يرسل لطرف الصفقة طلب تقييم الطرف الآخر بعد اكتمالها (1-5 نجوم)."""
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("⭐" * n, callback_data=f"rate:{deal_id}:{rated_id}:{n}") for n in range(1, 6)
    ]])
    try:
        await context.bot.send_message(
            chat_id=rater_id,
            text=f"🙏 قيّم تجربتك مع الطرف الآخر في الصفقة #{deal_id}:",
            reply_markup=keyboard,
        )
    except Exception as e:
        logger.warning(f"تعذر إرسال طلب تقييم لـ {rater_id}: {e}")


async def release(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("هذا الأمر للأدمن فقط.")
        return
    await safe_delete(context, chat_id, update.message.message_id)
    deal_id = _parse_single_id(update, context)
    if deal_id is None:
        stash_message(context, user_id, "الاستخدام: /rel <رقم_الصفقة>")
        await goto(context, chat_id, user_id, "msg")
        return
    ok, msg = await do_release(context, deal_id)
    if ok:
        await goto(context, chat_id, user_id, "dealstatus", deal_id)
    else:
        stash_message(context, user_id, f"⚠️ {msg}")
        await goto(context, chat_id, user_id, "msg")


async def admrel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not is_admin(query.from_user.id):
        await query.answer("هذا الزر للأدمن فقط.", show_alert=True)
        return
    deal_id = int(query.data.split(":")[1])
    conn = get_db()
    deal = conn.execute("SELECT claimed_by FROM deals WHERE id=?", (deal_id,)).fetchone()
    conn.close()
    if not deal or deal["claimed_by"] != query.from_user.id:
        await query.answer("يجب أن تكون مستلماً لهذه المهمة لتحويل المبلغ.", show_alert=True)
        return
    await query.answer()
    ok, msg = await do_release(context, deal_id)
    await query.edit_message_text(msg if ok else f"⚠️ {msg}")
    if ok:
        await release_claim(deal_id)


async def release_claim(deal_id: int):
    """يحرر ارتباط الصفقة بالأدمن الذي استلمها (بعد اكتمالها أو عند تسليمها لغيره)."""
    conn = get_db()
    conn.execute("UPDATE deals SET claimed_by=NULL, claimed_at=NULL WHERE id=?", (deal_id,))
    conn.commit()
    conn.close()


async def claim_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """يستلم أدمن مهمة صفقة معيّنة. لا يمكن لأدمن استلام أكثر من مهمة واحدة
    نشطة في نفس الوقت — يجب إنهاء الحالية أولاً (بإكمالها أو بالضغط على
    🏁 إنهاء المهمة)."""
    query = update.callback_query
    admin_id = query.from_user.id
    if not is_admin(admin_id):
        await query.answer("هذا الزر للأدمن فقط.", show_alert=True)
        return
    deal_id = int(query.data.split(":")[1])

    active = get_admin_active_deal(admin_id)
    if active and active != deal_id:
        await query.answer(f"لديك مهمة أخرى قيد التنفيذ (الصفقة #{active}). أنهها أولاً قبل استلام مهمة جديدة.", show_alert=True)
        return

    conn = get_db()
    deal = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()
    if not deal:
        conn.close()
        await query.answer("الصفقة غير موجودة.", show_alert=True)
        return
    if deal["claimed_by"] and deal["claimed_by"] != admin_id:
        conn.close()
        await query.answer("تم استلام هذه المهمة من قِبل أدمن آخر بالفعل.", show_alert=True)
        return

    cur = conn.execute(
        "UPDATE deals SET claimed_by=?, claimed_at=? WHERE id=? AND (claimed_by IS NULL OR claimed_by=?)",
        (admin_id, datetime.utcnow().isoformat(), deal_id, admin_id),
    )
    if cur.rowcount != 1:
        conn.close()
        await query.answer("تعذر استلام المهمة، أعد المحاولة.", show_alert=True)
        return
    conn.commit()
    conn.close()

    await query.answer("🙋 تم استلام المهمة.")

    fee = calc_fee(deal["price"])
    total_to_pay = round(deal["price"] + fee, 2)
    admin_name = display_name(query.from_user)
    action_rows = []
    if deal["state"] == "pending_payment":
        action_rows.append([InlineKeyboardButton("✅ استلمت الدفع — تأكيد", callback_data=f"admpay:{deal_id}")])
        action_rows.append([InlineKeyboardButton("🔍 فحص المحفظة (USDT)", callback_data=f"checkwallet:{deal_id}")])
    elif deal["state"] == "handed_over":
        action_rows.append([InlineKeyboardButton("✅ استلمت الدفع — تحويل المبلغ للبائع", callback_data=f"admrel:{deal_id}")])
    action_rows.append([InlineKeyboardButton("🏁 إنهاء المهمة", callback_data=f"unclaim:{deal_id}")])

    try:
        await query.edit_message_text(
            f"🆕 صفقة #{deal_id} — 🙋 مستلَمة من: {admin_name}\n"
            f"المطلوب استلامه: ${total_to_pay:.2f}\n"
            f"استخدم الأزرار بالأسفل لمتابعة الصفقة.",
            reply_markup=InlineKeyboardMarkup(action_rows),
        )
    except Exception as e:
        logger.warning(f"تعذر تحديث رسالة استلام المهمة للصفقة {deal_id}: {e}")


async def unclaim_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """يترك الأدمن المهمة طوعياً دون إكمالها (مثلاً لتسليمها لأدمن آخر)،
    ما يتيح له استلام مهمة جديدة، ويعيد الصفقة قابلة للاستلام من جديد."""
    query = update.callback_query
    admin_id = query.from_user.id
    if not is_admin(admin_id):
        await query.answer("هذا الزر للأدمن فقط.", show_alert=True)
        return
    deal_id = int(query.data.split(":")[1])

    conn = get_db()
    deal = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()
    if not deal:
        conn.close()
        await query.answer("الصفقة غير موجودة.", show_alert=True)
        return
    if deal["claimed_by"] != admin_id:
        conn.close()
        await query.answer("أنت لست من استلم هذه المهمة.", show_alert=True)
        return
    conn.execute("UPDATE deals SET claimed_by=NULL, claimed_at=NULL WHERE id=?", (deal_id,))
    conn.commit()
    conn.close()

    await query.answer("🏁 تم إنهاء استلامك للمهمة.")
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🙋 استلام المهمة", callback_data=f"claim:{deal_id}")]])
    try:
        await query.edit_message_text(
            f"صفقة #{deal_id} — أصبحت متاحة للاستلام من جديد.",
            reply_markup=keyboard,
        )
    except Exception as e:
        logger.warning(f"تعذر تحديث رسالة إنهاء المهمة للصفقة {deal_id}: {e}")


async def buyerconfirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """يضغطها المشتري بعد التحقق من الحساب — يفتح لأول مرة إمكانية تحويل
    المبلغ للبائع من طرف الأدمن."""
    query = update.callback_query
    user_id = query.from_user.id
    deal_id = int(query.data.split(":")[1])

    conn = get_db()
    deal = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()
    if not deal:
        conn.close()
        await query.answer("الصفقة غير موجودة.", show_alert=True)
        return
    if user_id != deal["buyer_id"]:
        conn.close()
        await query.answer("فقط مشتري هذه الصفقة يمكنه تأكيد الاستلام.", show_alert=True)
        return
    if deal["state"] != "handed_over":
        conn.close()
        await query.answer("لا يمكن التأكيد في هذه المرحلة.", show_alert=True)
        return
    if deal["disputed"]:
        conn.close()
        await query.answer("هذه الصفقة عليها نزاع مفتوح حالياً.", show_alert=True)
        return

    cur = conn.execute(
        "UPDATE deals SET buyer_confirmed=1, updated_at=? WHERE id=? AND state='handed_over'",
        (datetime.utcnow().isoformat(), deal_id),
    )
    conn.commit()
    conn.close()
    if cur.rowcount != 1:
        await query.answer("⚠️ تعذر التأكيد، أعد المحاولة.", show_alert=True)
        return

    await query.answer("✅ تم التأكيد، شكراً.")
    await goto(context, update.effective_chat.id, user_id, "dealstatus", deal_id)

    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("✅ استلمت الدفع — تحويل المبلغ للبائع", callback_data=f"admrel:{deal_id}")]])
    await notify_admin_with_keyboard(
        context,
        f"✅ الصفقة #{deal_id}: المشتري أكّد استلام الحساب والتحقق منه.\n"
        f"اضغط الزر بالأسفل لتحويل المبلغ للبائع.",
        keyboard,
    )


async def dispute_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """يفتح أي طرف من طرفي الصفقة نزاعاً يجمّد الإجراءات حتى يراجعها الأدمن."""
    query = update.callback_query
    user_id = query.from_user.id
    deal_id = int(query.data.split(":")[1])

    conn = get_db()
    deal = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()
    if not deal:
        conn.close()
        await query.answer("الصفقة غير موجودة.", show_alert=True)
        return
    if user_id not in (deal["buyer_id"], deal["seller_id"]):
        conn.close()
        await query.answer("لست طرفاً في هذه الصفقة.", show_alert=True)
        return
    if deal["state"] in ("completed", "cancelled"):
        conn.close()
        await query.answer("لا يمكن فتح نزاع على صفقة منتهية.", show_alert=True)
        return
    if deal["disputed"]:
        conn.close()
        await query.answer("يوجد نزاع مفتوح بالفعل على هذه الصفقة.", show_alert=True)
        return

    conn.execute(
        "UPDATE deals SET disputed=1, updated_at=? WHERE id=?",
        (datetime.utcnow().isoformat(), deal_id),
    )
    conn.commit()
    conn.close()

    await query.answer("⚠️ تم فتح نزاع، سيتواصل معك الأدمن.", show_alert=True)
    await goto(context, update.effective_chat.id, user_id, "dealstatus", deal_id)

    other_id = deal["seller_id"] if user_id == deal["buyer_id"] else deal["buyer_id"]
    who = "المشتري" if user_id == deal["buyer_id"] else "البائع"
    await notify(context, other_id, f"⚠️ {who} فتح نزاعاً على الصفقة #{deal_id}. الإجراءات مجمّدة بانتظار الأدمن.")
    await bump_user_screen(context, other_id)
    await notify_admin(
        context,
        f"🔴 نزاع مفتوح على الصفقة #{deal_id} بواسطة {who} ({user_id}).\n"
        f"تواصل مع الطرفين، ثم استخدم /resolve {deal_id} بعد الحل.",
    )



async def rate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """يسجل تقييم أحد طرفي صفقة مكتملة للطرف الآخر (1-5 نجوم)، مرة واحدة فقط لكل طرف."""
    query = update.callback_query
    rater_id = query.from_user.id
    _, deal_id_s, rated_id_s, stars_s = query.data.split(":")
    deal_id, rated_id, stars = int(deal_id_s), int(rated_id_s), int(stars_s)

    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO ratings (deal_id, rater_id, rated_id, stars, created_at) VALUES (?, ?, ?, ?, ?)",
            (deal_id, rater_id, rated_id, stars, datetime.utcnow().isoformat()),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        await query.answer("لقد قيّمت هذه الصفقة من قبل.", show_alert=True)
        return
    conn.close()

    await query.answer("🙏 شكراً على تقييمك!")
    try:
        await query.edit_message_text(f"✅ تم تسجيل تقييمك: {'⭐' * stars}")
    except Exception:
        pass


async def edit_review_message(query, text: str):
    """يحرر رسالة مراجعة العرض في مجموعة الأدمن. قد تكون هذه الرسالة فيديو
    (النص caption) أو رسالة نصية عادية (fallback إذا فشل إرفاق الفيديو)،
    لذلك لا يصح استخدام edit_message_text على رسالة فيديو دائماً."""
    try:
        if query.message and (query.message.video or query.message.photo or query.message.document):
            await query.edit_message_caption(caption=text)
        else:
            await query.edit_message_text(text)
    except Exception as e:
        logger.warning(f"تعذر تحرير رسالة المراجعة: {e}")


async def approve_listing_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not is_admin(query.from_user.id):
        await query.answer("هذا الزر للأدمن فقط.", show_alert=True)
        return
    await query.answer()

    listing_id = int(query.data.split(":")[1])
    conn = get_db()
    listing = conn.execute("SELECT * FROM listings WHERE id=?", (listing_id,)).fetchone()
    if not listing:
        conn.close()
        await edit_review_message(query, "⚠️ العرض غير موجود.")
        return
    if listing["status"] != "awaiting_approval":
        conn.close()
        await edit_review_message(query, f"ℹ️ العرض #{listing_id} حالته الحالية: {listing['status']}")
        return

    now = datetime.utcnow().isoformat()
    cur = conn.execute(
        "UPDATE listings SET status='open', approved_at=?, approved_by=?, rejection_reason=NULL "
        "WHERE id=? AND status='awaiting_approval'",
        (now, query.from_user.id, listing_id),
    )
    conn.commit()
    conn.close()

    if cur.rowcount != 1:
        await edit_review_message(query, "⚠️ تغيرت حالة العرض أثناء المراجعة.")
        return

    await edit_review_message(query, f"✅ تمت الموافقة على العرض #{listing_id}. أصبح متاحاً للمشترين.")
    await notify(
        context,
        listing["seller_id"],
        f"✅ تمت الموافقة على عرضك #{listing_id} وأصبح متاحاً للمشترين."
    )
    await bump_user_screen(context, listing["seller_id"])


async def reject_listing_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not is_admin(query.from_user.id):
        await query.answer("هذا الزر للأدمن فقط.", show_alert=True)
        return
    await query.answer()

    listing_id = int(query.data.split(":")[1])
    conn = get_db()
    listing = conn.execute("SELECT * FROM listings WHERE id=?", (listing_id,)).fetchone()
    if not listing:
        conn.close()
        await edit_review_message(query, "⚠️ العرض غير موجود.")
        return
    if listing["status"] != "awaiting_approval":
        conn.close()
        await edit_review_message(query, f"ℹ️ العرض #{listing_id} حالته الحالية: {listing['status']}")
        return

    cur = conn.execute(
        "UPDATE listings SET status='rejected', approved_by=?, rejection_reason=? "
        "WHERE id=? AND status='awaiting_approval'",
        (query.from_user.id, "رفض من الأدمن", listing_id),
    )
    conn.commit()
    conn.close()

    if cur.rowcount != 1:
        await edit_review_message(query, "⚠️ تغيرت حالة العرض أثناء المراجعة.")
        return

    await edit_review_message(query, f"❌ تم رفض العرض #{listing_id}.")
    await notify(
        context,
        listing["seller_id"],
        f"❌ تم رفض عرضك #{listing_id} من الأدمن. يمكنك التواصل مع الإدارة لمعرفة السبب."
    )
    await bump_user_screen(context, listing["seller_id"])



async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    await safe_delete(context, chat_id, update.message.message_id)

    deal_id = _parse_single_id(update, context)
    if deal_id is None:
        stash_message(context, user_id, "الاستخدام: /cancel <رقم_الصفقة>")
        await goto(context, chat_id, user_id, "msg")
        return
    conn = get_db()
    deal = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()
    if not deal:
        conn.close()
        stash_message(context, user_id, "الصفقة غير موجودة.")
        await goto(context, chat_id, user_id, "msg")
        return
    if user_id not in (deal["buyer_id"], deal["seller_id"]) and not is_admin(user_id):
        conn.close()
        stash_message(context, user_id, "أنت لست طرفاً في هذه الصفقة.")
        await goto(context, chat_id, user_id, "msg")
        return
    # لا يمكن إلغاء الصفقة بعد تأكيد الدفع أو تسليم الحساب.
    # الإلغاء الذاتي مسموح فقط أثناء انتظار الدفع.
    if deal["state"] in ("completed", "cancelled"):
        conn.close()
        stash_message(context, user_id, "هذه الصفقة مغلقة بالفعل.")
        await goto(context, chat_id, user_id, "msg")
        return

    if deal["state"] != "pending_payment":
        conn.close()
        stash_message(
            context,
            user_id,
            "⚠️ لا يمكن إلغاء الصفقة بعد تأكيد الدفع أو بعد تسليم الحساب. "
            "تواصل مع الأدمن إذا كانت هناك مشكلة."
        )
        await goto(context, chat_id, user_id, "msg")
        return
    if deal["disputed"] and not is_admin(user_id):
        conn.close()
        stash_message(context, user_id, f"⚠️ الصفقة #{deal_id} عليها نزاع مفتوح، تواصل مع الأدمن.")
        await goto(context, chat_id, user_id, "msg")
        return

    # تحديث مشروط بالحالة الحالية لمنع سباق بين الإلغاء وتأكيد الدفع.
    cur = conn.execute(
        "UPDATE deals SET state='cancelled', updated_at=? "
        "WHERE id=? AND state='pending_payment'",
        (datetime.utcnow().isoformat(), deal_id),
    )
    if cur.rowcount != 1:
        conn.close()
        stash_message(context, user_id, "⚠️ تعذر إلغاء الصفقة؛ ربما تغيرت حالتها للتو.")
        await goto(context, chat_id, user_id, "msg")
        return

    listing_cur = conn.execute(
        "UPDATE listings SET status='open' WHERE id=? AND status='pending'",
        (deal["listing_id"],),
    )
    if listing_cur.rowcount != 1:
        conn.rollback()
        conn.close()
        stash_message(context, user_id, "⚠️ لم يتم إلغاء الصفقة لأن حالة العرض غير متوقعة.")
        await goto(context, chat_id, user_id, "msg")
        return

    conn.commit()
    conn.close()

    await goto(context, chat_id, user_id, "dealstatus", deal_id)
    notify_ids = {deal["buyer_id"], deal["seller_id"]} | get_admin_ids()
    for uid in notify_ids:
        if uid != user_id:
            await notify(context, uid, f"تم إلغاء الصفقة #{deal_id} من قبل أحد الأطراف.")
            await bump_user_screen(context, uid)


async def group_id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"معرّف هذه المحادثة (Chat ID): `{update.effective_chat.id}`", parse_mode=ParseMode.MARKDOWN)


async def track_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler عام (يعمل بصمت قبل باقي الـhandlers) يسجّل آيدي/يوزر أي شخص
    يتفاعل مع البوت، لتفعيل البحث بالـ username لاحقاً (أمر /makeadmin)."""
    user = update.effective_user
    if user and not user.is_bot:
        remember_user(user)


async def make_admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """يرقّي مستخدماً لأدمن في مجموعة الإدارة. الاستخدام: /makeadmin @username
    أو /makeadmin <user_id>. للمالك (OWNER_ID) فقط — وليس لكل أدمن — ويتطلب
    أن يكون البوت نفسه أدمن في المجموعة وله صلاحية 'ترقية الأعضاء' (Add New Admins)."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if update.message:
        await safe_delete(context, chat_id, update.message.message_id)

    if not is_owner(user_id):
        if update.message:
            await update.message.reply_text("هذا الأمر لمالك البوت فقط.")
        return

    if not ADMIN_GROUP_ID:
        await context.bot.send_message(chat_id, "⚠️ لم تُعيّن ADMIN_GROUP_ID، لا توجد مجموعة لترقية الأدمن فيها.")
        return

    if not context.args:
        await context.bot.send_message(
            chat_id,
            "الاستخدام: <code>/makeadmin username</code> أو <code>/makeadmin user_id</code>\n"
            "(يمكن أن يكون مع أو بدون @).",
            parse_mode=ParseMode.HTML,
        )
        return

    target_raw = context.args[0]

    target_id = None
    if target_raw.lstrip("@").isdigit():
        target_id = int(target_raw.lstrip("@"))
    else:
        target_id = lookup_user_id_by_username(target_raw)
        if target_id is None:
            # حل احتياطي: نحاول عبر Telegram مباشرة، يعمل فقط لو كان
            # للمستخدم يوزر عام معروف لتيليجرام.
            try:
                chat = await context.bot.get_chat(f"@{target_raw.lstrip('@')}")
                target_id = chat.id
            except Exception:
                target_id = None

    if target_id is None:
        await context.bot.send_message(
            chat_id,
            f"⚠️ لم أستطع إيجاد آيدي المستخدم @{target_raw.lstrip('@')}.\n"
            "يعمل الأمر فقط لمن تفاعل مع هذا البوت من قبل (أرسل /start مثلاً)، "
            "أو أرسل آيدي المستخدم الرقمي مباشرة بدل اليوزر.",
        )
        return

    # نتحقق أولاً من صلاحيات البوت الفعلية (المحفوظة فعلياً) في المجموعة،
    # بدل تخمين السبب لاحقاً من نص الخطأ فقط.
    try:
        bot_member = await context.bot.get_chat_member(ADMIN_GROUP_ID, context.bot.id)
    except Exception as e:
        await context.bot.send_message(
            chat_id,
            f"⚠️ تعذر التحقق من عضوية البوت في المجموعة: {html.escape(str(e))}\n"
            "تأكد أن ADMIN_GROUP_ID صحيح وأن البوت لا يزال عضواً في المجموعة.",
        )
        return

    if bot_member.status != "administrator":
        await context.bot.send_message(
            chat_id,
            "⚠️ البوت ليس أدمن في المجموعة أصلاً. اجعله أدمن أولاً من إعدادات المجموعة.",
        )
        return

    if not getattr(bot_member, "can_promote_members", False):
        await context.bot.send_message(
            chat_id,
            "⚠️ البوت أدمن لكن صلاحية 'إضافة أدمن جدد' (Add New Admins) غير مفعّلة له فعلياً.\n"
            "افتح إعدادات المجموعة ← صلاحيات هذا الأدمن ← فعّل 'Add New Admins' ← "
            "⚠️ لا تنسَ الضغط على علامة ✓ (حفظ) أعلى الشاشة، وإلا لن يُحفَظ التغيير.",
        )
        return

    try:
        await context.bot.promote_chat_member(
            chat_id=ADMIN_GROUP_ID,
            user_id=target_id,
            can_manage_chat=True,
            can_delete_messages=True,
            can_manage_video_chats=True,
            can_restrict_members=True,
            can_promote_members=False,
            can_change_info=True,
            can_invite_users=True,
            can_pin_messages=True,
        )
    except Exception as e:
        await context.bot.send_message(
            chat_id,
            f"⚠️ فشلت الترقية: {html.escape(str(e))}\n"
            "تأكد أن التغيير على صلاحيات البوت تم حفظه فعلاً (بالضغط على ✓)، ثم أعد المحاولة.",
        )
        return

    await context.bot.send_message(chat_id, f"✅ تم ترقية المستخدم ({target_id}) إلى أدمن في المجموعة.")
    await notify(context, target_id, "✅ تمت ترقيتك إلى أدمن في مجموعة الإدارة.")


def _resolve_target_id_sync(target_raw: str):
    """يحاول تحويل @username أو رقم آيدي إلى آيدي رقمي معروف للبوت (من بين
    من تفاعلوا معه سابقاً). لا يتصل بتيليجرام — للاتصال استخدم النسخة
    async في مكان الاستدعاء إن احتجت fallback عبر get_chat."""
    stripped = target_raw.lstrip("@").strip()
    if stripped.isdigit():
        return int(stripped)
    return lookup_user_id_by_username(stripped)


async def addadmin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """يمنح مستخدماً صلاحيات أدمن على مستوى البوت نفسه (أوامر /pay, /rel,
    /ban...الخ) — مباشرة وفورياً عبر قاعدة البيانات، بدون تعديل الكود ولا
    إعادة تشغيل البوت. الاستخدام: /addadmin <user_id أو @username>.
    للمالك (OWNER_ID) فقط."""
    user_id = update.effective_user.id
    if not is_owner(user_id):
        await update.message.reply_text("🚫 هذا الأمر لمالك البوت فقط.")
        return
    if not context.args:
        await update.message.reply_text(
            "الاستخدام: <code>/addadmin user_id</code> أو <code>/addadmin @username</code>\n"
            "ملاحظة: لو استخدمت @username، يجب أن يكون الشخص قد تفاعل مع البوت "
            "مرة واحدة على الأقل (مثلاً أرسل /start) حتى يعرف البوت آيدي حسابه.",
            parse_mode=ParseMode.HTML,
        )
        return

    target_raw = context.args[0]
    target_id = _resolve_target_id_sync(target_raw)
    if target_id is None:
        try:
            chat = await context.bot.get_chat(f"@{target_raw.lstrip('@')}")
            target_id = chat.id
        except Exception:
            target_id = None
    if target_id is None:
        await update.message.reply_text(
            f"⚠️ لم أستطع إيجاد آيدي {html.escape(target_raw)}.\n"
            "استخدم آيدي رقمي مباشرة، أو تأكد أن الشخص تفاعل مع البوت من قبل."
        )
        return

    if is_owner(target_id):
        await update.message.reply_text("هذا المستخدم هو المالك أصلاً، عنده كل الصلاحيات.")
        return

    conn = get_db()
    already = conn.execute("SELECT 1 FROM admins WHERE user_id=?", (target_id,)).fetchone()
    if already:
        conn.close()
        await update.message.reply_text(f"✅ <code>{target_id}</code> أدمن أصلاً.", parse_mode=ParseMode.HTML)
        return
    username_only = target_raw.lstrip("@") if not target_raw.lstrip("@").isdigit() else None
    conn.execute(
        "INSERT INTO admins (user_id, username, added_by, added_at) VALUES (?, ?, ?, ?)",
        (target_id, username_only, user_id, datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()

    try:
        await context.bot.set_my_commands(ADMIN_COMMANDS, scope=BotCommandScopeChat(chat_id=target_id))
    except Exception as e:
        logger.warning(f"تعذر تعيين أوامر الأدمن لـ {target_id}: {e}")

    await update.message.reply_text(
        f"✅ تمت إضافة <code>{target_id}</code> كأدمن للبوت — فعّال الآن فوراً.",
        parse_mode=ParseMode.HTML,
    )
    await notify(context, target_id, "🎉 تمت ترقيتك لأدمن في هذا البوت. أرسل /help لرؤية أوامرك الجديدة.")


async def removeadmin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """يسحب صلاحيات أدمن البوت من مستخدم. الاستخدام: /removeadmin <user_id
    أو @username>. للمالك (OWNER_ID) فقط."""
    user_id = update.effective_user.id
    if not is_owner(user_id):
        await update.message.reply_text("🚫 هذا الأمر لمالك البوت فقط.")
        return
    if not context.args:
        await update.message.reply_text("الاستخدام: <code>/removeadmin user_id</code> أو <code>/removeadmin @username</code>", parse_mode=ParseMode.HTML)
        return

    target_raw = context.args[0]
    target_id = _resolve_target_id_sync(target_raw)
    if target_id is None:
        await update.message.reply_text("⚠️ لم يتم العثور على هذا المستخدم.")
        return
    if is_owner(target_id):
        await update.message.reply_text("🚫 لا يمكن إزالة المالك من الأدمنية.")
        return

    conn = get_db()
    cur = conn.execute("DELETE FROM admins WHERE user_id=?", (target_id,))
    conn.commit()
    conn.close()

    if cur.rowcount == 0:
        await update.message.reply_text(f"ℹ️ <code>{target_id}</code> لم يكن أدمن أصلاً.", parse_mode=ParseMode.HTML)
        return

    try:
        await context.bot.set_my_commands(BASE_COMMANDS, scope=BotCommandScopeChat(chat_id=target_id))
    except Exception as e:
        logger.warning(f"تعذر إعادة ضبط أوامر {target_id}: {e}")

    await update.message.reply_text(f"✅ تمت إزالة <code>{target_id}</code> من أدمن البوت.", parse_mode=ParseMode.HTML)
    await notify(context, target_id, "ℹ️ تمت إزالة صلاحيات الأدمن الخاصة بك في هذا البوت.")


async def list_admins_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """يعرض قائمة أدمن البوت الحاليين. متاح لأي أدمن (وليس المالك فقط)."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("هذا الأمر للأدمن فقط.")
        return
    ids = sorted(get_admin_ids())
    lines = [f"👥 <b>أدمن البوت ({len(ids)}):</b>"]
    for aid in ids:
        tag = " — 👑 المالك" if is_owner(aid) else ""
        lines.append(f"• <code>{aid}</code>{tag}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


def format_dt(iso_str: str) -> str:
    try:
        dt = datetime.fromisoformat(iso_str)
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return iso_str


async def deal_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    await safe_delete(context, chat_id, update.message.message_id)

    deal_id = _parse_single_id(update, context)
    if deal_id is None:
        stash_message(context, user_id, "الاستخدام: /deal <رقم_الصفقة>")
        await goto(context, chat_id, user_id, "msg")
        return

    conn = get_db()
    deal = conn.execute("SELECT buyer_id, seller_id FROM deals WHERE id=?", (deal_id,)).fetchone()
    conn.close()
    if not deal:
        stash_message(context, user_id, "الصفقة غير موجودة.")
        await goto(context, chat_id, user_id, "msg")
        return
    if user_id not in (deal["buyer_id"], deal["seller_id"]) and not is_admin(user_id):
        stash_message(context, user_id, "لا يمكنك عرض صفقة لست طرفاً فيها.")
        await goto(context, chat_id, user_id, "msg")
        return

    await goto(context, chat_id, user_id, "dealstatus", deal_id)


# ---------- إلغاء تلقائي للصفقات المتوقفة ----------
async def auto_cancel_stale_deals(context: ContextTypes.DEFAULT_TYPE):
    """يلغي تلقائياً أي صفقة بقيت 'بانتظار الدفع' لفترة أطول من PAYMENT_TIMEOUT_HOURS،
    ويعيد فتح العرض حتى لا يبقى محجوزاً إلى الأبد بسبب مشترٍ لم يكمل الدفع."""
    if PAYMENT_TIMEOUT_HOURS <= 0:
        return
    conn = get_db()
    stale = conn.execute(
        "SELECT * FROM deals WHERE state='pending_payment' "
        "AND (julianday('now') - julianday(created_at)) * 24 > ?",
        (PAYMENT_TIMEOUT_HOURS,),
    ).fetchall()
    for deal in stale:
        cur = conn.execute(
            "UPDATE deals SET state='cancelled', updated_at=? "
            "WHERE id=? AND state='pending_payment'",
            (datetime.utcnow().isoformat(), deal["id"]),
        )
        if cur.rowcount != 1:
            conn.rollback()
            continue
        listing_cur = conn.execute(
            "UPDATE listings SET status='open' WHERE id=? AND status='pending'",
            (deal["listing_id"],),
        )
        if listing_cur.rowcount != 1:
            conn.rollback()
            logger.error("تعذر إعادة فتح العرض %s بعد الإلغاء التلقائي للصفقة %s", deal["listing_id"], deal["id"])
            continue
        conn.commit()
        await notify(context, deal["buyer_id"],
                     f"⏱️ تم إلغاء الصفقة #{deal['id']} تلقائياً لعدم تأكيد الدفع خلال {int(PAYMENT_TIMEOUT_HOURS)} ساعة.")
        await bump_user_screen(context, deal["buyer_id"])
        await notify(context, deal["seller_id"],
                     f"⏱️ تم إلغاء الصفقة #{deal['id']} تلقائياً لعدم تأكيد الدفع في الوقت المحدد. عرضك أصبح متاحاً مجدداً.")
        await bump_user_screen(context, deal["seller_id"])
        await notify_admin(context, f"⏱️ إلغاء تلقائي للصفقة #{deal['id']} (انتهت مهلة الدفع).")
    conn.close()


async def remind_expiring_deals(context: ContextTypes.DEFAULT_TYPE):
    """يرسل تذكيراً للمشتري قبل ساعة واحدة من الإلغاء التلقائي لصفقة لم يُدفع
    ثمنها بعد، حتى لا تُلغى مفاجأة. يُرسل مرة واحدة فقط لكل صفقة."""
    if PAYMENT_TIMEOUT_HOURS <= 1:
        return  # لا مجال لتذكير قبل ساعة إن كانت المهلة نفسها ساعة أو أقل
    conn = get_db()
    remind_before = PAYMENT_TIMEOUT_HOURS - 1
    soon = conn.execute(
        "SELECT * FROM deals WHERE state='pending_payment' AND payment_reminded=0 "
        "AND (julianday('now') - julianday(created_at)) * 24 > ?",
        (remind_before,),
    ).fetchall()
    for deal in soon:
        cur = conn.execute(
            "UPDATE deals SET payment_reminded=1 WHERE id=? AND payment_reminded=0",
            (deal["id"],),
        )
        if cur.rowcount != 1:
            continue
        conn.commit()
        await notify(context, deal["buyer_id"],
                     f"⏰ تذكير: متبقي أقل من ساعة على إلغاء الصفقة #{deal['id']} تلقائياً "
                     f"لعدم تأكيد الدفع. أكمل الدفع الآن إن كنت ترغب بإتمامها.")
        await bump_user_screen(context, deal["buyer_id"])
    conn.close()


async def daily_admin_digest(context: ContextTypes.DEFAULT_TYPE):
    """يرسل ملخصاً يومياً لمجموعة الأدمن: العروض المفتوحة، الصفقات بانتظار
    الدفع أو التحويل، وعدد الصفقات المكتملة خلال آخر 24 ساعة."""
    conn = get_db()
    open_listings = conn.execute("SELECT COUNT(*) c FROM listings WHERE status='open'").fetchone()["c"]
    awaiting_approval = conn.execute("SELECT COUNT(*) c FROM listings WHERE status='awaiting_approval'").fetchone()["c"]
    pending_payment = conn.execute("SELECT COUNT(*) c FROM deals WHERE state='pending_payment'").fetchone()["c"]
    awaiting_release = conn.execute("SELECT COUNT(*) c FROM deals WHERE state='handed_over'").fetchone()["c"]
    disputed = conn.execute("SELECT COUNT(*) c FROM deals WHERE disputed=1").fetchone()["c"]
    completed_24h = conn.execute(
        "SELECT COUNT(*) c FROM deals WHERE state='completed' AND (julianday('now') - julianday(updated_at)) * 24 <= 24"
    ).fetchone()["c"]
    conn.close()

    text = (
        "🗓️ <b>ملخص يومي</b>\n"
        f"🗂 عروض مفتوحة: {open_listings} | بانتظار مراجعة: {awaiting_approval}\n"
        f"⏳ صفقات بانتظار الدفع: {pending_payment}\n"
        f"📦 صفقات بانتظار تحويل المبلغ: {awaiting_release}\n"
        f"🔴 نزاعات مفتوحة: {disputed}\n"
        f"✅ صفقات اكتملت في آخر 24 ساعة: {completed_24h}"
    )
    target = ADMIN_GROUP_ID or (sorted(get_admin_ids())[0] if get_admin_ids() else None)
    if target:
        try:
            await context.bot.send_message(chat_id=target, text=text, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.warning(f"تعذر إرسال الملخص اليومي: {e}")


# ---------- إحصائيات (أدمن فقط) ----------
async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("هذا الأمر للأدمن فقط.")
        return

    conn = get_db()
    total_listings = conn.execute("SELECT COUNT(*) c FROM listings").fetchone()["c"]
    open_listings = conn.execute("SELECT COUNT(*) c FROM listings WHERE status='open'").fetchone()["c"]
    total_deals = conn.execute("SELECT COUNT(*) c FROM deals").fetchone()["c"]
    completed = conn.execute("SELECT COUNT(*) c, COALESCE(SUM(price),0) s FROM deals WHERE state='completed'").fetchone()
    completed_prices = [r["price"] for r in conn.execute("SELECT price FROM deals WHERE state='completed'").fetchall()]
    pending_payment = conn.execute("SELECT COUNT(*) c FROM deals WHERE state='pending_payment'").fetchone()["c"]
    awaiting_release = conn.execute("SELECT COUNT(*) c FROM deals WHERE state='handed_over'").fetchone()["c"]
    cancelled = conn.execute("SELECT COUNT(*) c FROM deals WHERE state='cancelled'").fetchone()["c"]
    conn.close()

    total_fees = round(sum(calc_fee(p) for p in completed_prices), 2)
    text = (
        "📊 <b>إحصائيات البوت</b>\n"
        f"🗂 العروض: {total_listings} (متاحة الآن: {open_listings})\n"
        f"🤝 الصفقات: {total_deals}\n"
        f"   ⏳ بانتظار الدفع: {pending_payment}\n"
        f"   📦 بانتظار تحويل المبلغ: {awaiting_release}\n"
        f"   ✅ مكتملة: {completed['c']} (مجموع قيمتها ${completed['s']:.2f})\n"
        f"   ❌ ملغاة: {cancelled}\n"
        f"💰 إجمالي العمولات المحصّلة (تقديري): ${total_fees:.2f}"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


# ---------- القائمة السوداء (أدمن فقط) ----------
async def ban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("هذا الأمر للأدمن فقط.")
        return
    if not context.args:
        await update.message.reply_text("الاستخدام: /ban <user_id أو @username> [السبب]")
        return
    target_raw = context.args[0]
    reason = " ".join(context.args[1:]) if len(context.args) > 1 else None
    target_id = int(target_raw.lstrip("@")) if target_raw.lstrip("@").isdigit() else lookup_user_id_by_username(target_raw)
    if target_id is None:
        await update.message.reply_text("⚠️ لم يتم العثور على هذا المستخدم (يجب أن يكون تفاعل مع البوت من قبل).")
        return
    conn = get_db()
    conn.execute(
        "INSERT INTO blacklist (user_id, reason, added_by, created_at) VALUES (?, ?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET reason=excluded.reason, added_by=excluded.added_by, created_at=excluded.created_at",
        (target_id, reason, user_id, datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()
    await update.message.reply_text(f"🚫 تم حظر المستخدم {target_id}" + (f" — السبب: {reason}" if reason else "") + ".")


async def unban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("هذا الأمر للأدمن فقط.")
        return
    if not context.args:
        await update.message.reply_text("الاستخدام: /unban <user_id أو @username>")
        return
    target_raw = context.args[0]
    target_id = int(target_raw.lstrip("@")) if target_raw.lstrip("@").isdigit() else lookup_user_id_by_username(target_raw)
    if target_id is None:
        await update.message.reply_text("⚠️ لم يتم العثور على هذا المستخدم.")
        return
    conn = get_db()
    cur = conn.execute("DELETE FROM blacklist WHERE user_id=?", (target_id,))
    conn.commit()
    conn.close()
    if cur.rowcount:
        await update.message.reply_text(f"✅ تم رفع الحظر عن المستخدم {target_id}.")
    else:
        await update.message.reply_text("هذا المستخدم غير محظور أصلاً.")


# ---------- حل النزاعات (أدمن فقط) ----------
async def resolve_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """يرفع علامة النزاع عن صفقة بعد أن يحلّه الأدمن يدوياً (خارج البوت أو بالمحادثة).
    الاستخدام: /resolve <رقم_الصفقة>"""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not is_admin(user_id):
        await update.message.reply_text("هذا الأمر للأدمن فقط.")
        return
    deal_id = _parse_single_id(update, context)
    if deal_id is None:
        await update.message.reply_text("الاستخدام: /resolve <رقم_الصفقة>")
        return
    conn = get_db()
    deal = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()
    if not deal:
        conn.close()
        await update.message.reply_text("⚠️ صفقة غير موجودة.")
        return
    conn.execute("UPDATE deals SET disputed=0, dispute_reason=NULL, updated_at=? WHERE id=?",
                 (datetime.utcnow().isoformat(), deal_id))
    conn.commit()
    conn.close()
    await update.message.reply_text(f"✅ تم إغلاق النزاع على الصفقة #{deal_id}، ويمكن متابعتها طبيعياً الآن.")
    for uid in (deal["buyer_id"], deal["seller_id"]):
        await notify(context, uid, f"✅ تم حل النزاع على الصفقة #{deal_id} من قِبل الأدمن، ويمكن المتابعة الآن.")
        await bump_user_screen(context, uid)


# ---------- بحث سريع (أدمن فقط) ----------
async def search_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """يبحث عن عروض/صفقات مرتبطة بمستخدم (آيدي أو يوزر) أو برقم عرض/صفقة.
    الاستخدام: /search <user_id أو @username أو رقم>"""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("هذا الأمر للأدمن فقط.")
        return
    if not context.args:
        await update.message.reply_text("الاستخدام: /search <user_id أو @username>")
        return
    target_raw = context.args[0]
    target_id = int(target_raw.lstrip("@")) if target_raw.lstrip("@").isdigit() else lookup_user_id_by_username(target_raw)
    if target_id is None:
        await update.message.reply_text("⚠️ لم يتم العثور على هذا المستخدم.")
        return

    conn = get_db()
    listings = conn.execute(
        "SELECT id, niche, price, status FROM listings WHERE seller_id=? ORDER BY id DESC LIMIT 10", (target_id,)
    ).fetchall()
    deals = conn.execute(
        "SELECT id, state, price, buyer_id, seller_id FROM deals WHERE buyer_id=? OR seller_id=? ORDER BY id DESC LIMIT 10",
        (target_id, target_id),
    ).fetchall()
    banned = conn.execute("SELECT reason FROM blacklist WHERE user_id=?", (target_id,)).fetchone()
    conn.close()

    avg, count = get_user_rating(target_id)
    lines = [f"🔎 <b>نتائج البحث عن {target_id}</b>"]
    if banned:
        lines.append(f"🚫 محظور (السبب: {banned['reason'] or 'غير محدد'})")
    lines.append(f"⭐ التقييم: {avg if count else '—'} ({count} تقييم)")
    lines.append("\n📄 <b>عروضه كبائع:</b>" if listings else "\n📄 لا عروض له.")
    for l in listings:
        lines.append(f"#{l['id']} — {html.escape(l['niche'] or '')} — ${l['price']} — {l['status']}")
    lines.append("\n🤝 <b>صفقاته:</b>" if deals else "\n🤝 لا صفقات له.")
    for d in deals:
        role = "مشترٍ" if d["buyer_id"] == target_id else "بائع"
        lines.append(f"#{d['id']} — {role} — ${d['price']} — {d['state']}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


# ---------- التشغيل ----------
# ---------- معالج الأخطاء العام ----------
async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """يلتقط أي استثناء غير متوقع في أي handler، بدل أن يفشل الطلب بصمت
    (الشاشة "تتجمد" عند المستخدم بدون أي رد). يسجّل الخطأ كاملاً في bot.log،
    يخبر المستخدم برسالة ودّية، ويبلّغ الأدمن بتفاصيل تقنية مختصرة."""
    logger.error("استثناء غير متوقع أثناء معالجة update:", exc_info=context.error)

    # إخبار المستخدم إن كان هناك تحديث فعلي (رسالة أو ضغطة زر) نستطيع الرد عليه.
    try:
        if isinstance(update, Update):
            chat_id = update.effective_chat.id if update.effective_chat else None
            if chat_id:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="⚠️ حدث خطأ غير متوقع أثناء تنفيذ طلبك. تم إبلاغ الفريق، حاول مرة أخرى بعد قليل.",
                )
    except Exception:
        pass

    # تبليغ الأدمن بملخص تقني (بدون إغراقهم بتفاصيل ضخمة). نص عادي بدون HTML
    # لأن notify_admin لا يستخدم parse_mode، وقد يحوي الخطأ رموزاً كـ "<" أو "&".
    try:
        err_type = type(context.error).__name__
        err_msg = str(context.error)[:300]
        user_info = ""
        if isinstance(update, Update) and update.effective_user:
            user_info = f" — من المستخدم {update.effective_user.id}"
        await notify_admin(
            context,
            f"🐞 خطأ في البوت{user_info}:\n{err_type}: {err_msg}\n"
            f"التفاصيل الكاملة في bot.log على السيرفر.",
        )
    except Exception:
        pass


async def post_init(app: Application):
    await app.bot.set_my_commands(BASE_COMMANDS)
    for admin_id in get_admin_ids():
        try:
            commands = OWNER_COMMANDS if is_owner(admin_id) else ADMIN_COMMANDS
            await app.bot.set_my_commands(commands, scope=BotCommandScopeChat(chat_id=admin_id))
        except Exception as e:
            logger.warning(f"تعذر تعيين أوامر الأدمن لـ {admin_id}: {e}")


def main():
    missing = []
    if not BOT_TOKEN:
        missing.append(("BOT_TOKEN", "التوكن من @BotFather"))
    if not WALLET_ADDRESS:
        missing.append(("WALLET_ADDRESS", "عنوان محفظتك لاستقبال المدفوعات"))
    if not OWNER_ID and not _BOOTSTRAP_ADMIN_IDS:
        missing.append(("OWNER_ID أو ADMIN_IDS", "رقم حسابك على تيليجرام (من @userinfobot) — بدونه لن يكون لأحد صلاحية أدمن"))
    if FEE_FLAT < 0:
        missing.append(("FEE_FLAT", "يجب ألا تكون العمولة الثابتة رقماً سالباً"))
    if not (0 <= FEE_PERCENT <= 100):
        missing.append(("FEE_PERCENT", "يجب أن تكون نسبة العمولة بين 0 و100"))
    if missing:
        print("⚠️  لا يمكن تشغيل البوت — الإعدادات التالية ناقصة:\n")
        for var, hint in missing:
            print(f"   • {var}  →  {hint}")
        print(
            "\nالحل: أنشئ ملف اسمه '.env' بجانب bot.py وضع بداخله مثل هذا "
            "(استبدل القيم بقيمك الحقيقية):\n"
        )
        print('   BOT_TOKEN=ضع_التوكن_هنا')
        print('   OWNER_ID=ضع_رقمك_هنا')
        print('   ADMIN_IDS=ضع_رقمك_هنا')
        print('   WALLET_ADDRESS=ضع_عنوان_محفظتك_هنا')
        print(
            "\nثم شغّل: pip install -r requirements.txt  ثم  python bot.py\n"
            "(انظر ملف .env.example المرفق كنموذج جاهز للنسخ)"
        )
        return
    init_db()
    if not get_admin_ids():
        logger.warning(
            "⚠️ لا يوجد أي أدمن حالياً (لا OWNER_ID ولا admins في قاعدة البيانات). "
            "عيّن OWNER_ID في .env حتى تتمكن من استخدام /addadmin."
        )
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .persistence(PicklePersistence(filepath=PERSISTENCE_PATH))
        .post_init(post_init)
        .build()
    )

    sell_conv = ConversationHandler(
        entry_points=[
            CommandHandler("sell", sell_start),
            CallbackQueryHandler(sell_start, pattern=r"^startsell$"),
        ],
        states={
            GAME: [
                CallbackQueryHandler(sell_game, pattern=r"^g:\d+$"),
                CallbackQueryHandler(sell_back, pattern=r"^back$"),
            ],
            LEVEL: [
                CallbackQueryHandler(sell_level_skip, pattern=r"^skiplevel$"),
                CallbackQueryHandler(sell_back, pattern=r"^back$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, sell_level),
            ],
            PRICE: [
                CallbackQueryHandler(sell_back, pattern=r"^back$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, sell_price),
            ],
            DESC: [
                CallbackQueryHandler(sell_back, pattern=r"^back$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, sell_desc),
            ],
            VIDEO: [
                CallbackQueryHandler(sell_video_back, pattern=r"^back$"),
                MessageHandler(filters.VIDEO, sell_video),
                MessageHandler(filters.ALL & ~filters.COMMAND, sell_video),
            ],
            CREDS: [
                CallbackQueryHandler(sell_creds_back, pattern=r"^back$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, sell_creds),
            ],
        },
        fallbacks=[
            CommandHandler("stop", sell_stop),
            MessageHandler(filters.Regex(f"^{re.escape(BTN_MENU)}$"), sell_stop),
        ],
    )

    wallet_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(wallet_edit_start, pattern=r"^editwallet$"),
        ],
        states={
            WALLET: [
                CallbackQueryHandler(sell_wallet_back, pattern=r"^back$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, wallet_save),
            ],
        },
        fallbacks=[
            CommandHandler("stop", sell_stop),
            MessageHandler(filters.Regex(f"^{re.escape(BTN_MENU)}$"), sell_stop),
        ],
    )

    editprice_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(editprice_start, pattern=r"^editprice:\d+$"),
        ],
        states={
            EDITPRICE: [
                CallbackQueryHandler(editprice_back, pattern=r"^back$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, editprice_save),
            ],
        },
        fallbacks=[
            CommandHandler("stop", sell_stop),
            MessageHandler(filters.Regex(f"^{re.escape(BTN_MENU)}$"), sell_stop),
        ],
    )

    app.add_error_handler(global_error_handler)
    app.add_handler(TypeHandler(Update, track_users), group=-1)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(sell_conv)
    app.add_handler(wallet_conv)
    app.add_handler(editprice_conv)
    app.add_handler(MessageHandler(filters.Regex(f"^{re.escape(BTN_MENU)}$"), start))
    app.add_handler(CommandHandler("list", listings_cmd))
    app.add_handler(CommandHandler("buy", buy_cmd))
    app.add_handler(CommandHandler("pay", paid))
    app.add_handler(CommandHandler("ho", handover))
    app.add_handler(CommandHandler("rel", release))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CommandHandler("deal", deal_status))
    app.add_handler(CommandHandler("groupid", group_id_cmd))
    app.add_handler(CommandHandler("my", my_deals_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("makeadmin", make_admin_cmd))
    app.add_handler(CommandHandler("addadmin", addadmin_cmd))
    app.add_handler(CommandHandler("removeadmin", removeadmin_cmd))
    app.add_handler(CommandHandler("admins", list_admins_cmd))
    app.add_handler(CommandHandler("ban", ban_cmd))
    app.add_handler(CommandHandler("unban", unban_cmd))
    app.add_handler(CommandHandler("resolve", resolve_cmd))
    app.add_handler(CommandHandler("search", search_cmd))
    app.add_handler(CommandHandler("referral", referral_cmd))

    app.add_handler(CallbackQueryHandler(doconfirm_callback, pattern=r"^doconfirm:\d+$"))
    app.add_handler(CallbackQueryHandler(claim_callback, pattern=r"^claim:\d+$"))
    app.add_handler(CallbackQueryHandler(unclaim_callback, pattern=r"^unclaim:\d+$"))
    app.add_handler(CallbackQueryHandler(admpay_callback, pattern=r"^admpay:\d+$"))
    app.add_handler(CallbackQueryHandler(checkwallet_callback, pattern=r"^checkwallet:\d+$"))
    app.add_handler(CallbackQueryHandler(admrel_callback, pattern=r"^admrel:\d+$"))
    app.add_handler(CallbackQueryHandler(buyerconfirm_callback, pattern=r"^buyerconfirm:\d+$"))
    app.add_handler(CallbackQueryHandler(delask_callback, pattern=r"^delask:\d+$"))
    app.add_handler(CallbackQueryHandler(delconfirm_callback, pattern=r"^delconfirm:\d+$"))
    app.add_handler(CallbackQueryHandler(delcancel_callback, pattern=r"^delcancel:\d+$"))
    app.add_handler(CallbackQueryHandler(dispute_callback, pattern=r"^dispute:\d+$"))
    app.add_handler(CallbackQueryHandler(rate_callback, pattern=r"^rate:\d+:\d+:\d+$"))
    app.add_handler(CallbackQueryHandler(approve_listing_callback, pattern=r"^approve:\d+$"))
    app.add_handler(CallbackQueryHandler(reject_listing_callback, pattern=r"^reject:\d+$"))
    app.add_handler(CallbackQueryHandler(nav_callback, pattern=r"^(go:[\w:]+|back)$"))

    if PAYMENT_TIMEOUT_HOURS > 0:
        if app.job_queue is None:
            logger.warning(
                "PAYMENT_TIMEOUT_HOURS مفعّل لكن job_queue غير متاح. "
                "ثبّت الحزمة عبر: pip install \"python-telegram-bot[job-queue]\""
            )
        else:
            app.job_queue.run_repeating(auto_cancel_stale_deals, interval=1800, first=60)
            app.job_queue.run_repeating(remind_expiring_deals, interval=1800, first=90)

    if app.job_queue is not None:
        app.job_queue.run_repeating(daily_admin_digest, interval=86400, first=120)

    print("Bot starting...")
    app.run_polling()


if __name__ == "__main__":
    main()
