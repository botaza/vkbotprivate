import vk_api
from vk_api.longpoll import VkLongPoll, VkEventType
from vk_api.keyboard import VkKeyboard, VkKeyboardColor
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import os, json, logging
from logging.handlers import RotatingFileHandler
import threading
import calendar
import time
import re
import sys

# ================= LOGGING (Enhancement 4: Rotation) =================
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
handler = RotatingFileHandler("bot.log", maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
log.addHandler(handler)
stream_handler = logging.StreamHandler(stream=open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1) if hasattr(sys.stdout, 'fileno') else sys.stdout)
stream_handler.setFormatter(formatter)
log.addHandler(stream_handler)

REMINDER_FILE = "sent_reminders.json"

# ================= CONFIG =================
TOKEN_FILE = "token.txt"
STATE_FILE = "states.json"
PLANNER_DIR = "planners"
DAYS_PER_BATCH = 4
os.makedirs(PLANNER_DIR, exist_ok=True)

# ================= STATES =================
STATE_START = "start"
STATE_SUGGEST_YEAR = "suggest_year"
STATE_SUGGEST_MONTH = "suggest_month"
STATE_SUGGEST_DAY = "suggest_day"
STATE_SUGGEST_HOUR = "suggest_hour"
STATE_SUGGEST_MINUTE = "suggest_minute"
STATE_SUGGEST_DESC = "suggest_desc"
STATE_SUGGEST_HASHTAG = "suggest_hashtag"
STATE_SUGGEST_RECURRENCE = "suggest_recurrence"
STATE_SUGGEST_COUNT = "suggest_count"
STATE_SUGGEST_DURATION = "suggest_duration"
STATE_SUGGEST_PLACE = "suggest_place"
STATE_LIST_MENU = "list_menu"
STATE_LIST_VIEW = "list_view"
STATE_FILTER = "filter"
STATE_DELETE_HASHTAG = "delete_hashtag"
STATE_DELETE_UID = "delete_uid"
STATE_EDIT_SELECT = "edit_select"
STATE_EDIT_INPUT = "edit_input"
STATE_COMPLETE = "complete"
STATE_EDIT_DONE_SELECT = "edit_done_select"
STATE_EDIT_DONE_INPUT = "edit_done_input"
STATE_DELETE_DONE = "delete_done"
STATE_DELETE_ARRAY = "delete_array"
STATE_QUICK_ADD = "quick_add"
STATE_DELETE_PHOTOS = "delete_photos"
STATE_DATE_QUERY = "date_query"
STATE_NUMBER_QUERY = "number_query"
STATE_EXTEND_SELECT = "extend_select"
STATE_EXTEND_PERIOD = "extend_period"
STATE_DELETE_MENU = "delete_menu"
STATE_LIST_MAIN_MENU = "list_main_menu"
STATE_EDIT_MENU = "edit_menu"
STATE_QUICK_COMMANDS = "quick_commands"
STATE_REMIND_SELECT = "remind_select"
STATE_REMIND_COUNT = "remind_count"
STATE_REMIND_MINUTES = "remind_minutes"
STATE_REMIND_INDEX = "remind_index"
STATE_BUDGET_MENU    = "budget_menu"
STATE_EXP_MENU       = "exp_menu"
# New states for expense date selection
STATE_EXP_DATE_CHOICE   = "exp_date_choice"     # "Today", "Yesterday", "Specific day"
STATE_EXP_YEAR          = "exp_year"            # this year / prev year / specific year
STATE_EXP_MONTH         = "exp_month"           # this month / prev month / specific month
STATE_EXP_DAY           = "exp_day"             # text input for day 1–31
STATE_EXP_AMOUNT     = "exp_amount"
STATE_EXP_CATEGORY   = "exp_category"
STATE_EXP_DESC       = "exp_desc"
STATE_EXP_LIST       = "exp_list"
STATE_EXP_DELETE     = "exp_delete"
STATE_EXP_STATS      = "exp_stats"
STATE_EXP_MONTH_PICK = "exp_month_pick"
STATE_EXP_TOOL = "exp_tool"
# Income states (parallel to expenses)
STATE_INC_MENU        = "inc_menu"
STATE_INC_DATE_CHOICE = "inc_date_choice"
STATE_INC_YEAR        = "inc_year"
STATE_INC_MONTH       = "inc_month"
STATE_INC_DAY         = "inc_day"
STATE_INC_AMOUNT      = "inc_amount"
STATE_INC_DESC        = "inc_desc"
STATE_INC_MONTH_PICK  = "inc_month_pick"
STATE_INC_DELETE      = "inc_delete"

# ================= TOKEN =================
with open(TOKEN_FILE, "r", encoding="utf-8") as f:
    TOKEN = f.read().strip()

# ================= THREAD SAFETY (Enhancement 1) =================
state_lock = threading.RLock()
reminder_lock = threading.RLock()

# ================= VK =================
vk_session = vk_api.VkApi(token=TOKEN)
vk = vk_session.get_api()
longpoll = VkLongPoll(vk_session, mode=2)

def send(uid, text, kb=None):
    if not text:
        text = "."
    vk.messages.send(user_id=uid, random_id=0, message=text, keyboard=kb)




# ================= KEYBOARDS =================
def year_kb():
    kb = VkKeyboard(one_time=True)
    now = datetime.now().year
    kb.add_button(str(now), VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button(str(now + 1), VkKeyboardColor.PRIMARY)
    return kb.get_keyboard()

def month_kb():
    kb = VkKeyboard(one_time=True)
    now = datetime.now().month
    kb.add_button(f"{now:02d}", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button(f"{((now % 12) + 1):02d}", VkKeyboardColor.PRIMARY)
    return kb.get_keyboard()

def day_kb():
    kb = VkKeyboard(one_time=True)
    today = datetime.now().day
    tomorrow = (datetime.now() + timedelta(days=1)).day
    kb.add_button(str(today), VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button(str(tomorrow), VkKeyboardColor.PRIMARY)
    return kb.get_keyboard()

def hour_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("08", VkKeyboardColor.PRIMARY)
    kb.add_button("10", VkKeyboardColor.PRIMARY)
    kb.add_button("11", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("13", VkKeyboardColor.PRIMARY)
    kb.add_button("15", VkKeyboardColor.PRIMARY)
    kb.add_button("16", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("18", VkKeyboardColor.PRIMARY)
    kb.add_button("20", VkKeyboardColor.PRIMARY)
    kb.add_button("23", VkKeyboardColor.PRIMARY)
    return kb.get_keyboard()

def minute_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("00", VkKeyboardColor.PRIMARY)
    kb.add_button("10", VkKeyboardColor.PRIMARY)
    kb.add_button("30", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("40", VkKeyboardColor.PRIMARY)
    kb.add_button("50", VkKeyboardColor.PRIMARY)
    kb.add_button("59", VkKeyboardColor.PRIMARY)
    return kb.get_keyboard()

def duration_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("?", VkKeyboardColor.PRIMARY)
    kb.add_button("60", VkKeyboardColor.PRIMARY)
    kb.add_button("90", VkKeyboardColor.PRIMARY)
    return kb.get_keyboard()

def place_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("?", VkKeyboardColor.PRIMARY)
    return kb.get_keyboard()

def delete_menu_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("Del P", VkKeyboardColor.NEGATIVE)
    kb.add_button("Del Hash", VkKeyboardColor.NEGATIVE)
    kb.add_button("Del ID", VkKeyboardColor.NEGATIVE)
    kb.add_line()
    kb.add_button("Del Ar", VkKeyboardColor.NEGATIVE)
    kb.add_button("Del C", VkKeyboardColor.NEGATIVE)
    kb.add_line()
    kb.add_button("Back to menu", VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()

def list_menu_main_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("List events", VkKeyboardColor.PRIMARY)
    kb.add_button("List completed", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("Filter by hashtag", VkKeyboardColor.SECONDARY)
    kb.add_button("Back to menu", VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()

def edit_menu_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("Edit event", VkKeyboardColor.SECONDARY)
    kb.add_button("Edit completed", VkKeyboardColor.SECONDARY)
    kb.add_line()
    kb.add_button("Back to menu", VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()

def quick_commands_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("/pics", VkKeyboardColor.PRIMARY)
    kb.add_button("/number", VkKeyboardColor.PRIMARY)
    kb.add_button("/date", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("/remind", VkKeyboardColor.PRIMARY)  
    kb.add_button("/extend", VkKeyboardColor.PRIMARY)
    kb.add_line()   
    kb.add_button("/today", VkKeyboardColor.PRIMARY)    
    kb.add_button("/tomorrow", VkKeyboardColor.PRIMARY)      
    kb.add_line()
    kb.add_button("Back to menu", VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()

def main_menu_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("Suggest", VkKeyboardColor.POSITIVE)
    kb.add_button("Quick note", VkKeyboardColor.POSITIVE)
    kb.add_button("Complete", VkKeyboardColor.POSITIVE)
    kb.add_line()
    kb.add_button("List", VkKeyboardColor.PRIMARY)
    kb.add_button("Delete", VkKeyboardColor.NEGATIVE)
    kb.add_button("Edit", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("Quick Commands", VkKeyboardColor.SECONDARY)
    kb.add_button("Budget", VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()

def budget_menu_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("Expenses", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("Income", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("Back to menu", VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()

def list_menu_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("Show all", VkKeyboardColor.PRIMARY)
    kb.add_button("Filter by hashtag", VkKeyboardColor.SECONDARY)
    kb.add_line()
    kb.add_button("Back to menu", VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()


def nav_kb(has_next):
    kb = VkKeyboard(one_time=True)
    if has_next:
        kb.add_button("Next", VkKeyboardColor.PRIMARY)
        kb.add_button("Back to menu", VkKeyboardColor.SECONDARY)
    else:
        kb.add_button("Back to menu", VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()

def recurrence_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("One-time", VkKeyboardColor.SECONDARY)
    kb.add_button("Weekly", VkKeyboardColor.SECONDARY)
    kb.add_line()
    kb.add_button("Biweekly", VkKeyboardColor.SECONDARY)
    kb.add_button("Monthly", VkKeyboardColor.SECONDARY)
    kb.add_line()
    kb.add_button("Yearly", VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()

def extend_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("Weekly", VkKeyboardColor.PRIMARY)
    kb.add_button("Biweekly", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("Monthly", VkKeyboardColor.PRIMARY)
    kb.add_button("Annually", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("Back to menu", VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()

def remind_minutes_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("5", VkKeyboardColor.PRIMARY)
    kb.add_button("10", VkKeyboardColor.PRIMARY)
    kb.add_button("30", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("120", VkKeyboardColor.PRIMARY)
    kb.add_button("180", VkKeyboardColor.PRIMARY)
    kb.add_button("720", VkKeyboardColor.PRIMARY)
    return kb.get_keyboard()


def inc_menu_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("➕ Add income", VkKeyboardColor.POSITIVE)
    kb.add_line()
    kb.add_button("📊 This month", VkKeyboardColor.PRIMARY)
    kb.add_button("📅 By month", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("🗑 Delete income", VkKeyboardColor.NEGATIVE)
    kb.add_line()
    kb.add_button("Back to menu", VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()






def exp_menu_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("➕ Add expense", VkKeyboardColor.POSITIVE)
    kb.add_line()
    kb.add_button("📊 This month", VkKeyboardColor.PRIMARY)
    kb.add_button("📅 By month", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("🗑 Delete expense", VkKeyboardColor.NEGATIVE)
    kb.add_line()
    kb.add_button("Back to menu", VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()

def exp_date_choice_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("For today",     VkKeyboardColor.POSITIVE)
    kb.add_line()
    kb.add_button("For yesterday", VkKeyboardColor.POSITIVE)
    kb.add_line()
    kb.add_button("For specific day", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("Back", VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()


def exp_year_choice_kb():
    kb = VkKeyboard(one_time=True)
    now = datetime.now()
    kb.add_button(f"This year ({now.year})",   VkKeyboardColor.POSITIVE)
    kb.add_line()
    kb.add_button(f"Previous year ({now.year-1})", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("Specific year", VkKeyboardColor.SECONDARY)
    kb.add_line()
    kb.add_button("Back", VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()


def exp_month_choice_kb():
    kb = VkKeyboard(one_time=True)
    now = datetime.now()
    prev = now - timedelta(days=now.day + 5)   # rough previous month
    kb.add_button(f"This month ({now.strftime('%Y-%m')})",   VkKeyboardColor.POSITIVE)
    kb.add_line()
    kb.add_button(f"Previous month ({prev.strftime('%Y-%m')})", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("Specific month", VkKeyboardColor.SECONDARY)
    kb.add_line()
    kb.add_button("Back", VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()


def exp_category_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("🍔 food",       VkKeyboardColor.PRIMARY)
    kb.add_button("🚗 transport",  VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("🏠 housing",    VkKeyboardColor.PRIMARY)
    kb.add_button("💊 health",     VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("🎮 fun",        VkKeyboardColor.PRIMARY)
    kb.add_button("🛒 shop",       VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("💰 savings",    VkKeyboardColor.PRIMARY)
    kb.add_button("🧾 bills",      VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("🎁 gifts",      VkKeyboardColor.PRIMARY)
    kb.add_button("📦 other",      VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()

def exp_desc_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("— skip —", VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()

def exp_month_kb():
    kb = VkKeyboard(one_time=True)
    today = datetime.now().date()
    months = []
    y, m = today.year, today.month
    for _ in range(6):
        months.append((y, m))
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    months.reverse()
    for i in range(0, 6, 2):
        for j in range(2):
            if i + j < len(months):
                yy, mm = months[i + j]
                kb.add_button(f"{yy}-{mm:02d}", VkKeyboardColor.PRIMARY)
        if i + 2 < 6:
            kb.add_line()
    kb.add_line()
    kb.add_button("Back to menu", VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()


def exp_tool_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("gp",   VkKeyboardColor.PRIMARY)
    kb.add_button("hal",  VkKeyboardColor.PRIMARY)
    kb.add_button("sb",   VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("ren",  VkKeyboardColor.PRIMARY)
    kb.add_button("oz",   VkKeyboardColor.PRIMARY)
    kb.add_button("ya",   VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("cert", VkKeyboardColor.PRIMARY)
    kb.add_button("cash", VkKeyboardColor.PRIMARY)
    kb.add_button("other",VkKeyboardColor.SECONDARY)
    kb.add_line()
    kb.add_button("— skip —", VkKeyboardColor.SECONDARY)
    return kb.get_keyboard()



def hashtag_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("pers", VkKeyboardColor.PRIMARY)
    kb.add_button("cons", VkKeyboardColor.PRIMARY)
    kb.add_button("job", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("event", VkKeyboardColor.PRIMARY)
    kb.add_button("control", VkKeyboardColor.PRIMARY)
    return kb.get_keyboard()

# ================= STATE STORAGE =================
def load_states():
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def save_states():
    with state_lock:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(states, f, indent=2)

states = load_states()

def user(uid):
    uid = str(uid)
    with state_lock:
        if uid not in states:
            states[uid] = {"state": STATE_START, "data": {}, "next_uid": 1}
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(states, f, indent=2)
        return states[uid]

def set_state(uid, s):
    with state_lock:
        user(uid)["state"] = s
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(states, f, indent=2)

def set_data(uid, k, v):
    with state_lock:
        user(uid)["data"][k] = v
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(states, f, indent=2)

def get_data(uid, k, default=None):
    with state_lock:
        return user(uid)["data"].get(k, default)

def clear_data(uid):
    with state_lock:
        user(uid)["data"] = {}
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(states, f, indent=2)

def next_uid(uid):
    with state_lock:
        val = user(uid).get("next_uid", 1)
        user(uid)["next_uid"] = val + 1
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(states, f, indent=2)
        return f"uid{val}"

# ================= HASHTAG & LINE PARSING =================
CATEGORIES = [
    ("🍔", "food"),
    ("🚗", "transport"),
    ("🏠", "housing"),
    ("💊", "health"),
    ("🎮", "fun"),
    ("🛒", "shop"),
    ("💰", "savings"),
    ("🧾", "bills"),
    ("🎁", "gifts"),
    ("📦", "other"),
]
CAT_SLUGS = [c[1] for c in CATEGORIES]
CAT_LABEL_MAP = {f"{e} {s}": s for e, s in CATEGORIES}
for s in CAT_SLUGS:
    CAT_LABEL_MAP[s] = s


HASHTAG_RE = re.compile(r"(#\w+)")
EVENT_OR_PERS_RE = re.compile(r"\b(event|pers|control)\b", re.IGNORECASE)

def extract_hashtag(text):
    """Return first hashtag in a line or None if missing"""
    m = HASHTAG_RE.search(text)
    return m.group(1) if m else None

def line_has_uid(line, uid_value):
    parsed = parse_event_line(line)
    if not parsed:
        return False
    _, _, _, uid_event, _ = parsed
    return uid_event == uid_value

# ================= PLANNER =================
# ================= INCOME FILE HELPERS =================
def inc_file(uid):
    return os.path.join(PLANNER_DIR, f"{uid}income.json")

def inc_totals_file(uid):
    return os.path.join(PLANNER_DIR, f"{uid}inc_totals.json")

def read_income(uid):
    return _read_json_list(inc_file(uid))

def write_income(uid, entries):
    _write_json(inc_file(uid), entries)

def read_inc_totals(uid):
    if not os.path.exists(inc_totals_file(uid)):
        return {}
    try:
        with open(inc_totals_file(uid), "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_inc_totals(uid, totals):
    _write_json(inc_totals_file(uid), totals)

def next_inc_id(uid):
    with state_lock:
        u = user(uid)
        val = u.get("next_inc_id", 1)
        u["next_inc_id"] = val + 1
        save_states()
        return f"i{val}"

def save_income(uid, amount, desc, dt: datetime = None):
    if dt is None:
        dt = datetime.now()
    entry = {
        "id": next_inc_id(uid),
        "dt": dt.strftime("%Y-%m-%dT%H:%M"),
        "amount": amount,
        "desc": desc,
    }
    entries = read_income(uid)
    entries.append(entry)
    write_income(uid, entries)
    _add_to_inc_totals(uid, entry)
    return entry

def delete_income_by_index(uid, idx):
    entries = read_income(uid)
    if not (0 <= idx < len(entries)):
        return None
    removed = entries.pop(idx)
    write_income(uid, entries)
    _subtract_from_inc_totals(uid, removed)
    return removed

def _add_to_inc_totals(uid, entry):
    totals = read_inc_totals(uid)
    mk = _month_key(entry["dt"])
    if mk not in totals:
        totals[mk] = {"total": 0.0}
    totals[mk]["total"] = round(totals[mk].get("total", 0) + entry["amount"], 2)
    _save_inc_totals(uid, totals)

def _subtract_from_inc_totals(uid, entry):
    totals = read_inc_totals(uid)
    mk = _month_key(entry["dt"])
    if mk not in totals:
        return
    totals[mk]["total"] = round(max(0, totals[mk].get("total", 0) - entry["amount"]), 2)
    _save_inc_totals(uid, totals)

def format_inc_entry(entry, idx=None):
    dt = entry["dt"][5:16].replace("T", " ")
    amt = f"{entry['amount']:,.0f}"
    desc = f" {entry['desc']}" if entry.get("desc") else ""
    prefix = f"{idx+1}. " if idx is not None else ""
    return f"{prefix}{dt} +{amt}{desc}"

def format_inc_month_stats(uid, month_key):
    totals = read_inc_totals(uid)
    if month_key not in totals:
        return f"No income recorded for {month_key}."
    grand = totals[month_key].get("total", 0)
    return f"📈 {month_key} income — {grand:,.0f}"

def format_recent_income(uid, month_key=None, limit=20):
    entries = read_income(uid)
    if month_key:
        entries = [e for e in entries if e["dt"][:7] == month_key]
    entries = list(reversed(entries[-limit:]))
    return [format_inc_entry(e, i) for i, e in enumerate(entries)]



# ================= EXPENSE FILE HELPERS =================
EXPENSE_ARCHIVE_MONTHS = 3

def exp_file(uid):
    return os.path.join(PLANNER_DIR, f"{uid}expenses.json")

def exp_totals_file(uid):
    return os.path.join(PLANNER_DIR, f"{uid}exp_totals.json")

def exp_archive_file(uid, year):
    return os.path.join(PLANNER_DIR, f"{uid}expenses_{year}.json")

def _read_json_list(path):
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def _write_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def read_expenses(uid):
    return _read_json_list(exp_file(uid))

def write_expenses(uid, entries):
    _write_json(exp_file(uid), entries)

def read_totals(uid):
    if not os.path.exists(exp_totals_file(uid)):
        return {}
    try:
        with open(exp_totals_file(uid), "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_totals(uid, totals):
    _write_json(exp_totals_file(uid), totals)

def _month_key(dt_str):
    return dt_str[:7]

def _cat_emoji(cat):
    for e, s in CATEGORIES:
        if s == cat:
            return e
    return "📦"

def _add_to_totals(uid, entry):
    totals = read_totals(uid)
    mk = _month_key(entry["dt"])
    if mk not in totals:
        totals[mk] = {"total": 0.0}
        for s in CAT_SLUGS:
            totals[mk][s] = 0.0
    cat = entry.get("category", "other")
    totals[mk]["total"] = round(totals[mk].get("total", 0) + entry["amount"], 2)
    totals[mk][cat]     = round(totals[mk].get(cat, 0)   + entry["amount"], 2)
    _save_totals(uid, totals)

def _subtract_from_totals(uid, entry):
    totals = read_totals(uid)
    mk = _month_key(entry["dt"])
    if mk not in totals:
        return
    cat = entry.get("category", "other")
    totals[mk]["total"] = round(max(0, totals[mk].get("total", 0) - entry["amount"]), 2)
    totals[mk][cat]     = round(max(0, totals[mk].get(cat, 0)   - entry["amount"]), 2)
    _save_totals(uid, totals)

def next_exp_id(uid):
    with state_lock:
        u = user(uid)
        val = u.get("next_exp_id", 1)
        u["next_exp_id"] = val + 1
        save_states()
        return f"e{val}"


def save_expense(uid, amount, category, desc, dt: datetime = None, tool: str = None):
    if dt is None:
        dt = datetime.now()
   
    entry = {
        "id": next_exp_id(uid),
        "dt": dt.strftime("%Y-%m-%dT%H:%M"),
        "amount": amount,
        "category": category,
        "desc": desc,
    }
    if tool and tool != "— skip —":
        entry["tool"] = tool.lower().strip()

    entries = read_expenses(uid)
    entries.append(entry)
    write_expenses(uid, entries)
    _add_to_totals(uid, entry)
    return entry


def delete_expense_by_index(uid, idx):
    entries = read_expenses(uid)
    if not (0 <= idx < len(entries)):
        return None
    removed = entries.pop(idx)
    write_expenses(uid, entries)
    _subtract_from_totals(uid, removed)
    return removed

def format_entry(entry, idx=None):
    dt = entry["dt"][5:16].replace("T", " ")          # e.g. "03-09 14:30"
    em = _cat_emoji(entry["category"])
    amt = f"{entry['amount']:,.0f}"
    
    desc_part = f" {entry['desc']}" if entry.get("desc") else ""
    tool_part = f"  → {entry['tool'].upper()}" if entry.get("tool") else ""
    
    prefix = f"{idx+1}. " if idx is not None else ""
    
    return f"{prefix}{dt} {em}{amt}{desc_part}{tool_part}"

def format_month_stats(uid, month_key):
    totals = read_totals(uid)
    if month_key not in totals:
        return f"No expenses recorded for {month_key}."
    data  = totals[month_key]
    grand = data.get("total", 0)
    lines = [f"📊 {month_key}  —  {grand:,.0f}"]
    for em, cat in CATEGORIES:
        amt = data.get(cat, 0)
        if amt == 0:
            continue
        pct = int(amt / grand * 100) if grand else 0
        bar = "█" * (pct // 10)
        lines.append(f"{em} {cat:<10} {amt:>8,.0f}  {pct:>3}% {bar}")
    return "\n".join(lines)

def format_all_month_totals(uid):
    totals = read_totals(uid)
    if not totals:
        return "No expense history yet."
    lines = ["📅 Monthly totals:"]
    for mk in sorted(totals.keys()):
        amt = totals[mk].get("total", 0)
        lines.append(f"  {mk}  {amt:>10,.0f}")
    return "\n".join(lines)

def format_all_inc_month_totals(uid):
    totals = read_inc_totals(uid)
    if not totals:
        return "No income history yet."
    lines = ["📅 Monthly income totals:"]
    for mk in sorted(totals.keys()):
        amt = totals[mk].get("total", 0)
        lines.append(f" {mk} {amt:>10,.0f}")
    return "\n".join(lines)

def format_recent_expenses(uid, month_key=None, limit=20):
    entries = read_expenses(uid)
    if month_key:
        entries = [e for e in entries if e["dt"][:7] == month_key]
    entries = list(reversed(entries[-limit:]))
    return [format_entry(e, i) for i, e in enumerate(entries)]

def recalc_all_totals(uid):
    all_entries = read_expenses(uid)
    for fname in os.listdir(PLANNER_DIR):
        if fname.startswith(f"{uid}expenses_") and fname.endswith(".json") and "totals" not in fname:
            all_entries.extend(_read_json_list(os.path.join(PLANNER_DIR, fname)))
    totals = {}
    for entry in all_entries:
        mk = _month_key(entry["dt"])
        if mk not in totals:
            totals[mk] = {"total": 0.0}
            for s in CAT_SLUGS:
                totals[mk][s] = 0.0
        cat = entry.get("category", "other")
        totals[mk]["total"] = round(totals[mk].get("total", 0) + entry["amount"], 2)
        totals[mk][cat]     = round(totals[mk].get(cat, 0)   + entry["amount"], 2)
    _save_totals(uid, totals)
    return totals


def planner(uid):
    return os.path.join(PLANNER_DIR, f"{uid}plan.txt")

def read_events(uid):
    if not os.path.exists(planner(uid)):
        return []
    with open(planner(uid), "r", encoding="utf-8") as f:
        return [l.rstrip() for l in f if l.strip()]

def write_events(uid, events):
    with open(planner(uid), "w", encoding="utf-8") as f:
        for e in events:
            f.write(e + "\n")

def append_event(uid, text):
    with open(planner(uid), "a", encoding="utf-8") as f:
        f.write(text.strip() + "\n")

def rearrange(uid):
    events = read_events(uid)
    parsed = []
    for l in events:
        try:
            dt = datetime.fromisoformat(l.split()[0])
            parsed.append((dt, l))
        except:
            pass
    parsed.sort(key=lambda x: x[0])
    write_events(uid, [l for _, l in parsed])

def parse_event_line(line):
    try:
        parts = line.split()
        dt = datetime.fromisoformat(parts[0])
        hashtag = extract_hashtag(line)
        uid_event = next((p for p in parts if p.startswith("uid")), None)
        desc_start = line.find(' ') + 1
        desc_end = line.find(uid_event) if uid_event else len(line)
        desc_text = line[desc_start:desc_end].strip()
        return dt, desc_text, hashtag, uid_event, line
    except Exception as e:
        log.warning(f"Failed parsing line: {line} | {e}")
        return None

# ================= REMINDER WORKERS =================
def cleanup_sent_reminders():
    """Enhancement 2: Remove old reminder keys to prevent memory leak"""
    now = datetime.now()
    keys_to_delete = []
    with reminder_lock:
        for key in sent_reminders:
            parts = key.split('|')
            if len(parts) >= 3:
                try:
                    event_date = datetime.fromisoformat(parts[2])
                    if now - event_date > timedelta(days=7):
                        keys_to_delete.append(key)
                except:
                    pass
        if keys_to_delete:
            for k in keys_to_delete:
                del sent_reminders[k]
            with open(REMINDER_FILE, "w", encoding="utf-8") as f:
                json.dump(sent_reminders, f, indent=2)
            log.info(f"Cleaned up {len(keys_to_delete)} old reminder keys.")


# ================= EXPENSE ARCHIVE WORKER =================
def expense_archive_worker():
    last_run_date = None
    while True:
        try:
            now = datetime.now()
            today = now.date()
            if now.hour == 3 and now.minute < 2:
                if last_run_date == today:
                    time.sleep(30)
                    continue
                last_run_date = today
                cutoff_month = today.month - EXPENSE_ARCHIVE_MONTHS
                cutoff_year  = today.year
                while cutoff_month <= 0:
                    cutoff_month += 12
                    cutoff_year  -= 1
                cutoff = datetime(cutoff_year, cutoff_month, 1).date()
                with state_lock:
                    uids = list(states.keys())
                for uid in uids:
                    entries = read_expenses(uid)
                    keep, archive = [], {}
                    for e in entries:
                        try:
                            e_date = datetime.fromisoformat(e["dt"]).date()
                        except Exception:
                            keep.append(e)
                            continue
                        if e_date < cutoff:
                            archive.setdefault(e_date.year, []).append(e)
                        else:
                            keep.append(e)
                    if not archive:
                        continue
                    for yr, archived_entries in archive.items():
                        arch_path = exp_archive_file(uid, yr)
                        existing  = _read_json_list(arch_path)
                        existing_ids = {e["id"] for e in existing}
                        new_ones = [e for e in archived_entries if e["id"] not in existing_ids]
                        _write_json(arch_path, existing + new_ones)
                        log.info(f"Archived {len(new_ones)} expense(s) for user {uid} → {yr}")
                    write_expenses(uid, keep)
                time.sleep(20)
            time.sleep(60)
        except Exception as ex:
            log.error(f"expense_archive_worker error: {ex}")
            time.sleep(60)


def daily_digest_worker():
    while True:
        try:
            now = datetime.now()
            if now.hour == 8 and now.minute == 0:
                cleanup_sent_reminders()
                today = now.date()
                with state_lock:
                    uids = list(states.keys())
                    for uid in uids:
                        events = read_events(uid)
                        todays = []
                        for l in events:
                            parsed = parse_event_line(l)
                            if not parsed:
                                continue
                            dt, _, _, _, _ = parsed
                            if dt.date() == today:
                                todays.append(l)
                        if todays:
                            msg = "📅 Events today:\n" + "\n".join(todays)
                            try:
                                send(int(uid), msg)
                            except Exception as e:
                                log.error(f"Daily digest send failed for {uid}: {e}")
            time.sleep(61)
        except Exception as e:
            log.error(f"Daily digest worker error: {e}")
            time.sleep(60)



def daily_tomorrow_reminder_worker():
    """Send full tomorrow's events reminder at 22:00 (10 PM)"""
    last_run_date = None
    while True:
        try:
            now = datetime.now()
            today = now.date()
            tomorrow = today + timedelta(days=1)
            # Run at 22:00 (10 PM), only once per day
            if now.hour == 22 and now.minute < 2:
                if last_run_date == today:
                    time.sleep(30)
                    continue
                last_run_date = today
                with state_lock:
                    uids = list(states.keys())
                    for uid in uids:
                        events = read_events(uid)
                        tomorrows_events = []
                        for l in events:
                            parsed = parse_event_line(l)
                            if not parsed:
                                continue
                            dt, _, _, _, raw = parsed
                            if dt.date() == tomorrow:
                                tomorrows_events.append(raw)
                        if tomorrows_events:
                            weekday = tomorrow.strftime("%A")
                            msg = f"📅 Events for tomorrow ({tomorrow} {weekday}):\n" + "\n".join(tomorrows_events)
                            try:
                                send(int(uid), msg)
                                log.info(f"Sent tomorrow's events reminder to {uid}")
                            except Exception as e:
                                log.error(f"Tomorrow reminder send failed for {uid}: {e}")
                time.sleep(20)
            time.sleep(60)
        except Exception as e:
            log.error(f"Daily tomorrow reminder worker error: {e}")
            time.sleep(60)


def events_for_date(uid, target_date):
    events = read_events(uid)
    matched = []
    for line in events:
        parsed = parse_event_line(line)
        if not parsed:
            continue
        dt, _, _, _, raw = parsed
        if dt.date() == target_date:
            matched.append(raw)
    return matched

# Patch for hourly_reminder_worker indentation error

def hourly_reminder_worker():
    while True:
        try:
            now = datetime.now()
            with state_lock:
                uids = list(states.keys())
                for uid in uids:
                    events = read_events(uid)
                    for l in events:
                        parsed = parse_event_line(l)
                        if not parsed:
                            continue
                        dt, desc, hashtag, uid_event, _ = parsed
                        delta = (dt - now).total_seconds()
                        if 0 < delta <= 3600:
                            key = f"{uid}|{uid_event}|{dt.isoformat()}"
                            with reminder_lock:
                                if key in sent_reminders:
                                    continue
                                # Use the full original line for the reminder
                                full_event_line = l  # 'l' is the original line from the planner
                                msg = f"⏰ Reminder:\n{full_event_line}"
                                try:
                                    send(int(uid), msg)
                                    sent_reminders[key] = True
                                    with open(REMINDER_FILE, "w", encoding="utf-8") as f:
                                        json.dump(sent_reminders, f, indent=2)
                                except Exception as e:
                                    log.error(f"Reminder send failed for {uid}: {e}")
            time.sleep(60)
        except Exception as e:
            log.error(f"Hourly reminder worker error: {e}")
            time.sleep(60)

def daily_event_reminder_worker():
    """Send reminders for #event hashtag at 17:00"""
    last_run_date = None
    event_re = re.compile(r"\b(event)\b", re.IGNORECASE)
    while True:
        try:
            now = datetime.now()
            today = now.date()
            if now.hour == 17 and now.minute < 2:
                if last_run_date == today:
                    time.sleep(30)
                    continue
                last_run_date = today
                with state_lock:
                    uids = list(states.keys())
                    for uid in uids:
                        events = read_events(uid)
                        day_map = {}
                        for line in events:
                            parsed = parse_event_line(line)
                            if not parsed:
                                continue
                            dt, _, _, _, raw_line = parsed
                            if dt >= now and event_re.search(raw_line):
                                day = dt.date()
                                day_map.setdefault(day, []).append(raw_line)
                        for day in sorted(day_map):
                            weekday_num = datetime.combine(day, datetime.min.time()).isoweekday()
                            weekday_emoji = WEEKDAY_EMOJI[weekday_num]
                            block = "\n".join(day_map[day])
                            msg = f"📌 {weekday_emoji} Event reminders for {day}:\n{block}"
                            try:
                                send(int(uid), msg)
                            except Exception as e:
                                log.error(f"17:00 event reminder failed for {uid}: {e}")
                time.sleep(20)
        except Exception as e:
            log.error(f"Daily event reminder worker error: {e}")
            time.sleep(60)

def daily_control_reminder_worker():
    """Send reminders for #control hashtag at 18:00"""
    last_run_date = None
    control_re = re.compile(r"\b(control)\b", re.IGNORECASE)
    while True:
        try:
            now = datetime.now()
            today = now.date()
            if now.hour == 18 and now.minute < 2:
                if last_run_date == today:
                    time.sleep(30)
                    continue
                last_run_date = today
                with state_lock:
                    uids = list(states.keys())
                    for uid in uids:
                        events = read_events(uid)
                        day_map = {}
                        for line in events:
                            parsed = parse_event_line(line)
                            if not parsed:
                                continue
                            dt, _, _, _, raw_line = parsed
                            if dt >= now and control_re.search(raw_line):
                                day = dt.date()
                                day_map.setdefault(day, []).append(raw_line)
                        for day in sorted(day_map):
                            weekday_num = datetime.combine(day, datetime.min.time()).isoweekday()
                            weekday_emoji = WEEKDAY_EMOJI[weekday_num]
                            block = "\n".join(day_map[day])
                            msg = f"📌 {weekday_emoji} Control reminders for {day}:\n{block}"
                            try:
                                send(int(uid), msg)
                            except Exception as e:
                                log.error(f"18:00 control reminder failed for {uid}: {e}")
                time.sleep(20)
        except Exception as e:
            log.error(f"Daily control reminder worker error: {e}")
            time.sleep(60)

def daily_pers_reminder_worker():
    """Send reminders for #pers hashtag at 21:00"""
    last_run_date = None
    pers_re = re.compile(r"\b(pers)\b", re.IGNORECASE)
    while True:
        try:
            now = datetime.now()
            today = now.date()
            if now.hour == 21 and now.minute < 2:
                if last_run_date == today:
                    time.sleep(30)
                    continue
                last_run_date = today
                with state_lock:
                    uids = list(states.keys())
                    for uid in uids:
                        events = read_events(uid)
                        day_map = {}
                        for line in events:
                            parsed = parse_event_line(line)
                            if not parsed:
                                continue
                            dt, _, _, _, raw_line = parsed
                            if dt >= now and pers_re.search(raw_line):
                                day = dt.date()
                                day_map.setdefault(day, []).append(raw_line)
                        for day in sorted(day_map):
                            weekday_num = datetime.combine(day, datetime.min.time()).isoweekday()
                            weekday_emoji = WEEKDAY_EMOJI[weekday_num]
                            block = "\n".join(day_map[day])
                            msg = f"📌 {weekday_emoji} Personal reminders for {day}:\n{block}"
                            try:
                                send(int(uid), msg)
                            except Exception as e:
                                log.error(f"21:00 pers reminder failed for {uid}: {e}")
                time.sleep(20)
        except Exception as e:
            log.error(f"Daily pers reminder worker error: {e}")
            time.sleep(60)

def multi_day_reminder_worker():
    """Send reminders at 14, 7, and 3 days prior to events (for event/pers/control tags)"""
    reminder_intervals = [
        (14, "🗓️ Two weeks before"),
        (7, "🗓️ One week before"),
        (3, "🗓️ Three days before"),
    ]
    last_run_date = None
    while True:
        try:
            now = datetime.now()
            today = now.date()
            if now.hour == 9 and now.minute < 2:
                if last_run_date == today:
                    time.sleep(30)
                    continue
                last_run_date = today
                with state_lock:
                    uids = list(states.keys())
                    for uid in uids:
                        events = read_events(uid)
                        for l in events:
                            parsed = parse_event_line(l)
                            if not parsed:
                                continue
                            dt, desc, hashtag, uid_event, raw_line = parsed
                            if dt.date() <= today:
                                continue
                            if not EVENT_OR_PERS_RE.search(raw_line):
                                continue
                            days_until = (dt.date() - today).days
                            for days_prior, reminder_prefix in reminder_intervals:
                                if days_until == days_prior:
                                    key = f"{uid}|{uid_event}|{dt.isoformat()}|{days_prior}d"
                                    with reminder_lock:
                                        if key in sent_reminders:
                                            continue
                                        msg = f"{reminder_prefix}:\n{dt.strftime('%Y-%m-%d %H:%M')} {desc} {hashtag}"
                                        try:
                                            send(int(uid), msg)
                                            sent_reminders[key] = True
                                            with open(REMINDER_FILE, "w", encoding="utf-8") as f:
                                                json.dump(sent_reminders, f, indent=2)
                                            log.info(f"Sent {days_prior}d reminder to {uid} for {uid_event}")
                                        except Exception as e:
                                            log.error(f"Multi-day reminder failed for {uid}: {e}")
                time.sleep(20)
        except Exception as e:
            log.error(f"Multi-day reminder worker error: {e}")
            time.sleep(60)



def custom_reminder_worker():
    """Check and send user-defined custom reminders based on minutes-before-event"""
    while True:
        try:
            now = datetime.now()
            with state_lock:
                uids = list(states.keys())
                for uid in uids:
                    with reminder_lock:
                        for key in list(sent_reminders.keys()):
                            parts = key.split('|')
                            if len(parts) >= 4 and parts[3].startswith("custom_"):
                                if sent_reminders[key].get("notified", False):
                                    continue
                                
                                stored_uid, event_uid, event_dt_str, reminder_tag = parts
                                
                                if stored_uid != str(uid):
                                    continue
                                
                                try:
                                    event_dt = datetime.fromisoformat(event_dt_str)
                                    minutes_before = sent_reminders[key]["minutes_before"]
                                    notify_time = event_dt - timedelta(minutes=minutes_before)
                                    time_diff = (notify_time - now).total_seconds()
                                    
                                    if -60 <= time_diff <= 60:
                                        desc = sent_reminders[key].get("event_desc", "Event")
                                        msg = f"⏰ Custom Reminder ({minutes_before}m before):\n{event_dt.strftime('%H:%M')} {desc}"
                                        
                                        try:
                                            send(int(uid), msg)
                                            sent_reminders[key]["notified"] = True
                                            with open(REMINDER_FILE, "w", encoding="utf-8") as f:
                                                json.dump(sent_reminders, f, indent=2)
                                        except Exception as e:
                                            log.error(f"Custom reminder send failed for {uid}: {e}")
                                except Exception as e:
                                    log.warning(f"Failed processing custom reminder key {key}: {e}")
            
            time.sleep(30)
        
        except Exception as e:
            log.error(f"Custom reminder worker error: {e}")
            time.sleep(60)

# ================= COMPLETED EVENTS =================
def done_file(uid):
    return os.path.join(PLANNER_DIR, f"{uid}done.txt")

def append_done(uid, text):
    with open(done_file(uid), "a", encoding="utf-8") as f:
        f.write(text.strip() + "\n")

def read_done(uid):
    if not os.path.exists(done_file(uid)):
        return []
    with open(done_file(uid), "r", encoding="utf-8") as f:
        return [l.rstrip() for l in f if l.strip()]

# ================= DATE HELPERS =================
def safe_add_months(dt, months):
    month = dt.month - 1 + months
    year = dt.year + month // 12
    month = month % 12 + 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return dt.replace(year=year, month=month, day=day)

def safe_add_years(dt, years):
    try:
        return dt.replace(year=dt.year + years)
    except ValueError:
        return dt.replace(year=dt.year + years, day=28)

# ================= GROUPING =================
WEEKDAY_EMOJI = ["", "1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣"]

def group_by_day(events):
    parsed = []
    for i, l in enumerate(events):
        try:
            dt = datetime.fromisoformat(l.split()[0])
            parsed.append((dt, i, l))
        except:
            continue
    parsed.sort(key=lambda x: x[0])
    day_map = {}
    for dt, idx, line in parsed:
        day = dt.date()
        day_map.setdefault(day, []).append((idx, line))
    messages = []
    today = datetime.now().date()
    for day in sorted(day_map):
        weekday_num = datetime.combine(day, datetime.min.time()).isoweekday()
        wd = WEEKDAY_EMOJI[weekday_num]
        overdue_prefix = "⏳ " if day < today else ""
        block = "\n".join(f"{i+1}. {l}" for i, l in day_map[day])
        messages.append(f"{overdue_prefix}{wd} {day}\n{block}")
    return messages

def send_today_with_weekday(uid):
    now = datetime.now()
    msg = now.strftime("Today: %Y-%m-%d (%A)")
    send(uid, msg)

def save_photos(uid, message_id, peer_id):
    os.makedirs("user_photos", exist_ok=True)
    path = os.path.join("user_photos", f"{uid}photo.txt")
    msg = vk.messages.getById(message_ids=message_id)["items"][0]
    if not msg.get("attachments"):
        return
    desc = msg.get("text", "").strip()
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{peer_id}|{message_id}||{desc}\n")
    send(uid, "Saved photo reference.", main_menu_kb())

def days_per_month_message(year: int, selected_month: Optional[int] = None) -> str:
    now = datetime.now()
    current_year = now.year
    current_month = now.month
    lines = [f"📅 Days per month for {year}:"]
    for m in range(1, 13):
        days = calendar.monthrange(year, m)[1]
        marks = ""
        if year == current_year and m == current_month:
            marks += " ✔"
        if selected_month == m:
            marks += " 🎯"
        lines.append(f"{m:02d}: {days} days{marks}")
    return "\n".join(lines)

def two_month_calendar_message():
    today = datetime.now().date()
    calendar.setfirstweekday(calendar.MONDAY)
    def render_month(year, month):
        lines = [f"📆 {calendar.month_name[month]} {year}"]
        for week in calendar.monthcalendar(year, month):
            cells = []
            for d in week:
                if d == 0:
                    cells.append("  ")
                elif d == today.day and month == today.month and year == today.year:
                    cells.append(f"[{d:02}]")
                else:
                    cells.append(f"{d:02}")
            lines.append("Mo " + " ".join(cells) + " Su")
        return lines
    y, m = today.year, today.month
    ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
    output = []
    output.extend(render_month(y, m))
    output.append("")
    output.extend(render_month(ny, nm))
    return "\n".join(output)

# ================= PAGINATION =================
def send_batch(uid, key_msgs, key_offset):
    data = user(uid)["data"]
    msgs = data.get(key_msgs, [])
    offset = data.get(key_offset, 0)
    batch = msgs[offset:offset + DAYS_PER_BATCH]
    if not batch:
        send(uid, "— End —", main_menu_kb())
        clear_data(uid)
        set_state(uid, STATE_START)
        return
    today_str = datetime.now().date().isoformat()
    send(uid, f"📅 Today: {today_str}")
    for m in batch:
        send(uid, m)
    data[key_offset] = offset + DAYS_PER_BATCH
    save_states()
    kb = nav_kb(data[key_offset] < len(msgs))
    send(uid, "Navigation:", kb)




# ================= REMINDER TRACKING =================
def load_sent_reminders():
    if not os.path.exists(REMINDER_FILE):
        return {}
    try:
        with open(REMINDER_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def save_sent_reminders(data):
    with reminder_lock:
        with open(REMINDER_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

sent_reminders = load_sent_reminders()

# ===== PHOTO RETRIEVAL =====
def send_photos(uid):
    path = os.path.join("user_photos", f"{uid}photo.txt")
    if not os.path.exists(path):
        send(uid, "You have no saved photos.", main_menu_kb())
        return
    with open(path, "r", encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip()]
    for i, line in enumerate(lines, 1):
        ref, desc = line.split("||", 1)
        peer_id, msg_id = ref.split("|")
        if desc:
            send(uid, f"Photo {i}:\n{desc}")
        vk.messages.send(user_id=uid, random_id=0, forward_messages=int(msg_id))
    send(uid, "Menu:", main_menu_kb())

def read_photo_entries(uid):
    path = os.path.join("user_photos", f"{uid}photo.txt")
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [l.rstrip() for l in f if l.strip()]





# ================= MAIN LOOP =================
threading.Thread(target=daily_digest_worker, daemon=True).start()
threading.Thread(target=hourly_reminder_worker, daemon=True).start()
threading.Thread(target=daily_event_reminder_worker, daemon=True).start()
threading.Thread(target=daily_control_reminder_worker, daemon=True).start()
threading.Thread(target=daily_pers_reminder_worker, daemon=True).start()
threading.Thread(target=multi_day_reminder_worker, daemon=True).start()
threading.Thread(target=custom_reminder_worker, daemon=True).start()
threading.Thread(target=daily_tomorrow_reminder_worker, daemon=True).start()
threading.Thread(target=expense_archive_worker, daemon=True).start()

for ev in longpoll.listen():
    if ev.type != VkEventType.MESSAGE_NEW or not ev.to_me:
        continue
    uid = ev.user_id
    text = ev.text.strip()
    u = user(uid)
    state = u["state"]
    log.info(f"{uid} | {state} | {text}")

    # ===== PHOTO HANDLING =====
    if getattr(ev, "attachments", None):
        save_photos(uid, ev.message_id, ev.peer_id)

    # ===== GLOBAL COMMANDS =====
    if text.strip() == "/":
        commands = [
            ("/reset", "Reset bot state"),
            ("/date", "Query events by date"),
            ("/number", "Search events by text"),
            ("/pics", "Show saved photos"),
            ("/rearrange", "Rearrange your planner events"),
            ("/today", "Show today's events"),
            ("/tomorrow", "Show next day's events"),
            ("/extend", "Extend existing event"),
            ("/remind", "Set custom reminders")
        ]
        send(uid, "📖 Available commands:")
        for cmd, desc in commands:
            send(uid, cmd)
            send(uid, desc)
        continue

    if text.lower() == "/reset":
        clear_data(uid)
        set_state(uid, STATE_START)
        send(uid, "Reset.", main_menu_kb())
        continue

    if text.lower() == "/date":
        clear_data(uid)
        set_state(uid, STATE_DATE_QUERY)
        send(uid, "📅 Enter date in format YYYY-MM-DD:")
        continue

    if text.lower() == "/number":
        clear_data(uid)
        set_state(uid, STATE_NUMBER_QUERY)
        send(uid, "Enter a text to search for in your planner:")
        continue

    if text.lower() == "/extend":
        events = read_events(uid)
        if not events:
            send(uid, "No events to extend.", main_menu_kb())
        else:
            clear_data(uid)
            set_data(uid, "msgs", group_by_day(events))
            set_data(uid, "offset", 0)
            set_state(uid, STATE_EXTEND_SELECT)
            send(uid, "Select event number to extend:")
            send_batch(uid, "msgs", "offset")
        continue

    if text.lower() == "/remind":
        events = read_events(uid)
        if not events:
            send(uid, "No events to set reminders for.", main_menu_kb())
        else:
            clear_data(uid)
            set_data(uid, "remind_msgs", group_by_day(events))
            set_data(uid, "remind_offset", 0)
            set_state(uid, STATE_REMIND_SELECT)
            send(uid, "Select event number to set reminder for:")
            send_batch(uid, "remind_msgs", "remind_offset")
        continue

    if text.lower() == "/today":
        today = datetime.now().date()
        weekday = datetime.now().strftime("%A")
        send(uid, f"📅 Today: {today} ({weekday})")
        matches = events_for_date(uid, today)
        if not matches:
            send(uid, "No events for today.")
        else:
            send(uid, "Today's events:")
            for line in matches:
                send(uid, line)
        clear_data(uid)
        set_state(uid, STATE_START)
        send(uid, "Menu:", main_menu_kb())
        continue

    if text.lower() == "/tomorrow":
        tomorrow = (datetime.now() + timedelta(days=1)).date()
        weekday = (datetime.now() + timedelta(days=1)).strftime("%A")
        send(uid, f"📅 Tomorrow: {tomorrow} ({weekday})")
        matches = events_for_date(uid, tomorrow)
        if not matches:
            send(uid, "No events for tomorrow.")
        else:
            send(uid, "Tomorrow's events:")
            for line in matches:
                send(uid, line)
        clear_data(uid)
        set_state(uid, STATE_START)
        send(uid, "Menu:", main_menu_kb())
        continue


    if text.lower() == "/pics":
        send_photos(uid)
        clear_data(uid)
        set_state(uid, STATE_START)
        continue

    if text.lower() == "/rearrange":
        rearrange(uid)
        send(uid, "Rearranged.", main_menu_kb())
        continue

    # ===== BACK TO MENU (GLOBAL) =====
# =====    if text == "Back to menu":
# =====        clear_data(uid)
# =====        set_state(uid, STATE_START)
# =====        send(uid, "Menu:", main_menu_kb())
# =====        continue

    # ===== START MENU =====
    if state == STATE_START:
        if text == "Suggest":
            clear_data(uid)
            send_today_with_weekday(uid)
            send(uid, two_month_calendar_message())
            set_state(uid, STATE_SUGGEST_YEAR)
            send(uid, "Enter year (YYYY):", year_kb())
        elif text == "Quick note":
            clear_data(uid)
            set_state(uid, STATE_QUICK_ADD)
            send(uid, "Send text to save:")
        elif text == "Complete":
            events = read_events(uid)
            if not events:
                send(uid, "No events to complete.", main_menu_kb())
            else:
                clear_data(uid)
                set_data(uid, "msgs", group_by_day(events))
                set_data(uid, "offset", 0)
                set_state(uid, STATE_COMPLETE)
                send_batch(uid, "msgs", "offset")
        elif text == "List":
            set_state(uid, STATE_LIST_MAIN_MENU)
            send(uid, "Choose list type:", list_menu_main_kb())
        elif text == "Delete":
            set_state(uid, STATE_DELETE_MENU)
            send(uid, "Choose deletion type:", delete_menu_kb())
        elif text == "Edit":
            set_state(uid, STATE_EDIT_MENU)
            send(uid, "Choose edit type:", edit_menu_kb())
        elif text == "Quick Commands":
            set_state(uid, STATE_QUICK_COMMANDS)
            send(uid, "Choose quick command:", quick_commands_kb())
        elif text == "Budget":
            clear_data(uid)
            set_state(uid, STATE_BUDGET_MENU)
            send(uid, "💼 Budget:", budget_menu_kb())
        else:
            send(uid, "Menu:", main_menu_kb())
        continue



    # ===== QUICK COMMANDS MENU =====
    if state == STATE_QUICK_COMMANDS:
        if text == "Back to menu":
            clear_data(uid)
            set_state(uid, STATE_START)
            send(uid, "Menu:", main_menu_kb())
        elif text == "/today":
            today = datetime.now().date()
            weekday = datetime.now().strftime("%A")
            send(uid, f"📅 Today: {today} ({weekday})")
            matches = events_for_date(uid, today)
            if not matches:
                send(uid, "No events for today.")
            else:
                send(uid, "Today's events:")
                for line in matches:
                    send(uid, line)
            clear_data(uid)
            set_state(uid, STATE_START)
            send(uid, "Menu:", main_menu_kb())
        elif text == "/tomorrow":
            tomorrow = (datetime.now() + timedelta(days=1)).date()
            weekday = (datetime.now() + timedelta(days=1)).strftime("%A")
            send(uid, f"📅 Tomorrow: {tomorrow} ({weekday})")
            matches = events_for_date(uid, tomorrow)
            if not matches:
                send(uid, "No events for tomorrow.")
            else:
                send(uid, "Tomorrow's events:")
                for line in matches:
                    send(uid, line)
            clear_data(uid)
            set_state(uid, STATE_START)
            send(uid, "Menu:", main_menu_kb())
        elif text == "/number":
            clear_data(uid)
            set_state(uid, STATE_NUMBER_QUERY)
            send(uid, "Enter a text to search for in your planner:")
        elif text == "/extend":
            events = read_events(uid)
            if not events:
                send(uid, "No events to extend.", main_menu_kb())
                set_state(uid, STATE_START)
            else:
                clear_data(uid)
                set_data(uid, "msgs", group_by_day(events))
                set_data(uid, "offset", 0)
                set_state(uid, STATE_EXTEND_SELECT)
                send(uid, "Select event number to extend:")
                send_batch(uid, "msgs", "offset")
        else:
            send(uid, "Choose quick command:", quick_commands_kb())
        continue


    # ===== DELETE MENU SUBMENU =====
    if state == STATE_DELETE_MENU:
        if text == "Back to menu":
            clear_data(uid)
            set_state(uid, STATE_START)
            send(uid, "Menu:", main_menu_kb())
        elif text == "Del P":
            photo_file = os.path.join("user_photos", f"{uid}photo.txt")
            if not os.path.exists(photo_file):
                send(uid, "No saved photo entries.", main_menu_kb())
                set_state(uid, STATE_START)
            else:
                with open(photo_file, "r", encoding="utf-8") as f:
                    entries = [l.rstrip() for l in f if l.strip()]
                if not entries:
                    send(uid, "No saved photo entries.", main_menu_kb())
                    set_state(uid, STATE_START)
                else:
                    clear_data(uid)
                    set_data(uid, "photo_entries", entries)
                    set_state(uid, STATE_DELETE_PHOTOS)
                    send(uid, "Saved photo entries:")
                    for i, line in enumerate(entries, start=1):
                        desc = line.split("||", 1)[1] if "||" in line else ""
                        send(uid, f"{i}. {desc or '[no description]'}")
                    send(uid, "Send numbers separated by spaces (e.g. 1 3 5):")
        elif text == "Del Hash":
            events = read_events(uid)
            if not events:
                send(uid, "No events to delete.", main_menu_kb())
                set_state(uid, STATE_START)
            else:
                clear_data(uid)
                set_state(uid, STATE_DELETE_HASHTAG)
                send(uid, "Enter hashtag to delete:")
        elif text == "Del ID":
            events = read_events(uid)
            if not events:
                send(uid, "No events to delete.", main_menu_kb())
                set_state(uid, STATE_START)
            else:
                clear_data(uid)
                set_state(uid, STATE_DELETE_UID)
                send(uid, "Send UID to delete:")
        elif text == "Del Ar":
            events = read_events(uid)
            if not events:
                send(uid, "No events to delete.", main_menu_kb())
                set_state(uid, STATE_START)
            else:
                clear_data(uid)
                set_data(uid, "msgs", group_by_day(events))
                set_data(uid, "offset", 0)
                set_state(uid, STATE_DELETE_ARRAY)
                send(uid, "Send numbers separated by spaces (e.g. 1 3 5):")
                send_batch(uid, "msgs", "offset")
        elif text == "Del C":
            events = read_done(uid)
            if not events:
                send(uid, "No completed events to delete.", main_menu_kb())
                set_state(uid, STATE_START)
            else:
                clear_data(uid)
                set_data(uid, "msgs", group_by_day(events))
                set_data(uid, "offset", 0)
                set_state(uid, STATE_DELETE_DONE)
                send_batch(uid, "msgs", "offset")
        else:
            send(uid, "Choose deletion type:", delete_menu_kb())
        continue

    # ===== LIST MENU SUBMENU =====
    if state == STATE_LIST_MAIN_MENU:
        if text == "Back to menu":
            clear_data(uid)
            set_state(uid, STATE_START)
            send(uid, "Menu:", main_menu_kb())
        elif text == "List events":
            events = read_events(uid)
            if not events:
                send(uid, "No events.", main_menu_kb())
                set_state(uid, STATE_START)
            else:
                clear_data(uid)
                set_data(uid, "msgs", group_by_day(events))
                set_data(uid, "offset", 0)
                set_state(uid, STATE_LIST_VIEW)
                send_batch(uid, "msgs", "offset")
        elif text == "List completed":
            events = read_done(uid)
            if not events:
                send(uid, "No completed events.", main_menu_kb())
                set_state(uid, STATE_START)
            else:
                clear_data(uid)
                set_data(uid, "msgs", group_by_day(events))
                set_data(uid, "offset", 0)
                set_state(uid, STATE_LIST_VIEW)
                send_batch(uid, "msgs", "offset")
        elif text == "Filter by hashtag":
            events = read_events(uid)
            if not events:
                send(uid, "No events to filter.", main_menu_kb())
                set_state(uid, STATE_START)
            else:
                clear_data(uid)
                set_state(uid, STATE_FILTER)
                send(uid, "Enter hashtag to filter (e.g., event, pers, control):")
        else:
            send(uid, "Choose list type:", list_menu_main_kb())
        continue

    # ===== EDIT MENU SUBMENU =====
# ===== BUDGET MENU =====
    if state == STATE_BUDGET_MENU:
        if text == "Back to menu":
            clear_data(uid)
            set_state(uid, STATE_START)
            send(uid, "Menu:", main_menu_kb())
        elif text == "Expenses":
            clear_data(uid)
            set_state(uid, STATE_EXP_MENU)
            send(uid, "💰 Expense tracker:", exp_menu_kb())
        elif text == "Income":
            clear_data(uid)
            set_state(uid, STATE_INC_MENU)
            send(uid, "💵 Income tracker:", inc_menu_kb())

        else:
            send(uid, "💼 Budget:", budget_menu_kb())
        continue



    # ===== INCOME MENU =====
    if state == STATE_INC_MENU:
        if text == "Back to menu":
            clear_data(uid)
            set_state(uid, STATE_BUDGET_MENU)
            send(uid, "💼 Budget:", budget_menu_kb())
        elif text == "➕ Add income":
            clear_data(uid)
            set_state(uid, STATE_INC_DATE_CHOICE)
            send(uid, "For which day do you want to add the income?", exp_date_choice_kb())
        elif text == "📊 This month":
            mk = datetime.now().strftime("%Y-%m")
            send(uid, format_inc_month_stats(str(uid), mk))
            history = format_recent_income(str(uid), month_key=mk)
            send(uid, "\n".join(history) if history else "No income this month yet.")
            send(uid, "Income menu:", inc_menu_kb())
        elif text == "📅 By month":
            # Reuse expense monthly totals display logic (you can make separate later)
            send(uid, format_all_inc_month_totals(str(uid)))
            set_state(uid, STATE_INC_MONTH_PICK)
            send(uid, "Pick a month:", exp_month_kb())
        elif text == "🗑 Delete income":
            entries = read_income(uid)
            if not entries:
                send(uid, "No income to delete.", inc_menu_kb())
            else:
                clear_data(uid)
                display = list(reversed(entries[-15:]))
                lines = [format_inc_entry(e, i) for i, e in enumerate(display)]
                orig_indices = list(reversed(range(max(0, len(entries)-15), len(entries))))
                set_data(uid, "inc_del_orig", orig_indices)
                set_state(uid, STATE_INC_DELETE)
                send(uid, "Recent income entries (newest first):\n" + "\n".join(lines))
                send(uid, "Send number(s) to delete (e.g. 1 or 1 3 5):")
        else:
            send(uid, "💵 Income tracker:", inc_menu_kb())
        continue


    # ===== EXPENSE MENU =====
    if state == STATE_EXP_MENU:
        if text == "Back to menu":
            clear_data(uid)
            set_state(uid, STATE_BUDGET_MENU)
            send(uid, "💼 Budget:", budget_menu_kb())
        elif text == "➕ Add expense":
            clear_data(uid)
            set_state(uid, STATE_EXP_DATE_CHOICE)
            send(uid, "For which day do you want to add the expense?", exp_date_choice_kb())
        elif text == "📊 This month":
            mk = datetime.now().strftime("%Y-%m")
            send(uid, format_month_stats(str(uid), mk))
            history = format_recent_expenses(str(uid), month_key=mk)
            send(uid, "\n".join(history) if history else "No entries this month yet.")
            send(uid, "Expense menu:", exp_menu_kb())
        elif text == "📅 By month":
            send(uid, format_all_month_totals(str(uid)))
            set_state(uid, STATE_EXP_MONTH_PICK)
            send(uid, "Pick a month:", exp_month_kb())
        elif text == "🗑 Delete expense":
            entries = read_expenses(uid)
            if not entries:
                send(uid, "No expenses to delete.", exp_menu_kb())
            else:
                clear_data(uid)
                display = list(reversed(entries[-15:]))
                lines = [format_entry(e, i) for i, e in enumerate(display)]
                orig_indices = list(reversed(range(max(0, len(entries)-15), len(entries))))
                set_data(uid, "exp_del_orig", orig_indices)
                set_state(uid, STATE_EXP_DELETE)
                send(uid, "Recent expenses (newest first):\n" + "\n".join(lines))
                send(uid, "Send number(s) to delete (e.g. 1 or 1 3 5):")
        else:
            send(uid, "💰 Expense tracker:", exp_menu_kb())
        continue

    # ===== EXPENSE: AMOUNT =====

    # ===== EXPENSE: PAYMENT TOOL =====
    if state == STATE_EXP_TOOL:
        tool = text.strip()
        valid_tools = {"gp", "hal", "sb", "ren", "oz", "ya", "cert", "cash", "other", "— skip —"}
        
        if tool.lower() in valid_tools or tool == "— skip —":
            amount    = get_data(uid, "exp_amount")
            category  = get_data(uid, "exp_category")
            desc      = get_data(uid, "exp_desc", "")
            exp_date_str = get_data(uid, "exp_date")
            
            if exp_date_str is None:
                exp_date_str = datetime.now().strftime("%Y-%m-%d")
                
            selected_date = datetime.strptime(exp_date_str, "%Y-%m-%d").date()
            dt_for_expense = datetime.combine(selected_date, datetime.min.time())
            
            # Save with tool
            entry = save_expense(
                str(uid),
                amount,
                category,
                desc,
                dt=dt_for_expense,
                tool=tool
            )
            
            em    = _cat_emoji(category)
            tool_str = f"  → {tool.upper()}" if tool != "— skip —" else ""
            mk    = selected_date.strftime("%Y-%m")
            month_tot = read_totals(str(uid)).get(mk, {}).get("total", 0)
            note_line = f" 📝 {desc}" if desc else ""
            
            send(uid,
                f"✅ {em} {amount:,.0f}{note_line}{tool_str}\n"
                f"📊 {mk} total: {month_tot:,.0f}",
                exp_menu_kb()
            )
            clear_data(uid)
            set_state(uid, STATE_EXP_MENU)
        else:
            send(uid, "Choose payment method or skip:", exp_tool_kb())
        continue


# ────────────────────────────────────────────────
#  New date selection flow
# ────────────────────────────────────────────────


    # ────────────────────────────────────────────────
    # Income date selection flow
    # ────────────────────────────────────────────────
    if state == STATE_INC_DATE_CHOICE:
        if text == "For today":
            set_data(uid, "inc_date", datetime.now().strftime("%Y-%m-%d"))
            set_state(uid, STATE_INC_AMOUNT)
            send(uid, "💵 Enter amount:")
        elif text == "For yesterday":
            yesterday_str = (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
            set_data(uid, "inc_date", yesterday_str)
            set_state(uid, STATE_INC_AMOUNT)
            send(uid, f"Adding income for yesterday ({yesterday_str}):")
            send(uid, "💵 Enter amount:")
        elif text == "For specific day":
            set_state(uid, STATE_INC_YEAR)
            send(uid, "Choose year:", exp_year_choice_kb())
        elif text == "Back":
            clear_data(uid)
            set_state(uid, STATE_INC_MENU)
            send(uid, "Income menu:", inc_menu_kb())
        else:
            send(uid, "Choose option:", exp_date_choice_kb())
        continue

    if state == STATE_INC_YEAR:
        now = datetime.now()
        if text.startswith("This year"):
            set_data(uid, "inc_year", now.year)
            set_state(uid, STATE_INC_MONTH)
            send(uid, "Choose month:", exp_month_choice_kb())
        elif text.startswith("Previous year"):
            set_data(uid, "inc_year", now.year - 1)
            set_state(uid, STATE_INC_MONTH)
            send(uid, "Choose month:", exp_month_choice_kb())
        elif text == "Specific year":
            set_data(uid, "inc_asking_year", True)
            send(uid, "Enter year (YYYY):")
        elif text == "Back":
            set_state(uid, STATE_INC_DATE_CHOICE)
            send(uid, "For which day?", exp_date_choice_kb())
        elif get_data(uid, "inc_asking_year"):
            try:
                y = int(text)
                if 2000 <= y <= now.year + 1:
                    set_data(uid, "inc_year", y)
                    del user(uid)["data"]["inc_asking_year"]
                    set_state(uid, STATE_INC_MONTH)
                    send(uid, "Choose month:", exp_month_choice_kb())
                else:
                    send(uid, f"Enter reasonable year (2000–{now.year+1}):")
            except:
                send(uid, "Enter 4-digit year:")
        else:
            send(uid, "Choose year option:", exp_year_choice_kb())
        continue

    if state == STATE_INC_MONTH:
        now = datetime.now()
        if text.startswith("This month"):
            set_data(uid, "inc_month", now.month)
            set_data(uid, "inc_year", now.year)
            set_state(uid, STATE_INC_DAY)
            send(uid, f"Enter day (1–{calendar.monthrange(now.year, now.month)[1]}):")
        elif text.startswith("Previous month"):
            prev = now - timedelta(days=now.day + 5)
            set_data(uid, "inc_year", prev.year)
            set_data(uid, "inc_month", prev.month)
            set_state(uid, STATE_INC_DAY)
            send(uid, f"Enter day (1–{calendar.monthrange(prev.year, prev.month)[1]}):")
        elif text == "Specific month":
            set_data(uid, "inc_asking_month", True)
            send(uid, "Enter month number (1–12):")
        elif text == "Back":
            set_state(uid, STATE_INC_YEAR)
            send(uid, "Choose year:", exp_year_choice_kb())
        elif get_data(uid, "inc_asking_month"):
            try:
                m = int(text)
                if 1 <= m <= 12:
                    y = get_data(uid, "inc_year")
                    set_data(uid, "inc_month", m)
                    del user(uid)["data"]["inc_asking_month"]
                    set_state(uid, STATE_INC_DAY)
                    send(uid, f"Enter day (1–{calendar.monthrange(y, m)[1]}):")
                else:
                    send(uid, "Month must be 1–12:")
            except:
                send(uid, "Enter month number 1–12:")
        else:
            send(uid, "Choose month:", exp_month_choice_kb())
        continue

    if state == STATE_INC_DAY:
        try:
            d = int(text)
            y = get_data(uid, "inc_year")
            m = get_data(uid, "inc_month")
            maxd = calendar.monthrange(y, m)[1]
            if 1 <= d <= maxd:
                chosen_date = datetime(y, m, d)
                set_data(uid, "inc_date", chosen_date.strftime("%Y-%m-%d"))
                set_state(uid, STATE_INC_AMOUNT)
                send(uid, f"Adding income for {chosen_date.strftime('%Y-%m-%d')}:")
                send(uid, "💵 Enter amount:")
            else:
                send(uid, f"Day must be between 1 and {maxd}:")
        except:
            send(uid, "Enter valid day number:")
        continue


    if state == STATE_INC_AMOUNT:
        text_clean = text.replace(",", ".").replace(" ", "")
        try:
            amount = float(text_clean)
            if amount <= 0:
                raise ValueError
            set_data(uid, "inc_amount", amount)
            set_state(uid, STATE_INC_DESC)
            send(uid, f"Amount: {amount:,.0f} — Add a note? (or tap skip):", exp_desc_kb())
        except ValueError:
            send(uid, "❌ Enter a positive number (e.g. 15000):")
        continue

    if state == STATE_INC_DESC:
        desc = "" if text == "— skip —" else text.strip()
        amount = get_data(uid, "inc_amount")
        inc_date_str = get_data(uid, "inc_date")
        if inc_date_str is None:
            inc_date_str = datetime.now().strftime("%Y-%m-%d")
        selected_date = datetime.strptime(inc_date_str, "%Y-%m-%d").date()
        dt_for_income = datetime.combine(selected_date, datetime.min.time())
        entry = save_income(str(uid), amount, desc, dt=dt_for_income)
        mk = selected_date.strftime("%Y-%m")
        month_tot = read_inc_totals(str(uid)).get(mk, {}).get("total", 0)
        note_line = f" 📝 {desc}" if desc else ""
        send(uid, f"✅ +{amount:,.0f}{note_line}\n📈 {mk} total: {month_tot:,.0f}", inc_menu_kb())
        clear_data(uid)
        set_state(uid, STATE_INC_MENU)
        continue


    if state == STATE_INC_MONTH_PICK:
        if text == "Back to menu":
            clear_data(uid)
            set_state(uid, STATE_START)
            send(uid, "Menu:", main_menu_kb())
        elif re.match(r"^\d{4}-\d{2}$", text):
            send(uid, format_inc_month_stats(str(uid), text))
            history = format_recent_income(str(uid), month_key=text)
            send(uid, "\n".join(history) if history else "No income entries for this month.")
            set_state(uid, STATE_INC_MENU)
            send(uid, "Income menu:", inc_menu_kb())
        else:
            send(uid, "Pick a month:", exp_month_kb())
        continue

    if state == STATE_INC_DELETE:
        orig_indices = get_data(uid, "inc_del_orig", [])
        raw_numbers = [x for x in text.split() if x.isdigit()]
        if not raw_numbers:
            send(uid, "Enter one or more numbers from the list (e.g. 1 or 1 3 5):")
            continue
        display_indices = sorted({int(x) - 1 for x in raw_numbers})
        invalid = [i for i in display_indices if not (0 <= i < len(orig_indices))]
        if invalid:
            send(uid, f"Invalid number(s): {', '.join(str(i+1) for i in invalid)}. Try again:")
            continue
        real_indices = sorted({orig_indices[i] for i in display_indices}, reverse=True)
        removed_list = []
        for real_idx in real_indices:
            removed = delete_income_by_index(str(uid), real_idx)
            if removed:
                removed_list.append(removed)
        if not removed_list:
            send(uid, "Nothing deleted.")
        else:
            lines = []
            for removed in removed_list:
                desc_part = f" {removed['desc']}" if removed.get("desc") else ""
                lines.append(f"+{removed['amount']:,.0f}{desc_part}")
            affected_months = {r["dt"][:7] for r in removed_list}
            totals = read_inc_totals(str(uid))
            totals_lines = [f"📈 {mk}: {totals.get(mk, {}).get('total', 0):,.0f}" for mk in sorted(affected_months)]
            send(uid, f"🗑 Deleted {len(removed_list)} entr{'y' if len(removed_list)==1 else 'ies'}:\n" + "\n".join(lines))
            send(uid, "\n".join(totals_lines))
        clear_data(uid)
        set_state(uid, STATE_INC_MENU)
        send(uid, "Income menu:", inc_menu_kb())
        continue


    # ────────────────────────────────────────────────
    #  Expense date selection flow
    # ────────────────────────────────────────────────

    if state == STATE_EXP_DATE_CHOICE:
        if text == "For today":
            set_data(uid, "exp_date", datetime.now().strftime("%Y-%m-%d"))
            set_state(uid, STATE_EXP_AMOUNT)
            send(uid, "💸 Enter amount:")

        elif text == "For yesterday":
            yesterday = datetime.now().date() - timedelta(days=1)
            set_data(uid, "exp_date", yesterday.strftime("%Y-%m-%d")) # same result
            set_state(uid, STATE_EXP_AMOUNT)
            send(uid, f"Adding expense for yesterday ({yesterday}):")
            send(uid, "💸 Enter amount:")

        elif text == "For specific day":
            set_state(uid, STATE_EXP_YEAR)
            send(uid, "Choose year:", exp_year_choice_kb())

        elif text == "Back":
            clear_data(uid)
            set_state(uid, STATE_EXP_MENU)
            send(uid, "Expense menu:", exp_menu_kb())

        else:
            send(uid, "Choose option:", exp_date_choice_kb())
        continue


    if state == STATE_EXP_YEAR:
        now = datetime.now()
        if text.startswith("This year"):
            set_data(uid, "exp_year", now.year)
            set_state(uid, STATE_EXP_MONTH)
            send(uid, "Choose month:", exp_month_choice_kb())

        elif text.startswith("Previous year"):
            set_data(uid, "exp_year", now.year - 1)
            set_state(uid, STATE_EXP_MONTH)
            send(uid, "Choose month:", exp_month_choice_kb())

        elif text == "Specific year":
            set_data(uid, "exp_asking_year", True)
            send(uid, "Enter year (YYYY):")

        elif text == "Back":
            set_state(uid, STATE_EXP_DATE_CHOICE)
            send(uid, "For which day?", exp_date_choice_kb())

        elif get_data(uid, "exp_asking_year"):  # user entered year manually
            try:
                y = int(text)
                if 2000 <= y <= now.year + 1:
                    set_data(uid, "exp_year", y)
                    del user(uid)["data"]["exp_asking_year"]
                    set_state(uid, STATE_EXP_MONTH)
                    send(uid, "Choose month:", exp_month_choice_kb())
                else:
                    send(uid, f"Enter reasonable year (2000–{now.year+1}):")
            except:
                send(uid, "Enter 4-digit year:")
        else:
            send(uid, "Choose year option:", exp_year_choice_kb())
        continue


    if state == STATE_EXP_MONTH:
        now = datetime.now()
        if text.startswith("This month"):
            set_data(uid, "exp_month", now.month)
            set_data(uid, "exp_year", now.year)  # just in case
            set_state(uid, STATE_EXP_DAY)
            send(uid, f"Enter day (1–{calendar.monthrange(now.year, now.month)[1]}):")

        elif text.startswith("Previous month"):
            prev = now - timedelta(days=now.day + 5)
            set_data(uid, "exp_year", prev.year)
            set_data(uid, "exp_month", prev.month)
            set_state(uid, STATE_EXP_DAY)
            send(uid, f"Enter day (1–{calendar.monthrange(prev.year, prev.month)[1]}):")

        elif text == "Specific month":
            set_data(uid, "exp_asking_month", True)
            send(uid, "Enter month number (1–12):")

        elif text == "Back":
            set_state(uid, STATE_EXP_YEAR)
            send(uid, "Choose year:", exp_year_choice_kb())

        elif get_data(uid, "exp_asking_month"):
            try:
                m = int(text)
                if 1 <= m <= 12:
                    y = get_data(uid, "exp_year")
                    set_data(uid, "exp_month", m)
                    del user(uid)["data"]["exp_asking_month"]
                    set_state(uid, STATE_EXP_DAY)
                    send(uid, f"Enter day (1–{calendar.monthrange(y, m)[1]}):")
                else:
                    send(uid, "Month must be 1–12:")
            except:
                send(uid, "Enter month number 1–12:")
        else:
            send(uid, "Choose month:", exp_month_choice_kb())
        continue


    if state == STATE_EXP_DAY:
        try:
            d = int(text)
            y = get_data(uid, "exp_year")
            m = get_data(uid, "exp_month")
            maxd = calendar.monthrange(y, m)[1]
            if 1 <= d <= maxd:
                chosen_date = datetime(y, m, d)
                # You can also let user choose time, but for expenses usually just date is enough
                set_data(uid, "exp_date", chosen_date.strftime("%Y-%m-%d"))   # ← string!
                set_state(uid, STATE_EXP_AMOUNT)
                send(uid, f"Adding expense for {chosen_date.strftime('%Y-%m-%d')}:")
                send(uid, "💸 Enter amount:")
            else:
                send(uid, f"Day must be between 1 and {maxd}:")
        except:
            send(uid, "Enter valid day number:")
        continue


    if state == STATE_EXP_AMOUNT:
        text_clean = text.replace(",", ".").replace(" ", "")
        try:
            amount = float(text_clean)
            if amount <= 0:
                raise ValueError
            set_data(uid, "exp_amount", amount)
            set_state(uid, STATE_EXP_CATEGORY)
            send(uid, f"Amount: {amount:,.0f} — Pick category:", exp_category_kb())
        except ValueError:
            send(uid, "❌ Enter a positive number (e.g. 1500):")
        continue


    # ===== EXPENSE: CATEGORY =====
    if state == STATE_EXP_CATEGORY:
        cat = CAT_LABEL_MAP.get(text)
        if cat:
            set_data(uid, "exp_category", cat)
            set_state(uid, STATE_EXP_DESC)
            send(uid, "Add a note? (or tap skip):", exp_desc_kb())
        else:
            send(uid, "Tap a category button:", exp_category_kb())
        continue

    # ===== EXPENSE: DESC =====
    if state == STATE_EXP_DESC:
        desc = "" if text == "— skip —" else text.strip()
        set_data(uid, "exp_desc", desc)
        amount   = get_data(uid, "exp_amount")
        category = get_data(uid, "exp_category")

        send(uid,
            f"Amount: {amount:,.0f} • {category}\n"
            f"Note: {desc or '—'}",
            exp_tool_kb()
        )
        set_state(uid, STATE_EXP_TOOL)
        continue

    # ===== EXPENSE: MONTH PICK =====
    if state == STATE_EXP_MONTH_PICK:
        if text == "Back to menu":
            clear_data(uid)
            set_state(uid, STATE_START)
            send(uid, "Menu:", main_menu_kb())
        elif re.match(r"^\d{4}-\d{2}$", text):
            send(uid, format_month_stats(str(uid), text))
            history = format_recent_expenses(str(uid), month_key=text)
            send(uid, "\n".join(history) if history else "No entries in active log for this month.")
            set_state(uid, STATE_EXP_MENU)
            send(uid, "Expense menu:", exp_menu_kb())
        else:
            send(uid, "Pick a month:", exp_month_kb())
        continue

    # ===== EXPENSE: DELETE =====
    # ===== EXPENSE: DELETE =====
    if state == STATE_EXP_DELETE:
        orig_indices = get_data(uid, "exp_del_orig", [])
        raw_numbers = [x for x in text.split() if x.isdigit()]
        if not raw_numbers:
            send(uid, "Enter one or more numbers from the list (e.g. 1 or 1 3 5):")
            continue
        display_indices = sorted({int(x) - 1 for x in raw_numbers})
        invalid = [i for i in display_indices if not (0 <= i < len(orig_indices))]
        if invalid:
            send(uid, f"Invalid number(s): {', '.join(str(i+1) for i in invalid)}. Try again:")
            continue
        # Map display indices to real indices, then sort descending to delete safely
        real_indices = sorted({orig_indices[i] for i in display_indices}, reverse=True)
        removed_list = []
        for real_idx in real_indices:
            removed = delete_expense_by_index(str(uid), real_idx)
            if removed:
                removed_list.append(removed)
        if not removed_list:
            send(uid, "Nothing deleted.")
        else:
            lines = []
            for removed in removed_list:
                em = _cat_emoji(removed["category"])
                desc_part = f"  {removed['desc']}" if removed.get("desc") else ""
                lines.append(f"{em} {removed['amount']:,.0f}{desc_part}")
            affected_months = {r["dt"][:7] for r in removed_list}
            totals = read_totals(str(uid))
            totals_lines = [f"📊 {mk}: {totals.get(mk, {}).get('total', 0):,.0f}" for mk in sorted(affected_months)]
            send(uid, f"🗑 Deleted {len(removed_list)} entr{'y' if len(removed_list)==1 else 'ies'}:\n" + "\n".join(lines))
            send(uid, "\n".join(totals_lines))
        clear_data(uid)
        set_state(uid, STATE_EXP_MENU)
        send(uid, "Expense menu:", exp_menu_kb())
        continue

    if state == STATE_EDIT_MENU:
        if text == "Back to menu":
            clear_data(uid)
            set_state(uid, STATE_START)
            send(uid, "Menu:", main_menu_kb())
        elif text == "Edit event":
            events = read_events(uid)
            if not events:
                send(uid, "No events.", main_menu_kb())
                set_state(uid, STATE_START)
            else:
                clear_data(uid)
                set_data(uid, "msgs", group_by_day(events))
                set_data(uid, "offset", 0)
                set_state(uid, STATE_EDIT_SELECT)
                send_batch(uid, "msgs", "offset")
        elif text == "Edit completed":
            events = read_done(uid)
            if not events:
                send(uid, "No completed events.", main_menu_kb())
                set_state(uid, STATE_START)
            else:
                clear_data(uid)
                set_data(uid, "msgs", group_by_day(events))
                set_data(uid, "offset", 0)
                set_state(uid, STATE_EDIT_DONE_SELECT)
                send_batch(uid, "msgs", "offset")
        else:
            send(uid, "Choose edit type:", edit_menu_kb())
        continue

    # ===== SUGGEST EVENT FLOW =====
    if state == STATE_SUGGEST_YEAR:
        if text.isdigit() and len(text) == 4:
            set_data(uid, "year", int(text))
            set_state(uid, STATE_SUGGEST_MONTH)
            send(uid, "Enter month (1-12):", month_kb())
        else:
            send(uid, "Invalid year. Enter YYYY:", year_kb())
        continue

    if state == STATE_SUGGEST_MONTH:
        if text.isdigit() and 1 <= int(text) <= 12:
            set_data(uid, "month", int(text))
            set_state(uid, STATE_SUGGEST_DAY)
            year = get_data(uid, "year")
            month = get_data(uid, "month")
            send(uid, days_per_month_message(year, month))
            send(uid, "Enter day:", day_kb())
        else:
            send(uid, "Invalid month. Enter 1-12:", month_kb())
        continue

    if state == STATE_SUGGEST_DAY:
        year = int(get_data(uid, "year"))
        month = int(get_data(uid, "month"))
        if text.isdigit():
            day = int(text)
            max_day = calendar.monthrange(year, month)[1]
            if 1 <= day <= max_day:
                set_data(uid, "day", day)
                set_state(uid, STATE_SUGGEST_HOUR)
                send(uid, "Enter hour (0-23):", hour_kb())
            else:
                send(uid, f"Invalid day. {calendar.month_name[month]} {year} has {max_day} days.")
                send(uid, "Enter day again:", day_kb())
        else:
            send(uid, "Please enter a number for the day.")
            send(uid, "Enter day again:", day_kb())
        continue

    if state == STATE_SUGGEST_HOUR:
        if text.isdigit() and 0 <= int(text) <= 23:
            set_data(uid, "hour", int(text))
            set_state(uid, STATE_SUGGEST_MINUTE)
            send(uid, "Enter minute (0-59):", minute_kb())
        else:
            send(uid, "Invalid hour. Enter 0-23:", hour_kb())
        continue

    if state == STATE_SUGGEST_MINUTE:
        if text.isdigit() and 0 <= int(text) <= 59:
            set_data(uid, "minute", int(text))
            set_state(uid, STATE_SUGGEST_DESC)
            send(uid, "Send description:")
        else:
            send(uid, "Invalid minute. Enter 0-59:", minute_kb())
        continue

    if state == STATE_SUGGEST_DESC:
        set_data(uid, "desc", text)
        set_state(uid, STATE_SUGGEST_HASHTAG)
        send(uid, "Enter hashtag:", hashtag_kb())
        continue

    if state == STATE_SUGGEST_HASHTAG:
        if text in ["pers", "cons", "job", "event", "control"]:
            set_data(uid, "hashtag", text)
            set_state(uid, STATE_SUGGEST_RECURRENCE)
            send(uid, f"Hashtag {text} accepted.")
            send(uid, "Select recurrence:", recurrence_kb())
        else:
            set_data(uid, "hashtag", text)
            set_state(uid, STATE_SUGGEST_RECURRENCE)
            send(uid, "Select recurrence:", recurrence_kb())
        # Remove the unconditional send() above
        continue

    if state == STATE_SUGGEST_RECURRENCE:
        recurrence_options = ["One-time", "Weekly", "Biweekly", "Monthly", "Yearly"]
        if text in recurrence_options:
            recurrence = text.lower()
            set_data(uid, "recurrence", recurrence)
            if recurrence == "one-time":
                set_data(uid, "count", 1)
                set_state(uid, STATE_SUGGEST_DURATION)
                send(uid, "Enter duration in minutes (or ? for unknown):", duration_kb())
            else:
                set_state(uid, STATE_SUGGEST_COUNT)
                send(uid, "Enter number of occurrences:")
        else:
            send(uid, "Select recurrence:", recurrence_kb())
        continue

    if state == STATE_SUGGEST_COUNT:
        if text.isdigit() and int(text) >= 1:
            set_data(uid, "count", int(text))
            set_state(uid, STATE_SUGGEST_DURATION)
            send(uid, "Enter duration in minutes (or ? for unknown):", duration_kb())
        else:
            send(uid, "Enter valid number of occurrences:")
        continue

    if state == STATE_SUGGEST_DURATION:
        set_data(uid, "duration", text)
        set_state(uid, STATE_SUGGEST_PLACE)
        send(uid, "Enter place (can be ?):", place_kb())
        continue

    if state == STATE_SUGGEST_PLACE:
        year = get_data(uid, "year")
        month = get_data(uid, "month")
        day = get_data(uid, "day")
        hour = get_data(uid, "hour")
        minute = get_data(uid, "minute")
        desc = get_data(uid, "desc")
        hashtag = get_data(uid, "hashtag")
        recurrence = get_data(uid, "recurrence")
        count = get_data(uid, "count")
        duration = get_data(uid, "duration")
        place = get_data(uid, "place", text)
        base_dt = datetime(year, month, day, hour, minute)
        uid_event = next_uid(uid)
        delta_map = {
            "one-time": timedelta(),
            "weekly": timedelta(days=7),
            "biweekly": timedelta(days=14),
            "monthly": None,
            "yearly": None
        }
        events_to_append = []
        for i in range(count):
            dt = base_dt
            if recurrence == "monthly":
                dt = safe_add_months(base_dt, i)
            elif recurrence == "yearly":
                dt = safe_add_years(base_dt, i)
            else:
                dt = dt + i * delta_map.get(recurrence, timedelta())
            line = f"{dt.isoformat()} {desc} {hashtag} {uid_event} {duration} {place}".strip()
            events_to_append.append(line)
        for e in events_to_append:
            append_event(uid, e)
        rearrange(uid)
        clear_data(uid)
        set_state(uid, STATE_START)
        send(uid, f"Saved {count} events.", main_menu_kb())
        continue

    # ===== EXTEND FLOW =====
    if state == STATE_EXTEND_SELECT:
        if text == "Next":
            send_batch(uid, "msgs", "offset")
        else:
            try:
                idx = int(text) - 1
                events = read_events(uid)
                if 0 <= idx < len(events):
                    set_data(uid, "extend_idx", idx)
                    set_state(uid, STATE_EXTEND_PERIOD)
                    send(uid, "Select extension period:", extend_kb())
                else:
                    send(uid, "Invalid number.", nav_kb(True))
            except:
                send(uid, "Enter number.", nav_kb(True))
        continue

    if state == STATE_EXTEND_PERIOD:
        period_map = {
            "Weekly": timedelta(days=7),
            "Biweekly": timedelta(days=14),
            "Monthly": "monthly",
            "Annually": "yearly"
        }
        if text not in period_map:
            send(uid, "Select extension period:", extend_kb())
            continue
        idx = get_data(uid, "extend_idx")
        events = read_events(uid)
        if idx is None or not (0 <= idx < len(events)):
            send(uid, "Extension failed.", main_menu_kb())
            clear_data(uid)
            set_state(uid, STATE_START)
            continue
        original_line = events.pop(idx)
        parsed = parse_event_line(original_line)
        if not parsed:
            send(uid, "Failed parsing event.", main_menu_kb())
            clear_data(uid)
            set_state(uid, STATE_START)
            continue
        dt, desc_text, hashtag, uid_event, _ = parsed
        period = period_map[text]
        if period == "monthly":
            new_dt = safe_add_months(dt, 1)
        elif period == "yearly":
            new_dt = safe_add_years(dt, 1)
        else:
            new_dt = dt + period
        tail = original_line.split(" ", 1)[1]
        new_line = f"{new_dt.isoformat()} {tail}"
        events.append(new_line)
        write_events(uid, events)
        rearrange(uid)
        send(uid, "✅ Your event got extended.")
        send(uid, f"📅 It was rewritten to new date: {new_dt.date()}")
        send(uid, f"New entry:\n{new_line}")
        clear_data(uid)
        set_state(uid, STATE_START)
        send(uid, "Menu:", main_menu_kb())
        continue

    # ===== LIST MENU (OLD) =====
    if state == STATE_LIST_MENU:
        if text == "Show all":
            events = read_events(uid)
            if not events:
                send(uid, "No events.", main_menu_kb())
                set_state(uid, STATE_START)
            else:
                clear_data(uid)
                set_data(uid, "msgs", group_by_day(events))
                set_data(uid, "offset", 0)
                set_state(uid, STATE_LIST_VIEW)
                send_batch(uid, "msgs", "offset")
        elif text == "Filter by hashtag":
            set_state(uid, STATE_FILTER)
            send(uid, "Enter hashtag:")
        else:
            set_state(uid, STATE_START)
            send(uid, "Menu.", main_menu_kb())
        continue

    # ===== FILTER =====
    if state == STATE_FILTER:
        tag = text.strip().lower()
        if tag.startswith('#'):
            tag = tag[1:]
        events = [e for e in read_events(uid) if tag in e.lower()]
        if not events:
            send(uid, f"No matches for {tag}.", main_menu_kb())
            set_state(uid, STATE_START)
        else:
            clear_data(uid)
            set_data(uid, "msgs", group_by_day(events))
            set_data(uid, "offset", 0)
            set_state(uid, STATE_LIST_VIEW)
            send(uid, f"🔍 Found {len(events)} event(s) with {tag}:")
            send_batch(uid, "msgs", "offset")
        continue

    # ===== LIST VIEW =====
    if state == STATE_LIST_VIEW:
        if text == "Next":
            send_batch(uid, "msgs", "offset")
        else:
            clear_data(uid)
            set_state(uid, STATE_START)
            send(uid, "Menu.", main_menu_kb())
        continue

    # ===== DATE QUERY =====
    if state == STATE_DATE_QUERY:
        try:
            target_date = datetime.strptime(text, "%Y-%m-%d").date()
        except ValueError:
            send(uid, "❌ Invalid format. Please use YYYY-MM-DD:")
            continue
        matches = events_for_date(uid, target_date)
        if not matches:
            send(uid, f"No events for {target_date}.")
        else:
            send(uid, f"📅 Events for {target_date}:")
            for line in matches:
                send(uid, line)
        clear_data(uid)
        set_state(uid, STATE_START)
        send(uid, "Menu:", main_menu_kb())
        continue

    # ===== DELETE BY ARRAY =====
    if state == STATE_DELETE_ARRAY:
        if text == "Next":
            send_batch(uid, "msgs", "offset")
        else:
            try:
                numbers = sorted({int(x) - 1 for x in text.split() if x.isdigit()}, reverse=True)
                events = read_events(uid)
                removed = []
                for idx in numbers:
                    if 0 <= idx < len(events):
                        removed.append(events.pop(idx))
                if not removed:
                    send(uid, "No valid numbers.", nav_kb(True))
                else:
                    write_events(uid, events)
                    rearrange(uid)
                    send(uid, "You've deleted entries:")
                    for r in removed:
                        send(uid, r)
                    send(uid, "Done.", main_menu_kb())
            except:
                send(uid, "Enter numbers separated by spaces.", nav_kb(True))
            clear_data(uid)
            set_state(uid, STATE_START)
        continue

    # ===== COMPLETE EVENT =====
    if state == STATE_COMPLETE:
        if text == "Next":
            send_batch(uid, "msgs", "offset")
        else:
            try:
                idx = int(text) - 1
                events = read_events(uid)
                if 0 <= idx < len(events):
                    completed = events.pop(idx)
                    append_done(uid, completed)
                    write_events(uid, events)
                    rearrange(uid)
                    send(uid, f"✅ Completed:\n{completed}", main_menu_kb())
                else:
                    send(uid, "Invalid number.", nav_kb(True))
            except:
                send(uid, "Enter number.", nav_kb(True))
            clear_data(uid)
            set_state(uid, STATE_START)
        continue

    # ===== DELETE BY HASHTAG =====
    if state == STATE_DELETE_HASHTAG:
        tag = text.strip()
        events = [e for e in read_events(uid) if tag not in e]
        write_events(uid, events)
        rearrange(uid)
        send(uid, f"Deleted events with hashtag {tag}.", main_menu_kb())
        clear_data(uid)
        set_state(uid, STATE_START)
        continue

    # ===== DELETE BY UID =====
    if state == STATE_DELETE_UID:
        del_uid = text.strip()
        events = [e for e in read_events(uid) if not line_has_uid(e, del_uid)]
        write_events(uid, events)
        rearrange(uid)
        send(uid, f"Deleted events with UID {del_uid}.", main_menu_kb())
        clear_data(uid)
        set_state(uid, STATE_START)
        continue

    # ===== DELETE COMPLETED BY NUMBER =====
    if state == STATE_DELETE_DONE:
        if text == "Next":
            send_batch(uid, "msgs", "offset")
        else:
            try:
                idx = int(text) - 1
                events = read_done(uid)
                if 0 <= idx < len(events):
                    removed = events.pop(idx)
                    with open(done_file(uid), "w", encoding="utf-8") as f:
                        for e in events:
                            f.write(e + "\n")
                    send(uid, "Deleted:")
                    send(uid, f"{removed}", main_menu_kb())
                else:
                    send(uid, "Invalid number.", nav_kb(True))
            except:
                send(uid, "Enter number.", nav_kb(True))
            clear_data(uid)
            set_state(uid, STATE_START)
        continue

    # ===== EDIT COMPLETED =====
    if state == STATE_EDIT_DONE_SELECT:
        if text == "Next":
            send_batch(uid, "msgs", "offset")
        else:
            try:
                idx = int(text) - 1
                events = read_done(uid)
                if 0 <= idx < len(events):
                    set_data(uid, "edit_idx", idx)
                    set_state(uid, STATE_EDIT_DONE_INPUT)
                    send(uid, "Текст оригинального сообщения для правки")
                    send(uid, events[idx])
                    send(uid, "Отправь измененную версию")
                else:
                    send(uid, "Invalid number.", nav_kb(True))
            except:
                send(uid, "Enter number.", nav_kb(True))
        continue

    if state == STATE_EDIT_DONE_INPUT:
        idx = get_data(uid, "edit_idx")
        events = read_done(uid)
        if idx is not None and 0 <= idx < len(events):
            events[idx] = text.strip()
            with open(done_file(uid), "w", encoding="utf-8") as f:
                for e in events:
                    f.write(e + "\n")
            send(uid, "Updated.", main_menu_kb())
        else:
            send(uid, "Edit failed.", main_menu_kb())
        clear_data(uid)
        set_state(uid, STATE_START)
        continue

    # ===== QUICK ADD =====
    if state == STATE_QUICK_ADD:
        append_event(uid, text)
        clear_data(uid)
        set_state(uid, STATE_START)
        send(uid, "Saved.", main_menu_kb())
        continue

    # ===== NUMBER QUERY =====
    if state == STATE_NUMBER_QUERY:
        query = text.strip()
        events = read_events(uid)
        found = []
        for idx, line in enumerate(events):
            parsed = parse_event_line(line)
            if not parsed:
                continue
            dt, desc, _, _, raw = parsed
            if query in desc:
                line_no = idx + 1
                weekday = dt.strftime("%A")
                found.append((line_no, weekday, raw))
        if not found:
            send(uid, "No matches found.")
        else:
            send(uid, "🔎 Matches in planner (absolute line numbers):")
            for line_no, weekday, raw in found:
                send(uid, f"#{line_no} | {weekday}\n{raw}")
        clear_data(uid)
        set_state(uid, STATE_START)
        send(uid, "Menu:", main_menu_kb())
        continue

    # ===== EDIT =====
    if state == STATE_EDIT_SELECT:
        if text == "Next":
            send_batch(uid, "msgs", "offset")
        else:
            try:
                idx = int(text) - 1
                events = read_events(uid)
                if 0 <= idx < len(events):
                    set_data(uid, "edit_idx", idx)
                    set_state(uid, STATE_EDIT_INPUT)
                    send(uid, "Текст оригинального сообщения для правки")
                    send(uid, f"{events[idx]}")
                    send(uid, "Отправь измененную версию")
                else:
                    send(uid, "Invalid number.", nav_kb(True))
            except:
                send(uid, "Enter number.", nav_kb(True))
        continue

    # ===== DELETE PHOTOS =====
    if state == STATE_DELETE_PHOTOS:
        try:
            numbers = sorted({int(x) - 1 for x in text.split() if x.isdigit()}, reverse=True)
            entries = get_data(uid, "photo_entries", [])
            removed = []
            for idx in numbers:
                if 0 <= idx < len(entries):
                    removed.append(entries.pop(idx))
            if not removed:
                send(uid, "No valid numbers.", main_menu_kb())
            else:
                photo_file = os.path.join("user_photos", f"{uid}photo.txt")
                with open(photo_file, "w", encoding="utf-8") as f:
                    for e in entries:
                        f.write(e + "\n")
                send(uid, "You've deleted photo entries:")
                for r in removed:
                    desc = r.split("||", 1)[1] if "||" in r else ""
                    send(uid, desc or "[no description]")
                send(uid, "Done.", main_menu_kb())
        except Exception as e:
            log.error(f"Photo delete failed for {uid}: {e}")
            send(uid, "Enter numbers separated by spaces.", main_menu_kb())
        clear_data(uid)
        set_state(uid, STATE_START)
        continue



    # ===== REMIND FLOW: SELECT EVENT =====
    if state == STATE_REMIND_SELECT:
        if text == "Next":
            send_batch(uid, "remind_msgs", "remind_offset")
        else:
            try:
                idx = int(text) - 1
                events = read_events(uid)
                if 0 <= idx < len(events):
                    parsed = parse_event_line(events[idx])
                    if not parsed:
                        send(uid, "Failed to parse event.", nav_kb(True))
                        continue
                    dt, desc, hashtag, uid_event, raw_line = parsed
                    set_data(uid, "remind_event_idx", idx)
                    set_data(uid, "remind_event_uid", uid_event)
                    set_data(uid, "remind_event_dt", dt.isoformat())
                    set_data(uid, "remind_event_desc", f"{desc} {hashtag}".strip())
                    set_state(uid, STATE_REMIND_COUNT)
                    send(uid, f"Selected: {dt.strftime('%Y-%m-%d %H:%M')} {desc} {hashtag}")
                    send(uid, "How many reminders do you want to set for this event? (1-5):")
                else:
                    send(uid, "Invalid number.", nav_kb(True))
            except:
                send(uid, "Enter a valid number.", nav_kb(True))
        continue

    # ===== REMIND FLOW: NUMBER OF REMINDERS =====
    if state == STATE_REMIND_COUNT:
        if text.isdigit() and 1 <= int(text) <= 5:
            count = int(text)
            set_data(uid, "remind_count", count)
            set_data(uid, "remind_index", 0)
            set_data(uid, "remind_minutes_list", [])
            set_state(uid, STATE_REMIND_MINUTES)
            send(uid, f"Reminder 1 of {count}: How many minutes before the event should I notify you?", remind_minutes_kb())
        else:
            send(uid, "Please enter a number between 1 and 5:")
        continue

    # ===== REMIND FLOW: MINUTES FOR EACH REMINDER =====
    if state == STATE_REMIND_MINUTES:
        if text.isdigit() and int(text) >= 0:
            minutes = int(text)
            minutes_list = get_data(uid, "remind_minutes_list", [])
            minutes_list.append(minutes)
            set_data(uid, "remind_minutes_list", minutes_list)
            current_index = get_data(uid, "remind_index", 0) + 1
            total_count = get_data(uid, "remind_count", 1)
            if current_index < total_count:
                set_data(uid, "remind_index", current_index)
                send(uid, f"Reminder {current_index + 1} of {total_count}: How many minutes before the event?", remind_minutes_kb())
            else:
                # All reminders collected, save them
                event_uid = get_data(uid, "remind_event_uid")
                event_dt_str = get_data(uid, "remind_event_dt")
                event_desc = get_data(uid, "remind_event_desc")
                event_dt = datetime.fromisoformat(event_dt_str)
                for mins in minutes_list:
                    reminder_key = f"{uid}|{event_uid}|{event_dt_str}|custom_{mins}m"
                    with reminder_lock:
                        sent_reminders[reminder_key] = {
                            "minutes_before": mins,
                            "notified": False,
                            "event_desc": event_desc
                        }
                    save_sent_reminders(sent_reminders)
                send(uid, f"✅ Set {len(minutes_list)} custom reminder(s) for: {event_desc}")
                send(uid, f"Notifications will be sent {', '.join(str(m) + 'm' for m in minutes_list)} before the event.")
                clear_data(uid)
                set_state(uid, STATE_START)
                send(uid, "Menu:", main_menu_kb())
        else:
            send(uid, "Please select or enter a valid number of minutes (0 or more):", remind_minutes_kb())
        continue

    # ===== EDIT INPUT =====
    if state == STATE_EDIT_INPUT:
        idx = get_data(uid, "edit_idx")
        events = read_events(uid)
        if idx is not None and 0 <= idx < len(events):
            events[idx] = text.strip()
            write_events(uid, events)
            rearrange(uid)
            send(uid, "Updated.", main_menu_kb())
        else:
            send(uid, "Edit failed.", main_menu_kb())
        clear_data(uid)
        set_state(uid, STATE_START)
        continue
