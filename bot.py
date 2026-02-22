import vk_api
from vk_api.longpoll import VkLongPoll, VkEventType
from vk_api.keyboard import VkKeyboard, VkKeyboardColor
from datetime import datetime, timedelta
from typing import Optional
import os, json, logging
from logging.handlers import RotatingFileHandler
import threading
import calendar
import time
import re

# ================= LOGGING (Enhancement 4: Rotation) =================
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
handler = RotatingFileHandler("bot.log", maxBytes=5*1024*1024, backupCount=3)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
log.addHandler(handler)

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

# ================= TOKEN =================
with open(TOKEN_FILE, "r", encoding="utf-8") as f:
    TOKEN = f.read().strip()

# ================= THREAD SAFETY (Enhancement 1) =================
state_lock = threading.RLock()
reminder_lock = threading.RLock()

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
HASHTAG_RE = re.compile(r"(#\w+)")
EVENT_OR_PERS_RE = re.compile(r"\b(event|pers|control)\b", re.IGNORECASE)

def extract_hashtag(text):
    """Return first hashtag in a line or None if missing"""
    m = HASHTAG_RE.search(text)
    return m.group(1) if m else None

def line_has_uid(line, uid_value):
    return uid_value in line.split()

# ================= PLANNER =================
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
                                msg = f"⏰ Reminder:\n{dt.strftime('%H:%M')} {desc} {hashtag}"
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
    kb.add_button("/today", VkKeyboardColor.PRIMARY)
    kb.add_button("/number", VkKeyboardColor.PRIMARY)
    kb.add_button("/extend", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("/date", VkKeyboardColor.PRIMARY)
    kb.add_button("/pics", VkKeyboardColor.PRIMARY)
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
        vk.messages.send(peer_id=uid, random_id=0, forward_messages=int(msg_id))
    send(uid, "Menu:", main_menu_kb())

def read_photo_entries(uid):
    path = os.path.join("user_photos", f"{uid}photo.txt")
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [l.rstrip() for l in f if l.strip()]

# ================= VK =================
vk_session = vk_api.VkApi(token=TOKEN)
vk = vk_session.get_api()
longpoll = VkLongPoll(vk_session)

def send(uid, text, kb=None):
    if not text:
        text = "."
    vk.messages.send(user_id=uid, random_id=0, message=text, keyboard=kb)



# ================= MAIN LOOP =================
threading.Thread(target=daily_digest_worker, daemon=True).start()
threading.Thread(target=hourly_reminder_worker, daemon=True).start()
threading.Thread(target=daily_event_reminder_worker, daemon=True).start()
threading.Thread(target=daily_control_reminder_worker, daemon=True).start()
threading.Thread(target=daily_pers_reminder_worker, daemon=True).start()
threading.Thread(target=multi_day_reminder_worker, daemon=True).start()

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
            ("/extend", "Extend existing event")
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
    if text == "Back to menu":
        clear_data(uid)
        set_state(uid, STATE_START)
        send(uid, "Menu:", main_menu_kb())
        continue

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
                send(uid, "Enter hashtag to filter (e.g., #event, #pers, #control):")
        else:
            send(uid, "Choose list type:", list_menu_main_kb())
        continue

    # ===== EDIT MENU SUBMENU =====
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
        if text.isdigit() and 1 <= int(text) <= 31:
            set_data(uid, "day", int(text))
            set_state(uid, STATE_SUGGEST_HOUR)
            send(uid, "Enter hour (0-23):", hour_kb())
        else:
            send(uid, "Invalid day. Enter 1-31:", day_kb())
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
        send(uid, "Enter hashtag:")
        continue

    if state == STATE_SUGGEST_HASHTAG:
        set_data(uid, "hashtag", text)
        set_state(uid, STATE_SUGGEST_RECURRENCE)
        send(uid, "Select recurrence:", recurrence_kb())
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
        tag = text.strip()
        if not tag.startswith('#'):
            tag = '#' + tag
        tag_lower = tag.lower()
        events = [e for e in read_events(uid) if tag_lower in e.lower()]
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