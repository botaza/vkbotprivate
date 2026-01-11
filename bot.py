import vk_api
from vk_api.longpoll import VkLongPoll, VkEventType
from vk_api.keyboard import VkKeyboard, VkKeyboardColor
from datetime import datetime, timedelta
import os, json, logging

# ================= LOGGING =================
logging.basicConfig(
    filename="bot.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ================= CONFIG =================
TOKEN_FILE = "token.txt"
STATE_FILE = "states.json"
PLANNER_DIR = "planners"
DAYS_PER_BATCH = 10

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

STATE_DELETE = "delete"
STATE_DELETE_HASHTAG = "delete_hashtag"
STATE_DELETE_UID = "delete_uid"

STATE_EDIT_SELECT = "edit_select"
STATE_EDIT_INPUT = "edit_input"

# ================= TOKEN =================
with open(TOKEN_FILE, "r", encoding="utf-8") as f:
    TOKEN = f.read().strip()

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
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(states, f, indent=2)

states = load_states()

def user(uid):
    uid = str(uid)
    if uid not in states:
        states[uid] = {"state": STATE_START, "data": {}, "next_uid": 1}
        save_states()
    return states[uid]

def set_state(uid, s):
    user(uid)["state"] = s
    save_states()

def set_data(uid, k, v):
    user(uid)["data"][k] = v
    save_states()

def get_data(uid, k, default=None):
    return user(uid)["data"].get(k, default)

def clear_data(uid):
    user(uid)["data"] = {}
    save_states()

def next_uid(uid):
    val = user(uid).get("next_uid", 1)
    user(uid)["next_uid"] = val + 1
    save_states()
    return f"uid{val}"

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
    for day in sorted(day_map):
        wd = WEEKDAY_EMOJI[datetime.combine(day, datetime.min.time()).isoweekday()]
        block = "\n".join(f"{i+1}. {l}" for i, l in day_map[day])
        messages.append(f"{wd} {day}\n{block}")

    return messages

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

    for m in batch:
        send(uid, m)

    data[key_offset] = offset + DAYS_PER_BATCH
    save_states()

    kb = nav_kb(data[key_offset] < len(msgs))
    send(uid, "Navigation:", kb)

# ================= KEYBOARDS (YEAR / MONTH / DAY) =================
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
    kb.add_button(str(now), VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button(str((now % 12) + 1), VkKeyboardColor.PRIMARY)
    return kb.get_keyboard()

def day_kb():
    kb = VkKeyboard(one_time=True)
    today = datetime.now().day
    tomorrow = (datetime.now() + timedelta(days=1)).day
    kb.add_button(str(today), VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button(str(tomorrow), VkKeyboardColor.PRIMARY)
    return kb.get_keyboard()
# ================= KEYBOARDS (HOUR / MINUTE / DURATION / PLACE) =================
def hour_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("00", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("23", VkKeyboardColor.PRIMARY)
    return kb.get_keyboard()

def minute_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("00", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("59", VkKeyboardColor.PRIMARY)
    return kb.get_keyboard()

def duration_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("?", VkKeyboardColor.PRIMARY)
    return kb.get_keyboard()

def place_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("?", VkKeyboardColor.PRIMARY)
    return kb.get_keyboard()

# ================= EXISTING KEYBOARDS =================
def main_menu_kb():
    kb = VkKeyboard(one_time=True)
    kb.add_button("Suggest events", VkKeyboardColor.POSITIVE)
    kb.add_line()
    kb.add_button("List events", VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("Delete by number", VkKeyboardColor.NEGATIVE)
    kb.add_button("Delete by hashtag", VkKeyboardColor.NEGATIVE)
    kb.add_button("Delete by UID", VkKeyboardColor.NEGATIVE)
    kb.add_line()
    kb.add_button("Edit event", VkKeyboardColor.SECONDARY)
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

# ================= VK =================
vk_session = vk_api.VkApi(token=TOKEN)
vk = vk_session.get_api()
longpoll = VkLongPoll(vk_session)

def send(uid, text, kb=None):
    vk.messages.send(user_id=uid, random_id=0, message=text, keyboard=kb)

# ================= MAIN LOOP =================
for ev in longpoll.listen():
    if ev.type != VkEventType.MESSAGE_NEW or not ev.to_me:
        continue

    uid = ev.user_id
    text = ev.text.strip()
    u = user(uid)
    state = u["state"]
    log.info(f"{uid} | {state} | {text}")

    # ===== GLOBAL COMMANDS =====
    if text.lower() == "/reset":
        clear_data(uid)
        set_state(uid, STATE_START)
        send(uid, "Reset.", main_menu_kb())
        continue

    if text.lower() == "/rearrange":
        rearrange(uid)
        send(uid, "Rearranged.", main_menu_kb())
        continue

    # ===== START MENU =====
    if state == STATE_START:
        if text == "Suggest events":
            set_state(uid, STATE_SUGGEST_YEAR)
            clear_data(uid)
            send(uid, "Enter year (YYYY):", year_kb())
        elif text == "List events":
            set_state(uid, STATE_LIST_MENU)
            send(uid, "Choose:", list_menu_kb())
        elif text == "Delete by number":
            events = read_events(uid)
            if not events:
                send(uid, "No events to delete.", main_menu_kb())
            else:
                clear_data(uid)
                set_data(uid, "msgs", group_by_day(events))
                set_data(uid, "offset", 0)
                set_state(uid, STATE_DELETE)
                send_batch(uid, "msgs", "offset")
        elif text == "Delete by hashtag":
            events = read_events(uid)
            if not events:
                send(uid, "No events to delete.", main_menu_kb())
            else:
                hashtags = set()
                for e in events:
                    parts = e.split()
                    if len(parts) >= 3:
                        hashtags.add(parts[2])
                if not hashtags:
                    send(uid, "No hashtags found.", main_menu_kb())
                else:
                    clear_data(uid)
                    set_data(uid, "hashtags", list(hashtags))
                    set_state(uid, STATE_DELETE_HASHTAG)
                    send(uid, "Existing hashtags:\n" + "\n".join(hashtags))
        elif text == "Delete by UID":
            events = read_events(uid)
            if not events:
                send(uid, "No events to delete.", main_menu_kb())
            else:
                clear_data(uid)
                set_state(uid, STATE_DELETE_UID)
                send(uid, "Send UID to delete:")
        elif text == "Edit event":
            events = read_events(uid)
            if not events:
                send(uid, "No events.", main_menu_kb())
            else:
                clear_data(uid)
                set_data(uid, "msgs", group_by_day(events))
                set_data(uid, "offset", 0)
                set_state(uid, STATE_EDIT_SELECT)
                send_batch(uid, "msgs", "offset")
        else:
            send(uid, "Menu:", main_menu_kb())
        continue

# ===== Suggest Event flow continues in Part 3 =====
# ================= SUGGEST EVENT FLOW =================

    # ===== YEAR =====
    if state == STATE_SUGGEST_YEAR:
        if text.isdigit() and len(text) == 4:
            set_data(uid, "year", int(text))
            set_state(uid, STATE_SUGGEST_MONTH)
            send(uid, "Enter month (1-12):", month_kb())
        else:
            send(uid, "Invalid year. Enter YYYY:", year_kb())
        continue

    # ===== MONTH =====
    if state == STATE_SUGGEST_MONTH:
        if text.isdigit() and 1 <= int(text) <= 12:
            set_data(uid, "month", int(text))
            set_state(uid, STATE_SUGGEST_DAY)
            send(uid, "Enter day (1-31):", day_kb())
        else:
            send(uid, "Invalid month. Enter 1-12:", month_kb())
        continue

    # ===== DAY =====
    if state == STATE_SUGGEST_DAY:
        if text.isdigit() and 1 <= int(text) <= 31:
            set_data(uid, "day", int(text))
            set_state(uid, STATE_SUGGEST_HOUR)
            send(uid, "Enter hour (0-23):", hour_kb())
        else:
            send(uid, "Invalid day. Enter 1-31:", day_kb())
        continue

    # ===== HOUR =====
    if state == STATE_SUGGEST_HOUR:
        if text.isdigit() and 0 <= int(text) <= 23:
            set_data(uid, "hour", int(text))
            set_state(uid, STATE_SUGGEST_MINUTE)
            send(uid, "Enter minute (0-59):", minute_kb())
        else:
            send(uid, "Invalid hour. Enter 0-23:", hour_kb())
        continue

    # ===== MINUTE =====
    if state == STATE_SUGGEST_MINUTE:
        if text.isdigit() and 0 <= int(text) <= 59:
            set_data(uid, "minute", int(text))
            set_state(uid, STATE_SUGGEST_DESC)
            send(uid, "Send description:")
        else:
            send(uid, "Invalid minute. Enter 0-59:", minute_kb())
        continue

    # ===== DESCRIPTION =====
    if state == STATE_SUGGEST_DESC:
        set_data(uid, "desc", text)
        set_state(uid, STATE_SUGGEST_HASHTAG)
        send(uid, "Enter hashtag:")
        continue

    # ===== HASHTAG =====
    if state == STATE_SUGGEST_HASHTAG:
        set_data(uid, "hashtag", text)
        set_state(uid, STATE_SUGGEST_RECURRENCE)
        send(uid, "Select recurrence:", recurrence_kb())
        continue

    # ===== RECURRENCE =====
    if state == STATE_SUGGEST_RECURRENCE:
        recurrence_options = ["One-time", "Weekly", "Biweekly", "Monthly", "Yearly"]
        if text in recurrence_options:
            set_data(uid, "recurrence", text.lower())
            set_state(uid, STATE_SUGGEST_COUNT)
            send(uid, "Enter number of occurrences:")
        else:
            send(uid, "Select recurrence:", recurrence_kb())
        continue

    # ===== COUNT =====
    if state == STATE_SUGGEST_COUNT:
        if text.isdigit() and int(text) >= 1:
            set_data(uid, "count", int(text))
            set_state(uid, STATE_SUGGEST_DURATION)
            send(uid, "Enter duration in minutes (or ? for unknown):", duration_kb())
        else:
            send(uid, "Enter valid number of occurrences:")
        continue

    # ===== DURATION =====
    if state == STATE_SUGGEST_DURATION:
        set_data(uid, "duration", text)
        set_state(uid, STATE_SUGGEST_PLACE)
        send(uid, "Enter place (can be blank or ?):", place_kb())
        continue

    # ===== PLACE & SAVE EVENT =====
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
                month_new = dt.month + i
                year_new = dt.year + (month_new - 1) // 12
                month_new = (month_new - 1) % 12 + 1
                dt = dt.replace(year=year_new, month=month_new)
            elif recurrence == "yearly":
                dt = dt.replace(year=dt.year + i)
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

# ===== LIST MENU =====
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
        tag = text.lower()
        events = [e for e in read_events(uid) if tag in e.lower()]
        if not events:
            send(uid, "No matches.", main_menu_kb())
            set_state(uid, STATE_START)
        else:
            clear_data(uid)
            set_data(uid, "msgs", group_by_day(events))
            set_data(uid, "offset", 0)
            set_state(uid, STATE_LIST_VIEW)
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

# ===== DELETE BY NUMBER =====
    if state == STATE_DELETE:
        if text == "Next":
            send_batch(uid, "msgs", "offset")
        else:
            try:
                idx = int(text) - 1
                events = read_events(uid)
                if 0 <= idx < len(events):
                    removed = events.pop(idx)
                    write_events(uid, events)
                    rearrange(uid)
                    send(uid, f"Deleted:\n{removed}", main_menu_kb())
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
        events = [e for e in read_events(uid) if not tag in e]
        write_events(uid, events)
        rearrange(uid)
        send(uid, f"Deleted events with hashtag {tag}.", main_menu_kb())
        clear_data(uid)
        set_state(uid, STATE_START)
        continue

# ===== DELETE BY UID =====
    if state == STATE_DELETE_UID:
        del_uid = text.strip()
        events = [e for e in read_events(uid) if del_uid not in e]
        write_events(uid, events)
        rearrange(uid)
        send(uid, f"Deleted events with UID {del_uid}.", main_menu_kb())
        clear_data(uid)
        set_state(uid, STATE_START)
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
                    send(uid, f"Original line:\n{events[idx]}\n\nSend edited version:")
                else:
                    send(uid, "Invalid number.", nav_kb(True))
            except:
                send(uid, "Enter number.", nav_kb(True))
        continue

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
