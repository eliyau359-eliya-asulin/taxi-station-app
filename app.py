"""
מערכת נהגים ותחנות - שרת Flask
--------------------------------
שרת מינימלי שתפקידו:
  1. להגיש את קבצי ה-Frontend הסטטיים (index.html / style.css / script.js).
  2. לספק נתיב API חי לעדכוני תנועה: GET /api/traffic-updates

לגבי עדכוני התנועה: לנתיבי ישראל (וגם למשרד התחבורה) אין נכון להיום API
ציבורי יציב ומתועד המספק "עומסי תנועה בזמן אמת" בפורמט JSON פשוט. הפתרון
הפתוח והרשמי הקיים הוא פורטל הנתונים הממשלתי data.gov.il (CKAN), שדרכו ניתן
לשלוף מאגרי מידע פתוחים של משרד התחבורה/נתיבי ישראל. לכן הנתיב מטה מנסה,
בזמן ריצה, לשלוף רשומות עדכניות ממאגרי תנועה/דרכים פתוחים ב-data.gov.il,
ואם השליפה נכשלת (אין אינטרנט, המאגר השתנה, טיים-אאוט וכו') - מוחזר מידע
לדוגמה (fallback) כדי שהווידג'ט בלקוח תמיד יציג תוכן, עם ציון ברור שמדובר
במידע לדוגמה.
"""

from flask import Flask, jsonify, request, send_from_directory
from flask_socketio import SocketIO
from werkzeug.security import generate_password_hash, check_password_hash
from pathlib import Path
from datetime import datetime
import requests
import threading
import re
import os
import time

try:
    from dotenv import load_dotenv
    load_dotenv()  # טוען .env מקומי אם קיים (ריצה מקומית בלבד - ב-Render המשתנים
    # מגיעים מהגדרות ה-Environment של השירות, לא מקובץ, אז זה לא-אופרטיבי שם ולא מזיק)
except ImportError:
    pass

try:
    from pymongo import MongoClient
except ImportError:
    MongoClient = None

BASE_DIR = Path(__file__).resolve().parent

app = Flask(__name__, static_folder=str(BASE_DIR), static_url_path="")

# עדכון "live" - מודיע ללקוחות מחוברים לרענן (ר' script.js: initLiveSocket) ברגע שמשהו
# משתנה, בלי לחכות לפולינג הבא (עד שנייה). async_mode='threading' נבחר בכוונה במקום
# eventlet/gevent - עובד עם threading הרגיל של Python (ר' Procfile: gthread + threads),
# בלי תלות בספריה שהוכרזה deprecated (eventlet) ובלי לשנות את מודל הפרוסס היחיד
# שעליו SHARED_STATE מסתמך. זו רק "נודניקית לרענן עכשיו" - הפולינג הקיים ממשיך לרוץ
# כרשת ביטחון אם החיבור הזה נופל מכל סיבה (רשת/דפדפן חוסם WebSocket וכו')
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")


def _notify_clients():
    try:
        socketio.emit("refresh")
    except Exception:
        pass

# --- חשבונות תחנה: שם ייחודי + סיסמה --------------------------------------------------
# מנרמל שם תחנה להשוואת ייחודיות/התחברות: מסיר תווי בקרת כיווניות/פורמט בלתי-נראים
# (יכולים להידבק בהעתקה מוואטסאפ/וורד ולגרום לשני שמות שנראים זהים לא להתנגש בבדיקה),
# מכווץ רווחים כפולים ומאחד אותיות רישיות/קטנות - כדי ששתי תחנות לא יוכלו להירשם
# בפועל תחת אותו שם (גם אם הקלידו אותו במעט שונה חזותית). מוגדר כאן (לפני SHARED_STATE/
# _load_state_from_db) כי המיגרציה מהמבנה הישן למטה צריכה אותו כבר בעליית השרת
_BIDI_CONTROL_CODEPOINTS = [
    0x200B, 0x200C, 0x200D, 0x200E, 0x200F,  # zero-width space/joiners, LRM/RLM
    0x202A, 0x202B, 0x202C, 0x202D, 0x202E,  # LRE/RLE/PDF/LRO/RLO
    0x2066, 0x2067, 0x2068, 0x2069,          # LRI/RLI/FSI/PDI
]
_BIDI_CONTROL_RE = re.compile("[" + "".join(chr(c) for c in _BIDI_CONTROL_CODEPOINTS) + "]")


def _normalize_station_name(name):
    cleaned = _BIDI_CONTROL_RE.sub("", name or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned.casefold()


def _default_station_bucket():
    """המבנה הריק של הנתונים השייכים לתחנה בודדת - כל מה שהיה קודם גלובלי-לכל-האפליקציה
    (managerStation/managerDrivers/וכו') ועכשיו מבודד תחת stationData[stationId]"""
    return {
        "managerStation": None,
        "managerDrivers": [],
        "managerCharges": [],
        "managerDispatchers": [],
        "paymentApprovals": [],
        "joinRequests": [],
        "phoneSystemConnected": False,
        "managerDispatchedRides": [],
    }


def _station_bucket(station_id):
    bucket = SHARED_STATE["stationData"].setdefault(station_id, _default_station_bucket())
    # דלי שנוצר לפני שדה חדש נוסף ל-_default_station_bucket (למשל managerDispatchedRides) -
    # משלימים את השדה החסר על הדלי הקיים, כדי ש-GET /api/state לא יחזיר אותו כ-undefined
    for key, default in _default_station_bucket().items():
        bucket.setdefault(key, default)
    return bucket

# מזהי דמו ישנים שהיו נשלחים כברירת מחדל מ-loadAppData (ר' DEMO_RIDE_IDS ב-script.js) -
# הפיצ'ר הזה בוטל, אבל עותקים שכבר נשמרו (מקומית או כאן בזיכרון/DB) עדיין עלולים להכיל
# אותם; מסננים החוצה בכל נקודה שכותבת ל-availableRides כדי שלא "יחזרו לחיים" בטעות
_DEMO_RIDE_IDS = {"ride-1", "ride-2"}


def _strip_demo_rides(rides):
    return [r for r in (rides or []) if r.get("id") not in _DEMO_RIDE_IDS]


# מצב משותף בזיכרון השרת - מאפשר לדסקטופ ולמובייל להתעדכן זה מזה באמצעות polling
# מהלקוח (ראו syncSharedStateFromServer ב-script.js). stationAccounts/stationData מבודדים
# לגמרי בין תחנות שונות (ר' /api/state למטה - נגישים רק לפי station= מתאים); availableRides/
# rideRequests נשארים גלובליים-משותפים כפי שהיו - סדרן מפרסם נסיעה אמיתית דרך
# publishDispatcherRide (script.js), אין יותר תוכן דמו קבוע. לא כולל שדות שהם
# זהות המכשיר/המשתמש הנוכחי בלבד (למשל currentDriverName), שנשארים מקומיים בכל דפדפן.
# מוגבל לפרוסס עובד יחיד של gunicorn (ראו Procfile - "web: gunicorn app:app" בלי דגל -w),
# אחרת כל עובד יחזיק עותק זיכרון נפרד ולא יראו עדכונים אחד של השני.
_state_lock = threading.Lock()
SHARED_STATE = {
    "stationAccounts": [],
    "stationData": {},
    "availableRides": [],
    "rideRequests": [],
    # חשבונות נהג - גלובליים, לא שייכים לאף תחנה ספציפית (נהג נרשם פעם אחת ואז מצטרף/יוצא
    # מתחנות לפי בחירה, ר' /api/driver-register/-login למטה). לא נחשף דרך /api/state -
    # רק דרך שני הנתיבים הייעודיים, כמו stationAccounts שגם הוא לא נחשף כמות שהוא
    "driverAccounts": [],
}

# --- התמדה חיצונית (MongoDB Atlas - שכבה חינמית, לא בתשלום) -------------------------
# בלי זה, SHARED_STATE נמחק בכל דיפלוי/הפעלה מחדש של Render (מערכת הקבצים והזיכרון
# של שירות חינמי ב-Render לא שורדים בין הפעלות). אם מוגדר משתנה הסביבה MONGODB_URI
# (בפאנל ה-Environment של השירות ב-Render) - המצב נטען מה-DB בעליית השרת (_load_state_from_db
# למטה) ונשמר חזרה אליו (_save_state_to_db) אחרי כל שינוי, כמסמך יחיד. בלי המשתנה הזה
# האפליקציה ממשיכה לעבוד בדיוק כמו קודם (זיכרון בלבד, לא נדרש DB כדי להריץ מקומית) -
# שמירה/טעינה הן best-effort: כשל DB זמני לא אמור להפיל בקשה שכבר עדכנה את הזיכרון
MONGODB_URI = os.environ.get("MONGODB_URI")
_db_collection = None

if MONGODB_URI and MongoClient:
    try:
        _mongo_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        try:
            _db = _mongo_client.get_default_database()
        except Exception:
            _db = _mongo_client["taxi_station"]
        _db_collection = _db["app_state"]
    except Exception:
        _db_collection = None


def _save_state_to_db():
    if _db_collection is None:
        return
    try:
        payload = dict(SHARED_STATE)
        payload["_id"] = "shared_state"
        _db_collection.replace_one({"_id": "shared_state"}, payload, upsert=True)
    except Exception:
        pass


def _migrate_legacy_shape(doc):
    """דואג לתאימות לאחור: לפני הוספת חשבונות תחנה, כל הפריסה החיה החזיקה תחנה גלובלית
    יחידה (doc["managerStation"] כמילון בודד ברמה העליונה, בלי stationAccounts/stationData).
    אם ה-doc שנטען מ-Mongo עדיין במבנה הזה - עוטפים אותו כאן (במקום, לפני שהוא נטען
    ל-SHARED_STATE) לחשבון תחנה יחיד עם passwordHash=None + needsPasswordSetup=True (לא
    הייתה סיסמה קודם - station_login למעלה מטפל בזה בהתאם), ומעבירים את כל השדות שהיו
    גלובליים ושייכים בפועל לתחנה הזו (managerDrivers/managerCharges/וכו') לתוך הדלי שלה.
    מחזיר True אם בוצעה מיגרציה (כדי שהקורא ישמור מיד את הצורה החדשה בחזרה ל-DB). לא
    צריך בדיקת "כבר מוגר" נפרדת - ברגע שהמיגרציה רצה היא מוחקת את doc["managerStation"]
    (למטה), ולכן בעלייה הבאה הבדיקה הראשונה כאן כבר מחזירה False מעצמה. בכוונה לא נבדק
    stationAccounts כתנאי "כבר מוגר" - הוא יכול להיות לא-ריק גם משום שנרשמו תחנות חדשות
    לפני שהמיגרציה של הנתונים הישנים רצה, ואז הבדיקה הזו הייתה מדלגת בטעות על מיגרציה
    אמיתית שעדיין נדרשת ומאבדת את נתוני התחנה הישנים (managerDrivers/managerCharges/וכו')"""
    legacy_station = doc.get("managerStation")
    if not isinstance(legacy_station, dict) or not (legacy_station.get("name") or "").strip():
        return False

    station_id = f"station-{int(time.time() * 1000)}"
    # append, לא דריסה - stationAccounts/stationData יכולים כבר להכיל תחנות שנרשמו
    # כרגיל (לא-ממוגרות) לפני שהמיגרציה הזו רצה על הנתונים הישנים
    doc.setdefault("stationAccounts", []).append({
        "id": station_id,
        "name": legacy_station["name"],
        "nameKey": _normalize_station_name(legacy_station["name"]),
        "passwordHash": None,
        "createdAt": datetime.now().isoformat(),
        "needsPasswordSetup": True,
    })
    doc.setdefault("stationData", {})[station_id] = {
        "managerStation": legacy_station,
        "managerDrivers": doc.pop("managerDrivers", []),
        "managerCharges": doc.pop("managerCharges", []),
        "managerDispatchers": doc.pop("managerDispatchers", []),
        "paymentApprovals": doc.pop("paymentApprovals", []),
        "joinRequests": doc.pop("joinRequests", []),
        "phoneSystemConnected": doc.pop("phoneSystemConnected", False),
    }
    doc.pop("managerStation", None)
    doc.pop("stations", None)  # היה נשמר כרשימה סטטית - מעכשיו נגזר תמיד ב-runtime (ר' _public_station_list)
    return True


def _load_state_from_db():
    if _db_collection is None:
        return
    try:
        doc = _db_collection.find_one({"_id": "shared_state"})
        if not doc:
            return
        migrated = _migrate_legacy_shape(doc)
        for key in SHARED_STATE:
            if key in doc:
                SHARED_STATE[key] = doc[key]
        SHARED_STATE["availableRides"] = _strip_demo_rides(SHARED_STATE["availableRides"])
        if migrated:
            _save_state_to_db()
    except Exception:
        pass


_load_state_from_db()

DATA_GOV_IL_BASE = "https://data.gov.il/api/3/action"
REQUEST_TIMEOUT = 6  # שניות

# מאגרי מידע רלוונטיים ב-data.gov.il שכדאי לנסות (לפי סדר עדיפות)
CANDIDATE_QUERIES = ["עומסי תנועה", "אירועים בדרכים", "עדכוני תנועה", "נתיבי ישראל"]

FALLBACK_UPDATES = [
    {
        "title": "עומס כבד בכביש 1 לכיוון ירושלים",
        "description": "עומס תנועה כבד באזור מחלף שער הגיא, זמן נסיעה מוארך.",
        "road": "כביש 1",
        "severity": "high",
        "updated": None,
        "source": "דוגמה (אין חיבור זמין למאגר הנתונים)",
    },
    {
        "title": "עבודות תחזוקה בכביש 4 סמוך לנתניה",
        "description": "נתיב ימני חסום עקב עבודות תשתית, האטה מקומית.",
        "road": "כביש 4",
        "severity": "medium",
        "updated": None,
        "source": "דוגמה (אין חיבור זמין למאגר הנתונים)",
    },
    {
        "title": "תנועה סדירה בכביש 6 (כביש חוצה ישראל)",
        "description": "אין דיווחי עומס חריגים כרגע.",
        "road": "כביש 6",
        "severity": "low",
        "updated": None,
        "source": "דוגמה (אין חיבור זמין למאגר הנתונים)",
    },
]


def _severity_from_text(text: str) -> str:
    text = text or ""
    heavy_words = ["כבד", "חסום", "תאונה", "עצירה"]
    medium_words = ["עבודות", "האטה", "חלקי", "עומס"]
    if any(w in text for w in heavy_words):
        return "high"
    if any(w in text for w in medium_words):
        return "medium"
    return "low"


def fetch_live_traffic_updates():
    """מנסה לשלוף עדכוני תנועה אמיתיים מ-data.gov.il. מחזיר רשימה או None בכשלון."""
    for query in CANDIDATE_QUERIES:
        try:
            search_resp = requests.get(
                f"{DATA_GOV_IL_BASE}/package_search",
                params={"q": query, "rows": 5},
                timeout=REQUEST_TIMEOUT,
            )
            search_resp.raise_for_status()
            results = search_resp.json().get("result", {}).get("results", [])
        except Exception:
            continue

        for package in results:
            for resource in package.get("resources", []):
                if not resource.get("datastore_active"):
                    continue
                resource_id = resource.get("id") or resource.get("resource_id")
                if not resource_id:
                    continue
                try:
                    ds_resp = requests.get(
                        f"{DATA_GOV_IL_BASE}/datastore_search",
                        params={"resource_id": resource_id, "limit": 8},
                        timeout=REQUEST_TIMEOUT,
                    )
                    ds_resp.raise_for_status()
                    records = ds_resp.json().get("result", {}).get("records", [])
                except Exception:
                    continue

                if not records:
                    continue

                updates = []
                for rec in records[:8]:
                    text_fields = [str(v) for v in rec.values() if isinstance(v, str)]
                    title = text_fields[0] if text_fields else package.get("title", query)
                    description = " | ".join(text_fields[1:4]) if len(text_fields) > 1 else ""
                    updates.append(
                        {
                            "title": title[:120],
                            "description": description[:200],
                            "road": package.get("title", ""),
                            "severity": _severity_from_text(title + " " + description),
                            "updated": package.get("metadata_modified"),
                            "source": "data.gov.il",
                        }
                    )
                if updates:
                    return updates
    return None


@app.route("/api/traffic-updates")
def traffic_updates():
    live = fetch_live_traffic_updates()
    if live:
        return jsonify({"success": True, "live": True, "updates": live, "fetched_at": datetime.now().isoformat()})

    fallback = [dict(u, updated=datetime.now().isoformat()) for u in FALLBACK_UPDATES]
    return jsonify({"success": True, "live": False, "updates": fallback, "fetched_at": datetime.now().isoformat()})


def _station_account(station_id):
    return next((a for a in SHARED_STATE["stationAccounts"] if a["id"] == station_id), None)


def _resolve_real_station_id(maybe_composite_id):
    """מזהה תחנה שמגיע מהלקוח (בבקשות join/payment/charge) יכול להיות המזהה האמיתי לבדו
    או מזהה מורכב "<stationId>:<groupId>" (ר' _public_station_list) - כדי לדעת לאיזה
    stationData[...] לכתוב, תמיד לוקחים רק את החלק הראשון ומוודאים שהוא חשבון תחנה קיים"""
    if not maybe_composite_id:
        return None
    real_id = str(maybe_composite_id).split(":", 1)[0]
    return real_id if _station_account(real_id) else None


def _public_station_list():
    """הרשימה שנהגים דפדפים בה כדי להצטרף לתחנה - נגזרת בכל בקשה מתוך חשבונות התחנה
    האמיתיים (לא נשמרת/נדחפת יותר כשדה נפרד, ר' SHARED_STATE_KEYS ב-script.js). תחנה
    שנרשמה אך עדיין לא סיימה את הגדרת "פרטי התחנה" הראשונית (managerStation.name ריק)
    לא מוצגת - היא עדיין לא מוכנה לקבל הצטרפויות. מזהה כל קבוצת-נהגים כ-"<stationId>:<groupId>"
    כדי שאפשר יהיה לפענח חזרה לאיזו תחנה אמיתית היא שייכת (ר' _resolve_real_station_id)"""
    out = []
    for account in SHARED_STATE["stationAccounts"]:
        bucket = SHARED_STATE["stationData"].get(account["id"]) or {}
        manager_station = bucket.get("managerStation")
        if not manager_station or not (manager_station.get("name") or "").strip():
            continue
        groups = manager_station.get("driverGroups") or [{"id": "grp-main"}]
        for group in groups:
            out.append({
                "id": f"{account['id']}:{group['id']}",
                "stationId": account["id"],
                "name": group.get("name") or manager_station["name"],
                "monthlyFee": group.get("fee", manager_station.get("monthlyFee", 0)),
                "commission": group.get("commission", manager_station.get("commission", 0)),
            })
    return out


@app.route("/api/stations")
def list_stations():
    """רשימת התחנות הציבורית לדפדוף/הצטרפות - נתיב פתוח (בלי station=) כי נהג שעדיין
    לא הצטרף לאף תחנה אין לו הקשר-תחנה משלו לשלוח (ר' syncSharedStateFromServer ב-script.js)."""
    with _state_lock:
        return jsonify({"success": True, "stations": _public_station_list()})


@app.route("/api/state")
def get_state():
    """availableRides/rideRequests נשארים גלובליים ומוחזרים תמיד, גם בלי station= (נהג
    שעדיין לא מחובר לאף תחנה עדיין צריך לסנכרן אותם כדי לדפדף/לבקש נסיעות). כשמצוין
    station= (מנהל/סדרן שכן מחוברים לתחנה) - מתווסף גם דלי הנתונים המבודד של אותה תחנה
    בלבד. נקרא ע"י הלקוח כל 3 שניות לסנכרון בין מכשירים (ראו syncSharedStateFromServer)."""
    station_id = request.args.get("station")
    with _state_lock:
        result = {
            "availableRides": SHARED_STATE["availableRides"],
            "rideRequests": SHARED_STATE["rideRequests"],
        }
        if station_id:
            if not _station_account(station_id):
                return jsonify({"success": False, "error": "תחנה לא נמצאה"}), 404
            result.update(_station_bucket(station_id))
        return jsonify(result)


@app.route("/api/state", methods=["POST"])
def update_state():
    """מקבל מהלקוח את השדות ששינה (הפעולה שקראה ל-saveAppData) - availableRides/rideRequests
    (גלובליים) מתעדכנים תמיד אם נשלחו, גם בלי station=; שאר השדות מתעדכנים רק בדלי התחנה
    המצוינת ב-station= (ואם station= לא צוין, מתעלמים מהם - אין דלי אחר לכתוב אליו). אי
    אפשר לגעת בדלי של תחנה אחרת דרך ה-payload - רק דרך station=."""
    station_id = request.args.get("station")
    payload = request.get_json(silent=True) or {}
    with _state_lock:
        if "availableRides" in payload:
            SHARED_STATE["availableRides"] = _strip_demo_rides(payload["availableRides"])
        if "rideRequests" in payload:
            SHARED_STATE["rideRequests"] = payload["rideRequests"]

        if station_id:
            if not _station_account(station_id):
                return jsonify({"success": False, "error": "תחנה לא נמצאה"}), 404
            bucket = _station_bucket(station_id)
            for key, value in payload.items():
                if key in bucket:
                    bucket[key] = value
        _save_state_to_db()
        _notify_clients()
    return jsonify({"success": True})


@app.route("/api/station-register", methods=["POST"])
def station_register():
    payload = request.get_json(silent=True) or {}
    name = (payload.get("name") or "").strip()
    password = payload.get("password") or ""
    if not name or not password:
        return jsonify({"success": False, "error": "שם תחנה וסיסמה נדרשים"}), 400

    name_key = _normalize_station_name(name)
    if not name_key:
        return jsonify({"success": False, "error": "שם תחנה לא תקין"}), 400

    with _state_lock:
        if any(a["nameKey"] == name_key for a in SHARED_STATE["stationAccounts"]):
            return jsonify({"success": False, "error": "שם התחנה כבר תפוס, בחר שם אחר"}), 409

        station_id = f"station-{int(time.time() * 1000)}"
        account = {
            "id": station_id,
            "name": name,
            "nameKey": name_key,
            "passwordHash": generate_password_hash(password),
            "createdAt": datetime.now().isoformat(),
            "needsPasswordSetup": False,
        }
        SHARED_STATE["stationAccounts"].append(account)
        SHARED_STATE["stationData"][station_id] = _default_station_bucket()
        _save_state_to_db()
        _notify_clients()

    return jsonify({"success": True, "stationId": station_id, "stationName": name})


@app.route("/api/station-login", methods=["POST"])
def station_login():
    payload = request.get_json(silent=True) or {}
    name = (payload.get("name") or "").strip()
    password = payload.get("password") or ""
    name_key = _normalize_station_name(name)

    with _state_lock:
        account = next((a for a in SHARED_STATE["stationAccounts"] if a["nameKey"] == name_key), None)
        if not account:
            return jsonify({"success": False, "error": "שם תחנה או סיסמה שגויים"}), 401

        if account.get("passwordHash") is None:
            # תחנה שמוגרה ממבנה ישן (ר' _migrate_legacy_shape) - עדיין אין לה סיסמה
            # משלה; מקבלים כניסה פעם אחת ומכריחים הגדרת סיסמה לפני כניסה רגילה הבאה
            return jsonify({
                "success": True, "stationId": account["id"], "stationName": account["name"],
                "needsPasswordSetup": True,
            })

        if not check_password_hash(account["passwordHash"], password):
            return jsonify({"success": False, "error": "שם תחנה או סיסמה שגויים"}), 401

    return jsonify({
        "success": True, "stationId": account["id"], "stationName": account["name"],
        "needsPasswordSetup": False,
    })


@app.route("/api/station-set-password", methods=["POST"])
def station_set_password():
    """קורא לזה רק תחנה שמוגרה ממבנה ישן (needsPasswordSetup=True) - ר' station_login
    למעלה. אחרי קריאה מוצלחת אחת needsPasswordSetup הופך ל-False וההתחברות הבאה כבר
    דורשת את הסיסמה הזו כרגיל, כמו כל תחנה אחרת."""
    payload = request.get_json(silent=True) or {}
    station_id = payload.get("stationId")
    new_password = payload.get("newPassword") or ""
    if not station_id or not new_password:
        return jsonify({"success": False, "error": "stationId וסיסמה חדשה נדרשים"}), 400

    with _state_lock:
        account = _station_account(station_id)
        if not account:
            return jsonify({"success": False, "error": "תחנה לא נמצאה"}), 404
        if not account.get("needsPasswordSetup"):
            return jsonify({"success": False, "error": "לתחנה זו כבר מוגדרת סיסמה"}), 409

        account["passwordHash"] = generate_password_hash(new_password)
        account["needsPasswordSetup"] = False
        _save_state_to_db()
        _notify_clients()

    return jsonify({"success": True})


@app.route("/api/station-verify-password", methods=["POST"])
def station_verify_password():
    """בדיקה בלבד (לא משנה כלום) - נקראת מטאב "פרטיות" בדשבורד המנהל לפני שהכפתור "שינוי
    סיסמה" בכלל נחשף (ר' startChangeStationPassword ב-script.js), כדי לוודא שמי שמבקש
    לשנות באמת מכיר את הסיסמה הנוכחית"""
    payload = request.get_json(silent=True) or {}
    station_id = payload.get("stationId")
    password = payload.get("password") or ""
    if not station_id or not password:
        return jsonify({"success": False, "error": "יש להזין סיסמה"}), 400

    with _state_lock:
        account = _station_account(station_id)
        if not account or not account.get("passwordHash"):
            return jsonify({"success": False, "error": "תחנה לא נמצאה"}), 404
        if not check_password_hash(account["passwordHash"], password):
            return jsonify({"success": False, "error": "הסיסמה שגויה"}), 401

    return jsonify({"success": True})


@app.route("/api/station-change-password", methods=["POST"])
def station_change_password():
    """משנה בפועל את סיסמת התחנה - שונה מ-station_set_password למעלה (שרק ממלא סיסמה
    ראשונה לתחנה שמוגרה ממבנה ישן): כאן חייבים לספק ולאמת את הסיסמה הנוכחית"""
    payload = request.get_json(silent=True) or {}
    station_id = payload.get("stationId")
    current_password = payload.get("currentPassword") or ""
    new_password = payload.get("newPassword") or ""
    if not station_id or not current_password or not new_password:
        return jsonify({"success": False, "error": "יש למלא סיסמה נוכחית וסיסמה חדשה"}), 400
    if len(new_password) < 4:
        return jsonify({"success": False, "error": "הסיסמה החדשה חייבת להכיל לפחות 4 תווים"}), 400

    with _state_lock:
        account = _station_account(station_id)
        if not account or not account.get("passwordHash"):
            return jsonify({"success": False, "error": "תחנה לא נמצאה"}), 404
        if not check_password_hash(account["passwordHash"], current_password):
            return jsonify({"success": False, "error": "הסיסמה הנוכחית שגויה"}), 401

        account["passwordHash"] = generate_password_hash(new_password)
        _save_state_to_db()
        _notify_clients()

    return jsonify({"success": True})


@app.route("/api/dispatcher-login", methods=["POST"])
def dispatcher_login():
    """סדרן לא "מחובר" לאף תחנה לפני הכניסה (בניגוד למנהל, שמקליד את שם התחנה עצמו) -
    יש לו רק שם משתמש+קוד גישה בני 7 תווים שקיבל מבעל התחנה, ולכן חייבים לסרוק את כל
    התחנות כדי למצוא לאיזו מהן הוא שייך. קוד בן 7 תווים מתוך 33 אפשריים (ר' generateDispatcherCode
    ב-script.js) נותן כ-4.2×10^10 צירופים - התנגשות מקרית בין תחנות שונות (אותו username+code)
    אינה ריאלית בפועל, כך שאין צורך לאלץ שמות משתמש ייחודיים גלובלית או שדה תחנה ידני."""
    payload = request.get_json(silent=True) or {}
    username = (payload.get("username") or "").strip()
    code = (payload.get("code") or "").strip()
    if not username or not code:
        return jsonify({"success": False, "error": "שם משתמש וקוד גישה נדרשים"}), 400

    with _state_lock:
        for account in SHARED_STATE["stationAccounts"]:
            bucket = SHARED_STATE["stationData"].get(account["id"]) or {}
            match = next(
                (d for d in bucket.get("managerDispatchers", [])
                 if d.get("username") == username and d.get("code") == code),
                None,
            )
            if match:
                return jsonify({
                    "success": True, "stationId": account["id"], "stationName": account["name"],
                    "dispatcherName": match.get("name") or username,
                })

    return jsonify({"success": False, "error": "שם משתמש או קוד גישה שגויים. פנה לבעל התחנה לקבלת קוד גישה."}), 401


def _driver_account(driver_id):
    return next((a for a in SHARED_STATE["driverAccounts"] if a["id"] == driver_id), None)


@app.route("/api/driver-register", methods=["POST"])
def driver_register():
    """הרשמת נהג חדש - שם מלא לא חייב להיות ייחודי (בניגוד לתחנות), כי הזהות בפועל בכניסה
    היא הצירוף שם+סיסמה יחד (ר' driver_login למטה). הנהג בוחר את הסיסמה בעצמו בטופס
    ההרשמה (בדיוק כמו סיסמת תחנה ב-station_register) - השרת רק שומר hash שלה, כדי שאותה
    התחברות (שם+סיסמה) תעבוד מכל מכשיר, כולל נייד, ולא רק מהמכשיר שבו נרשם. שם השדה
    codeHash נשאר כפי שהיה (לא codeHash->passwordHash) כדי לא לשבור חשבונות נהג אמיתיים
    שכבר נרשמו לפני השינוי הזה עם קוד שנוצר אוטומטית - הם ממשיכים לעבוד בדיוק כמו קודם,
    רק שמעכשיו גם ניתן לשנות את הסיסמה (ר' driver_change_password) והרשמה חדשה כבר לא
    מייצרת קוד אקראי"""
    payload = request.get_json(silent=True) or {}
    full_name = (payload.get("fullName") or "").strip()
    phone = (payload.get("phone") or "").strip()
    password = payload.get("password") or ""
    if not full_name or not phone:
        return jsonify({"success": False, "error": "שם מלא וטלפון נדרשים"}), 400
    if len(password) < 4:
        return jsonify({"success": False, "error": "הסיסמה חייבת להכיל לפחות 4 תווים"}), 400

    with _state_lock:
        driver_id = f"driver-{int(time.time() * 1000)}"
        account = {
            "id": driver_id,
            "fullName": full_name,
            "nameKey": _normalize_station_name(full_name),
            "codeHash": generate_password_hash(password),
            "age": payload.get("age"),
            "idNumber": (payload.get("idNumber") or "").strip(),
            "carModel": (payload.get("carModel") or "").strip(),
            "carYear": payload.get("carYear"),
            "phone": phone,
            "sector": payload.get("sector"),
            "createdAt": datetime.now().isoformat(),
        }
        SHARED_STATE["driverAccounts"].append(account)
        _save_state_to_db()
        _notify_clients()

    return jsonify({"success": True, "driverId": driver_id, "fullName": full_name})


@app.route("/api/driver-login", methods=["POST"])
def driver_login():
    payload = request.get_json(silent=True) or {}
    full_name = (payload.get("fullName") or "").strip()
    password = (payload.get("password") or "").strip()
    if not full_name or not password:
        return jsonify({"success": False, "error": "שם מלא וסיסמה נדרשים"}), 400

    name_key = _normalize_station_name(full_name)
    with _state_lock:
        # שם מלא לא ייחודי - בודקים את כל החשבונות עם אותו שם עד שנמצא אחד שהסיסמה שלו תואמת
        match = next(
            (a for a in SHARED_STATE["driverAccounts"]
             if a["nameKey"] == name_key and check_password_hash(a["codeHash"], password)),
            None,
        )
        if not match:
            return jsonify({"success": False, "error": "שם או סיסמה שגויים"}), 401

        return jsonify({
            "success": True, "driverId": match["id"], "fullName": match["fullName"],
            "age": match.get("age"), "idNumber": match.get("idNumber"),
            "carModel": match.get("carModel"), "carYear": match.get("carYear"),
            "phone": match.get("phone"), "sector": match.get("sector"),
        })


@app.route("/api/driver-change-password", methods=["POST"])
def driver_change_password():
    """מאפשר לנהג מחובר לשנות את הסיסמה שלו (ר' modalAccountSettings/saveAccountSettings
    ב-script.js) - בלי זה, הטופס "שינוי סיסמה" באזור האישי לא באמת עדכן שום דבר בשרת,
    כך שסיסמה "חדשה" שנהג הקליד שם מעולם לא עבדה בהתחברות הבאה ממכשיר אחר (בדיוק הבאג
    שדווח - "מגדיר סיסמה, במובייל זה לא נותן להיכנס")."""
    payload = request.get_json(silent=True) or {}
    driver_id = payload.get("driverId")
    current_password = payload.get("currentPassword") or ""
    new_password = payload.get("newPassword") or ""
    if not driver_id or not current_password or not new_password:
        return jsonify({"success": False, "error": "יש למלא סיסמה נוכחית וסיסמה חדשה"}), 400
    if len(new_password) < 4:
        return jsonify({"success": False, "error": "הסיסמה החדשה חייבת להכיל לפחות 4 תווים"}), 400

    with _state_lock:
        account = _driver_account(driver_id)
        if not account:
            return jsonify({"success": False, "error": "חשבון נהג לא נמצא"}), 404
        if not check_password_hash(account["codeHash"], current_password):
            return jsonify({"success": False, "error": "הסיסמה הנוכחית שגויה"}), 401

        account["codeHash"] = generate_password_hash(new_password)
        _save_state_to_db()
        _notify_clients()

    return jsonify({"success": True})


@app.route("/api/join-requests", methods=["POST"])
def create_join_request():
    payload = request.get_json(silent=True) or {}
    station_id = payload.get("stationId")  # יכול להיות מזהה מורכב "<realId>:<groupId>" - ר' _resolve_real_station_id
    station_name = payload.get("stationName")
    driver_name = payload.get("driverName")
    driver_profile = payload.get("driverProfile")  # פרטי ההרשמה של הנהג (ר' /api/driver-register) - נשמרים
    # על הבקשה עצמה כדי שיוצגו לתחנה כבר בכרטיס הבקשה הממתינה, לפני האישור (ר' approve_join_request למטה)
    if not station_id or not driver_name:
        return jsonify({"success": False, "error": "stationId ו-driverName נדרשים"}), 400

    with _state_lock:
        real_station_id = _resolve_real_station_id(station_id)
        if not real_station_id:
            return jsonify({"success": False, "error": "תחנה לא נמצאה"}), 404
        bucket = _station_bucket(real_station_id)

        existing = next(
            (r for r in bucket["joinRequests"]
             if r["stationId"] == station_id and r["driverName"] == driver_name and r["status"] != "rejected"),
            None,
        )
        if existing:
            return jsonify({"success": True, "request": existing})

        record = {
            "id": f"join-{int(datetime.now().timestamp() * 1000)}",
            "stationId": station_id,
            "stationName": station_name,
            "driverName": driver_name,
            "driverProfile": driver_profile,
            "status": "pending",
            "timestamp": datetime.now().strftime("%d/%m/%Y | %H:%M"),
        }
        bucket["joinRequests"].append(record)
        _save_state_to_db()
        _notify_clients()

    return jsonify({"success": True, "request": record})


@app.route("/api/join-requests/<request_id>/approve", methods=["POST"])
def approve_join_request(request_id):
    """station= חובה - כדי שמנהל תחנה לא יוכל לאשר (אפילו בניחוש id) בקשה ששייכת בפועל
    לתחנה אחרת; הבקשה נחפשת אך ורק בתוך דלי התחנה שצוינה."""
    station_id = request.args.get("station")
    if not station_id:
        return jsonify({"success": False, "error": "station נדרש"}), 400

    with _state_lock:
        if not _station_account(station_id):
            return jsonify({"success": False, "error": "תחנה לא נמצאה"}), 404
        bucket = _station_bucket(station_id)

        record = next((r for r in bucket["joinRequests"] if r["id"] == request_id), None)
        if not record:
            return jsonify({"success": False, "error": "בקשה לא נמצאה"}), 404

        record["status"] = "approved"

        driver = next((d for d in bucket["managerDrivers"] if d["name"] == record["driverName"]), None)
        if not driver:
            profile = record.get("driverProfile") or {}
            driver = {
                "id": f"drv-{int(datetime.now().timestamp() * 1000)}",
                "name": record["driverName"],
                "phone": profile.get("phone", ""),
                "vehicleModel": profile.get("carModel", ""),
                "vehicleYear": profile.get("carYear", ""),
                "dressCode": "",
                "age": profile.get("age", ""),
                "idNumber": profile.get("idNumber", ""),
                "sector": profile.get("sector", ""),
                "groupId": "",
                "status": "offline",
                "rides": 0,
            }
            bucket["managerDrivers"].append(driver)

        _save_state_to_db()
        _notify_clients()

    return jsonify({"success": True, "request": record, "driver": driver})


@app.route("/api/payment-approvals", methods=["POST"])
def create_payment_approval():
    """יוצר ושומר מיידית (כתיבה אטומית עם append, לא דריסה מלאה) בקשת אישור תשלום
    חדשה בדלי התחנה הרלוונטית - כך שהיא לעולם לא תלך לאיבוד אם POST /api/state
    (שדורס את כל דלי התחנה לפי המצב המקומי של שולח הבקשה) רץ במקביל ממכשיר אחר עם
    עותק מקומי שעדיין לא הכיל את הבקשה הזו. אידמפוטנטי לפי id."""
    payload = request.get_json(silent=True) or {}
    record_id = payload.get("id")
    station_id = payload.get("stationId")
    if not record_id or not station_id:
        return jsonify({"success": False, "error": "id ו-stationId נדרשים"}), 400

    with _state_lock:
        real_station_id = _resolve_real_station_id(station_id)
        if not real_station_id:
            return jsonify({"success": False, "error": "תחנה לא נמצאה"}), 404
        bucket = _station_bucket(real_station_id)

        existing = next((a for a in bucket["paymentApprovals"] if a["id"] == record_id), None)
        if not existing:
            bucket["paymentApprovals"].append(payload)
            _save_state_to_db()
            _notify_clients()

    return jsonify({"success": True})


@app.route("/api/manager-charges", methods=["POST"])
def create_manager_charge():
    """יוצר חיוב נהג באופן אטומי (append, לא דריסה מלאה) - נקרא מדפדפן הנהג עצמו
    (closeRideWithCustomer ב-script.js) בסיום נסיעה. בניגוד לפעולות מנהל/סדרן, לדפדפן
    הנהג אין station= "משלו" (הוא לא מחובר לאף תחנה ספציפית), ולכן הוא לא יכול להסתמך
    על POST /api/state הרגיל - הנתיב הזה מקבל את מזהה התחנה כחלק מהרשומה עצמה, כמו
    create_payment_approval/create_join_request. גם יוצר את רשומת הנהג בתחנה אם עוד
    אינה קיימת (אותו דפוס בדיוק כמו approve_join_request למעלה) - מאותה סיבה: לדפדפן
    הנהג אין station= כדי לעדכן managerDrivers דרך POST /api/state הרגיל. אידמפוטנטי לפי id."""
    payload = request.get_json(silent=True) or {}
    record_id = payload.get("id")
    station_id = payload.get("stationId")
    driver_name = payload.get("driverName")
    if not record_id or not station_id:
        return jsonify({"success": False, "error": "id ו-stationId נדרשים"}), 400

    with _state_lock:
        real_station_id = _resolve_real_station_id(station_id)
        if not real_station_id:
            return jsonify({"success": False, "error": "תחנה לא נמצאה"}), 404
        bucket = _station_bucket(real_station_id)

        existing = next((c for c in bucket["managerCharges"] if c["id"] == record_id), None)
        if not existing:
            bucket["managerCharges"].append(payload)
            if driver_name and not any(d.get("name") == driver_name for d in bucket["managerDrivers"]):
                bucket["managerDrivers"].append({
                    "id": f"drv-{int(datetime.now().timestamp() * 1000)}",
                    "name": driver_name,
                    "phone": "", "vehicleModel": "", "vehicleYear": "", "dressCode": "",
                    "groupId": "", "status": "offline", "rides": 0,
                })
            _save_state_to_db()
            _notify_clients()

    return jsonify({"success": True})


@app.route("/api/ride-requests", methods=["POST"])
def create_ride_request():
    """יוצר בקשת נסיעה של נהג באופן אטומי (append, לא דריסה מלאה) - אותו דפוס כמו
    create_payment_approval/create_join_request, כדי ששתי בקשות שנשלחות כמעט בו-זמנית
    (משני נהגים שונים) לא ידרסו זו את זו דרך POST /api/state הכללי. אידמפוטנטי לפי id."""
    payload = request.get_json(silent=True) or {}
    record_id = payload.get("id")
    if not record_id or not payload.get("rideId") or not payload.get("driverName"):
        return jsonify({"success": False, "error": "id, rideId ו-driverName נדרשים"}), 400

    with _state_lock:
        existing = next((r for r in SHARED_STATE["rideRequests"] if r["id"] == record_id), None)
        if not existing:
            SHARED_STATE["rideRequests"].append(payload)
            _save_state_to_db()
            _notify_clients()

    return jsonify({"success": True})


@app.route("/api/ride-requests/<request_id>/approve", methods=["POST"])
def approve_ride_request(request_id):
    """מאשר בקשת נסיעה אטומית תחת הנעילה - זהו התיקון בפועל למרוץ שבו שני נהגים
    יכולים "לקבל" את אותה נסיעה: אם היא כבר assigned לנהג אחר, מסרבים כאן ולא מסתמכים
    על הלקוח. בקשות "אחיות" (pending לאותה rideId) מסומנות rejected עם
    rejectionReason='taken_by_other' כדי שהנהגים האחרים יראו הודעה מדויקת."""
    with _state_lock:
        record = next((r for r in SHARED_STATE["rideRequests"] if r["id"] == request_id), None)
        if not record:
            return jsonify({"success": False, "error": "בקשה לא נמצאה"}), 404

        ride = next((r for r in SHARED_STATE["availableRides"] if r["id"] == record["rideId"]), None)
        if ride and ride.get("status") == "assigned":
            return jsonify({"success": False, "error": "הנסיעה כבר שובצה לנהג אחר"}), 409

        record["status"] = "approved"

        if ride:
            ride["status"] = "assigned"
            ride["assignedDriverName"] = record["driverName"]

        for other in SHARED_STATE["rideRequests"]:
            if other["rideId"] == record["rideId"] and other["id"] != record["id"] and other.get("status") == "pending":
                other["status"] = "rejected"
                other["rejectionReason"] = "taken_by_other"

        _save_state_to_db()
        _notify_clients()

    return jsonify({"success": True, "request": record})


@app.route("/api/ride-requests/<request_id>/reject", methods=["POST"])
def reject_ride_request(request_id):
    """דוחה בקשת נסיעה אטומית - קריאה סימטרית ל-approve_ride_request למעלה."""
    with _state_lock:
        record = next((r for r in SHARED_STATE["rideRequests"] if r["id"] == request_id), None)
        if not record:
            return jsonify({"success": False, "error": "בקשה לא נמצאה"}), 404
        record["status"] = "rejected"
        record["rejectionReason"] = "dispatcher"
        _save_state_to_db()
        _notify_clients()

    return jsonify({"success": True, "request": record})


@app.route("/")
def root():
    return send_from_directory(str(BASE_DIR), "index.html")


@app.route("/<path:filename>")
def static_files(filename):
    return send_from_directory(str(BASE_DIR), filename)


if __name__ == "__main__":
    socketio.run(app, host='0.0.0.0', debug=True, port=5000, allow_unsafe_werkzeug=True)
