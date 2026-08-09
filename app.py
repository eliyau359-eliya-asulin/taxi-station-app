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

from flask import Flask, jsonify, send_from_directory
from pathlib import Path
from datetime import datetime
import requests

BASE_DIR = Path(__file__).resolve().parent

app = Flask(__name__, static_folder=str(BASE_DIR), static_url_path="")

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


@app.route("/")
def root():
    return send_from_directory(str(BASE_DIR), "index.html")


@app.route("/<path:filename>")
def static_files(filename):
    return send_from_directory(str(BASE_DIR), filename)


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=5000)
