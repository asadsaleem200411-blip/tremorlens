import os
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
engine = create_engine(os.environ["DATABASE_URL"])
with engine.connect() as conn:
    rows = conn.execute(text(
        "SELECT CAST(EXTRACT(YEAR FROM event_time) AS INTEGER), COUNT(*) FROM usgs_events GROUP BY 1")).all()
db = {int(y): int(n) for y, n in rows}

total_api = total_db = 0
for year in range(2000, 2027):
    end = f"{year + 1}-01-01" if year < 2026 else "2026-10-25"
    r = requests.get("https://earthquake.usgs.gov/fdsnws/event/1/count",
                     params={"starttime": f"{year}-01-01", "endtime": end,
                             "minmagnitude": 4.5, "eventtype": "earthquake"}, timeout=60)
    api = int(r.text)
    total_api += api
    total_db += db.get(year, 0)
    print(year, "API", api, "DB", db.get(year, 0), "difference", api - db.get(year, 0))
print("TOTAL  API", total_api, " DB", total_db)