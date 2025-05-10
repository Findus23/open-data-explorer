import json

import pandas as pd
from requests import Response
from sqlite_utils import Database

from meta.globals import s
from meta.progress_logger import RecordLogger


def get_api_response(datagv_id: str) -> Response:
    if datagv_id == "7e162b48-8c2a-4abd-9b57-3f56d4c1e120":
        # Aktuelle Mitglieder des BR
        body = {
            "NRBR": [
                "BR"
            ],
            "GP": [
                "AKT"
            ],
            "R_WF": [
                "WP"
            ],
            "M": [
                "M"
            ],
            "W": [
                "W"
            ]
        }
        r = s.post(
            "https://www.parlament.gv.at/Filter/api/json/post?jsMode=EVAL&FBEZ=WFW_005&listeId=10005&showAll=true",
            data=json.dumps(body),
        )
        r.raise_for_status()
        print(r.json())
        print(len(r.json()["rows"]))
        return r
    elif datagv_id == "8bdf0efa-7f2c-4bd3-b762-5983f02153ea":
        # Aktuelle Abgeordnete zum NR
        body = {
            "STEP": [
                "1000"
            ],
            "NRBR": [
                "NR"
            ],
            "GP": [
                "AKT"
            ],
            "R_WF": [
                "FR"
            ],
            "R_PBW": [
                "WK"
            ],
            "M": [
                "M"
            ],
            "W": [
                "W"
            ]
        }
        r = s.post(
            "https://www.parlament.gv.at/Filter/api/json/post?jsMode=EVAL&FBEZ=WFW_002&listeId=10002&showAll=true",
            data=json.dumps(body),
        )
        r.raise_for_status()
        print(r.json())
        print(len(r.json()["rows"]))
        return r

    datagv_id_to_api = {
        "7ff5a640-634d-4206-9548-a318cb5b4f67": 600,
        "58e0db32-8633-48d2-8d6c-0272ba2242ff": 101,
        "ebc58372-cde4-45b6-952f-651fd075ddca": 211
    }

    api_path = datagv_id_to_api[datagv_id]
    r = s.get(f"https://www.parlament.gv.at/Filter/api/filter/data/{api_path}?js=eval&showAll=true")
    r.raise_for_status()

    return r


def import_parlament(db: Database, datagv_id: str, logger: RecordLogger, name: str, i: int, num_res: int):
    logger.set_status(f"reading parlament data {i}/{num_res}")
    r = get_api_response(datagv_id)
    header_titles = []
    for f in r.json()["header"]:
        if "feld_name" in f and f["feld_name"].lower() != "datum":
            header_titles.append(f["feld_name"])
        else:
            header_titles.append(f["label"])
    print(header_titles)
    df = pd.DataFrame(r.json()["rows"], columns=header_titles)
    df.to_sql(name, db.conn)
