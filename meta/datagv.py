from .globals import s

BASE_URL = "https://www.data.gv.at/katalog"
API_BASE = BASE_URL + "/api/3/action/package_show?id="


def get_metadata(package_id):
    url = API_BASE + package_id
    r = s.get(url)
    r.raise_for_status()
    assert r.json()["success"]
    data = r.json()["result"]
    return data
