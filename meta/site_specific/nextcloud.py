import xml.etree.ElementTree as ET

import requests

from meta.progress_logger import RecordLogger, FakeRecordLogger

ns = {"d": "DAV:"}  # namespace mapping


def get_nextcloud_shared_url(url: str, logger: RecordLogger):
    """
    fetch a URL like https://noe.gemeindecloud.at/index.php/s/j8mpAMgMrWLTBcL
    by using webDAV directly
    """
    uses_index_php = "index.php" in url
    if uses_index_php:
        nextcloud_base_url = url.split("/index.php/s/")[0]
    else:
        nextcloud_base_url = url.split("/s/")[0]
    webdav_url = nextcloud_base_url + "/public.php/webdav/"
    share_id = url.split("/")[-1]

    logger.set_status(f"URL is a shared nextcloud directory. Fetching {share_id} from {webdav_url}")
    r = requests.request("PROPFIND", webdav_url, auth=(share_id, ""))
    root = ET.fromstring(r.text)
    responses = root.findall(".//d:response", ns)
    if len(responses) > 1:
        logger.set_status(f"found {len(responses)} files in shared NextCloud directory. Only using the first one.")
        response = responses[1]
    else:
        response = responses[0]
    href = response.find(".//d:href", ns).text

    logger.set_status(f"fetching {href} from NextCloud shared URL")

    r = requests.get(nextcloud_base_url + href, auth=(share_id, ""))
    return r

if __name__ == '__main__':
    logger = FakeRecordLogger()
    get_nextcloud_shared_url("https://noe.gemeindecloud.at/index.php/s/j8mpAMgMrWLTBcL", logger)
    # get_nextcloud_shared_url("https://cloud.lw1.at/s/7BrWCEz6W7ZPrFH", logger)
