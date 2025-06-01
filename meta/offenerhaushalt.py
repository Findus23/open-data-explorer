from bs4 import BeautifulSoup
from requests import Response

from meta.globals import s


def fetch_offenerhaushalt(html: str) -> Response:
    soup = BeautifulSoup(html, "html.parser")
    download_form = soup.find("form", {"class": "download"})
    form_data = {}
    print(download_form)
    assert download_form.attrs["method"] == "POST"
    post_url = download_form.attrs["action"]

    for input_tag in soup.find_all("input"):
        name = input_tag.get("name")
        if not name:
            continue
        # ignore checkbox and radio
        form_data[name] = input_tag.get("value")

    for select_tag in soup.find_all("select"):
        name = select_tag.get("name")
        if not name:
            continue
        selected_option = select_tag.find("option", selected=True)
        if not selected_option:
            raise ValueError("no option selected")
        form_data[name] = selected_option.get("value")

    # ignore textarea
    print(form_data)

    r=s.post(post_url, data=form_data)
    r.raise_for_status()
    print(r.content)
    return r



if __name__ == '__main__':
    with open("tmp.html") as f:
        html = f.read()
    fetch_offenerhaushalt(html)
