def fix_url(url: str) -> str:
    return url.replace("e-gov. ooe.gv.at", "e-gov.ooe.gv.at")


formats = {
    "CSV": [".csv", "csv-datei"]
}
format_mapping = {}
for correct_format, wrong_formats in formats.items():
    for wrong_format in wrong_formats:
        format_mapping[wrong_format] = correct_format


def format_normalizer(format: str) -> str:
    if format in format_mapping:
        return format_mapping[format]
    return format
