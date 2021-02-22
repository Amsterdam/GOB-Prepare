import requests


class APIException(IOError):
    pass


def post_stream(url, json, **kwargs):
    result = requests.post(url, stream=True, json=json, **kwargs)

    try:
        result.raise_for_status()
    except requests.exceptions.RequestException:
        raise APIException(f"Request failed due to API exception, response code {result.status_code}")
    return result.iter_lines()
