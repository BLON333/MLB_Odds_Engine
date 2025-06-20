import time
import requests
from requests.exceptions import RequestException


def post_with_retries(url: str, logger=None, attempts: int = 3, **kwargs):
    """POST to a URL with retry logic."""
    for attempt in range(attempts):
        try:
            resp = requests.post(url, **kwargs)
            if resp.status_code in (200, 204):
                return resp
        except RequestException as exc:
            if logger:
                logger.warning(
                    "Discord post attempt %d failed: %s",
                    attempt + 1,
                    exc,
                )
        time.sleep(2 ** attempt)
    if logger:
        logger.error("Failed to post to %s after %d attempts", url, attempts)
    return None
