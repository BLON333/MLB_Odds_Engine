import time
import requests
from requests.exceptions import RequestException

# Status codes that are safe to retry. Anything else could result in the
# message being delivered despite a non-2xx response, so we avoid retries in
# those cases to prevent duplicate Discord posts.
RETRY_STATUS_CODES = {500, 502, 503, 504, 429}

def post_with_retries(
    url: str,
    logger=None,
    attempts: int = 3,
    retry_statuses=RETRY_STATUS_CODES,
    **kwargs,
):
    """POST to a URL with retry logic.

    Parameters
    ----------
    url : str
        The webhook URL to post to.
    logger : logging.Logger, optional
        Logger for error messages.
    attempts : int, optional
        Number of attempts before giving up.
    **kwargs :
        Additional arguments passed to ``requests.post``.
    """
    for attempt in range(attempts):
        try:
            resp = requests.post(url, **kwargs)
            if resp.status_code in (200, 204):
                return resp
            if resp.status_code not in retry_statuses:
                break
        except RequestException as exc:
            if logger:
                logger.warning(
                    "Discord post attempt %d failed: %s", attempt + 1, exc
                )
            break  # don't retry on network errors to avoid duplicate posts
        time.sleep(2 ** attempt)
    if logger:
        logger.error("Failed to post to %s after %d attempts", url, attempt + 1)
    return None
