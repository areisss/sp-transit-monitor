import logging

import httpx

from sptrans_producer.config import Config
from sptrans_producer.models import ApiPositionResponse

logger = logging.getLogger(__name__)

# Module-level session cookie — reused across warm Lambda invocations
_cached_session_cookies: httpx.Cookies | None = None


def _authenticate(client: httpx.Client, config: Config) -> httpx.Cookies:
    """Authenticate with SPTrans API and return session cookies."""
    url = f"{config.sptrans_api_base_url}/Login/Autenticar"
    resp = client.post(url, params={"token": config.sptrans_api_token})
    resp.raise_for_status()

    authenticated = resp.json()
    if not authenticated:
        raise RuntimeError("SPTrans authentication failed — API returned false")

    logger.info("SPTrans authentication successful")
    return resp.cookies


def _ensure_authenticated(client: httpx.Client, config: Config) -> None:
    """Authenticate if no cached session, storing cookies on the client."""
    global _cached_session_cookies

    if _cached_session_cookies is not None:
        client.cookies = _cached_session_cookies
        return

    cookies = _authenticate(client, config)
    _cached_session_cookies = cookies
    client.cookies = cookies


def clear_session_cache() -> None:
    """Clear the cached session — useful for testing or forced re-auth."""
    global _cached_session_cookies
    _cached_session_cookies = None


def fetch_vehicle_positions(client: httpx.Client, config: Config) -> ApiPositionResponse:
    """Fetch all vehicle positions from the SPTrans /Posicao endpoint.

    Re-authenticates once if the first attempt returns 401.
    """
    global _cached_session_cookies

    _ensure_authenticated(client, config)

    url = f"{config.sptrans_api_base_url}/Posicao"
    resp = client.get(url)

    if resp.status_code == 401:
        logger.warning("Session expired, re-authenticating")
        _cached_session_cookies = None
        _ensure_authenticated(client, config)
        resp = client.get(url)

    resp.raise_for_status()
    return ApiPositionResponse.model_validate(resp.json())
