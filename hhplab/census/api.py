"""Shared helpers for Census API credentials and error handling."""

from __future__ import annotations

import os

import httpx

from hhplab.sources import CENSUS_API_ACS5

CENSUS_API_KEY_ENV = "CENSUS_API_KEY"
CENSUS_API_KEY_SIGNUP_URL = "https://api.census.gov/data/key_signup.html"
CENSUS_API_KEY_MISSING_MESSAGE = (
    "CENSUS_API_KEY not set - ingest commands that call the Census API will fail. "
    f"Get a free key at {CENSUS_API_KEY_SIGNUP_URL} and export CENSUS_API_KEY."
)
CENSUS_API_KEY_RETRY_MESSAGE = (
    "Census API requires a Census API key for this request. "
    f"{CENSUS_API_KEY_MISSING_MESSAGE} You can also pass --api-key where supported."
)
CENSUS_API_PROBE_URL = CENSUS_API_ACS5.format(year=2023)


def get_census_api_key(api_key: str | None = None) -> str | None:
    """Return the explicit or environment-provided Census API key."""
    return api_key if api_key is not None else os.environ.get(CENSUS_API_KEY_ENV)


def census_api_credentials_status() -> dict[str, object]:
    """Return machine-readable Census API credential status."""
    present = bool(os.environ.get(CENSUS_API_KEY_ENV))
    return {
        "env_var": CENSUS_API_KEY_ENV,
        "required": True,
        "present": present,
        "status": "ok" if present else "missing",
        "message": None if present else CENSUS_API_KEY_MISSING_MESSAGE,
        "signup_url": CENSUS_API_KEY_SIGNUP_URL,
    }


def is_census_missing_key_response(response: httpx.Response) -> bool:
    """Return True when the Census API identifies a missing-key failure."""
    if response.headers.get("X-DataWebAPI-KeyError") == "1":
        return True
    location = response.headers.get("location", "")
    if "missing_key" in location:
        return True
    return "missing_key" in str(response.url)


def raise_for_census_api_status(response: httpx.Response) -> None:
    """Raise actionable errors for known Census API failures."""
    if is_census_missing_key_response(response):
        raise ValueError(CENSUS_API_KEY_RETRY_MESSAGE)


def probe_census_api_reachability(api_key: str | None = None) -> dict[str, object]:
    """Run a lightweight Census API probe and return structured status."""
    key = get_census_api_key(api_key)
    params = {"get": "NAME", "for": "us:*"}
    if key:
        params["key"] = key

    try:
        with httpx.Client(timeout=10.0, follow_redirects=False) as client:
            response = client.get(CENSUS_API_PROBE_URL, params=params)
    except httpx.HTTPError as exc:
        return {
            "status": "error",
            "reachable": False,
            "message": f"Census API probe failed: {exc}",
        }

    if is_census_missing_key_response(response):
        return {
            "status": "missing_key",
            "reachable": True,
            "message": CENSUS_API_KEY_MISSING_MESSAGE,
            "status_code": response.status_code,
        }

    if response.is_success:
        return {
            "status": "ok",
            "reachable": True,
            "message": None,
            "status_code": response.status_code,
        }

    return {
        "status": "error",
        "reachable": False,
        "message": f"Census API probe returned HTTP {response.status_code}.",
        "status_code": response.status_code,
    }
