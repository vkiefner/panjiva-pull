"""
client.py — shared Panjiva API client.

Responsibilities:
  * Load config/secrets from .env
  * Generate & cache a 24h OAuth Bearer token from client_id/client_secret
  * Enforce a polite request rate (documented hard limit: 20 req/s per token)
  * Retry sensibly on 429 / 5xx and refresh the token once on 401

Endpoint paths are read from .env so you can correct them against the API
Reference (https://panjiva.com/api-guide/spec) without editing this file.
"""

from __future__ import annotations

import os
import time
import threading
from dataclasses import dataclass

import requests
from dotenv import load_dotenv

load_dotenv()


def _env(key: str, default: str | None = None, required: bool = False) -> str:
    val = os.getenv(key, default)
    if required and (val is None or val.strip() == "" or val.endswith("_here")):
        raise RuntimeError(f"Missing required env var {key}. Set it in your .env file.")
    return val if val is not None else ""


@dataclass
class Settings:
    client_id: str = _env("PANJIVA_CLIENT_ID", required=True)
    client_secret: str = _env("PANJIVA_CLIENT_SECRET", required=True)
    base_url: str = _env("PANJIVA_BASE_URL", "https://api.panjiva.com").rstrip("/")
    token_path: str = _env("PANJIVA_TOKEN_PATH", "/oauth/token")
    shipment_path: str = _env("PANJIVA_SHIPMENT_SEARCH_PATH", "/shipments/search")
    company_path: str = _env("PANJIVA_COMPANY_SEARCH_PATH", "/companies/search")
    hs_path: str = _env("PANJIVA_HS_SEARCH_PATH", "/hs/search")
    async_bulk_path: str = _env("PANJIVA_ASYNC_BULK_PATH", "/bulk/shipments/search")
    async_status_path: str = _env("PANJIVA_ASYNC_STATUS_PATH", "/bulk/status")
    data_sources_path: str = _env("PANJIVA_DATA_SOURCES_PATH", "/metadata/data-sources")
    schema_path: str = _env("PANJIVA_SCHEMA_PATH", "/metadata/data-source/{src}/schema")
    data_dir: str = _env("PANJIVA_DATA_DIR", "../data")
    max_rps: float = float(_env("PANJIVA_MAX_RPS", "8"))
    notify_email: str = _env("PANJIVA_NOTIFY_EMAIL", "")


class RateLimiter:
    """Simple min-interval limiter. Keeps us safely under the 20 req/s cap."""

    def __init__(self, max_rps: float):
        self.min_interval = 1.0 / max_rps if max_rps > 0 else 0.0
        self._last = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            delta = now - self._last
            if delta < self.min_interval:
                time.sleep(self.min_interval - delta)
            self._last = time.monotonic()


class PanjivaClient:
    # Token is valid 24h per the docs; refresh a little early to be safe.
    TOKEN_TTL = 23 * 3600

    def __init__(self, settings: Settings | None = None):
        self.s = settings or Settings()
        self.session = requests.Session()
        self.limiter = RateLimiter(self.s.max_rps)
        # Bulk/batch endpoints are capped at 1 req/s (separate, stricter limit).
        self.bulk_limiter = RateLimiter(1.0)
        self._token: str | None = None
        self._token_ts: float = 0.0

    # --- auth ---------------------------------------------------------------
    def _generate_token(self) -> str:
        # Verified: POST /auth/token  ->  {"token": ..., "expires_at": ...}
        url = self.s.base_url + self.s.token_path
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.s.client_id,
            "client_secret": self.s.client_secret,
        }
        self.limiter.wait()
        r = self.session.post(url, json=payload, timeout=60)
        r.raise_for_status()
        data = r.json()
        token = data.get("token") or data.get("access_token")
        if not token:
            raise RuntimeError(f"Could not find token in response keys: {list(data)}")
        return token

    def token(self, force: bool = False) -> str:
        if force or self._token is None or (time.time() - self._token_ts) > self.TOKEN_TTL:
            self._token = self._generate_token()
            self._token_ts = time.time()
        return self._token

    # --- core request -------------------------------------------------------
    def request(self, method: str, path: str, *, json=None, params=None,
                max_retries: int = 5) -> requests.Response:
        url = self.s.base_url + path
        attempt = 0
        refreshed = False
        while True:
            attempt += 1
            self.limiter.wait()
            headers = {"Authorization": f"Bearer {self.token()}",
                       "Accept": "application/json"}
            resp = self.session.request(method, url, json=json, params=params,
                                        headers=headers, timeout=120)

            if resp.status_code == 401 and not refreshed:
                self.token(force=True)  # token expired/invalid — refresh once
                refreshed = True
                continue
            if resp.status_code == 429:
                wait = float(resp.headers.get("Retry-After", min(2 ** attempt, 60)))
                time.sleep(wait)
                if attempt <= max_retries:
                    continue
            if resp.status_code >= 500 and attempt <= max_retries:
                time.sleep(min(2 ** attempt, 60))
                continue

            resp.raise_for_status()
            return resp

    def post(self, path: str, body: dict) -> dict:
        return self.request("POST", path, json=body).json()

    def post_bulk(self, path: str, body: dict) -> dict:
        # Bulk/batch endpoints are limited to 1 req/s; gate before sending.
        self.bulk_limiter.wait()
        return self.request("POST", path, json=body).json()

    def get(self, path: str, params: dict | None = None) -> dict:
        return self.request("GET", path, params=params).json()


if __name__ == "__main__":
    # Smoke test: can we authenticate?
    c = PanjivaClient()
    print("Requesting token...")
    tok = c.token()
    print("OK — token acquired (len=%d, prefix=%s...)" % (len(tok), tok[:6]))
