import asyncio
import os
from functools import partial

import requests
from itsdangerous import URLSafeTimedSerializer

SESSION_MAX_AGE = 86400  # 24 hours


def _get_serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(os.getenv("SECRET_KEY", "change-me-in-production"))


def _check_neon_auth(email: str, password: str, origin: str) -> bool:
    neon_auth_url = os.getenv("NEON_AUTH_URL", "")
    if not neon_auth_url:
        return False
    try:
        resp = requests.post(
            f"{neon_auth_url}/sign-in/email",
            json={"email": email, "password": password},
            headers={"Origin": origin},
            timeout=10,
        )
        return resp.status_code == 200
    except requests.RequestException:
        return False


async def verify_password(email: str, password: str, origin: str = "http://localhost:8000") -> bool:
    admin_email = os.getenv("ADMIN_EMAIL", "alexpihq@gmail.com")
    if email != admin_email:
        return False
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, partial(_check_neon_auth, email, password, origin))


def create_session_token(email: str) -> str:
    return _get_serializer().dumps(email)


def verify_session_token(token: str) -> str | None:
    try:
        return _get_serializer().loads(token, max_age=SESSION_MAX_AGE)
    except Exception:
        return None
