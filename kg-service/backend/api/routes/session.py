"""Signing in to the console when it's on the network (see api/security.py)."""
import time

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field

from backend.api import security

router = APIRouter(prefix="/api/console", tags=["Console"])
_MONTH = 30 * 24 * 3600


class Login(BaseModel):
    password: str = Field(max_length=512)


@router.get("/session")
def session(request: Request):
    """Whether a password is required here, and whether this browser has signed in."""
    return {"required": bool(security.password()),
            "signed_in": security.signed_in(request.headers.get("cookie"))}


@router.post("/login")
def login(body: Login, response: Response):
    pw = security.password()
    if not pw:
        return {"signed_in": True}
    if not security.hmac.compare_digest(body.password.encode(), pw.encode()):
        time.sleep(1)  # slows guessing; the password itself is long and random
        raise HTTPException(status_code=401, detail="wrong password")
    response.set_cookie(security.SESSION_COOKIE, security.session_token(pw), max_age=_MONTH,
                        httponly=True, samesite="strict", path="/")
    return {"signed_in": True}


@router.post("/logout")
def logout(response: Response):
    response.delete_cookie(security.SESSION_COOKIE, path="/")
    return {"signed_in": False}
