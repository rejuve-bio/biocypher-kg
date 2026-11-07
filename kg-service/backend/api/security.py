"""Browser-facing protections for the console's pages and API.

The console has no sign-in: it is a single-person tool that only listens on this
computer (the compose files publish it on 127.0.0.1). What a loopback-only web app
still has to stop is other websites using the person's own browser against it:

- DNS rebinding: a website whose name points at 127.0.0.1 would count as "the same
  site" as the console. Every request must name this computer (localhost,
  127.0.0.1, [::1]) as its Host, so such names are refused.
- Cross-site requests (CSRF): any page can make the browser send requests here.
  Browsers label those (Sec-Fetch-Site, Origin), and labelled ones from elsewhere
  are refused. Tools on this computer (curl, scripts) send neither and still work.
- Clickjacking: other sites can't show the console in a frame.

Every response also says not to cache it (some hold a password), not to guess its
type, and which sources the page may load (only the console itself).

Behind a reverse proxy or under another name, list the extra names in
CONSOLE_ALLOWED_HOSTS and CONSOLE_ALLOWED_ORIGINS (comma-separated).
"""
from __future__ import annotations

import os
from typing import Iterable, Optional

LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}

# Only the console's own files: no inline scripts, nothing from other sites.
# Styles allow inline (React style attributes, chart libraries).
CSP = "; ".join([
    "default-src 'self'",
    "script-src 'self'",
    "style-src 'self' 'unsafe-inline'",
    "img-src 'self' data: blob:",
    "font-src 'self' data:",
    "connect-src 'self'",
    "worker-src 'self' blob:",
    "object-src 'none'",
    "base-uri 'none'",
    "form-action 'self'",
    "frame-ancestors 'none'",
])

HEADERS = [
    (b"x-content-type-options", b"nosniff"),
    (b"x-frame-options", b"DENY"),
    (b"referrer-policy", b"no-referrer"),
    (b"cross-origin-opener-policy", b"same-origin"),
    (b"cross-origin-resource-policy", b"same-origin"),
    (b"permissions-policy", b"camera=(), microphone=(), geolocation=(), payment=(), usb=()"),
    (b"content-security-policy", CSP.encode()),
]


def _listed(name: str) -> set:
    return {v.strip().lower().rstrip("/") for v in os.environ.get(name, "").split(",")
            if v.strip()}


def hostname(host_header: str) -> str:
    """"localhost:9000" -> "localhost"; "[::1]:9000" -> "::1"."""
    h = (host_header or "").strip().lower()
    if h.startswith("["):
        return h[1:h.find("]")] if "]" in h else h
    return h.rsplit(":", 1)[0] if h.count(":") == 1 else h


def allowed_host(host_header: str) -> bool:
    name = hostname(host_header)
    # *.localhost always means this computer (browsers resolve it themselves).
    return (name in LOCAL_HOSTS or name.endswith(".localhost")
            or name in _listed("CONSOLE_ALLOWED_HOSTS"))


def allowed_origin(origin: str, host_header: str, scheme: str) -> bool:
    origin = (origin or "").strip().lower().rstrip("/")
    if not origin or origin == "null":
        return False
    same = f"{scheme}://{(host_header or '').strip().lower()}"
    return origin == same or origin in _listed("CONSOLE_ALLOWED_ORIGINS")


def allowed_cors_origins() -> list:
    """Origins allowed to call the API from another page (none by default: the
    console's pages are served by the API itself)."""
    return sorted(_listed("CONSOLE_ALLOWED_ORIGINS"))


def refusal(host: str, path: str, method: str, origin: Optional[str],
            fetch_site: Optional[str], scheme: str) -> Optional[str]:
    """Why a request is refused, or None to let it through."""
    if not allowed_host(host):
        return "This console only answers on this computer (localhost)."
    if path.startswith("/api"):
        if (fetch_site or "").lower() == "cross-site":
            return "Requests from other websites aren't allowed."
        if origin is not None and not allowed_origin(origin, host, scheme):
            return "Requests from other websites aren't allowed."
    return None


class SecurityMiddleware:
    """Pure ASGI (so streamed logs aren't buffered)."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        headers = {k.decode("latin-1").lower(): v.decode("latin-1")
                   for k, v in scope.get("headers") or []}
        path = scope.get("path") or ""
        why = refusal(headers.get("host", ""), path, scope.get("method", "GET"),
                      headers.get("origin"), headers.get("sec-fetch-site"),
                      scope.get("scheme", "http"))
        if why:
            body = why.encode()
            status = 400 if why.startswith("This console only") else 403
            await send({"type": "http.response.start", "status": status,
                        "headers": [(b"content-type", b"text/plain; charset=utf-8"),
                                    (b"content-length", str(len(body)).encode()),
                                    *HEADERS]})
            await send({"type": "http.response.body", "body": body})
            return

        extra = list(HEADERS)
        if path.startswith("/api"):
            extra.append((b"cache-control", b"no-store"))

        async def send_with_headers(message):
            if message["type"] == "http.response.start":
                present = {k.lower() for k, _ in message.get("headers") or []}
                message["headers"] = list(message.get("headers") or []) + [
                    (k, v) for k, v in extra if k not in present]
            await send(message)

        return await self.app(scope, receive, send_with_headers)


def install(app, cors_middleware) -> None:
    """Add the protections (and CORS only for explicitly listed origins)."""
    origins: Iterable[str] = allowed_cors_origins()
    if origins:
        app.add_middleware(cors_middleware, allow_origins=list(origins),
                           allow_credentials=False, allow_methods=["*"],
                           allow_headers=["*"])
    app.add_middleware(SecurityMiddleware)  # added last: runs first
