import os
import ssl
import requests
from typing import Any, Dict, Optional
from tenacity import retry, wait_exponential, stop_after_attempt
from unidecode import unidecode

HEADERS = {"User-Agent": "AFUBRA-IBGE/1.0 (+automation sidra)"}

# --- Adapter TLS tolerante a servidores legados ---
class TLSAdapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        # Habilita renegociação legada e reduz SECLEVEL quando necessário
        try:
            # ssl.OP_LEGACY_SERVER_CONNECT (OpenSSL 3) — pode não existir, por isso o try
            ctx.options |= 0x4
        except Exception:
            pass
        try:
            ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        except Exception:
            pass
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)

_SESSION = requests.Session()
_SESSION.mount("https://", TLSAdapter())

def normalize_name(name: str) -> str:
    if not name:
        return ""
    s = name.strip().lower()
    s = unidecode(s)
    s = " ".join(s.split())
    return s

def try_float(x: str):
    if x is None:
        return None
    s = str(x).strip()
    if s in {"...", "-", "", "X", "x"}:
        return None
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def _maybe_http_fallback(url: str) -> Optional[str]:
    if url.startswith("https://"):
        return "http://" + url[len("https://"):]
    return None

@retry(wait=wait_exponential(multiplier=1, min=1, max=20), stop=stop_after_attempt(6))
def http_get_json(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """
    GET com tolerância a TLS em OpenSSL 3 e fallback para HTTP.
    Controles por ambiente:
      - IBGE_SSL_NO_VERIFY=1  -> desabilita verificação de certificado (apenas último recurso)
      - IBGE_FORCE_HTTP=1     -> força uso de http:// para os hosts do IBGE
    """
    force_http = os.getenv("IBGE_FORCE_HTTP", "0") == "1"
    verify = os.getenv("IBGE_SSL_NO_VERIFY", "0") != "1"

    def _do_get(u: str, vfy: bool):
        r = _SESSION.get(u, headers=HEADERS, params=params, timeout=60, verify=vfy)
        r.raise_for_status()
        return r.json()

    try:
        if force_http and url.startswith("https://"):
            http_url = _maybe_http_fallback(url)
            if http_url:
                return _do_get(http_url, True)  # HTTP não precisa de verify
        return _do_get(url, verify)
    except requests.exceptions.SSLError:
        # 1) tenta novamente com verify=False (se permitido)
        if verify is True and os.getenv("IBGE_SSL_NO_VERIFY", "0") == "1":
            return _do_get(url, False)
        # 2) tenta fallback http://
        http_url = _maybe_http_fallback(url)
        if http_url:
            return _do_get(http_url, True)
        raise
