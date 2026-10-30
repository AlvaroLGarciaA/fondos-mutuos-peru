"""
nombres.py — la forma comparable del nombre de un fondo.

La SMV y su catálogo escriben el mismo fondo de formas distintas: con y sin
tildes, mayúsculas o minúsculas, dobles espacios, puntuación, y a veces con la
moneda pegada al final ("... EN DOLARES"). Para cruzarlos se compara la clave
que devuelve clave_fondo().
"""
from __future__ import annotations

import re
import unicodedata


def normalizar(s) -> str:
    """Sin tildes, en mayúsculas, sin puntuación y con un solo espacio."""
    if not isinstance(s, str):
        return ""
    t = unicodedata.normalize("NFKD", s)
    t = "".join(c for c in t if not unicodedata.combining(c)).upper()
    t = re.sub(r"[^A-Z0-9 ]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def clave_fondo(x) -> str:
    """Nombre comparable: normalizado y sin el sufijo de moneda."""
    return re.sub(r"\s+EN\s+(NUEVOS SOLES|SOLES|DOLARES)$", "", normalizar(x)).strip()
