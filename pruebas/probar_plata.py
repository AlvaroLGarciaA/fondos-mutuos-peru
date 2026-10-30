"""
probar_plata.py — comprueba construir.py con un bronce inventado, sin red.

    uv run pruebas/probar_plata.py

Arma en una carpeta temporal un panel de tres fondos, un catálogo y un tipo de
cambio del BCRP pensados para pisar cada decisión de F2, corre construir.py
sobre ellos y verifica:

    P3  la fecha en que el fondo pasa a Operativa, y vacía si nunca operó
    P4  dos fondos homónimos con distinta fecha de inicio tienen ids distintos,
        y el fondo sin fecha de inicio tiene id (no queda vacío)
    P5  ninguna fila se pierde; valor cuota 0 queda vacío y marcado; patrimonio
        negativo se marca; tipo de cambio 1 en dólares es dudoso, en soles no
    P2  el sábado toma el tipo de cambio del viernes
    Tipo: el catálogo se cruza aunque escriba el nombre distinto, y la moneda
        sale del nombre del tipo
    Idempotencia: dos corridas dan archivos idénticos byte a byte

Tarda un par de segundos. Si algo falla, sale con un AssertionError.
"""
from __future__ import annotations

import hashlib
import logging
import sys
import tempfile
from pathlib import Path

import duckdb
import pandas as pd

AQUI = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(AQUI))

import construir  # después de añadir la raíz a la ruta

logging.basicConfig(level=logging.WARNING)


def bronce_inventado(raiz: Path) -> None:
    """Tres fondos de una SAF, del jueves 1 al sábado 3 de octubre de 2026."""
    f = pd.to_datetime
    dias = f(["2026-10-01", "2026-10-02", "2026-10-03"])
    base = {"empresa_id": "1", "empresa": "SAF UNO", "saf_retirada": False,
            "estado": "vigente", "captura": "00000000"}
    filas = []
    # A: homónimo de B, en dólares; pasa a Operativa el día 2; el día 2 reporta
    #    tipo de cambio 1 (error) y el día 3 valor cuota 0 (error). Como en la
    #    SMV, el patrimonio viene en soles: valor cuota × cuotas × tipo de cambio
    #    de verdad (3,50 el día 1 y 3,60 el día 2, el promedio del BCRP).
    #    Sin suscripciones: las cuotas no cambian, así que el flujo debe ser 0
    #    aunque el patrimonio en soles suba por el dólar.
    for d, sit, vc, tc, pat in zip(dias, ["Preoperativa", "Operativa", "Operativa"],
                                   [10.0, 10.1, 0.0], [3.5, 1.0, 3.5], [350.0, 363.6, 360.0]):
        filas.append({**base, "fondo": "RENTA FMIV", "moneda": "DOLARES",
                      "fecha_inicio_operaciones": f("2026-10-02"), "fecha": d, "situacion": sit,
                      "valor_cuota": vc, "patrimonio": pat, "n_participes": 5.0,
                      "cuotas_en_circulacion": 10.0, "tipo_cambio": tc})
    # B: mismo nombre y moneda que A, otra fecha de inicio; patrimonio negativo el día 1.
    for d, pat in zip(dias, [-1.0, 50.0, 50.0]):
        filas.append({**base, "fondo": "RENTA FMIV", "moneda": "DOLARES",
                      "fecha_inicio_operaciones": f("2020-01-01"), "fecha": d, "situacion": "Operativa",
                      "valor_cuota": 20.0, "patrimonio": pat, "n_participes": 3.0,
                      "cuotas_en_circulacion": 2.0, "tipo_cambio": 3.5})
    # C: en soles, sin fecha de inicio, nunca operó, tipo de cambio 1 (correcto en soles).
    for d in dias:
        filas.append({**base, "fondo": "Ahorro Soles FMIV", "moneda": "SOLES",
                      "fecha_inicio_operaciones": pd.NaT, "fecha": d, "situacion": "Preoperativa",
                      "valor_cuota": None, "patrimonio": 7.0, "n_participes": 1.0,
                      "cuotas_en_circulacion": 1.0, "tipo_cambio": 1.0})
    (raiz / "bcrp").mkdir(parents=True)
    (raiz / "catalogo").mkdir()
    pd.DataFrame(filas).to_parquet(raiz / "panel.parquet", index=False)
    # El catálogo escribe el nombre distinto (minúsculas, doble espacio): el cruce debe resolverlo.
    pd.DataFrame({"fondo": ["Renta  Fmiv", "Ahorro Soles Fmiv"],
                  "tipo_fondo": ["Inst. Deuda Corto Plazo $", "Etapa Preoperativa"],
                  "moneda": ["DOLARES", "SOLES"], "empresa_id": ["1", "1"]}
                 ).to_csv(raiz / "catalogo" / "catalogo_20261003.csv", index=False)
    # El BCRP publica jueves y viernes; el sábado no.
    pd.DataFrame({"fecha": f(["2026-10-01", "2026-10-02"]),
                  "interbancario_compra": [3.49, 3.59], "interbancario_venta": [3.51, 3.61],
                  "sbs_compra": [3.49, 3.59], "sbs_venta": [3.51, 3.61]}
                 ).to_parquet(raiz / "bcrp" / "tipo_cambio.parquet", index=False)


def huellas(carpeta: Path) -> dict[str, str]:
    return {p.name: hashlib.md5(p.read_bytes()).hexdigest() for p in sorted(carpeta.glob("*.parquet"))}


def main(tmp: Path | None = None) -> None:
    tmp = tmp or Path(tempfile.mkdtemp())
    construir.BRONCE = tmp / "bronce"
    construir.PLATA = tmp / "plata"
    construir.ORO = tmp / "oro"
    construir.BRONCE.mkdir()
    bronce_inventado(construir.BRONCE)

    assert construir.main() == 0, "construir.py no terminó bien o falló un control"
    primera = huellas(construir.PLATA)
    assert construir.main() == 0, "la segunda corrida no terminó bien"
    assert huellas(construir.PLATA) == primera, "Idempotencia: dos corridas dieron archivos distintos"
    print("OK  Idempotencia: dos corridas dan los mismos archivos, byte a byte")

    def q(sql: str) -> pd.DataFrame:
        """Consulta sobre plata; '@' se reemplaza por la carpeta de plata temporal."""
        return duckdb.sql(sql.replace("@", construir.PLATA.as_posix() + "/")).df()

    fondos = q("SELECT * FROM '@dim_fondo.parquet' ORDER BY fecha_inicio_operaciones NULLS LAST")
    hecho = q("""SELECT f.fondo, f.fecha_inicio_operaciones AS inicio, h.*
                 FROM '@hecho_valor_cuota.parquet' h JOIN '@dim_fondo.parquet' f USING (fondo_id)
                 ORDER BY h.fecha""")
    tc = q("SELECT * FROM '@dim_tipo_cambio.parquet' ORDER BY fecha")

    assert len(fondos) == 3 and fondos.fondo_id.nunique() == 3 and fondos.fondo_id.notna().all(), \
        f"P4: esperaba 3 ids distintos y ninguno vacío, hay {fondos.fondo_id.tolist()}"
    print("OK  P4: homónimos con distinta fecha de inicio, ids distintos; sin fecha de inicio, con id")

    _b, a, c = fondos.itertuples()   # ordenados por fecha de inicio: B (2020), A (2026), C (vacía)
    assert str(a.fecha_operativa)[:10] == "2026-10-02", f"P3: A pasa a Operativa el 2, salió {a.fecha_operativa}"
    assert pd.isna(c.fecha_operativa), "P3: C nunca operó, su fecha_operativa debía quedar vacía"
    print("OK  P3: fecha_operativa es el primer día Operativa, y vacía si nunca operó")

    assert len(hecho) == 9, f"P5: el panel tiene 9 filas y el hecho {len(hecho)}"
    ha = hecho[hecho.inicio.astype(str).str.startswith("2026")]
    ha = ha.set_index(ha.fecha.astype(str).str[:10])
    assert pd.isna(ha.loc["2026-10-03", "valor_cuota"]) and ha.loc["2026-10-03", "vc_invalido"], \
        "P5: el valor cuota 0 debía quedar vacío y marcado"
    assert ha.loc["2026-10-02", "tc_dudoso"] and not ha.loc["2026-10-01", "tc_dudoso"], \
        "P5: en dólares, tipo de cambio 1 es dudoso y 3,5 junto a un BCRP de 3,5 no"
    assert hecho.patrimonio_negativo.sum() == 1, "P5: había un patrimonio negativo"
    hc = hecho[hecho.inicio.isna()]
    assert not hc.tc_dudoso.any(), "P5: en soles el tipo de cambio 1 no es dudoso"
    print("OK  P5: no se pierde ninguna fila y cada problema queda marcado donde corresponde")

    sab = tc[tc.fecha.astype(str).str.startswith("2026-10-03")].iloc[0]
    assert abs(sab.interbancario_promedio - 3.60) < 1e-9 and not sab.es_dato_del_dia, \
        "P2: el sábado debía tomar el promedio del viernes (3,60) y marcarse como arrastrado"
    print("OK  P2: el sábado toma el tipo de cambio del viernes y se marca como arrastrado")

    assert (fondos.loc[fondos.moneda == "DOLARES", "tipo"] == "Deuda corto plazo").all(), \
        f"Tipo: A y B debían cruzar con el catálogo, salió {fondos.tipo.tolist()}"
    assert pd.isna(c.tipo) and c.tipo_smv == "Etapa Preoperativa", \
        "Tipo: 'Etapa Preoperativa' es un estado, no un tipo: tipo vacío y el crudo visible"
    print("OK  Tipo: el catálogo cruza aunque escriba distinto el nombre, y los estados no son tipos")

    # Las tres reglas para las series que el catálogo no tipa, en su orden.
    cat = pd.DataFrame({
        "empresa_id": ["1"] * 5, "moneda": ["DOLARES"] * 5,
        "k": ["RENTA EN DOLARES I FMIV", "RENTA EN DOLARES II FMIV", "RENTA EN DOLARES III FMIV",
              "RENTA EN DOLARES IV FMIV", "RENTA FIJA DOLARES FMIV"],
        "tipo_fondo": ["Inst Deuda Duración Flexibl $"] * 4 + ["Estructurado"]})
    regla = construir.tipo_por_reglas
    assert regla("CORIL RENTA MIXTA SOLES FONDO DE INVERSION RET", "1", "DOLARES", cat)[1] == \
        "regla_vehiculo", "Reglas: un fondo de inversión se reconoce antes que su 'RENTA MIXTA'"
    assert regla("RENTA", "1", "DOLARES", cat) == ("Deuda duración flexible", "regla_familia", "media"), \
        "Reglas: 4 de 5 hermanos (80 %) comparten tipo: debía asignarse ese"
    assert regla("RENTA", "1", "SOLES", cat)[0] is None, \
        "Reglas: los hermanos de otra moneda no cuentan"
    assert regla("DIVISO ACCIONES FMIV", "9", "DOLARES", cat)[0] == "Renta variable", \
        "Reglas: 'ACCIONES' en el nombre es renta variable"
    assert regla("DIVISO CONSERVADOR DOLARES FMIV", "9", "DOLARES", cat) == (None, None, None), \
        "Reglas: 'CONSERVADOR' no dice el tipo: debía quedar sin tipo, no adivinarse"
    print("OK  Reglas: vehículo, familia con mayoría de 80 %, palabra clave, y sin tipo si no hay pista")

    # Oro: el fondo A en dólares, días 1 y 2.
    oro = construir.ORO.as_posix() + "/"
    fa = duckdb.sql(f"""SELECT * FROM '{oro}fondos.parquet'
                        WHERE fondo_id = '{a.fondo_id}' ORDER BY fecha""").df()
    d1, d2 = fa.iloc[0], fa.iloc[1]
    assert abs(d1.patrimonio_moneda - 100) < 1e-9, \
        f"Oro: 350 soles a 3,50 son 100 dólares, salió {d1.patrimonio_moneda}"
    assert abs(d2.tipo_cambio - 3.60) < 1e-9, \
        "Oro: con el tipo de cambio reportado dudoso (1) se usa el promedio del BCRP (3,60)"
    assert abs(d2.retorno - 0.01) < 1e-9, f"Oro: de 10 a 10,1 es 1 %, salió {d2.retorno}"
    assert abs(d2.flujo) < 1e-9, \
        f"Oro: sin cambio de cuotas el flujo es 0 aunque suba el dólar, salió {d2.flujo}"
    assert d1.situacion == "Preoperativa" and d2.situacion == "Operativa", \
        "Oro: la situación se reconstruye desde fecha_operativa"
    mm = duckdb.sql(f"SELECT * FROM '{oro}mercado_mensual.parquet'").df()
    assert abs(mm.patrimonio_soles.sum() - (360.0 + 50.0 + 7.0)) < 1e-9, \
        f"Oro: el cierre de octubre suma el último día de cada fondo, salió {mm.patrimonio_soles.sum()}"
    meta = duckdb.sql(f"SELECT key, value FROM parquet_kv_metadata('{oro}fondos.parquet')").df()
    assert any(b"Superintendencia del Mercado de Valores" in v for v in meta.value), \
        "Oro: el archivo debe llevar dentro la atribución a la SMV"
    print("OK  Oro: patrimonio en la moneda del fondo, flujo sin efecto del dólar, cierre mensual "
          "y atribución dentro del archivo")

    print("\nTodo en orden.")


if __name__ == "__main__":
    main()
