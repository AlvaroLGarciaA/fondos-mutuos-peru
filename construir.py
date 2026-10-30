"""
construir.py — de bronce a plata y oro: el panel de la SMV convertido en un
esquema estrella (plata) y en las tablas que se publican (oro).

CÓMO SE USA
    uv run construir.py

    No toca la red. Lee datos/bronce/ y reescribe datos/plata/ y datos/oro/
    enteras, en unos segundos. Correrlo dos veces da exactamente lo mismo
    (idempotente): no hay contadores, fechas de ejecución ni números
    aleatorios en la salida.

QUÉ LEE (bronce, tal como llegó)
    datos/bronce/panel.parquet               valores cuota de la SMV (actualizar.py)
    datos/bronce/catalogo/catalogo_*.csv     tipo de cada fondo (el más reciente)
    datos/bronce/bcrp/tipo_cambio.parquet    tipo de cambio oficial (actualizar.py)

QUÉ ESCRIBE (plata, en datos/plata/)
    dim_administradora.parquet   una fila por administradora (SAF)
    dim_fondo.parquet            una fila por fondo, con su tipo
    dim_tipo_cambio.parquet      una fila por día calendario, con el BCRP
    hecho_valor_cuota.parquet    una fila por fondo y día  ← la tabla de hechos
    correspondencia_tipo.parquet los 32 tipos sucios del catálogo y su versión limpia

    Es un esquema estrella: la tabla de hechos en el centro y las
    dimensiones alrededor, unidas por tres relaciones de uno a varios:
        dim_administradora[empresa_id] 1 ── * dim_fondo[empresa_id]
        dim_fondo[fondo_id]            1 ── * hecho_valor_cuota[fondo_id]
        dim_tipo_cambio[fecha]         1 ── * hecho_valor_cuota[fecha]

QUÉ ESCRIBE (oro, en datos/oro/: lo que se publica)
    fondos.parquet           una fila por fondo y día, ancha y lista para consultar:
                             nombres en vez de claves, patrimonio en soles y en
                             la moneda del fondo, retorno diario y flujo neto de
                             suscripciones
    mercado_mensual.parquet  cierre de cada mes por administradora, familia y
                             moneda: fondos, patrimonio en soles, partícipes, flujo
    fondos_catalogo.parquet  una fila por fondo: sus atributos, su último dato y
                             si sigue vivo

    Plata no tiene nada calculado; oro es donde viven los cálculos. Si mañana
    cambia la forma de calcular el retorno, se rehace oro desde plata sin
    volver a descargar nada. Cada archivo de oro lleva dentro, como metadatos
    del Parquet, la atribución a la SMV y al BCRP (sección 10 del plan).

LAS DECISIONES VIENEN DE exploracion/f2_diseno.py (ejercicio 1 de F2)
    P1  qué columna va a qué tabla
    P2  tipo_cambio: el reportado se queda en el hecho; el oficial es el BCRP
    P3  situacion: no se guarda por día; basta la fecha en que pasó a Operativa
    P4  fondo_id: huella md5 de la clave, no un número correlativo
    P5  calidad: no se borra ninguna fila; los problemas se marcan
    Cada bloque de abajo cita la pregunta de la que sale.

POR QUÉ CASI TODO ES SQL
    Plata se arma con DuckDB porque cada paso es una consulta que se puede
    leer y ejecutar por separado para ver qué da.
    Solo hay pandas donde SQL no llega: la normalización de nombres del
    catálogo (una función de Python del motor) y el calendario del BCRP.
"""
from __future__ import annotations

import logging
import re
import sys
import time
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, ValueError):  # consola que no se deja reconfigurar
    pass

import duckdb
import pandas as pd

AQUI = Path(__file__).resolve().parent
sys.path.insert(0, str(AQUI / "motor"))
from nombres import clave_fondo  # después de añadir motor/ a la ruta

BRONCE = AQUI / "datos" / "bronce"
PLATA = AQUI / "datos" / "plata"
ORO = AQUI / "datos" / "oro"

# Sección 10 del plan: no se declara una licencia que no se tiene. Texto fijo;
# solo cambia la fecha de descarga.
ATRIBUCION_SMV = ("Datos de la Superintendencia del Mercado de Valores del Perú, descargados el "
                  "{fecha}. La fuente no declara licencia de reutilización. Se republican aquí "
                  "con atribución y se retiran a solicitud. Sin afiliación con la SMV.")
ATRIBUCION_BCRP = ("Tipo de cambio: Banco Central de Reserva del Perú, API de series estadísticas "
                   "(PD04637PD a PD04640PD). Sin afiliación con el BCRP.")

# Un retorno diario mayor que esto, en valor absoluto, casi seguro es un error
# de la fuente (un valor cuota mal digitado) y no un movimiento del mercado.
# Se marca, no se borra: el que analiza decide.
RETORNO_ATIPICO = 0.20

log = logging.getLogger("construir")


# ---------------------------------------------------------------------------
# P4 · La clave del fondo
# ---------------------------------------------------------------------------
# La SMV no da un id de fondo. Un fondo se identifica con cuatro columnas
# (hay fondos distintos que comparten nombre y solo se distinguen por moneda y
# fecha de inicio). Se condensan en una huella md5:
#   - los mismos cuatro valores dan siempre el mismo id, corra cuando corra;
#   - un fondo nuevo no corre los ids de los demás, como sí pasaría con una
#     numeración 1, 2, 3... por orden alfabético.
# La fecha de inicio vacía (52 series) se reemplaza por 'sin_inicio': con NULL,
# concat_ws la saltaría en silencio. La fecha se escribe como AAAA-MM-DD para
# que el id no cambie si mañana la columna llega como DATE en vez de TIMESTAMP.

FONDO_ID = """md5(concat_ws('|', empresa_id, fondo, moneda,
                  coalesce(strftime(fecha_inicio_operaciones, '%Y-%m-%d'), 'sin_inicio')))"""


# ---------------------------------------------------------------------------
# Correspondencia de tipos (sección 8.7 del plan)
# ---------------------------------------------------------------------------
# El catálogo trae 32 tipos sucios: la moneda metida en el nombre ("... $",
# "... S/", "Dólares"), abreviaturas distintas para lo mismo y dos valores que
# no son tipos sino estados del fondo. La moneda ya está en su propia columna,
# así que aquí se quita. Dos niveles:
#   tipo     la categoría de la SMV sin moneda (para filtrar con detalle)
#   familia  la agrupación para comparar mercados (para los gráficos)
# Un tipo nuevo que aparezca en el catálogo y no esté aquí NO rompe la
# corrida: queda con tipo vacío y se avisa en el resumen, para decidir a mano.
DEUDA = "Deuda"
TIPOS = {
    # tipo crudo del catálogo           (tipo,                          familia)
    "Inst.Deuda Muy Corto Plazo S/":    ("Deuda muy corto plazo",       DEUDA),
    "Inst Deuda Muy Corto Plazo $":     ("Deuda muy corto plazo",       DEUDA),
    "Inst. Deuda Corto Plazo S/":       ("Deuda corto plazo",           DEUDA),
    "Inst. Deuda Corto Plazo $":        ("Deuda corto plazo",           DEUDA),
    "Corto Plazo Soles":                ("Deuda corto plazo",           DEUDA),
    "Corto Plazo Dólares":              ("Deuda corto plazo",           DEUDA),
    "Inst. Deuda Mediano Plazo S/":     ("Deuda mediano plazo",         DEUDA),
    "Inst. Deuda Mediano Plazo $":      ("Deuda mediano plazo",         DEUDA),
    "Inst. Deuda Largo Plazo $":        ("Deuda largo plazo",           DEUDA),
    "Inst Deuda Duración Flexibl S/":   ("Deuda duración flexible",     DEUDA),
    "Inst Deuda Duración Flexibl $":    ("Deuda duración flexible",     DEUDA),
    # "Renta fija" e "Instrumento de deuda" no dicen el plazo: no se adivina.
    "Instrumento de Deuda":             ("Deuda sin plazo declarado",   DEUDA),
    "Renta Fija Soles":                 ("Deuda sin plazo declarado",   DEUDA),
    "Renta Fija Dólares":               ("Deuda sin plazo declarado",   DEUDA),
    # Conservador, moderado, crecimiento y balanceado son matices del mismo
    # tipo: se juntan. El detalle sigue en tipo_smv si alguien lo necesita.
    "Renta Mixta Soles":                ("Renta mixta",                 "Renta mixta"),
    "Renta Mixta Dólares":              ("Renta mixta",                 "Renta mixta"),
    "Renta Mixta Balanceado Soles":     ("Renta mixta",                 "Renta mixta"),
    "Renta Mixta Balanceado Dólares":   ("Renta mixta",                 "Renta mixta"),
    "Renta Mixta Crecimiento Soles":    ("Renta mixta",                 "Renta mixta"),
    "Renta Mixta Crecimiento Dolars":   ("Renta mixta",                 "Renta mixta"),
    "Renta Mixta Moderado Soles":       ("Renta mixta",                 "Renta mixta"),
    "Renta Mixta Moderado Dólares":     ("Renta mixta",                 "Renta mixta"),
    "Renta Variable":                   ("Renta variable",              "Renta variable"),
    "Flexible":                         ("Flexible",                    "Flexible"),
    "Flexible Dólares":                 ("Flexible",                    "Flexible"),
    "Estructurado":                     ("Estructurado",                "Estructurado"),
    "Garantizado":                      ("Garantizado",                 "Estructurado"),
    "Fondo de Fondos":                  ("Fondo de fondos",             "Fondo de fondos"),
    "Internacional":                    ("Internacional",               "Internacional"),
    "Fondo Bursátil - ETF":             ("Fondo bursátil (ETF)",        "Renta variable"),
    # No son tipos: son etapas del fondo. Quedan sin tipo, con el crudo visible.
    "Etapa Preoperativa":               (None,                          None),
    "En Liquidación":                   (None,                          None),
}
FAMILIA_DE = {tipo: familia for tipo, familia in TIPOS.values() if tipo}


# ---------------------------------------------------------------------------
# Reglas para las series que el catálogo no tipa (sección 8.7 del plan)
# ---------------------------------------------------------------------------
# El catálogo solo cubre las administradoras vigentes y escribe algunos nombres
# distinto que el panel. Medido el 28-oct: 45 de 790 series quedan sin tipo. Se
# aplican tres reglas, en este orden, y SOLO a esas series (la SMV siempre gana).
# Cada fila guarda qué regla la tipó (`tipo_fuente`) y cuánto confiar en ella
# (`tipo_confianza`), para poder filtrarlas o revisarlas.
#
# 1. Vehículo: el nombre dice "FONDO DE INVERSION" o "FI". No son fondos mutuos:
#    son otro vehículo, cerrado y para otro público, que la SMV mezcla en esta
#    consulta (17 series, casi todas con 1 a 45 observaciones). Se marcan con su
#    propia familia y `es_fondo_mutuo = false`, para sacarlos de las métricas
#    del mercado de fondos mutuos en oro. Confianza alta: el nombre lo declara.
# 2. Familia truncada: el panel corta algunos nombres. "CREDICORP CAPITAL RENTA"
#    (20 series, el mayor patrimonio de las 45) es el principio de 49 fondos del
#    catálogo de la misma administradora y moneda; 40 de ellos (82 %) son "Deuda
#    duración flexible". Si al menos el 80 % de los fondos que empiezan así
#    comparten tipo, se asigna ese. Confianza media: es la mayoría, no el fondo.
# 3. Palabra clave en el nombre: "ACCIONES", "FLEXIBLE", "RENTA FIJA"... Para
#    fondos retirados que el catálogo ya no muestra. Confianza media.
# Lo que ninguna regla resuelve queda sin tipo (2 series: "DIVISO CONSERVADOR",
# que puede ser deuda o mixto). No se adivina.
REGLA_VEHICULO = r"\bFONDO DE INVERSION\b|\bFI\b"
MAYORIA_FAMILIA = 0.80
PALABRAS_CLAVE = [   # el orden importa: gana la primera que aparezca en la lista
    (r"\bFONDO DE FONDOS\b",                     "Fondo de fondos"),
    (r"\bACCIONES\b|\bEQUITY\b|\bRENTA VARIABLE\b", "Renta variable"),
    (r"\bRENTA MIXTA\b",                         "Renta mixta"),
    (r"\bFLEXIBLE\b",                            "Flexible"),
    (r"\bCORTO PLAZO\b",                         "Deuda corto plazo"),
    (r"\bRF\b|\bRENTA FIJA\b",                   "Deuda sin plazo declarado"),
]
FAMILIA_DE["Fondo de inversión"] = "Fondo de inversión"


# ---------------------------------------------------------------------------
# Lectura de bronce
# ---------------------------------------------------------------------------
def catalogo_reciente() -> Path:
    """El catálogo más nuevo. Los que empiezan con '_' son capturas descartadas."""
    archivos = sorted(p for p in (BRONCE / "catalogo").glob("catalogo_*.csv"))
    if not archivos:
        raise FileNotFoundError(f"no hay catálogo en {BRONCE / 'catalogo'}")
    return archivos[-1]   # el nombre lleva la fecha AAAAMMDD: el último es el más nuevo


def tipos_por_fondo(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """fondo_id -> tipo del catálogo de la SMV.

    El catálogo y el panel escriben distinto el mismo fondo ("Bbva Renta
    Mediano Plazo Soles  Fmiv" frente a "BBVA RENTA MEDIANO PLAZO SOLES FMIV").
    Se cruzan por un nombre normalizado (clave_fondo de motor/nombres.py: mayúsculas,
    sin tildes, sin puntuación y sin el sufijo "EN SOLES/DOLARES"), dentro de
    la misma administradora y moneda. Es el mismo cruce de la sección 8.7 del
    plan: resuelve 746 de las 790 series y el 99 % del patrimonio.
    """
    cat = pd.read_csv(catalogo_reciente(), dtype={"empresa_id": str})
    cat["k"] = cat["fondo"].map(clave_fondo)
    # 7 fondos salen dos veces en el catálogo (medido el 28-oct), siempre con el
    # mismo tipo: quedarse con uno no pierde nada.
    cat = cat.drop_duplicates(["empresa_id", "k", "moneda"])

    series = con.sql(f"SELECT DISTINCT {FONDO_ID} AS fondo_id, empresa_id, fondo, moneda FROM panel").df()
    series["k"] = series["fondo"].map(clave_fondo)

    t = series.merge(cat[["empresa_id", "k", "moneda", "tipo_fondo"]],
                     on=["empresa_id", "k", "moneda"], how="left")
    t = t.rename(columns={"tipo_fondo": "tipo_smv"})
    t["tipo"] = t["tipo_smv"].map(lambda x: TIPOS.get(x, (None, None))[0])
    t["tipo_fuente"] = t["tipo"].notna().map({True: "smv", False: None})
    t["tipo_confianza"] = t["tipo"].notna().map({True: "alta", False: None})

    nuevos = sorted(set(t["tipo_smv"].dropna()) - set(TIPOS))
    if nuevos:
        log.warning("Tipos del catálogo sin correspondencia (quedan sin tipo): %s", nuevos)

    # Las reglas, solo donde el catálogo no dio tipo.
    for i in t.index[t["tipo"].isna()]:
        tipo, fuente, confianza = tipo_por_reglas(t.at[i, "k"], t.at[i, "empresa_id"],
                                                  t.at[i, "moneda"], cat)
        t.loc[i, ["tipo", "tipo_fuente", "tipo_confianza"]] = [tipo, fuente, confianza]

    t["familia"] = t["tipo"].map(FAMILIA_DE)
    t["es_fondo_mutuo"] = ~t["k"].str.contains(REGLA_VEHICULO, regex=True)
    return t[["fondo_id", "tipo_smv", "tipo", "familia", "tipo_fuente", "tipo_confianza",
              "es_fondo_mutuo"]]


def tipo_por_reglas(k: str, empresa_id: str, moneda: str,
                    cat: pd.DataFrame) -> tuple[str | None, str | None, str | None]:
    """(tipo, regla, confianza) para una serie sin tipo. Ver las tres reglas arriba.

    `k` es el nombre ya normalizado (clave_fondo): mayúsculas, sin tildes, sin
    puntuación. Por eso las expresiones regulares no llevan tildes.
    """
    if re.search(REGLA_VEHICULO, k):
        return "Fondo de inversión", "regla_vehiculo", "alta"

    # Familia truncada: los fondos del catálogo de la misma SAF y moneda cuyo
    # nombre empieza con este. Solo si son varios y casi todos del mismo tipo.
    hermanos = cat.loc[(cat["empresa_id"] == empresa_id) & (cat["moneda"] == moneda)
                       & cat["k"].str.startswith(k + " "), "tipo_fondo"].map(
                           lambda x: TIPOS.get(x, (None, None))[0]).dropna()
    if len(hermanos) >= 3:
        cuota = hermanos.value_counts(normalize=True)
        if cuota.iloc[0] >= MAYORIA_FAMILIA:
            return cuota.index[0], "regla_familia", "media"

    for patron, tipo in PALABRAS_CLAVE:
        if re.search(patron, k):
            return tipo, "regla_palabra", "media"
    return None, None, None


def calendario_tipo_cambio(desde: pd.Timestamp, hasta: pd.Timestamp) -> pd.DataFrame:
    """P2 · Una fila por día calendario con el tipo de cambio del BCRP.

    El BCRP publica solo días hábiles; los fondos reportan también sábados,
    domingos y feriados. Para que el cruce con el hecho sea un JOIN simple por
    fecha, aquí se rellenan todos los
    días con el último dato publicado. `es_dato_del_dia` dice si ese día el BCRP
    publicó o si se arrastró el anterior.
    """
    tc = pd.read_parquet(BRONCE / "bcrp" / "tipo_cambio.parquet")
    tc["fecha"] = pd.to_datetime(tc["fecha"])
    # La referencia para convertir a soles (decisión de la sección 8.8 del plan):
    # el promedio del interbancario compra y venta. Es la serie más cercana a lo
    # que reportan las administradoras y no depende de quién reporta.
    tc["interbancario_promedio"] = (tc["interbancario_compra"] + tc["interbancario_venta"]) / 2
    publicado = tc.dropna(subset=["interbancario_promedio"])["fecha"]

    dias = pd.DataFrame({"fecha": pd.date_range(desde, hasta, freq="D")})
    cal = dias.merge(tc, on="fecha", how="left")
    cal["es_dato_del_dia"] = cal["fecha"].isin(publicado)
    series = ["interbancario_compra", "interbancario_venta", "sbs_compra", "sbs_venta",
              "interbancario_promedio"]
    # ffill: "forward fill", rellenar hacia adelante con el último valor visto.
    # No hay bfill: antes del primer dato del BCRP queda vacío, no se inventa.
    cal[series] = cal[series].ffill()
    cal["fecha"] = cal["fecha"].dt.date
    return cal[["fecha", *series, "es_dato_del_dia"]]


# ---------------------------------------------------------------------------
# Las tablas de plata
# ---------------------------------------------------------------------------
SQL_DIM_ADMINISTRADORA = """
-- P1 · empresa, saf_retirada: un solo valor por administradora (max_por_saf = 1).
SELECT empresa_id, empresa, saf_retirada
FROM panel
GROUP BY empresa_id, empresa, saf_retirada
ORDER BY empresa_id
"""

SQL_DIM_FONDO = f"""
-- P1 · fondo, moneda y fecha de inicio: un solo valor por fondo.
-- P3 · situacion cambia una sola vez y siempre de Preoperativa a Operativa, así
--      que en lugar de repetirla en cada día se guarda cuándo cambió:
--      fecha < fecha_operativa -> Preoperativa; si no -> Operativa.
--      Vacía si el fondo nunca llegó a operar (55 series).
-- P5 · tiene_valor_cuota: 22 series traen patrimonio pero ningún valor cuota.
--      Sirven para medir el tamaño del mercado, no para calcular rentabilidad.
WITH f AS (
    SELECT {FONDO_ID}                                                   AS fondo_id,
           empresa_id, fondo, moneda,
           CAST(fecha_inicio_operaciones AS DATE)                       AS fecha_inicio_operaciones,
           CAST(min(fecha) FILTER (WHERE situacion = 'Operativa') AS DATE) AS fecha_operativa,
           CAST(min(fecha) AS DATE)                                     AS primera_fecha,
           CAST(max(fecha) AS DATE)                                     AS ultima_fecha,
           count(*)                                                     AS n_observaciones,
           count(*) FILTER (WHERE valor_cuota > 0) > 0                  AS tiene_valor_cuota
    FROM panel
    GROUP BY ALL
)
SELECT f.*, t.tipo, t.familia, t.tipo_smv, t.tipo_fuente, t.tipo_confianza, t.es_fondo_mutuo
FROM f LEFT JOIN tipos t USING (fondo_id)
ORDER BY empresa_id, fondo, fecha_inicio_operaciones
"""

SQL_HECHO = f"""
-- P1 · las medidas que cambian cada día. La clave del hecho es (fondo_id, fecha).
-- P5 · no se borra ninguna fila. Cada problema queda en una columna de marca:
--      vc_invalido          el valor cuota original era 0 o negativo (572 filas);
--                           se guarda vacío, porque un precio así no existe
--      patrimonio_negativo  patrimonio < 0 (168 filas); el valor se conserva
--      tc_dudoso            fondo en dólares cuyo tipo de cambio reportado está
--                           vacío, es 0 o se aleja más de 1 % del BCRP. En
--                           fondos en soles es siempre falso: ahí el tipo de
--                           cambio no se usa (P2). Vacío solo si ese día aún no
--                           hay dato del BCRP (el 1-ene-2003).
-- P2 · patrimonio_soles: la SMV publica el patrimonio de TODOS los fondos en
--      soles. En los fondos en dólares, la administradora lo convierte con su
--      propio tipo de cambio: medido el 28-oct, en 586.941 de 587.764 filas en
--      dólares (99,9 %), patrimonio = valor cuota × cuotas × tipo_cambio_reportado.
--      Por eso aquí la columna se llama con su moneda. tipo_cambio_reportado es
--      ese factor, tal cual; para volver a la moneda del fondo se usa en oro.
-- coalesce(x, false): si la comparación da NULL (porque el dato está vacío),
-- la marca queda en falso y no en vacío.
SELECT {FONDO_ID}                                          AS fondo_id,
       CAST(p.fecha AS DATE)                               AS fecha,
       CASE WHEN p.valor_cuota > 0 THEN p.valor_cuota END  AS valor_cuota,
       p.patrimonio                                        AS patrimonio_soles,
       p.n_participes,
       p.cuotas_en_circulacion,
       p.tipo_cambio                                       AS tipo_cambio_reportado,
       coalesce(p.valor_cuota <= 0, false)                 AS vc_invalido,
       coalesce(p.patrimonio < 0, false)                   AS patrimonio_negativo,
       CASE
           WHEN p.moneda <> 'DOLARES'              THEN false
           WHEN tc.interbancario_promedio IS NULL  THEN NULL
           ELSE coalesce(abs(p.tipo_cambio / tc.interbancario_promedio - 1) > 0.01, true)
       END                                                 AS tc_dudoso
FROM panel p
LEFT JOIN tc ON CAST(p.fecha AS DATE) = tc.fecha
ORDER BY fondo_id, fecha
"""


# ---------------------------------------------------------------------------
# Las tablas de oro: aquí sí hay cálculos
# ---------------------------------------------------------------------------
SQL_ORO_FONDOS = f"""
-- Una fila por fondo y día, con nombres en vez de claves para consultar sin JOIN.
--   situacion          se reconstruye desde fecha_operativa (P3)
--   patrimonio_soles   tal como lo publica la SMV: ya está en soles (P2)
--   tipo_cambio        soles por unidad de la moneda del fondo. En soles, 1. En
--                      dólares, el que usó la administradora para convertir su
--                      patrimonio; si ese es dudoso (vacío, 0, 1 o a más de 1 %
--                      del BCRP), el promedio interbancario del BCRP del día
--   patrimonio_moneda  el patrimonio en la moneda del fondo:
--                      patrimonio_soles / tipo_cambio
--   retorno            valor_cuota / valor_cuota anterior - 1. Si entre las dos
--                      observaciones hay un hueco, cubre todo el hueco:
--                      dias_desde_anterior lo dice
--   flujo              suscripciones menos rescates, en la moneda del fondo: lo
--                      que cambió el patrimonio y no explica la rentabilidad.
--                      patrimonio_moneda - patrimonio_moneda_anterior × (1 + retorno).
--                      Se calcula en la moneda del fondo y no en soles para que
--                      una subida del dólar no parezca una suscripción
--   flujo_soles        flujo × tipo_cambio del día
--   retorno_atipico    |retorno| > {RETORNO_ATIPICO:.0%}: casi seguro un error de la fuente
-- lag(x) OVER w trae el valor de la observación anterior del mismo fondo.
WITH base AS (
    SELECT h.*, f.fondo, f.moneda, f.tipo, f.familia, f.es_fondo_mutuo, f.fecha_operativa,
           a.empresa AS administradora,
           CASE WHEN f.moneda = 'SOLES'  THEN 1.0
                -- tc_dudoso vacío = aún no hay BCRP para comparar: se confía en el reportado
                WHEN coalesce(NOT h.tc_dudoso, h.tipo_cambio_reportado > 1.5)
                                         THEN h.tipo_cambio_reportado
                ELSE t.interbancario_promedio END              AS tipo_cambio
    FROM hecho_valor_cuota h
    JOIN dim_fondo f          USING (fondo_id)
    JOIN dim_administradora a USING (empresa_id)
    LEFT JOIN dim_tipo_cambio t USING (fecha)
),
h AS (
    SELECT *, patrimonio_soles / tipo_cambio                   AS patrimonio_moneda
    FROM base
),
l AS (
    SELECT *,
           lag(fecha)             OVER w AS fecha_anterior,
           lag(valor_cuota)       OVER w AS vc_anterior,
           lag(patrimonio_moneda) OVER w AS patrimonio_moneda_anterior
    FROM h
    WINDOW w AS (PARTITION BY fondo_id ORDER BY fecha)
),
c AS (
    SELECT *,
           valor_cuota / vc_anterior - 1                       AS retorno,
           patrimonio_moneda - patrimonio_moneda_anterior * valor_cuota / vc_anterior
                                                               AS flujo
    FROM l
)
SELECT fecha, fondo_id, fondo, administradora, moneda, tipo, familia, es_fondo_mutuo,
       CASE WHEN fecha >= fecha_operativa THEN 'Operativa' ELSE 'Preoperativa' END AS situacion,
       valor_cuota, patrimonio_soles, patrimonio_moneda, tipo_cambio,
       n_participes, cuotas_en_circulacion,
       retorno, date_diff('day', fecha_anterior, fecha) AS dias_desde_anterior,
       flujo, flujo * tipo_cambio AS flujo_soles,
       coalesce(abs(retorno) > {RETORNO_ATIPICO}, false) AS retorno_atipico,
       vc_invalido, patrimonio_negativo, tc_dudoso
FROM c
ORDER BY administradora, fondo, fondo_id, fecha
"""

SQL_ORO_MERCADO_MENSUAL = """
-- El mercado de fondos mutuos mes a mes. Solo fondos mutuos: los fondos de
-- inversión (es_fondo_mutuo = false) son otro vehículo y se dejan fuera.
--   patrimonio_soles  la última observación de cada fondo en el mes, sumada
--   participes        igual; una persona con dos fondos cuenta dos veces
--   flujo_soles       la suma de los flujos diarios del mes
--   fondos            fondos con al menos una observación en el mes
--   mes_completo      falso para el mes en curso, que todavía no cerró
-- Los días con retorno atípico no suman flujo: un valor cuota mal digitado
-- inventaría una suscripción o un rescate que no existió.
-- Las sumas se redondean a la diezmilésima de sol: DuckDB suma en paralelo,
-- el orden cambia de una corrida a otra y sin redondeo la sexta decimal
-- variaba, con lo que dos corridas iguales daban archivos distintos.
WITH m AS (
    SELECT *, CAST(date_trunc('month', fecha) AS DATE) AS mes,
           row_number() OVER (PARTITION BY fondo_id, date_trunc('month', fecha)
                              ORDER BY fecha DESC) AS desde_el_final
    FROM fondos
    WHERE es_fondo_mutuo
),
por_fondo AS (
    SELECT mes, fondo_id,
           any_value(administradora) AS administradora, any_value(familia) AS familia,
           any_value(moneda) AS moneda,
           max(patrimonio_soles) FILTER (WHERE desde_el_final = 1) AS patrimonio_soles,
           max(n_participes)     FILTER (WHERE desde_el_final = 1) AS participes,
           round(sum(flujo_soles) FILTER (WHERE NOT retorno_atipico), 4) AS flujo_soles
    FROM m
    GROUP BY mes, fondo_id
)
SELECT mes, administradora, coalesce(familia, 'Sin tipo') AS familia, moneda,
       count(*)              AS fondos,
       round(sum(patrimonio_soles), 4) AS patrimonio_soles,
       sum(participes)                 AS participes,
       round(sum(flujo_soles), 4)      AS flujo_soles,
       mes < (SELECT CAST(date_trunc('month', max(fecha)) AS DATE) FROM fondos) AS mes_completo
FROM por_fondo
GROUP BY ALL
ORDER BY mes, administradora, familia, moneda
"""

SQL_ORO_CATALOGO = """
-- Una fila por fondo: sus atributos y su último dato. `vigente` es verdadero si
-- reportó en los últimos 7 días del panel; los fondos que cerraron siguen aquí,
-- que es lo que permite estudiar el sesgo de supervivencia.
WITH ultimo AS (
    SELECT fondo_id, valor_cuota, patrimonio_soles, n_participes,
           row_number() OVER (PARTITION BY fondo_id ORDER BY fecha DESC) AS n
    FROM fondos
)
SELECT f.fondo_id, f.fondo, a.empresa AS administradora, a.saf_retirada, f.moneda,
       f.tipo, f.familia, f.tipo_fuente, f.tipo_confianza, f.es_fondo_mutuo,
       f.fecha_inicio_operaciones, f.fecha_operativa, f.primera_fecha, f.ultima_fecha,
       f.n_observaciones, f.tiene_valor_cuota,
       f.ultima_fecha >= (SELECT max(fecha) FROM fondos) - INTERVAL 7 DAY AS vigente,
       u.valor_cuota      AS ultimo_valor_cuota,
       u.patrimonio_soles AS ultimo_patrimonio_soles,
       u.n_participes     AS ultimos_participes
FROM dim_fondo f
JOIN dim_administradora a USING (empresa_id)
JOIN ultimo u ON u.fondo_id = f.fondo_id AND u.n = 1
ORDER BY administradora, fondo, fecha_inicio_operaciones
"""


def fecha_descarga() -> str:
    """Cuándo se bajó el último bloque de la SMV, para la atribución.

    Es la fecha de modificación del CSV más reciente de bronce: la de la última
    descarga real. No cambia al reconstruir, así que oro sigue siendo idempotente.
    """
    puntos = list((BRONCE / "puntos").glob("*/*.csv"))
    origen = max(puntos, key=lambda p: p.stat().st_mtime) if puntos else BRONCE / "panel.parquet"
    return time.strftime("%d/%m/%Y", time.localtime(origen.stat().st_mtime))


def escribir(con: duckdb.DuckDBPyConnection, nombre: str, consulta: str,
             carpeta: Path | None = None, metadatos: dict[str, str] | None = None) -> int:
    """Materializa la consulta como tabla en memoria y la escribe en Parquet zstd.

    Se escribe primero a un archivo temporal y luego se renombra: si algo falla
    a mitad de camino, la versión anterior sigue entera. `metadatos` se guarda
    dentro del Parquet (clave: valor) y viaja con el archivo a donde vaya.
    """
    con.sql(f"CREATE OR REPLACE TABLE {nombre} AS {consulta}")
    destino = (carpeta or PLATA) / f"{nombre}.parquet"
    temporal = destino.with_suffix(".parquet.tmp")
    opciones = "FORMAT parquet, COMPRESSION zstd"
    if metadatos:
        pares = ", ".join(f"{k}: '{v.replace(chr(39), chr(39) * 2)}'" for k, v in metadatos.items())
        opciones += f", KV_METADATA {{{pares}}}"
    con.sql(f"COPY {nombre} TO '{temporal.as_posix()}' ({opciones})")
    temporal.replace(destino)
    return con.sql(f"SELECT count(*) FROM {nombre}").fetchall()[0][0]


# ---------------------------------------------------------------------------
# Controles: si alguno falla, plata no es confiable y la corrida termina en error
# ---------------------------------------------------------------------------
# Cada control es una consulta que debe devolver 0. Si alguno no da 0, la
# corrida termina con error en vez de dejar pasar una tabla rota.
CONTROLES = {
    "el hecho tiene las mismas filas que el panel (no se perdió ni se duplicó nada)":
        "SELECT abs((SELECT count(*) FROM panel) - (SELECT count(*) FROM hecho_valor_cuota))",
    "no hay dos filas del mismo fondo el mismo día":
        "SELECT count(*) FROM (SELECT fondo_id, fecha FROM hecho_valor_cuota "
        "GROUP BY ALL HAVING count(*) > 1)",
    "cada fondo aparece una sola vez en dim_fondo":
        "SELECT count(*) - count(DISTINCT fondo_id) FROM dim_fondo",
    "cada administradora aparece una sola vez en dim_administradora":
        "SELECT count(*) - count(DISTINCT empresa_id) FROM dim_administradora",
    "todo fondo del hecho existe en dim_fondo":
        "SELECT count(*) FROM hecho_valor_cuota h ANTI JOIN dim_fondo f USING (fondo_id)",
    "toda administradora de dim_fondo existe en dim_administradora":
        "SELECT count(*) FROM dim_fondo f ANTI JOIN dim_administradora a USING (empresa_id)",
    "toda fecha del hecho existe en dim_tipo_cambio":
        "SELECT count(*) FROM hecho_valor_cuota h ANTI JOIN dim_tipo_cambio t USING (fecha)",
    "el calendario de tipo de cambio no tiene días repetidos":
        "SELECT count(*) - count(DISTINCT fecha) FROM dim_tipo_cambio",
    "ningún valor cuota guardado es 0 o negativo":
        "SELECT count(*) FROM hecho_valor_cuota WHERE valor_cuota <= 0",
    "oro: fondos tiene una fila por cada fila del hecho":
        "SELECT abs((SELECT count(*) FROM fondos) - (SELECT count(*) FROM hecho_valor_cuota))",
    "oro: el catálogo tiene un fondo por cada fondo de dim_fondo":
        "SELECT abs((SELECT count(*) FROM fondos_catalogo) - (SELECT count(*) FROM dim_fondo))",
    "oro: el patrimonio del último mes cuadra entre fondos y mercado_mensual":
        """SELECT CAST(abs(
             (SELECT sum(patrimonio_soles) FROM mercado_mensual
               WHERE mes = (SELECT max(mes) FROM mercado_mensual))
           - (SELECT sum(ultimo_patrimonio_soles) FROM fondos_catalogo
               WHERE es_fondo_mutuo AND ultima_fecha >= (SELECT max(mes) FROM mercado_mensual)
                 AND ultimo_patrimonio_soles IS NOT NULL)) > 1 AS INT)""",
    "oro: todo fondo en dólares con tipo de cambio tiene patrimonio en su moneda":
        "SELECT count(*) FROM fondos WHERE moneda = 'DOLARES' AND patrimonio_soles IS NOT NULL "
        "AND tipo_cambio IS NOT NULL AND patrimonio_moneda IS NULL",
}


def controlar(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Devuelve la lista de controles que fallaron (vacía si todo está bien)."""
    fallos = []
    for descripcion, consulta in CONTROLES.items():
        n = con.sql(consulta).fetchall()[0][0]
        if n:
            fallos.append(f"{descripcion}: {n:,}")
    return fallos


def resumir(con: duckdb.DuckDBPyConnection) -> None:
    r = con.sql("""
        SELECT count(*)                                           AS series,
               count(*) FILTER (WHERE tipo IS NOT NULL)           AS con_tipo,
               count(*) FILTER (WHERE fecha_inicio_operaciones IS NULL) AS sin_inicio,
               count(*) FILTER (WHERE fecha_operativa IS NULL)    AS nunca_operaron,
               count(*) FILTER (WHERE NOT tiene_valor_cuota)      AS sin_valor_cuota,
               count(*) FILTER (WHERE NOT es_fondo_mutuo)         AS fondos_de_inversion
        FROM dim_fondo""").fetchall()[0]
    log.info("  dim_fondo: %d series · %d con tipo · %d sin fecha de inicio · "
             "%d nunca operaron · %d sin valor cuota · %d fondos de inversión", *r)
    fuentes = con.sql("""SELECT coalesce(tipo_fuente, 'sin tipo'), count(*) FROM dim_fondo
                         GROUP BY 1 ORDER BY 2 DESC""").fetchall()
    log.info("  tipo por fuente: %s", " · ".join(f"{f} {n}" for f, n in fuentes))
    m = con.sql("""
        SELECT count(*) FILTER (WHERE vc_invalido), count(*) FILTER (WHERE patrimonio_negativo),
               count(*) FILTER (WHERE tc_dudoso), count(*) FILTER (WHERE valor_cuota IS NULL)
        FROM hecho_valor_cuota""").fetchall()[0]
    log.info("  marcas de calidad: %s vc inválido · %s patrimonio negativo · %s tc dudoso · "
             "%s sin valor cuota", *(f"{x:,}" for x in m))


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
    t0 = time.time()
    panel = BRONCE / "panel.parquet"
    for falta in (panel, BRONCE / "bcrp" / "tipo_cambio.parquet"):
        if not falta.exists():
            log.error("Falta %s. Corre antes: uv run actualizar.py", falta)
            return 1
    PLATA.mkdir(parents=True, exist_ok=True)

    log.info("Plata desde %s", BRONCE)
    con = duckdb.connect()   # en memoria: plata se escribe a Parquet, no a una base
    con.sql(f"CREATE VIEW panel AS SELECT * FROM read_parquet('{panel.as_posix()}')")

    # Insumos que se preparan en pandas y se le pasan a DuckDB como tablas.
    tipos = tipos_por_fondo(con)
    rango = con.sql("SELECT min(fecha), max(fecha) FROM panel").fetchall()[0]
    tc = calendario_tipo_cambio(pd.Timestamp(rango[0]), pd.Timestamp(rango[1]))
    con.register("tipos", tipos)   # register: un DataFrame se vuelve consultable como tabla
    con.register("tc_df", tc)
    con.sql("CREATE TABLE tc AS SELECT * FROM tc_df")
    correspondencia = pd.DataFrame(
        [(crudo, t, f) for crudo, (t, f) in TIPOS.items()],
        columns=["tipo_smv", "tipo", "familia"])
    con.register("correspondencia_df", correspondencia)

    # El orden importa: el hecho usa tc, y los controles usan todo.
    filas = {
        "dim_administradora": escribir(con, "dim_administradora", SQL_DIM_ADMINISTRADORA),
        "dim_fondo": escribir(con, "dim_fondo", SQL_DIM_FONDO),
        "dim_tipo_cambio": escribir(con, "dim_tipo_cambio", "SELECT * FROM tc ORDER BY fecha"),
        "hecho_valor_cuota": escribir(con, "hecho_valor_cuota", SQL_HECHO),
        "correspondencia_tipo": escribir(con, "correspondencia_tipo",
                                         "SELECT * FROM correspondencia_df ORDER BY tipo_smv"),
    }
    for nombre, n in filas.items():
        mb = (PLATA / f"{nombre}.parquet").stat().st_size / 1e6
        log.info("  %-22s %10s filas  %6.2f MB", nombre, f"{n:,}", mb)
    resumir(con)

    # Oro, desde las tablas de plata que ya están en memoria.
    ORO.mkdir(parents=True, exist_ok=True)
    datos_hasta = con.sql("SELECT max(fecha) FROM hecho_valor_cuota").fetchall()[0][0]
    meta = {"fuente": ATRIBUCION_SMV.format(fecha=fecha_descarga()),
            "tipo_cambio": ATRIBUCION_BCRP,
            "datos_hasta": f"{datos_hasta:%d/%m/%Y}",
            "garantia": "Sin garantía. No es una recomendación de inversión."}
    log.info("Oro desde plata")
    oro = {
        "fondos": escribir(con, "fondos", SQL_ORO_FONDOS, ORO, meta),
        "mercado_mensual": escribir(con, "mercado_mensual", SQL_ORO_MERCADO_MENSUAL, ORO, meta),
        "fondos_catalogo": escribir(con, "fondos_catalogo", SQL_ORO_CATALOGO, ORO, meta),
    }
    for nombre, n in oro.items():
        mb = (ORO / f"{nombre}.parquet").stat().st_size / 1e6
        log.info("  %-22s %10s filas  %6.2f MB", nombre, f"{n:,}", mb)
    a = con.sql("""SELECT count(*) FILTER (WHERE retorno_atipico), count(retorno)
                   FROM fondos""").fetchall()[0]
    log.info("  retornos atípicos (|r| > %.0f%%): %s de %s", RETORNO_ATIPICO * 100,
             f"{a[0]:,}", f"{a[1]:,}")

    fallos = controlar(con)
    if fallos:
        for f in fallos:
            log.error("  CONTROL FALLIDO · %s", f)
        log.error("Plata y oro quedaron escritas pero NO son confiables. Revisa antes de usarlas.")
        return 2
    log.info("  %d controles en orden", len(CONTROLES))
    log.info("LISTO en %.1f s  ->  %s y %s", time.time() - t0, PLATA, ORO)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
