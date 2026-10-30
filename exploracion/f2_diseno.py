# %% [markdown]
# # F2 · Ejercicio 1: diseñar las tablas de plata
# Cada bloque que empieza con `# %%` es una celda: Shift+Enter la ejecuta y el
# resultado sale en la ventana interactiva de la derecha, como en un notebook.

# %%
from pathlib import Path

import duckdb

# El archivo vive en exploracion/, el panel en datos/: se sube un nivel.
PANEL = (Path(__file__).resolve().parent.parent / "datos" / "bronce" / "panel.parquet").as_posix() \
    if "__file__" in globals() else "../datos/bronce/panel.parquet"
con = duckdb.connect()
con.sql(f"CREATE VIEW p AS SELECT * FROM '{PANEL}'")
con.sql("DESCRIBE p").df()

# %% [markdown]
# ## Ejemplo del patrón: valores distintos y nulos de una columna

# %%
con.sql("SELECT count(DISTINCT moneda) AS distintos, count(*) - count(moneda) AS nulos FROM p").df()

# %% [markdown]
# ## Pregunta 1 · ¿A qué tabla va cada una de las 15 columnas?

# %%
cols = [c[0] for c in con.sql("DESCRIBE p").fetchall()]
print(cols)

FONDO = "empresa_id, fondo, moneda, fecha_inicio_operaciones"

partes = [f"""SELECT '{c}' AS columna,
  (SELECT max(n) FROM (SELECT count(DISTINCT "{c}") n FROM p GROUP BY {FONDO})) AS max_por_fondo,
  (SELECT max(n) FROM (SELECT count(DISTINCT "{c}") n FROM p GROUP BY empresa_id)) AS max_por_saf,
  (SELECT max(n) FROM (SELECT count(DISTINCT "{c}") n FROM p GROUP BY fecha)) AS max_por_dia"""
  for c in cols]

print(partes[0])
con.sql(" UNION ALL ".join(partes)).df()

# %% [markdown]
# **Respuesta 1**
# - dim_administradora: empresa_id (clave), empresa, saf_retirada
# - dim_fondo: fondo_id (clave, pregunta 4), empresa_id, fondo, moneda, fecha_inicio_operaciones, tipo (del catálogo)
# - hecho_valor_cuota: fondo_id, fecha, valor_cuota, patrimonio, n_participes, cuotas_en_circulacion
# - tipo_cambio: pendiente, pregunta 2
# - situacion: pendiente, pregunta 3
# - ninguna: estado y captura (son de nuestra descarga, no de la SMV)
# %% [markdown]
# ## Pregunta 2 · ¿Hay un solo tipo de cambio por fecha?

# %%
tc = con.sql("""SELECT count(DISTINCT tipo_cambio) AS dist_tc FROM p
  GROUP BY fecha, empresa_id, moneda
""").df()

tc["dist_tc"].value_counts().sort_index()

# %%
tc2 = con.sql("""
  SELECT empresa, moneda, tipo_cambio FROM p
  WHERE fecha = (SELECT max(fecha) FROM p)
  GROUP BY ALL
  ORDER BY tipo_cambio

""").df()

print(tc2)

# %% [markdown]
# **Respuesta 2**
# - No hay un solo tipo de cambio por día: hasta 5 valores distintos en una misma fecha.
# - Depende de la moneda y de la administradora: BBVA e Interfondos reportan el tipo de cambio
#   del mercado incluso en fondos en soles (127.002 filas); las demás reportan 1 en soles.
# - Hay errores: 247 filas en dólares con 1 y 178 filas con 0 (van a la pregunta 5).
# - Decisión: `tipo_cambio` va en hecho_valor_cuota como dato reportado por la administradora.
# - Decisión: `tipo_cambio` va en hecho_valor_cuota como dato reportado (auditoría).
#   dim_tipo_cambio: 4 series del BCRP por día. Para convertir a soles: factor 1 en soles;
#   en dólares, promedio interbancario del BCRP del mismo día. Reportado a >1 % del BCRP = dudoso.

# %% [markdown]
# ## Pregunta 3 · `situacion`: ¿describe al fondo o al día?

# %%
sit = con.sql("""
  SELECT count(DISTINCT situacion) AS dis_sit FROM p
  GROUP BY fecha, fondo
""").df()

sit["dis_sit"].value_counts().sort_index()

sit2= con.sql("""
   SELECT count(DISTINCT situacion) AS dis_sit FROM p
   GROUP BY empresa_id, fondo, moneda, fecha_inicio_operaciones
""").df()

sit2["dis_sit"].value_counts().sort_index()

sit3 = con.sql("""
WITH o AS (
    SELECT situacion,
      lag(situacion) OVER (PARTITION BY empresa_id,
fondo, moneda, fecha_inicio_operaciones
ORDER BY fecha) AS antes FROM p)

SELECT antes, situacion AS despues, count(*) AS cambios
FROM o
WHERE antes <> situacion
GROUP BY ALL
""").df()

print(sit3)

f = con.sql("""
SELECT empresa_id, fondo, moneda, fecha_inicio_operaciones,
       max(fecha) FILTER (WHERE situacion = 'Preoperativa') AS ultimo_pre,
       min(fecha) FILTER (WHERE situacion = 'Operativa')    AS primer_op
FROM p
GROUP BY empresa_id, fondo, moneda, fecha_inicio_operaciones
""").df()

(f["ultimo_pre"] > f["primer_op"]).sum()

# %% [markdown]
# **Respuesta 3**
# - La columna `situacion` varía dentro del fondo: 670 de 790 series pasan de Preoperativa a
#   Operativa. No es un atributo fijo.
# - Pero cambia una sola vez y nunca vuelve atrás (0 casos). 65 series fueron siempre Operativas
#   y 55 nunca llegaron a operar.
# - Decisión: no va en hechos. En dim_fondo se guarda `fecha_operativa` (el primer día Operativa;
#   vacía si nunca operó). La situación de cualquier día se deduce:
#   fecha < fecha_operativa -> Preoperativa; si no -> Operativa.

# %% [markdown]
# ## Pregunta 4 · ¿Cuál es el `fondo_id`? ¿Qué pasa con la fecha de inicio vacía?

# %%
fvac = con.sql ("""

SELECT DISTINCT fondo, empresa_id, moneda
FROM p
WHERE fecha_inicio_operaciones IS NULL

""").df()

print(fvac)

# %%
ids = con.sql("""
SELECT DISTINCT empresa_id, fondo, moneda, fecha_inicio_operaciones,
       md5(concat_ws('|', empresa_id, fondo, moneda,
           coalesce(CAST(fecha_inicio_operaciones AS VARCHAR), 'sin_inicio'))) AS fondo_id
FROM p
""").df()

print(len(ids), ids["fondo_id"].nunique())
# %% [markdown]
# **Respuesta 4**
# - 52 de 790 series no tienen fecha_inicio_operaciones: casi todas son fondos que vivieron poco
#   o que siguen en etapa preoperativa.
# - fondo_id = huella md5 de las 4 columnas de la clave, con la fecha vacía reemplazada por
#   'sin_inicio' (si no, la huella de esos 52 fondos sale vacía).
# - Por qué huella y no numeración: los mismos 4 valores dan siempre el mismo id, así que el proceso
#   es idempotente y un fondo nuevo no corre los ids de los demás.
# - Verificado: 790 series -> 790 ids distintos, sin choques.

# %%
ids = con.sql("""
SELECT DISTINCT empresa_id, fondo, moneda, fecha_inicio_operaciones,
       md5(concat_ws('|', empresa_id, fondo, moneda,
           coalesce(CAST(fecha_inicio_operaciones AS VARCHAR), 'sin_inicio'))) AS fondo_id
FROM p
""").df()

print(len(ids), ids["fondo_id"].nunique())   # deben salir 790 y 790

# %% [markdown]
# ## Pregunta 5 · Calidad: valor cuota ≤ 0, patrimonio < 0, valor cuota vacío

# %%
cal = con.sql("""

SELECT DISTINCT empresa_id, fondo, moneda
FROM p
WHERE valor_cuota <=0 OR patrimonio < 0 OR valor_cuota IS NULL;

""").df()
print(cal)

# %%
con.sql("""
SELECT count(*) FILTER (WHERE valor_cuota <= 0)    AS vc_cero_o_negativo,
       count(*) FILTER (WHERE patrimonio < 0)      AS patrimonio_negativo,
       count(*) FILTER (WHERE valor_cuota IS NULL) AS vc_vacio
FROM p
""").df()
# %% [markdown]
# **Respuesta 5**
# - Valor cuota <= 0: 572 filas. Patrimonio < 0: 168. Valor cuota vacío: 59.053.
#   En total, 59.783 filas con algún problema, en 202 series.
# - 22 series no tienen ningún valor cuota pero sí patrimonio y partícipes; 27 tienen vacíos parciales.
# - Decisión: en plata no se borra ninguna fila. Valor cuota <= 0 pasa a vacío; cada fila lleva
#   una marca de calidad; en dim_fondo, `tiene_valor_cuota` dice si la serie sirve para rentabilidad.
#   Tipo de cambio reportado a más de 1 % del BCRP también se marca (viene de la pregunta 2).


# %%
