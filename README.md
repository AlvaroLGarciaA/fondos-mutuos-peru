# Fondos mutuos del Perú

La limpieza y el modelado de los datos diarios de los fondos mutuos que publica la
Superintendencia del Mercado de Valores (SMV): cómo se pasa del dato crudo, fondo por fondo y
día por día, a las tablas que se consultan en la web.

- Terminal SQL en el navegador: https://alvarolgarciaa.github.io/datos/fondos/
- Dashboard: https://alvarolgarciaa.github.io/datos/fondos/dashboard/

## Qué hay en este repositorio

| archivo | qué hace |
|---|---|
| `construir.py` | de bronce (lo descargado) a plata (un esquema estrella) y a oro (las tablas que se consultan), con 13 controles que detienen la corrida si algo no cuadra |
| `motor/nombres.py` | la clave de nombre con la que se cruza cada fondo con el catálogo de tipos de la SMV |
| `pruebas/probar_plata.py` | arma un bronce inventado de tres fondos, corre `construir.py` y comprueba cada decisión |
| `exploracion/f2_diseno.py` | las decisiones de diseño de plata, cada una con la consulta que la midió |

No están los datos (se consultan en la terminal), el descargador de la SMV y del BCRP, ni el
paso que arma los archivos que se publican.

## Cómo probarlo

Con [uv](https://docs.astral.sh/uv/):

```
uv run pruebas/probar_plata.py
```

No necesita red ni datos reales. Comprueba, entre otras cosas, que ninguna fila se pierde, que
dos fondos homónimos tienen ids distintos, que el sábado toma el tipo de cambio del viernes,
que el flujo no se mueve cuando solo sube el dólar y que dos corridas dan archivos idénticos
byte a byte.

Con un bronce propio en `datos/bronce/` (`panel.parquet`, `catalogo/catalogo_*.csv` y
`bcrp/tipo_cambio.parquet`, con las columnas que arma la prueba), `uv run construir.py`
escribe `datos/plata/` y `datos/oro/` en unos segundos.

## Las decisiones que importan

- **El patrimonio ya viene en soles.** La SMV publica en soles el patrimonio de todos los
  fondos, también el de los fondos en dólares. Convertirlo otra vez con el tipo de cambio
  infla el mercado casi tres veces. `patrimonio_moneda` se calcula dividiendo entre el tipo de
  cambio.
- **Tipo de cambio.** Se guarda el que reporta cada administradora. Si en un fondo en
  dólares se aleja más de 1 % del promedio interbancario del BCRP, se usa el del BCRP; el fin de
  semana toma el del último día hábil.
- **Nada se borra.** Un valor cuota en cero pasa a vacío y queda marcado; un retorno diario
  mayor a 20 % se marca como atípico. Quien analiza decide qué excluir.
- **El id del fondo** es un md5 de administradora, nombre, moneda y fecha de inicio: los
  mismos datos dan siempre el mismo id y un fondo nuevo no mueve los demás.
- **Idempotencia.** Correr `construir.py` dos veces da los mismos archivos. Las sumas se
  redondean porque DuckDB suma en paralelo y el orden cambia los últimos decimales.

## Licencias y cita

- El código de este repositorio: MIT (`LICENSE`).
- Los datos de la SMV son información pública. La compilación y las variables calculadas
  están bajo CC BY-NC 4.0: uso libre no comercial, citando la fuente.
- Sin afiliación con la SMV ni con el BCRP. Sin garantía; no es una recomendación de
  inversión.

Cita: García Arroyo, Á. L. (2026). Plataforma de datos de fondos mutuos del Perú [base de
datos]. Elaborada con datos de la SMV. https://alvarolgarciaa.github.io/datos/fondos/
