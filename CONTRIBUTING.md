# 🛠️ Guía técnica — Matilda Data Hub

Este documento es la referencia interna para desarrolladores que quieran **entender, mantener o extender** el proyecto. Cubre la funcionalidad de cada capa, las decisiones de diseño tomadas y los pasos concretos para agregar nuevas capacidades.

---

## 📌 ¿Qué hace Matilda?

Matilda es un **agente analítico conversacional** que responde preguntas de negocio en lenguaje natural usando datos reales de comportamiento web. El flujo completo es:

```
Pregunta del usuario
        │
        ▼
Gemini recibe el prompt + herramientas Python registradas
        │
        ▼
Gemini decide qué herramientas invocar (automatic function calling)
        │
        ▼
Las herramientas consultan parquets limpios con Polars
        │
        ▼
Gemini sintetiza los resultados en formato ejecutivo
        │
        ▼
Streamlit renderiza la respuesta + gráfica automática
```

El resultado siempre sigue el formato **Copilot**:
- `📊 El Dato` → hallazgo cuantitativo directo
- `💡 Interpretación` → conclusión ejecutiva + acción recomendada

---

## 📁 Anatomía de carpetas

### Raíz del proyecto

```
matilda/
├── app.py          ← Única entrypoint de Streamlit. Orquesta UI, historial y renderizado.
├── requirements.txt← Dependencias pip exactas para reproducibilidad.
├── .env            ← Variables secretas locales (nunca en git).
├── .env.example    ← Plantilla pública de variables de entorno.
├── .gitignore      ← Excluye .env, __pycache__, .venv, parquets si aplica.
├── README.md       ← Instrucciones de instalación y uso para usuarios finales.
└── CONTRIBUTING.md ← Este archivo: guía para desarrolladores.
```

### `src/`

Contiene **toda la lógica de negocio**. No hay UI aquí.

| Archivo | Responsabilidad |
|---------|-----------------|
| `agent.py` | Gestiona el ciclo de vida del cliente Gemini, el system prompt, el registro de tool calls y el fallback entre modelos. |
| `data_engine.py` | Pipeline ETL sobre los CSV crudos + las 8 herramientas analíticas que Gemini puede invocar. |

### `data/`

```
data/
├── raw/            ← CSVs originales. Fuente de verdad. No se modifican nunca.
│   ├── 1_Data_Recordings.csv   ← Sesiones individuales de navegación web
│   └── 2_Data_Metrics.csv      ← Métricas agregadas por página/evento
└── processed/      ← Parquets limpios generados por run_pipeline(). Auto-generados.
    ├── recordings_clean.parquet
    └── metrics_clean.parquet
```

> ⚠️ **Los archivos en `processed/` son artefactos derivados.** Nunca edites los parquets directamente. Si los datos crudos cambian, re-ejecuta el pipeline.

---

## 🔍 Funcionalidad capa por capa

### `data_engine.py` — El motor de datos

Este módulo tiene **dos responsabilidades** claramente separadas:

#### 1. Pipeline ETL (`run_pipeline`)

Transforma los CSVs crudos en parquets optimizados para consulta:

| Función | Qué hace |
|---------|----------|
| `read_csv_with_standardized_columns` | Lee CSV y normaliza nombres de columnas a `snake_case` |
| `clean_recordings` | Normaliza tipos, URLs, booleanos; ingenia columnas `ruta_flujo`, `llego_a_pricing`, `fecha_hora` |
| `clean_metrics` | Normaliza métricas de página; extrae rutas de URL |
| `run_pipeline` | Orquesta todo y exporta parquets; limpia el caché de Polars |

> El pipeline se activa ejecutando `python src/data_engine.py` o se llama internamente desde las herramientas si los parquets están desactualizados.

#### 2. Herramientas analíticas (expuestas a Gemini)

Cada función retorna un `dict` tipado (`JsonDict`) que Gemini puede leer e interpretar:

| Herramienta | Fuentes de datos | Qué responde |
|-------------|-----------------|--------------|
| `obtener_paginas_top(limite)` | metrics + recordings | Páginas más vistas con mayor interacción |
| `calcular_tasas_abandono()` | recordings | Tasa global de abandono y páginas críticas (mín. 25 sesiones) |
| `analizar_patrones_conversion()` | recordings | Rutas, dispositivos y canales que más llegan a pricing |
| `obtener_flujos_frecuentes()` | recordings | Secuencias entrada→salida más repetidas |
| `obtener_interaccion_promedio()` | recordings + metrics | Clics, scroll y tiempo promedio por página |
| `obtener_insight_frustracion()` | recordings + metrics | Frustración cruzada con tipo de dispositivo |
| `obtener_insight_calidad_trafico()` | recordings | Engagement y abandono por canal/referente |
| `obtener_insight_anatomia_abandono()` | recordings | Abandono por sistema operativo y rango de tiempo |

Todas las herramientas:
- Cargan los parquets con `lru_cache` (solo se leen del disco una vez por sesión).
- Filtran por volumen mínimo antes de devolver resultados (evita ruido estadístico).
- Retornan números redondeados a 2 decimales via `_safe_round`.

#### Lista registrada `ANALYTIC_TOOLS`

Al final del archivo existe esta lista:

```python
ANALYTIC_TOOLS: list[Callable[..., JsonDict]] = [
    obtener_paginas_top,
    calcular_tasas_abandono,
    ...
]
```

**Esta lista es la que Gemini ve.** Solo las funciones aquí incluidas pueden ser invocadas por el LLM.

---

### `agent.py` — El agente Gemini

| Componente | Descripción |
|-----------|-------------|
| `SYSTEM_PROMPT` | Instrucción de sistema que define el comportamiento, el tono y el formato de salida obligatorio de Matilda |
| `_candidate_models` | Construye la lista de modelos a intentar: preferido → fallbacks (`gemini-2.5-flash`, `gemini-2.5-pro`) |
| `TRACKED_ANALYTIC_TOOLS` | Envuelve cada herramienta en `_track_tool_call` para registrar qué llamó Gemini y con qué resultado |
| `consultar_matilda` | Función principal: itera sobre modelos candidatos, invoca Gemini con function calling y devuelve la respuesta + log de tools |
| `obtener_configuracion_modelo` | Helper para que la UI muestre qué modelo y cuántas herramientas están activas |

#### Fallback de modelos

Si `gemini-2.5-flash` falla (quota, error de API, respuesta vacía), el agente reintenta automáticamente con los siguientes modelos de la lista. Esto garantiza disponibilidad sin intervención manual.

---

### `app.py` — La interfaz Streamlit

| Función | Rol |
|---------|-----|
| `inject_styles` | Inyecta el design system completo (CSS custom, tipografía JetBrains Mono, paleta de colores) |
| `render_sidebar` | Selector de rol, preguntas rápidas por rol y panel de configuración del modelo |
| `render_header` | Hero card con nombre, badge y descripción del proyecto |
| `parse_copilot_output` | Parsea el texto de Gemini para separar `📊 El Dato` de `💡 Interpretación` via regex |
| `build_chart_payload` | Mapea la última herramienta invocada a un DataFrame para graficar |
| `render_chart` | Renderiza la gráfica correspondiente al tool call más reciente |
| `render_assistant_message` | Compone la card completa de respuesta (gráfica + dato + interpretación) |
| `process_prompt` | Agrega el mensaje al historial, llama al agente y renderiza la respuesta |
| `consume_pending_prompt` | Gestiona preguntas rápidas del sidebar que se inyectan como si el usuario las escribiera |

---

## ⚙️ Consideraciones de diseño

### Por qué Polars y no Pandas

Polars usa el modelo lazy/collect que permite construir el plan de ejecución antes de tocarlo. Para consultas que cruzan dos DataFrames (`join`, `group_by`), el rendimiento puede ser **10-50x mayor** que Pandas sobre los mismos datos. Los resultados se convierten a Pandas solo justo antes de renderizar gráficas en Streamlit (que requiere Pandas).

### Por qué Parquet y no consultas directas al CSV

Los CSV se leen una vez y se guardan como parquet comprimido. Esto significa:
- Tipado fuerte (columnas booleanas, fechas, floats) sin re-parseo.
- Lectura columnar: si una consulta necesita solo `ruta_entrada` y `abandono_rapido`, Polars solo lee esas columnas del disco.
- `lru_cache(maxsize=1)` garantiza que el parquet se cargue una sola vez por proceso Streamlit.

### Separación estricta de responsabilidades

```
app.py       → solo renderizado y UX
agent.py     → solo comunicación con Gemini
data_engine  → solo datos y lógica analítica
```

Ningún archivo de UI toca Polars directamente. Ningún archivo de datos conoce Streamlit. Esto facilita testear `data_engine.py` de forma aislada.

### El system prompt es la fuente de verdad del comportamiento

El tono, el formato de salida y las restricciones de Matilda están en `SYSTEM_PROMPT` dentro de `agent.py`. Cambiar el comportamiento del agente se hace ahí, **no en la UI**.

### Cómo se genera la gráfica automáticamente

`build_chart_payload` en `app.py` revisa cuál fue la **última herramienta** que Gemini invocó y construye un DataFrame para graficar. Si la herramienta no tiene un mapping definido, no se muestra gráfica (no da error). El mapeo es directo: `tool_name → columnas del resultado`.

---

## 🚀 Cómo agregar nuevas funcionalidades

### Caso 1: Agregar una nueva herramienta analítica

Esta es la extensión más común. Sigue estos cuatro pasos:

#### Paso 1 — Escribir la función en `data_engine.py`

```python
def mi_nueva_herramienta(parametro: int = 10) -> JsonDict:
    """Descripción clara de qué analiza esta herramienta.

    Args:
        parametro: Descripción del parámetro con rango válido.

    Returns:
        Diccionario con los resultados. Gemini leerá este dict
        para construir su respuesta.

    Raises:
        FileNotFoundError: Si los parquets aún no existen.
    """
    recordings = load_recordings()  # o load_metrics()

    resultado = (
        recordings.lazy()
        .group_by("columna_agrupacion")
        .agg([
            pl.len().alias("sesiones"),
            pl.col("mi_metrica").mean().alias("valor_promedio"),
        ])
        .sort("sesiones", descending=True)
        .limit(parametro)
        .collect()
    )

    return {
        "metrica": "mi_nueva_herramienta",   # clave descriptiva
        "resultados": _to_records(resultado), # siempre usar _to_records
    }
```

> **Reglas para que Gemini pueda invocarla:**
> - El docstring debe existir y ser claro (el SDK lo usa para describir la función al LLM).
> - Los parámetros deben tener type hints y valores por defecto.
> - El retorno debe ser `JsonDict` (= `dict[str, Any]`).

#### Paso 2 — Registrarla en `ANALYTIC_TOOLS`

Al final de `data_engine.py`:

```python
ANALYTIC_TOOLS: list[Callable[..., JsonDict]] = [
    obtener_paginas_top,
    calcular_tasas_abandono,
    ...
    mi_nueva_herramienta,  # ← agregar aquí
]
```

Con esto, Gemini ya puede invocarla automáticamente cuando la pregunta del usuario lo requiera.

#### Paso 3 — Agregar la gráfica automática en `app.py` (opcional)

En `build_chart_payload`, agrega un bloque nuevo:

```python
if tool_name == "mi_nueva_herramienta":
    records = result.get("resultados", [])
    if records:
        df = pd.DataFrame(records)[["columna_agrupacion", "valor_promedio"]].set_index("columna_agrupacion")
        return {"kind": "bar", "title": "Título descriptivo de la gráfica", "data": df}
```

#### Paso 4 — Reiniciar la app

```bash
# Ctrl+C para detener Streamlit, luego:
python -m streamlit run app.py
```

No es necesario regenerar el pipeline a menos que hayas cambiado los datos de entrada.

---

### Caso 2: Agregar un nuevo rol en el sidebar

Los roles y sus preguntas rápidas están en el diccionario `ROLE_QUICK_QUESTIONS` al inicio de `app.py`:

```python
ROLE_QUICK_QUESTIONS: dict[str, list[str]] = {
    "Marketing": [...],
    "Ventas": [...],
    "Management": [...],
    "NuevoRol": [                           # ← agregar aquí
        "¿Pregunta rápida 1 para este rol?",
        "¿Pregunta rápida 2 para este rol?",
        "¿Pregunta rápida 3 para este rol?",
    ],
}
```

No requiere cambios en ningún otro archivo. El sidebar se genera dinámicamente desde este dict.

---

### Caso 3: Cambiar el comportamiento o tono del agente

Edita `SYSTEM_PROMPT` en `agent.py`. Por ejemplo, para que Matilda responda en inglés:

```python
SYSTEM_PROMPT = """
You are Matilda, a conversational analytics engine for web navigation data.
...
"""
```

O para agregar una sección extra en sus respuestas:

```python
SYSTEM_PROMPT = """
...
- Todas tus respuestas deben usar exactamente esta estructura:
  📊 El Dato: ...
  💡 Interpretación: ...
  ✅ Próximo paso: una acción concreta de seguimiento en una sola línea.
"""
```

> Si cambias el formato del output, también debes actualizar `parse_copilot_output` en `app.py` para que el regex capture la nueva sección.

---

### Caso 4: Agregar una nueva fuente de datos

Si recibes un nuevo CSV, el flujo es:

1. **Colocar el archivo** en `data/raw/`.
2. **Agregar sus rutas** en `data_engine.py`:
   ```python
   NUEVO_CSV_PATH = RAW_DIR / "3_NuevoDato.csv"
   NUEVO_OUTPUT   = PROCESSED_DIR / "nuevo_dato_clean.parquet"
   ```
3. **Escribir la función de limpieza** (`clean_nuevo_dato`) siguiendo el patrón de `clean_recordings` o `clean_metrics`.
4. **Agregar al pipeline** en `run_pipeline()`:
   ```python
   nuevo_df = read_csv_with_standardized_columns(NUEVO_CSV_PATH)
   nuevo_clean = clean_nuevo_dato(nuevo_df)
   export_parquet(nuevo_clean, NUEVO_OUTPUT)
   ```
5. **Crear la función de carga** con `lru_cache`:
   ```python
   @lru_cache(maxsize=1)
   def load_nuevo_dato() -> pl.DataFrame:
       _ensure_parquet_exists(NUEVO_OUTPUT)
       return pl.read_parquet(NUEVO_OUTPUT)
   ```
6. **Actualizar `clear_caches`**:
   ```python
   def clear_caches() -> None:
       load_recordings.cache_clear()
       load_metrics.cache_clear()
       load_nuevo_dato.cache_clear()  # ← agregar
   ```
7. **Usar el nuevo dataframe** dentro de las herramientas analíticas existentes o en una nueva.

---

### Caso 5: Cambiar el modelo de Gemini

Cambia la variable de entorno en `.env`:

```env
GEMINI_MODEL=gemini-2.5-pro
```

O pasa el modelo directamente al llamar a `consultar_matilda`:

```python
consultar_matilda(prompt, model_name="gemini-2.5-pro")
```

La lista de fallbacks está hardcodeada en `agent.py`:

```python
FALLBACK_MODEL_NAMES = ("gemini-2.5-flash", "gemini-2.5-pro")
```

Actualiza esa tupla si quieres cambiar la cadena de fallback.

---

## ⚠️ Consideraciones importantes

### Sobre los datos

- **Nunca modificar los CSV en `data/raw/`** — son la fuente de verdad. Si hay correcciones, hacerlas upstream y re-correr el pipeline.
- Si el esquema del CSV cambia (nuevas columnas, renombre), actualizar `clean_recordings` o `clean_metrics` y **re-correr el pipeline antes de levantar la app**.
- Los parquets en `data/processed/` pueden borrarse sin riesgo: se regeneran con `python src/data_engine.py`.

### Sobre Gemini y las herramientas

- Gemini decide **por sí solo** qué herramientas invocar y en qué orden. No hay lógica de routing manual.
- Si una herramienta retorna un dict demasiado grande, Gemini puede truncar o ignorar parte de los datos. Mantén el resultado de cada herramienta **conciso y focalizado** (máx. 15-20 registros cuando sea posible).
- Los docstrings de las herramientas son parte del contrato con el LLM: si son vagos o incorrectos, Gemini puede invocarlas en los momentos equivocados.

### Sobre el caché

- `lru_cache` en `load_recordings` y `load_metrics` significa que los datos se leen **una vez por proceso** de Streamlit. Si regenras los parquets con la app corriendo, debes reiniciar el servidor para que tome los datos nuevos.
- `clear_caches()` se llama automáticamente al final de `run_pipeline()`, pero **no limpia el proceso de Streamlit** porque son procesos distintos.

### Sobre Streamlit

- Streamlit re-ejecuta el script completo en cada interacción del usuario. El estado persistente vive en `st.session_state`.
- El historial de mensajes del chat se guarda en `st.session_state.messages`. Este estado **se pierde al reiniciar el servidor**.
- Para persistir el historial entre sesiones, habría que serializar `st.session_state.messages` a disco o base de datos — lo que actualmente no está implementado.

---

## 🧪 Verificación de cambios

Antes de hacer merge de cualquier cambio, verifica lo siguiente manualmente:

### Pipeline ETL

```bash
python src/data_engine.py
```
Debe imprimir las filas procesadas sin errores:
```
Recordings procesado: X,XXX filas -> data/processed/recordings_clean.parquet
Metrics procesado: X,XXX filas -> data/processed/metrics_clean.parquet
```

### App funcional

```bash
python -m streamlit run app.py
```

Prueba al menos:
- [ ] Una pregunta rápida por cada rol (Marketing, Ventas, Management)
- [ ] Una pregunta libre que active cada herramienta nueva que agregaste
- [ ] Que la gráfica aparezca correctamente si agregaste el mapping en `build_chart_payload`
- [ ] Que los errores del agente se muestren en la UI sin crashear la app

### Importaciones

```bash
python -c "from src.agent import consultar_matilda, obtener_configuracion_modelo; print('OK')"
python -c "from src.data_engine import ANALYTIC_TOOLS; print(f'{len(ANALYTIC_TOOLS)} herramientas OK')"
```

---

## 📋 Checklist para agregar una nueva herramienta

```
[ ] Función escrita en data_engine.py con docstring, type hints y retorno JsonDict
[ ] Función registrada en ANALYTIC_TOOLS
[ ] Probada en aislamiento: python -c "from src.data_engine import mi_fn; print(mi_fn())"
[ ] Mapping de gráfica agregado en build_chart_payload (si aplica)
[ ] Probada desde la UI con una pregunta que la active
[ ] Documentada en CONTRIBUTING.md (esta tabla de herramientas)
```
