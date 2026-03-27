# 🎀 Matilda — Data Hub Corporativo

Matilda es un asistente analítico conversacional construido con **Streamlit** y **Google Gemini**. Permite explorar datos de comportamiento web (sesiones, páginas, engagement, abandono, patrones de conversión) a través de preguntas en lenguaje natural, respondiendo siempre con el formato Copilot:

> 📊 **El Dato** — cifras factuales extraídas de los datos reales  
> 💡 **Interpretación** — lectura ejecutiva y acción sugerida

---

## 📁 Estructura del proyecto

```
matilda/
├── app.py                  # Punto de entrada Streamlit (UI + chat)
├── src/
│   ├── agent.py            # Agente Gemini: cliente, configuración y llamada al LLM
│   └── data_engine.py      # Pipeline ETL + herramientas analíticas expuestas al LLM
├── data/
│   ├── raw/                # CSVs originales (1_Data_Recordings.csv, 2_Data_Metrics.csv)
│   └── processed/          # Parquets limpios generados por el pipeline (auto-generados)
├── requirements.txt        # Dependencias Python
├── .env.example            # Plantilla de variables de entorno
└── .gitignore
```

---

## ⚙️ Requisitos previos

| Requisito | Versión mínima |
|-----------|---------------|
| Python    | 3.10+         |
| pip       | Última estable |

Además necesitas:
- Una **API Key de Google Gemini** ([obtenerla aquí](https://aistudio.google.com/app/apikey))
- Los archivos de datos crudos en `data/raw/`:
  - `1_Data_Recordings.csv` — sesiones individuales de navegación
  - `2_Data_Metrics.csv` — métricas agregadas por página

---

## 🚀 Setup paso a paso

### 1. Clonar el repositorio

```bash
git clone [<url-del-repo>](https://github.com/quijotex/matilda.git)
cd matilda
```

### 2. Crear y activar el entorno virtual

```bash
# Windows
python -m venv .venv
.venv\Scripts\activate

# macOS / Linux
python -m venv .venv
source .venv/bin/activate
```

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 4. Configurar variables de entorno

Copia el archivo de ejemplo y rellena tus valores:

```bash
cp .env.example .env
```

Edita `.env`:

```env
GEMINI_API_KEY=tu_api_key_aqui
GEMINI_MODEL=gemini-2.5-flash
```

> **Nota:** Si `GEMINI_MODEL` se deja vacío, el agente usa `gemini-1.5-flash` por defecto y tiene fallback automático a `gemini-2.5-flash` y `gemini-2.5-pro`.

### 5. Colocar los datos crudos

Asegúrate de que los siguientes archivos existan antes de levantar la app:

```
data/raw/1_Data_Recordings.csv
data/raw/2_Data_Metrics.csv
```

### 6. Generar los datos procesados (primera vez)

El pipeline ETL convierte los CSVs a Parquet optimizado:

```bash
python src/data_engine.py
```

Esto crea automáticamente `data/processed/recordings_clean.parquet` y `data/processed/metrics_clean.parquet`.

> La app también puede ejecutar el pipeline internamente la primera vez que se realiza una consulta.

### 7. Levantar la aplicación

```bash
python -m streamlit run app.py
```

La interfaz estará disponible en [http://localhost:8501](http://localhost:8501).

---

## 🧠 Arquitectura
<img width="8192" height="3475" alt="Mermaid Chart - Create complex, visual diagrams with text -2026-03-27-181105" src="https://github.com/user-attachments/assets/5bfeb91c-4839-4b83-bc6c-34aec4f03668" />



### Flujo de una consulta

1. El usuario escribe una pregunta o selecciona una pregunta rápida según su rol (Marketing, Ventas, Management).
2. `app.py` llama a `consultar_matilda()` en `agent.py`.
3. Gemini recibe el prompt junto con las herramientas Python registradas vía **automatic function calling**.
4. Gemini invoca las herramientas necesarias; cada llamada se registra en `_TOOL_CALL_LOG`.
5. Gemini devuelve la respuesta en formato `📊 El Dato / 💡 Interpretación`.
6. `app.py` renderiza el mensaje, la gráfica asociada a la herramienta usada y el historial de chat.

---

## 🛠️ Herramientas analíticas disponibles

| Herramienta | Descripción |
|-------------|-------------|
| `obtener_paginas_top` | Ranking de páginas por vistas e interacciones |
| `calcular_tasas_abandono` | Páginas con mayor tasa de abandono rápido |
| `analizar_patrones_conversion` | Rutas y dispositivos que más llegan a pricing |
| `obtener_flujos_frecuentes` | Secuencias de navegación más repetidas |
| `obtener_interaccion_promedio` | Clics, scroll y tiempo promedio por página |
| `obtener_insight_frustracion` | Frustración del usuario cruzada con dispositivo |
| `obtener_insight_calidad_trafico` | Calidad de tráfico por canal de origen |
| `obtener_insight_anatomia_abandono` | Abandono segmentado por SO, dispositivo y hora |

---

## 🔑 Variables de entorno

| Variable | Requerida | Descripción |
|----------|-----------|-------------|
| `GEMINI_API_KEY` | ✅ Sí | API Key de Google AI Studio |
| `GEMINI_MODEL` | ❌ No | Modelo preferido (default: `gemini-1.5-flash`) |

---

## 📦 Dependencias principales

| Paquete | Uso |
|---------|-----|
| `streamlit` | Interfaz web |
| `google-genai` | Cliente de Google Gemini |
| `polars` | Procesamiento de datos de alta performance |
| `pandas` | Construcción de DataFrames para gráficas Streamlit |
| `python-dotenv` | Carga de variables de entorno desde `.env` |

---

## 🔒 Seguridad

- El archivo `.env` está en `.gitignore` y **nunca debe subirse al repositorio**.
- Usa `.env.example` como referencia para compartir la estructura sin exponer credenciales.
- Los datos en `data/raw/` también deberían excluirse del control de versiones si contienen información sensible.

---

## 🐛 Solución de problemas comunes

**`FileNotFoundError: No se encontro recordings_clean.parquet`**  
→ Ejecuta `python src/data_engine.py` para regenerar los parquets desde los CSVs.

**`ValueError: Define GEMINI_API_KEY en tu entorno o en el archivo .env`**  
→ Asegúrate de que `.env` existe y contiene una API key válida.

**`RuntimeError: Gemini no pudo responder con ninguno de los modelos configurados`**  
→ Verifica que tu API key tiene cuotas disponibles y que el modelo configurado existe.

**La app abre pero no muestra gráficas**  
→ Revisa que los parquets estén generados correctamente y que la consulta active una herramienta analítica conocida.
