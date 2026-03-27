# Usamos una versión ligera de Python
FROM python:3.10-slim

# Le decimos a la nube que trabaje en esta carpeta
WORKDIR /app

# Copiamos el archivo de dependencias primero
COPY requirements.txt .

# Instalamos las librerías
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos todo el resto de nuestro código y la carpeta /data/
COPY . .

# Exponemos el puerto que exige Google Cloud Run (8080 en lugar de 8501)
EXPOSE 8080

# El comando maestro (ajustado al puerto 8080)
CMD ["streamlit", "run", "app.py", "--server.port=8080", "--server.address=0.0.0.0"]