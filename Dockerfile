# Usa una imagen base de Python ligera
FROM python:3.9-slim-buster

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia el archivo de requisitos e instala las dependencias
# Esto asegura que las dependencias se instalen antes de copiar el código de la aplicación,
# lo que permite que Docker use el caché de capas si los requisitos no cambian.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el código de la aplicación al contenedor
COPY main.py .env asistentes-digitales-dev.json ./

# Define el comando que se ejecutará cuando se inicie el contenedor
# Para Cloud Run, el servidor web se inicia automáticamente si sigues el patrón de funciones HTTP.
# Aquí, simplemente ejecutamos el script main.py, que contiene la función 'main'
# que Cloud Run espera para manejar las solicitudes.
EXPOSE 8080
CMD ["gunicorn", "-b", "0.0.0.0:8080", "main:app"]