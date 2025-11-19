FROM python:3.11-slim

WORKDIR /app

# Устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем скрипт
COPY listen.py .

# Создаём директорию для медиафайлов
RUN mkdir -p downloaded_media

# Запускаем приложение
CMD ["python", "listen.py"]
