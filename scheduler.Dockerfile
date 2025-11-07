# scheduler.Dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Copy all our python scripts into the container
COPY . .
CMD ["python", "scheduler.py"]