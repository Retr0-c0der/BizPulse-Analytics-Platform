# website.Dockerfile

# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install the Python dependencies
# We add uvicorn here, which is needed to run the FastAPI app
RUN pip install --no-cache-dir -r requirements.txt && pip install "uvicorn[standard]"

# Copy the rest of the application's code into the container
# This includes main.py and the 'frontend' folder
COPY . .

# Expose the port the app runs on
EXPOSE 8000

# Command to run the uvicorn server
# --host 0.0.0.0 is crucial to make it accessible from outside the container
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]