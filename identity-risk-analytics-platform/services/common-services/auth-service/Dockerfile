# Use the official Python 3.9 slim image as the base image
FROM python:3.9-slim

# Set environment variables to prevent Python from writing .pyc files and to disable output buffering
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory to /app in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt /app/requirements.txt

# Upgrade pip and install dependencies from requirements.txt
RUN pip install --upgrade pip && \
    pip install -r /app/requirements.txt

# Copy the entire source code directory into the container
COPY src /app/src

# Expose a port if this service will run an API server (optional)
# EXPOSE 8000

# Set the default command to run the auth module.
# When the container starts, it will run the auth.py script.
CMD ["python", "src/auth.py"]
