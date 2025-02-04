# Use the official Python 3.9 slim image as the base image
FROM python:3.9-slim

# Set environment variables to prevent Python from writing .pyc files and buffering stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory in the container to /app
WORKDIR /app

# Copy the requirements.txt file into the container at /app/
COPY requirements.txt /app/requirements.txt

# Upgrade pip and install any needed packages specified in requirements.txt
RUN pip install --upgrade pip && \
    pip install -r /app/requirements.txt

# Copy the rest of the logging service code into the container
COPY . /app/

# Expose a port if needed (not typically required for a logging library)
# EXPOSE 8080

# Define the default command to run the logger script.
# This command executes the logger module's test block.
CMD ["python", "src/logger.py"]
