# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies required for Spark
RUN apt-get update && apt-get install -y openjdk-11-jre-headless

# Copy the requirements file into the container
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Set the default command to run when the container starts
# This application is designed to run as a one-off task. For a long-running service,
# you would implement a more sophisticated health check.
HEALTHCHECK NONE

CMD ["python", "spark_storacha_pipeline.py"]
