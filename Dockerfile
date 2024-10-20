# Use the base image for Apache Airflow
FROM apache/airflow:2.10.2

# Copy the requirements.txt file to the container
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt && pip check
