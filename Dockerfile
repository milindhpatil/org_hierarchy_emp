# Use a Python base image for the data loader
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install neo4j Python driver
RUN pip install neo4j

# Copy the Python script (assumes employee_hierarchy_neo4j.py is in the same directory)
COPY employee_hierarchy_neo4j.py .

# Command to run the data loader
CMD ["python", "employee_hierarchy_neo4j.py"]