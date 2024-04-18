# Use a specific version of python base image
FROM python:3.9-slim-buster

# Set the working directory
WORKDIR /app

# Copy just the requirements.txt first to leverage Docker cache
COPY Backend/requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY Backend/ .

# Install db-dtypes specifically to check if it's installed properly (for debugging purposes)
RUN pip install db-dtypes

# Check if db_dtypes can be imported without any error
RUN python -c "import db_dtypes"

# Inform Docker that the container is listening on the specified port at runtime.
EXPOSE 8080

# Environment variable to specify the Flask app
ENV FLASK_APP=app.py

# Run the application
CMD ["flask", "run", "--host", "0.0.0.0", "--port", "8080"]
