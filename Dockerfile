# Use a lightweight Python base image
FROM python:3.9-slim as builder

# Set a working directory
WORKDIR /app

# Install Poetry
RUN pip install poetry

# Copy only the files needed for the installation
COPY pyproject.toml poetry.lock* /app/

# Install dependencies in a virtual environment
RUN poetry config virtualenvs.create false \
    && poetry install --no-dev --no-interaction --no-ansi

# Final image to keep the size small
FROM python:3.9-slim

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages

# Set the working directory and copy your code
WORKDIR /app
COPY ./pyuap /app/pyuap
