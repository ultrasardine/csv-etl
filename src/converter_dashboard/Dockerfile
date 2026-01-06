FROM python:3.11-slim

WORKDIR /app

# Install uv for faster package management
RUN pip install uv

# Copy source code
COPY src/ /app/src/
COPY pyproject.toml /app/

# Install dependencies
RUN uv pip install --system flask

# Expose port
EXPOSE 5000

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV FLASK_DEBUG=0
ENV PYTHONPATH=/app/src
ENV DATA_DIR=/app/data

# Run the dashboard
CMD ["python", "-m", "converter_dashboard.run"]
