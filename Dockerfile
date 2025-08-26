# Dockerfile (root)
FROM python:3.11-slim

# basic Python hygiene
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# workdir
WORKDIR /app

# install deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy all source (incl. pipeline/, app/)
COPY . /app

# make /app importable: 'from pipeline import ...' works
ENV PYTHONPATH=/app

# streamlit settings
EXPOSE 8080
ENV PORT=8080

# launch the dashboard
CMD ["streamlit", "run", "app/streamlit_app.py", "--server.port=8080", "--server.address=0.0.0.0"]
