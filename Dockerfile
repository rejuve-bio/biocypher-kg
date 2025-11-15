FROM python:3.11-slim

RUN apt-get update && apt-get install -y nano && rm -rf /var/lib/apt/lists/*

WORKDIR /app 

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    python3-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*


RUN pip install --no-cache-dir poetry

COPY pyproject.toml poetry.lock* ./

RUN poetry install --no-root


COPY . .

RUN poetry install 

CMD ["poetry", "run", "python", "create_knowledge_graph.py", "--help"]