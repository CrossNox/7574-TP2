FROM python:3.8-slim
RUN apt update
RUN apt install -y graphviz
RUN pip install poetry
COPY pyproject.toml /app/pyproject.toml
COPY poetry.lock /app/poetry.lock
COPY README.md /app/README.md
COPY rma /app/rma
WORKDIR /app
RUN poetry install --no-dev --no-root
RUN poetry install
ENTRYPOINT ["poetry", "run", "rma", "-vv"]
