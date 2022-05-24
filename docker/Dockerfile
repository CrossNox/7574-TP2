FROM python:3.9.7-slim
COPY pyproject.toml /app/pyproject.toml
COPY poetry.lock /app/poetry.lock
COPY README.md /app/README.md
COPY rma /app/rma
WORKDIR /rma
RUN pip install poetry
RUN poetry install --no-dev
ENTRYPOINT ["poetry", "run", "rma"]
