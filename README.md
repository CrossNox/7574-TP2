# Reddit Memes Analyzer
TP2 para 7574 - Distribuidos I.

# Arquitectura
Referirse a `informe/informe.pdf`.

# Instalación
El proyecto fue empaquetado con [poetry](https://python-poetry.org/) para manejar dependencias cómodamente. Puede seguir la [guía de instalación](https://python-poetry.org/docs/#installation) para instalar la herramienta.

Teniendo `poetry` instalado, el siguiente comando creará un nuevo entorno para poder ejecutar el proyecto:

```bash
poetry install --no-dev
```

# Configuración
En el root del proyecto se provee un archivo `sample_settings.ini` con los posibles valores de configuración. Sin embargo, el archivo esperado se llama `settings.ini`. Por motivos obvios de seguridad, este archivo es ignorado en el sistema de versionado con `.gitignore`.

Puede copiar el archivo de prueba provisto, renombrarlo y modificar los valores según necesidad.

Cada posible configuración se puede sobreescribir con variables de entorno con la nomenclatura`<Seccion>_<Clave>`. Por ejemplo `SERVER_HOST`.

# Ejecución
## Server
Revisar el mensaje de ayuda del servidor:

```bash
poetry run rma --help
```

## Client
Revisar el mensaje de ayuda del cliente:

```bash
poetry run rma_client --help
```

# Ejecución con Docker
Desde la carpeta `docker`, ejecutar:

```bash
docker-compose up --build
```

## Ejecutar un cliente
```bash
docker run --network testing_net -v /home/nox/repos/fiuba/7574-DistribuidosI/7574-TP2/notebooks/data/the-reddit-irl-dataset-posts-reduced.csv:/data/posts.csv -v /home/nox/repos/fiuba/7574-DistribuidosI/7574-TP2/notebooks/data/the-reddit-irl-dataset-comments-reduced.csv:/data/comments.csv --entrypoint poetry 7574-tp2:latest run rma_client -vv /data/posts.csv /data/comments.csv tcp://posts_source:5555 tcp://comments_source:7777
```
