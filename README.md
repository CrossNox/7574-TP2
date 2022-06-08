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

# Descarga del set de datos
El comando `rma_dataset` sirve para descargar la data. Se requiere tener un `json` de autenticación de la API de Kaggle. Para descargarlo, se deben seguir los pasos bajo la sección `Authentication` de [esta guía](https://www.kaggle.com/docs/api). También genera los datasets reducidos.

Su ejecución se puede simplificar con `make download-dataset`. Se espera tener el json de autenticación en la ubicación estándar (`~/.kaggle`). Se puede cambiar el tamaño del dataset reducido con `make download-dataset SAMPLE_SIZE=<f>`, reemplazando `<f>` por un valor en el rango `(0, 1)`.

# Renderizar `docker-compose.yaml`
```bash
poetry run rma render-dag docker informe/images/ 3
```

Ver el significado de cada parámetro con:
```bash
poetry run rma render-dag --help
```

Alternativamente,
```bash
make render-dag NWORKERS=<n>
```

Reemplazando `<n>` por la cantidad de workers deseada.

# Configuración
En el root del proyecto se provee un archivo `sample_settings.ini` con los posibles valores de configuración. Sin embargo, el archivo esperado se llama `settings.ini`. Por motivos obvios de seguridad, este archivo es ignorado en el sistema de versionado con `.gitignore`.

Puede copiar el archivo de prueba provisto, renombrarlo y modificar los valores según necesidad.

Cada posible configuración se puede sobreescribir con variables de entorno con la nomenclatura`<Seccion>_<Clave>`. Por ejemplo `SERVER_HOST`.

# Ejecución
Revisar los subcomandos disponibles y sus usos con:

```bash
poetry run rma --help
```

# Ejecución con Docker
Desde la carpeta `docker`, ejecutar:

```bash
docker-compose up
```

# Ejecutar un cliente
```bash
make client
```
