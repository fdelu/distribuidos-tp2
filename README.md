# TP2 - Sistemas Distribuidos

## Bike Rides Analyzer

Implementación de un sistema distribuido que calcula estadísticas sobre un dataset de viajes en bicicleta. Ver [este link](https://www.kaggle.com/code/pablodroca/bike-rides-analyzer) para una implementación centralizada.

## Comandos

- `make get-dataset`: Descarga el dataset completo y lo guarda en `src/client/data/full`.
- `make get-dataset-medium`: Descarga un dataset reducido al $10\%$ y lo guarda en `src/client/data/medium`.
- `make build`: Crea los contenedores de Docker.
- `make docker-compose-up`: Inicia el sistema y el cliente.
- `make docker-compose-stop`: Detiene el sistema.
- `make docker-compose-down`: Detiene el sistema y limpia los recursos creados.
- `make docker-compose-logs`: Muestra los logs del sistema.
- `make debug`: `make docker-compose-down` + `make docker-compose-up` + `make docker-compose-logs`.

## Configuración

La cantidad de nodos a utilizar para los servicios escalables se configura en el archivo `.env`. El resto de la configuración se encuentra en el archivo `config.ini`. La configuración tiene comentarios que explican cada parámetro.

## Desarrollo

### Configuración de VSCode

Si se utiliza Visual Studio Code para el desarrollo, se recomienda utilizar las siguientes extensiones:

- [Black Formatter](https://marketplace.visualstudio.com/items?itemName=ms-python.black-formatter): Formateador de código
- [Mypy](https://marketplace.visualstudio.com/items?itemName=matangover.mypy): Linter para tipado estático
- [Flake8](https://marketplace.visualstudio.com/items?itemName=ms-python.flake8): Linter para estilo de código
- [Python](https://marketplace.visualstudio.com/items?itemName=ms-python.python): IntelliSense & otras utilidades
- [Markdown PDF](https://marketplace.visualstudio.com/items?itemName=yzane.markdown-pdf): Generador de PDF a partir de Markdown, para el informe

Estas extensiones se pueden configurar en el archivo `.vscode/settings.json`. La configuración recomendada es la siguiente:

```json
{
  "python.linting.enabled": true,
  "python.linting.flake8Enabled": true,
  "python.linting.flake8Args": ["--config=.flake8"],
  "python.formatting.provider": "none",
  "python.formatting.blackArgs": ["--experimental-string-processing"],
  "mypy.configFile": "mypy.ini",
  "python.languageServer": "Pylance",
  "markdown-pdf.styles": ["informe/markdown.css"],
  "markdown-pdf.displayHeaderFooter": true,
  "markdown-pdf.headerTemplate": "<div></div>",
  "[python]": {
    "editor.defaultFormatter": "ms-python.black-formatter"
  }
}
```

### Autodestrucción

El código tiene registrados en el código varios puntos de control donde, con una cierta probabilidad, el programa se detiene repentinamente. Esto se hace para poder probar el comportamiento del sistema ante fallas durante el desarrollo. Para activar esta funcionalidad, se puede establecer la probabilidad con variables de entorno de la forma `SELF_DESTRUCT_{KEY}`, donde _KEY_ es una de las siguientes:

- `RECEIVED_MESSAGE`: Apenas se recibe un mensaje, en **CommsReceive**.
- `PRE_SAVE`: Justo antes de guardar el estado en **StatePersistor**
- `MID_SAVE`: Justo en el medio de guardar el estado en **StatePersistor**
- `POST_SAVE`: Justo después de guardar el estado en **StatePersistor**
- `PRE_SEND`: Justo antes de flushear los mensajes, en **ReliableComms** y en las comunicaciones del input.
- `POST_SEND`: Justo despues de flushear los mensajes, antes de poder guardarlos como enviados, en **ReliableComms** y en las comunicaciones del input.
- `PRE_ACK`: Justo antes de ackear un mensaje, en **DuplicateFilter**, **ReliableReceive** y **CommsReceive**.

Estas variables de entorno se pueden configurar en el archivo [`self_destruct_medics.env`](./self_destruct_medics.env) para los medics y en [`self_destruct.env`](./self_destruct.env) para el resto de la pipeline.

También esta disponible un script [`kill_random.sh`](./kill_random.sh) que recibe como parámetro un intervalo de tiempo $n$ y un número $k$ entre $0$ y $1$. Al ejecutarlo, cada $n \pm k \cdot n$ segundos se mata a un proceso aleatorio del sistema.
