# TP2 - Sistemas Distribuidos

## Bike Rides Analyzer

Implementación de un sistema distribuido que calcula estadísticas sobre un dataset de viajes en bicicleta. Ver [este link](https://www.kaggle.com/code/pablodroca/bike-rides-analyzer) para una implementación centralizada.

## Comandos

- `make get-dataset`: Descarga el dataset completo y lo guarda en `src/client/data/full`.
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
- [Mypy Type Checker](https://marketplace.visualstudio.com/items?itemName=ms-python.mypy-type-checker): Linter para tipado estático
- [Flake8](https://marketplace.visualstudio.com/items?itemName=ms-python.flake8): Linter para estilo de código
- [Python](https://marketplace.visualstudio.com/items?itemName=ms-python.python): IntelliSense & otras utilidades
- [Markdown PDF](https://marketplace.visualstudio.com/items?itemName=yzane.markdown-pdf): Generador de PDF a partir de Markdown, para el informe

Estas extensiones se pueden configurar en el archivo `.vscode/settings.json`. La configuración recomendada es la siguiente:

```json
{
  "python.linting.enabled": true,
  "python.linting.flake8Enabled": true,
  "python.linting.flake8Args": ["--config=.flake8"],
  "python.linting.mypyEnabled": true,
  "python.linting.mypyArgs": ["--config-file mypy.ini"],
  "python.formatting.provider": "none",
  "python.formatting.blackArgs": ["--experimental-string-processing"],
  "python.languageServer": "Pylance",
  "markdown-pdf.styles": ["informe/markdown.css"],
  "markdown-pdf.displayHeaderFooter": true,
  "markdown-pdf.headerTemplate": "<div></div>",
  "[python]": {
    "editor.defaultFormatter": "ms-python.black-formatter"
  }
}
```
