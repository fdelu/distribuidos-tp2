# TP1 - Sistemas Distribuidos

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
