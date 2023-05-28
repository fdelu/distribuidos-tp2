FROM python:3.11.3-bullseye
SHELL ["/bin/bash", "-c"]

ENV PYTHONPATH=/app
COPY src/shared /app/shared
COPY src/client /app/client

RUN pip install -r /app/client/requirements.txt
WORKDIR /app
ENTRYPOINT python3 -m client.app