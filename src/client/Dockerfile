FROM python:3.11.3-bullseye
SHELL ["/bin/bash", "-c"]

ENV PYTHONPATH=/app
COPY src/shared /app/shared
COPY src/client /app/client
COPY src/client/data /tmp/data
#TODO: remove copies and add mount to medic
COPY config.ini /config.ini
COPY src/client/results /tmp/results

RUN pip install -r /app/client/requirements.txt
WORKDIR /app
ENTRYPOINT python3 -m client.app