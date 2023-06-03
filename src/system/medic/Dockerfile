FROM python:3.11.3-bullseye AS base
SHELL ["/bin/bash", "-c"]

# Copy client-shared & system-common modules
COPY shared /app/shared
COPY system/common /app/system/common

COPY system/compose-base-service.yaml /app/system/compose-base-service.yaml

# Install common dependencies
COPY system/requirements.txt /app/system/requirements.txt
RUN pip install -r /app/system/requirements.txt

RUN pip install python-on-whales
RUN python-on-whales download-cli

# install docker buildx, this step is optional
RUN mkdir -p ~/.docker/cli-plugins/
RUN wget https://github.com/docker/buildx/releases/download/v0.6.3/buildx-v0.6.3.linux-amd64 -O ~/.docker/cli-plugins/docker-buildx
RUN chmod a+x  ~/.docker/cli-plugins/docker-buildx

# install docker compose, this step is optional
RUN mkdir -p ~/.docker/cli-plugins/
RUN wget https://github.com/docker/compose/releases/download/v2.0.1/docker-compose-linux-x86_64 -O ~/.docker/cli-plugins/docker-compose
RUN chmod a+x  ~/.docker/cli-plugins/docker-compose

ENV PYTHONPATH=/app:/app/system

# Set up healtheck
ENV STATUS_FILE=/status.txt
HEALTHCHECK --interval=0.5s --timeout=3s --retries=50 CMD cat ${STATUS_FILE} | grep -q "OK" || exit 1

# Copy service modules and install dependencies, if any
ARG WORKDIR
ENV WORKDIR=${WORKDIR}
WORKDIR /app/system/${WORKDIR}
COPY system/${WORKDIR} .
RUN if test -e requirements.txt; then pip install -r requirements.txt; fi

WORKDIR /app
# replace / in WORKDIR with .
# ENTRYPOINT python3 -m system.${WORKDIR//\//.}.app
CMD ["/bin/bash"]