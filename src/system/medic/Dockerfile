FROM python:3.11.3-bullseye AS base
SHELL ["/bin/bash", "-c"]

RUN curl -fsSL https://get.docker.com | sh

# Copy client-shared & system-common modules
COPY shared /app/shared
COPY system/common /app/system/common

# Install common dependencies
COPY system/requirements.txt /app/system/requirements.txt
RUN pip install -r /app/system/requirements.txt

ENV PYTHONPATH=/app:/app/system

# Set up healtheck
ENV STATUS_FILE=/status.txt
HEALTHCHECK --interval=0.5s --timeout=3s --retries=50 CMD cat ${STATUS_FILE} | grep -q "HEALTHY" || exit 1

# Copy service modules and install dependencies, if any
ARG WORKDIR
ENV WORKDIR=${WORKDIR}
WORKDIR /app/system/${WORKDIR}
COPY system/${WORKDIR} .
RUN if test -e requirements.txt; then pip install -r requirements.txt; fi

WORKDIR /app
# replace / in WORKDIR with .
ENTRYPOINT python3 -m system.${WORKDIR//\//.}.app
# CMD ["/bin/bash"]