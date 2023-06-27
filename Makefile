SHELL := /bin/bash

DATASET_FOLDER := src/client/data

DATASET_ID := 1uivCqQV2ZI0lTtkNGwBcZpNw2XMwYfAT
DATASET_FILE := ${DATASET_FOLDER}/dataset.zip
DATASET_HASH := 8b80b71965721392857021c74bba58c6

DATASET_MEDIUM_ID := 1gXIMubCTOPXkfBx73-u8lU8225sdheS0
DATASET_MEDIUM_FILE := ${DATASET_FOLDER}/dataset_medium.zip
DATASET_MEDIUM_HASH := 5b37db3d9c28f6203b74816b5e8c6a16

SED_EXPR := s/^.*<form[^>]\+\?action=\"\([^\"]\+\?\)\"[^>]*\?>.*\$$/\1/p

.PHONY:
.SILENT:

default: docker-compose-up

get-dataset:
	HASH=$$([ -f ${DATASET_FILE} ] && (md5sum ${DATASET_FILE} | head -c 32) || ""); \
	if [ "$${HASH}" == "${DATASET_HASH}" ]; then \
		echo "Dataset already downloaded"; \
	else \
		if [ -z "$${HASH}" ]; then \
			echo "Dataset is not present, downloading..."; \
		else \
			echo "Dataset hash does not match (actual: '$${HASH}', expected: '${DATASET_HASH}')"; \
			echo "Re-downloading dataset..."; \
		fi; \
		wget --load-cookies /tmp/cookies.txt "https://docs.google.com/uc?export=download&confirm=$$(wget --quiet --save-cookies /tmp/cookies.txt --keep-session-cookies --no-check-certificate 'https://docs.google.com/uc?export=download&id=${DATASET_ID}' -O- | sed -rn 's/.*confirm=([0-9A-Za-z_]+).*/\1\n/p')&id=${DATASET_ID}" -O ${DATASET_FILE} && rm -rf /tmp/cookies.txt; \
	fi;
# The wget downloads from Google Drive. Source: https://superuser.com/a/1542118

	unzip -n ${DATASET_FILE} -d ${DATASET_FOLDER}/full

get-dataset-medium:
	HASH=$$([ -f ${DATASET_MEDIUM_FILE} ] && (md5sum ${DATASET_MEDIUM_FILE} | head -c 32) || ""); \
	if [ "$${HASH}" == "${DATASET_MEDIUM_HASH}" ]; then \
		echo "Dataset already downloaded"; \
	else \
		if [ -z "$${HASH}" ]; then \
			echo "Dataset is not present, downloading..."; \
		else \
			echo "Dataset hash does not match (actual: '$${HASH}', expected: '${DATASET_MEDIUM_HASH}')"; \
			echo "Re-downloading dataset..."; \
		fi; \
		wget --load-cookies /tmp/cookies.txt "https://docs.google.com/uc?export=download&confirm=$$(wget --quiet --save-cookies /tmp/cookies.txt --keep-session-cookies --no-check-certificate 'https://docs.google.com/uc?export=download&id=${DATASET_MEDIUM_ID}' -O- | sed -rn 's/.*confirm=([0-9A-Za-z_]+).*/\1\n/p')&id=${DATASET_MEDIUM_ID}" -O ${DATASET_MEDIUM_FILE} && rm -rf /tmp/cookies.txt; \
	fi;
# The wget downloads from Google Drive. Source: https://superuser.com/a/1542118

	unzip -n ${DATASET_MEDIUM_FILE} -d ${DATASET_FOLDER}/medium

build:
	docker compose build

docker-compose-up: build
	docker compose up -d

docker-compose-stop:
	docker compose stop -t 3

docker-compose-down: docker-compose-stop
	docker compose down -v

docker-compose-logs:
	docker compose logs -f

debug: docker-compose-down docker-compose-up docker-compose-logs

docker_python: # Run python in a docker container with access to the host's docker socket
	docker run --name docker_python -dit -v /var/run/docker.sock:/var/run/docker.sock --net=host ubuntu bash
	docker exec -it docker_python bash -c "apt -y update && apt -y upgrade && apt install -y python3 && apt install -y python3-pip && pip install docker && python3"

docker_python_stop: # Stop the docker_python container
	docker stop docker_python
	docker rm docker_python
