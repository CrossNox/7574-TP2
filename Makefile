SHELL := /bin/bash
PWD := $(shell pwd)

GIT_REMOTE = github.com/CrossNox/7574-TP2
DOCKER_BIN=docker
DOCKER_COMPOSE_BIN=docker-compose

default: docker-image

docker-image:
	$(DOCKER_BIN) build -f ./docker/Dockerfile -t "7574-tp2:latest" .
.PHONY: docker-image


