SHELL := /bin/bash
PWD := $(shell pwd)

GIT_REMOTE = github.com/CrossNox/7574-TP2
DOCKER_BIN=docker
DOCKER_COMPOSE_BIN=docker-compose

default: docker-image

docker-image:
	$(DOCKER_BIN) build -f ./docker/Dockerfile -t "7574-tp2:latest" .
.PHONY: docker-image

client:
	rm outputs/top_meme.img
	docker run --network testing_net -v /home/nox/repos/fiuba/7574-DistribuidosI/7574-TP2/outputs:/outputs -v /home/nox/repos/fiuba/7574-DistribuidosI/7574-TP2/notebooks/data/the-reddit-irl-dataset-posts-reduced.csv:/data/posts.csv -v /home/nox/repos/fiuba/7574-DistribuidosI/7574-TP2/notebooks/data/the-reddit-irl-dataset-comments-reduced.csv:/data/comments.csv --entrypoint poetry 7574-tp2:latest run rma_client -vv /data/posts.csv /data/comments.csv tcp://posts_source:9999 tcp://comments_source:9999 /outputs/top_meme.img tcp://sink_memes_url:9999 tcp://sink_mean_posts_score:9999 tcp://sink_download_meme:9999
.PHONY: client
