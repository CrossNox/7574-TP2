SHELL := /bin/bash
PWD := $(shell pwd)

GIT_REMOTE = github.com/CrossNox/7574-TP2
DOCKER_BIN=docker
DOCKER_COMPOSE_BIN=docker-compose

NWORKERS := 3

default: docker-image

docker-image:
	$(DOCKER_BIN) build -f ./docker/Dockerfile -t "7574-tp2:latest" .
.PHONY: docker-image

client:
	rm -f outputs/top_meme.img
	$(DOCKER_BIN) run --network testing_net -v $(CURDIR)/outputs:/outputs -v $(CURDIR)/notebooks/data/the-reddit-irl-dataset-posts-reduced.csv:/data/posts.csv -v $(CURDIR)/notebooks/data/the-reddit-irl-dataset-comments-reduced.csv:/data/comments.csv --entrypoint poetry 7574-tp2:latest run rma_client -vv /data/posts.csv /data/comments.csv tcp://posts_source:9999 tcp://comments_source:9999 /outputs/top_meme.img tcp://sink_memes_url:9999 tcp://sink_mean_posts_score:9999 tcp://sink_download_meme:9999
.PHONY: client

dag:
	$(DOCKER_BIN) run -v $(CURDIR)/docker:/app/docker -v $(CURDIR)/informe/images:/app/images --entrypoint poetry 7574-tp2:latest run rma render-dag /app/docker /app/images/ $(NWORKERS)

pyreverse:
	poetry run pyreverse rma --output=png --output-directory=informe/images/ --colorized --ignore=cli,client
	poetry run pyreverse rma.dag --output=png --output-directory=informe/images/dag/ --colorized
	poetry run pyreverse rma.tasks --output=png --output-directory=informe/images/tasks/ --colorized
