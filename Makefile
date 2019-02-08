IMAGE_NAME=videoamp/mediaocean-etl

build-docker:
	pipenv run pip wheel --find-links=dist --wheel-dir=dist .
	docker build -f Dockerfile -t ${IMAGE_NAME}:latest .

publish:
	docker push ${IMAGE_NAME}
