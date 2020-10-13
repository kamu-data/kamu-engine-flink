FLINK_BASE_IMAGE = flink:1.12-SHAPSHOT-scala_2.12
ENGINE_VERSION = 0.6.1
IMAGE_REPO = kamudata
ENGINE_IMAGE = $(IMAGE_REPO)/engine-flink:$(ENGINE_VERSION)


.PHONY: image
image:
	docker build \
		--build-arg BASE_IMAGE=$(FLINK_BASE_IMAGE) \
		-t $(ENGINE_IMAGE) \
		-f image/Dockerfile \
		.


.PHONY: image-push
image-push:
	docker push $(ENGINE_IMAGE)


.PHONY: run
run:
	FLINK_DOCKER_IMAGE_NAME=$(ENGINE_IMAGE) docker-compose up


.PHONY: run-all-in-one
run-all-in-one:
	docker run \
		-it --rm \
		-v $(PWD)/target/scala-2.12:/opt/engine/bin \
		-v $(PWD)/workspace:/opt/engine/workspace \
		--entrypoint bash \
		$(FLINK_BASE_IMAGE)
