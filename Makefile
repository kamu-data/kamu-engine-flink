FLINK_IMAGE_VERSION = 1.13.1-scala_2.12-java8
FLINK_IMAGE = flink:$(FLINK_IMAGE_VERSION)
ENGINE_IMAGE_VERSION = 0.8.0-flink_$(FLINK_IMAGE_VERSION)
ENGINE_IMAGE = kamudata/engine-flink:$(ENGINE_IMAGE_VERSION)


.PHONY: image
image:
	docker build \
		--build-arg BASE_IMAGE=$(FLINK_IMAGE) \
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
		$(FLINK_IMAGE)
