FLINK_BASE_IMAGE = flink:1.10.0-scala_2.12
ENGINE_VERSION = 0.1.0
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
