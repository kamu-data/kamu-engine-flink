FLINK_IMAGE_VERSION = 1.13.1-scala_2.12-java8
FLINK_IMAGE = flink:$(FLINK_IMAGE_VERSION)
ENGINE_IMAGE_VERSION = 0.10.0-flink_$(FLINK_IMAGE_VERSION)
ENGINE_IMAGE = kamudata/engine-flink:$(ENGINE_IMAGE_VERSION)


.PHONY: engine-assembly
engine-assembly:
	sbt assembly


.PHONY: adapter-assembly
adapter-assembly:
	cd adapter && \
	cross build --target x86_64-unknown-linux-gnu --release


.PHONY: image-build
image-build:
	docker build \
		--build-arg BASE_IMAGE=$(FLINK_IMAGE) \
		-t $(ENGINE_IMAGE) \
		-f image/Dockerfile \
		.


.PHONY: image
image: engine-assembly adapter-assembly image-build


.PHONY: image-push
image-push:
	docker push $(ENGINE_IMAGE)
