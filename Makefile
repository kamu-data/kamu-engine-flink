FLINK_IMAGE_VERSION = 1.16.0-scala_2.12-java8
FLINK_IMAGE = flink:$(FLINK_IMAGE_VERSION)
ENGINE_IMAGE_VERSION = 0.16.0-flink_$(FLINK_IMAGE_VERSION)
ENGINE_IMAGE = ghcr.io/kamu-data/engine-flink:$(ENGINE_IMAGE_VERSION)


engine-assembly:
	sbt assembly


adapter-assembly:
	cd adapter && \
	RUSTFLAGS="" cross build --target x86_64-unknown-linux-musl --release


image-build:
	docker build \
		--build-arg BASE_IMAGE=$(FLINK_IMAGE) \
		-t $(ENGINE_IMAGE) \
		-f image/Dockerfile \
		.


image: engine-assembly adapter-assembly image-build


image-push:
	docker push $(ENGINE_IMAGE)
