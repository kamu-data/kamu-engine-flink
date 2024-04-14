FLINK_IMAGE_VERSION = 1.16.0-scala_2.12-java8
FLINK_IMAGE = flink:$(FLINK_IMAGE_VERSION)
ENGINE_IMAGE_VERSION = 0.18.2-flink_$(FLINK_IMAGE_VERSION)
ENGINE_IMAGE = ghcr.io/kamu-data/engine-flink:$(ENGINE_IMAGE_VERSION)


.PHONY: engine-assembly
engine-assembly:
	sbt assembly


.PHONY: adapter-assembly
adapter-assembly:
	mkdir -p image/tmp/linux/amd64
	cd adapter && RUSTFLAGS="" cross build --target x86_64-unknown-linux-musl --release
	cp adapter/target/x86_64-unknown-linux-musl/release/kamu-engine-flink-adapter image/tmp/linux/amd64/adapter

.PHONY: adapter-assembly-multi-arch
adapter-assembly-multi-arch:
	rm -rf image/tmp
	mkdir -p image/tmp/linux/amd64 image/tmp/linux/arm64
	cd adapter && RUSTFLAGS="" cross build --target x86_64-unknown-linux-musl --release
	cd adapter && RUSTFLAGS="" cross build --target aarch64-unknown-linux-musl --release
	cp adapter/target/x86_64-unknown-linux-musl/release/kamu-engine-flink-adapter image/tmp/linux/amd64/adapter
	cp adapter/target/aarch64-unknown-linux-musl/release/kamu-engine-flink-adapter image/tmp/linux/arm64/adapter


.PHONY: image-build
image-build:
	docker build \
		--build-arg BASE_IMAGE=$(FLINK_IMAGE) \
		-t $(ENGINE_IMAGE) \
		-f image/Dockerfile \
		.

.PHONY: image-build-multi-arch
image-build-multi-arch:
	docker buildx build \
		--push \
		--platform linux/amd64,linux/arm64 \
		--build-arg BASE_IMAGE=$(FLINK_IMAGE) \
		-t $(ENGINE_IMAGE) \
		-f image/Dockerfile \
		.


.PHONY: image
image: engine-assembly adapter-assembly image-build

.PHONY: image-multi-arch
image-multi-arch: engine-assembly adapter-assembly-multi-arch image-build-multi-arch


image-push:
	docker push $(ENGINE_IMAGE)
