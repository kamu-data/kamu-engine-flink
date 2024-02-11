# Developer Guide
Flink engine image consists of the following main parts:
- **Flink application** - the engine itself
- **ODF adapter** - RPC server used to communicate with the engine

## Submodules
This repo includes submodules.

When cloning use:
```bash
git clone --recurse-submodules https://github.com/kamu-data/kamu-engine-flink.git
```

If you forgot to do so you can pull them later with:
```bash
git submodule update --init
```

## Build dependencies
You will need:
- `docker`
- `rust` toolchain (see [kamu-cli dev guide](https://docs.kamu.dev/cli/developer-guide/))
- Java/Scala toolchain

To install Java & Scala we recommend using [SdkMan](https://sdkman.io/). Install the tool itself and then you can use following versions of components:

```bash
sdk use java  8.0.292-zulu
sdk use maven 3.8.1
sdk use sbt   1.5.5
sdk use scala 2.12.13
```

Once installed, you can configure IntelliJ IDEA to use this runtime and tools and open the directory as an SBT project.

## Building ODF Adapter
ODF adapter is a `rust` application - to build it follow the same approach as for [kamu-cli](https://docs.kamu.dev/cli/developer-guide/).

## Building Flink Application
When developing the engine you can run `sbt` in the root directory and then use:
- `compile` - to compile the code
- `assembly` - to build a distribution
- `test` - to run all tests
- `testOnly dev.kamu.engine.flink.test.JoinStreamToStreamTest` - to run a specific test suite
- `testOnly dev.kamu.engine.flink.test.JoinStreamToStreamTest -- -z "Stream to stream join"` - to run a specific test case

> **IMPORTANT:** Integration tests (those named as `Engine*`) run engine inside a docker image and mount your local **assembly** into it - make sure you both have built a new image (or pulled an older one) and have ran `sbt assembly` before re-running such tests, otherwise your changes will not have an effect.

## Release
To release a new version:
- Commit your changes
- Increment the version number in `Makefile`
- Build image using `make image-multi-arch`
  - This will build the adapter and the engine assembly
  - See docker manual about [building multi-arch images](https://docs.docker.com/build/building/multi-platform/)
  - Note: on Linux to perform multi-arch build it seems to be enough to create a `buildx` runner as:
    ```
    docker buildx create --use --name multi-arch-builder
    ```
    The runner seems to come equipped with QEMU allowing us to cross-build
- Push image to the registry using `make image-push`
- Tag your last commit with `vX.Y.Z`
- Push changes to git repo
