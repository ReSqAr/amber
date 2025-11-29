#!/usr/bin/env bash
set -euo pipefail

# Regenerate the committed gRPC bindings. This requires protoc on PATH.
GENERATE_GRPC=1 cargo build --package amber --locked
