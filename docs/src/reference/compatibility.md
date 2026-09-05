# Compatibility and release policy

This release is a deliberate API break. The removed `event-store` base class,
old protocol declarations, and former compatibility names are not supported.
Consumers must migrate to the current generic-operation protocol; no runtime
shim or deprecated alias is provided.

## Runtime and build

The repository's test, coverage, and Nix paths use SBCL. The flake declares
these Nix systems:

| Nix system | Status |
| --- | --- |
| `x86_64-linux` | Declared by the flake. |
| `aarch64-darwin` | Declared by the flake. |

Run the checks on each target you intend to support. A successful build on one
platform does not establish behavior on the other.

## Dependency boundaries

The core ASDF system depends on `cl-boundary-kit` and `cl-weave`. The in-memory
and durable systems use `cl-concurrent-kit`; the repository's test system also
uses `cl-host-kit`. These are direct implementation dependencies, not an
backend-compatibility promise.

## Data compatibility

The core does not prescribe JSON, MessagePack, a database schema, UUIDs, or a
clock representation. Event payloads and metadata are opaque Lisp values.
Applications that persist them must define a compatible serializer and an
evolution policy; the durable reference serializer is only one bounded,
portable option.

## Deployment compatibility

The file runtime coordinates within one process and does not provide
replication, distributed locks, authorization, encryption, OS-level `fsync`,
or background supervision. Use a backend implementation and application controls
when those guarantees are required.
