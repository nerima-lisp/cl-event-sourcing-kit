# Compatibility

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

The core ASDF system depends on `cl-boundary-kit`. The in-memory and durable
systems use `cl-concurrent-kit`; the repository's test system also uses
`cl-host-kit` and `cl-weave`. These dependencies are adapter and development
choices, not requirements for an eventual database adapter's wire format.

## Data compatibility

The core does not prescribe JSON, MessagePack, a database schema, UUIDs, or a
clock representation. Event payloads and metadata are opaque Lisp values.
Applications that persist them must define a compatible serializer and an
evolution policy; the durable reference serializer is only one bounded,
portable option.

## Deployment compatibility

The file runtime coordinates within one process and does not provide
replication, distributed locks, authorization, encryption, OS-level `fsync`,
or background supervision. Use a backend adapter and application controls
when those guarantees are required.
