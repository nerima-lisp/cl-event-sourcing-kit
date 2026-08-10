# Development

## Test the ASDF system

From the repository root:

```sh
env CL_SOURCE_REGISTRY="$PWD//" sbcl --non-interactive \
  --eval '(require :asdf)' \
  --eval '(asdf:test-system "cl-event-sourcing-kit")'
```

The repository wrapper uses `cl-host-kit` to bootstrap the same test entry
point:

```sh
env CL_SOURCE_REGISTRY="$PWD//" sbcl --script run-tests.lisp
```

## Reproducible Nix checks

```sh
nix develop -c sbcl --non-interactive \
  --eval '(require :asdf)' \
  --eval '(asdf:test-system "cl-event-sourcing-kit")'
nix flake check
```

The flake covers the core, in-memory, projection, and durable systems. The
strict coverage gate is part of `nix flake check`; the HTML report is available
with:

```sh
nix build .#coverage
```

Coverage output is selected by `CL_EVENT_SOURCING_KIT_COVERAGE_DIR`, or a
temporary directory when that variable is absent.

The strict report targets executable operation and recovery modules. ASDF model,
condition, package, and macro declaration files are loaded and verified by the
tests, but are not treated as runtime expression-coverage targets.

## Build the documentation

The site is defined by `docs/mkdocs.yml` and `docs/src/`. The Nix flake exposes
the strict Material for MkDocs build as `.#docs`:

```sh
nix build .#docs
```

When MkDocs Material is available outside Nix, the equivalent local command is:

```sh
mkdocs build --config-file docs/mkdocs.yml --strict
```

Keep navigation entries and source files in sync. A missing page or broken
link should fail the strict build.

## Repository conventions

- Keep the core independent of storage and wire-format policy.
- Preserve opaque application values at the boundary; do not add accidental
  deep-copy or serialization assumptions.
- Add adapter guarantees to the adapter's documentation and tests.
- Run the narrowest meaningful check after a docs or code change, then report
  the command and its exit status.
