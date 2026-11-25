# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`types-confluent-kafka` is a stub-only PEP 561 package: partial typing stubs for `confluent-kafka`
(confluent-kafka-python). It has no runtime code. `README.md` lists what is covered and how consumers install it.

Unlike most stubbed libraries, `confluent-kafka` ships its own types (`py.typed`), but part of them is wrong or
incomplete. The stubs replace only the modules where that is so; type checkers use the library's own types for
every other module. See [Stubs next to a typed library](#stubs-next-to-a-typed-library).

## Commands

```bash
uv sync                  # install; the project itself goes in editable, so the editor follows the edits in src/
uv run ruff format
uv run ruff check

# Type-check the stubs and the cases in tests/ (strict), then compare the stubs with the installed confluent-kafka
uv run --no-editable --reinstall-package types-confluent-kafka pyright
uv run --no-editable \
  stubtest confluent_kafka --mypy-config-file pyproject.toml --ignore-missing-stub --allowlist tests/allowlist.txt
uv sync --reinstall-package types-confluent-kafka   # back to the editable install afterwards

uv build                 # dist/: the wheel holds only confluent_kafka-stubs/
npx markdownlint-cli2 "**/*.md"
```

ruff, pyright, stubtest and markdownlint must all report no issues before a commit.

- `pyright` and `stubtest` need the package installed for real (`--no-editable`). When pyright finds the partial
  stub package on a search path, as the editable install makes `src`, it merges it into the installed library and
  no longer checks the stub files: `uv run pyright` passes with `Found 2 source files` (`--verbose`), not 11.
  `[tool.pyright]` has an execution environment without `extraPaths` for the same reason, as pyright would add `src`
  by itself. mypy can't read the editable install's `src/confluent_kafka-stubs` either.
- `--reinstall-package` matters too: without it uv reuses its cached build of the same version, and the checks run
  on stale stubs.
- `--ignore-missing-stub` is what makes the stubs partial: stubtest skips runtime names the stubs leave out, but
  everything that is stubbed must match the runtime.
- `--mypy-config-file pyproject.toml` applies `[[tool.mypy.overrides]]`, which ignores the errors mypy finds in the
  library's own modules. Without it stubtest stops with `not checking stubs due to mypy build errors`.
- `cimpl` is a C extension without signatures, so stubtest only checks that its names exist. See
  [Writing stubs](#writing-stubs) for where its signatures come from.

## Layout

```text
src/confluent_kafka-stubs/
├── py.typed                       # "partial": modules not stubbed here fall back to the library's own types
├── __init__.pyi                   # the top-level re-exports, ThrottleEvent
├── cimpl.pyi                      # the C extension: Producer, Consumer, Message, TopicPartition, KafkaError, ...
├── serializing_producer.pyi, deserializing_consumer.pyi
├── admin/__init__.pyi             # AdminClient; the data classes stay in the library's admin/_*.py
└── schema_registry/_sync/         # schema_registry_client, avro, json_schema, protobuf
tests/
├── check_kafka.py       # type-checking cases (fallback, generics, overloads, rejected calls), never run
└── allowlist.txt        # stub-only names, and the unstubbed modules stubtest must not check
scripts/build_index.py   # writes the package index pages; used by the release workflow
.github/workflows/release.yml
```

`pyproject.toml` builds `confluent_kafka-stubs` with `uv_build` (`module-name`). The package has no dependencies:
`confluent-kafka` is only a dev dependency, pinned to the version the stubs are checked against, with the extras
whose modules are stubbed. `types-protobuf` is a dev dependency because `protobuf.pyi` imports `Message`.

## Stubs next to a typed library

A stub package overrides the library's inline types module by module. `py.typed` says `partial`, so a module with
no stub falls back to the library's own `.py` (in both pyright and mypy).

- **Stub a module only when its types are wrong, missing, or leak `Any` / `Unknown` to callers.** The others are
  left out and follow the library without maintenance.
- **A stub replaces its module completely**: stub every public name (its `__all__`). A name left out no longer
  exists for callers, and stubtest (`--ignore-missing-stub`) doesn't report it.
- **Stub the module that defines a class, not the one that re-exports it**: `schema_registry/_sync/avro.py`, not
  `schema_registry/avro.py`, which star-imports it and picks the stub up. A class stubbed in the re-exporting module
  would be a second, different type.
- **`cimpl.pyi` must stay stubbed**: once a stub package exists, pyright no longer finds the library's own
  `cimpl.pyi`, and its names become `Unknown`.
- **Imports are absolute** (`from confluent_kafka.cimpl import Message`), whether the module is stubbed or the
  library's, and name the module that defines the name: `KafkaError` comes from `cimpl`, as `confluent_kafka.error`
  imports it without re-exporting it (`reportPrivateImportUsage`).
- **Keep the names that unstubbed library modules import**, private ones included (`_RestClient`, for the library's
  `DekRegistryClient`); a star-imported module needs an `__all__` that lists them. To check, run mypy over every
  library module in a no-editable install and, with the same `$mods`, in a venv without the stubs: the stubs must
  add no `attr-defined` error.

  ```bash
  # `-p confluent_kafka` would skip the top-level modules that have no stub (`aio`, `avro`, `serialization`, ...)
  mods=$(cd .venv/lib/python*/site-packages && find confluent_kafka -name '*.py' |
    sed -e 's|/__init__\.py$||' -e 's|\.py$||' -e 's|/|.|g' -e 's|^|-m |')
  uv run --no-editable --reinstall-package types-confluent-kafka mypy --config-file /dev/null $mods
  ```

  Two errors of other kinds are expected: `arg-type` in `aio/producer/_kafka_batch_executor.py` (`produce_batch`
  gets a `list[dict[str, Any]]`) and `override` in `_sync/mock_schema_registry_client.py` (`set_config`).
- **`tests/allowlist.txt` lists the unstubbed modules** as regular expressions, or stubtest would check the
  library's source against itself. Update it when a module is stubbed or the library adds one.

## Writing stubs

- **Signatures: copy the runtime's completely**: every parameter, in its order and kind, none dropped and no `*`
  added (stubtest enforces this for Python modules). Print one with
  `uv run python -I -c "import inspect; from confluent_kafka.admin import AdminClient; print(inspect.signature(AdminClient.list_offsets))"`.
  The exceptions are in `AdminClient`: `list_topics`, `list_groups` and `cluster_id` are `(*args, **kwargs)` at
  runtime and get the C signature, and `elect_leaders` has no `**kwargs`, as either keyword it would pass on raises
  `SystemError`.
- **`cimpl` signatures come from the C sources** (`src/confluent_kafka/src/*.c` in confluent-kafka-python, at the
  pinned version's tag), as `inspect.signature` fails on them and stubtest can't check them:
  - Method table: `METH_VARARGS | METH_KEYWORDS` takes the names in `kws[]`; `METH_VARARGS` alone and `METH_O` are
    positional-only (`/`); `METH_NOARGS` has no parameters.
  - Format string: `s` is `str`, `z` is `str | None`, `z#` is `str | bytes | None`, `i` / `L` are `int`, `d` / `f`
    are `float`, `p` is `bool`, `O` is any object (the code says what it requires); those after `|` are optional.
  - A parameter parsed with `s` or `i` can be left out but not passed as `None`: `metadata: str = ...`, not
    `str | None = None`.
  - A type with `tp_richcompare` but no `tp_hash` can't be hashed, and stubtest doesn't report it:
    `__hash__: ClassVar[None]  # type: ignore[assignment]` (`Message`).
  - When in doubt, call the function: most clients can be created without a broker.
- **Types come from the docstrings and the code**, not from guesses or from library annotations the code
  contradicts: `ProtobufSerializer(msg_type)` is annotated `Message` but takes the class, so it is `type[M]`.
  - The C code requires real `list`s and `dict`s (`PyList_Check`, `PyDict_Check`): `list[...]` and `dict[...]`, not
    `Sequence` or `Mapping`. Python code that only reads a value takes `Mapping` (the serializers' `conf`).
  - A configuration `dict` is `dict[str, T]` with a type variable of the method
    (`def __init__[T](self, config: dict[str, T] = ..., /, **kwargs: object)`). Unlike `dict[str, object]`, it
    accepts a caller's `dict[str, str]` (`dict` is invariant), and it still rejects other mappings.
  - No `Any`: `**kwargs` passed on to a function is `Unpack[TypedDict]` of the keys it accepts (`_CreateKwargs` in
    `admin`), and configuration `**kwargs` are `object`. The exception is `_RestClient`, an internal class whose
    callers, all in the library, are written against `Any`.
  - Callbacks return `object`, as their result is ignored.
- **Stub-only type parameters** (`Message[K, V]`, `AvroSerializer[T]`) are allowed where they carry a type from a
  constructor argument to a result. Give them a default (`object`, or `bytes` for `Message`) and a comment that the
  runtime class can't be subscripted.
- **Defaults:** literals as they are (`timeout: float = -1`), `...` for the rest.
- **Private aliases (`_Headers`, `_DeliveryCallback`, ...) are redefined in each module**: importing one from
  another stub is `reportPrivateUsage` in pyright, and stubtest reports a public alias as missing at runtime.
- **Stub-only public names** need a `tests/allowlist.txt` entry and a comment that they don't exist at runtime. A
  `TypedDict` is public when callers annotate a variable with it (`list[BatchMessage]`), and private when it is only
  for `**kwargs` (`_CreateKwargs`).
- **Deprecated members** (a runtime warning, or "deprecated" in the docstring) get `@deprecated` from `warnings`.
- **C classes get `@disjoint_base`** (from `typing_extensions`), which stubtest asks for.
- **No suppressions**, except:
  - `# type: ignore[override]`, with a comment, where the runtime changes its base class's signature
    (`SerializingProducer.produce` takes `key` before `value`), and on `DeserializingConsumer.poll`, which returns
    `Message[K, V]`, not the `Message[bytes, bytes]` of `Consumer.poll`. A renamed parameter of a dunder method
    (`AvroDeserializer.__call__(data)`, `value` in its base class) needs none: pyright doesn't compare those names.
  - `# type: ignore[assignment]` on `__hash__: ClassVar[None]`.
  - `# pyright: ignore[reportPrivateUsage]` on the import of `_AdminClientImpl`, the private base class of
    `AdminClient`.
- **Tests cover only what the stubs don't spell out**, as stubtest checks the signatures: the fallback to unstubbed
  modules (through stubbed and unstubbed ones), what callers get from generics, type parameter defaults, overloads
  and `Unpack` kwargs, stub-only names (imported under `if TYPE_CHECKING:`), and a few realistic calls. A call the
  stubs must reject carries a rule-specific ignore
  (`consumer.subscribe(("topic",))  # pyright: ignore[reportArgumentType]`); `reportUnnecessaryTypeIgnoreComment` is
  an error, so the check fails if the stubs start accepting it.
- **README:** when stubbing a module or leaving members out, update the coverage table and the list of what is
  left to the library.

## Versions

The package has its own semantic version, independent of `confluent-kafka`'s; it is not updated for every
`confluent-kafka` release. The dev pin (`confluent-kafka[...]==X.Y.Z`) is the version stubtest checks against, and
the Versions table in `README.md` maps each release to it.

## Release

Releases go to the static PEP 503 index at `https://pkgs.agnos.one/simple/`, through `.github/workflows/release.yml`.
To release:

1. To check against a newer `confluent-kafka`, bump the dev pin, `uv sync`, and update the stubs until stubtest
   passes. For `cimpl.pyi`, compare the C sources of the two versions, as stubtest can't see their signatures. Check
   the unstubbed modules as well: one whose types have become wrong needs a stub, and a stub the library has made
   unnecessary can go.
2. Bump the version: `uv version --bump minor` for new stubs or changed types, `--bump patch` for fixes.
3. Add a row to the Versions table in `README.md`: the new version and the dev pin.
4. In `CHANGELOG.md`, move the `Unreleased` entries into a dated `## [X.Y.Z] - YYYY-MM-DD` section and update the
   links at the bottom.
5. Commit and push, then `git tag vX.Y.Z && git push origin vX.Y.Z`.

The workflow fails unless the tag is exactly `v` + the `pyproject.toml` version and the changelog section is dated.
It runs ruff, pyright and stubtest (not markdownlint), builds with `uv build --no-sources`, checks the files with
`twine check`, uploads them and rebuilds the project and root index pages in the R2 bucket, then creates a GitHub
Release from the changelog section and posts the result to Slack. It refuses to overwrite a published version, so a
bad release is fixed by publishing a new version.

The `publish` job runs in the `agnos-packages` environment, which only `v*` tags may deploy to. The R2 secrets
(`R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ACCOUNT_ID`, `R2_BUCKET`) are set on that environment;
`SLACK_WEBHOOK_URL` is an organization secret.

`scripts/build_index.py` writes the index pages with the standard library only, so it runs on the runner's system
Python. Keep it in sync with the copies in `keycloak-auth` and `types-google-cloud-storage`: all rewrite the shared
root index.
