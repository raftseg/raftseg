# raftseg

raftseg is a distributed data fabric for streams, queues and jobs, built on
Raft with tunable durability. It needs Go 1.26 or later.

Its dependencies are the Go standard library, first-party modules from
github.com/raftseg, and one third-party module: Google's protobuf runtime,
google.golang.org/protobuf, which raft's messages are generated for.
`go.mod` requires nothing else, a first-party module may require nothing
but protobuf, and every file imports only the standard library, this
module, first-party modules and protobuf. `internal/depcheck` enforces
this, and the SBOM tool refuses anything else.

## Packages

| Package | Contents |
|---|---|
| [`github.com/raftseg/raft`](https://github.com/raftseg/raft) | A separate, first-party module: our fork of etcd raft, based on upstream's `main` (commit `638f427`, which holds every change of v3.7.0), with a series of safety, read and liveness fixes and validation of messages from peers. Its `FORK.md` lists every change and the reason for it. Its messages are protobuf messages, held by pointer and read through getters. |
| `seglog/` | The durable raft log: one segmented log on disk shared by all of a node's raft groups, each exposed as a `raft.Storage`. [seglog/README.md](seglog/README.md) describes its design, its guarantees and how they are tested. |
| `vfs/` | The filesystem interface the storage packages use, and its implementation on the operating system. |
| `internal/raftsim/` | A deterministic, randomized cluster simulator that checks raft's safety and liveness under faults. Node storage is pluggable: in memory, in seglog on a crash-simulating filesystem, or both mixed in one cluster. Used by tests only. |
| `internal/crashfs/` | An in-memory filesystem that shows what a power cut would leave (torn and reordered writes, lost directory operations, writeback errors) and injects errors. It is checked against the operating system's filesystem. Used by tests only. |
| `internal/testutil/`, `internal/datadriven/` | Test helpers that replace testify and cockroachdb/datadriven. Used by tests only. |
| `internal/depcheck/` | The dependency check: `go.mod`, imports and builds may name only first-party modules and protobuf, and `go.sum` only those and what protobuf's `go.mod` requires. |
| `internal/tools/sbom/` | Writes the CycloneDX SBOM, and fails if anything but this module, first-party modules, protobuf and the Go standard library would be compiled into a product. |

### Working on raftseg and raft together

`go.mod` pins raft at an exact commit. To change both, check out
github.com/raftseg/raft next to this repository and put a `go.work` in the
directory above both:

```
go 1.26

use (
	./raftseg
	./raft
)
```

The go command then builds raftseg with the raft checkout, and the
simulator and the mutation runner exercise that checkout. Commit raft
first, then move raftseg's requirement to that commit. The SBOM tool
refuses a workspace checkout that is not exactly the version `go.mod`
requires.

The in-memory raft log, and the combinations of in-memory and persistent nodes and groups, follow as the next packages.

## Testing

```
go test ./...                              # everything, about 3.5 minutes on 2 cores
go test -short ./...                       # under a minute
go test -short -race ./...                 # about 4 minutes
RAFTSIM_SEEDS=20000 go test ./internal/raftsim -run 'TestSimulation$' -timeout 2h   # soak, in-memory nodes
RAFTSIM_SEEDS=5000 go test ./internal/raftsim -run 'Seglog|Mixed' -timeout 3h      # soak, seglog and mixed nodes
RAFTSIM_SEED=1234 go test ./internal/raftsim -run 'TestSimulation$' -v             # replay one run
SEGLOG_EXHAUSTIVE=1 go test ./seglog -run TestDamageEveryFile -timeout 2h          # damage every byte
go test ./seglog -run XXX -fuzz FuzzCorruptLog                                     # and the other fuzzers
```

Every simulation is a pure function of its seed, so a failure replays exactly, with a trace of its last events.

raft's own tests and fuzzers run in its repository (`go test ./...` there).

[docs/verification](docs/verification/README.md) summarizes what has been verified and at what scale: soaks, crash and damage testing, fuzzing, and mutation testing of raft and seglog, together with the bugs found in each. It also has the mutant catalogues and the runner that reproduces the mutation tests.

## Supply chain

```
go run ./internal/tools/sbom -version 0.1.0 > raftseg.cdx.json
```

writes a CycloneDX 1.5 SBOM. It lists every package compiled into a product, each with a hash of its sources: the module's own, raft's, protobuf's, and the Go standard library as one component. A dependency graph links each package to the packages it imports.

- raft's and protobuf's packages are listed at the versions `go.mod` requires, with the module hashes `go.sum` records. A module taken from the module cache must hash to that value, and the tool recomputes it to check.
- raft's packages carry their pedigree from raft's `upstream.json`: the upstream commit the fork is based on, every commit cherry-picked since, and the license.
- protobuf's packages carry their supplier (Google), license (BSD-3-Clause) and copyright.
- Code used only by tests is listed with scope "excluded".

The tool fails in any of these cases:
- `go.mod` requires a module other than first-party modules and protobuf, or replaces any module.
- A first-party module requires anything but protobuf.
- A module differs from what `go.mod` and `go.sum` pin.
- Any package outside this module, those modules and the standard library would be compiled into a product.

## Platforms

64-bit Linux is the supported platform. The module builds on other 64-bit systems and refuses to build on 32-bit ones (see seglog/README.md, Platforms).

## Licensing

The raftseg authors' own code is not yet under a license. Third-party code
in this module is listed in [NOTICE](NOTICE), with its license kept next to
it. raft is Apache 2.0, with its own LICENSE and NOTICE in its repository.
