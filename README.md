# SQLite replication for Go

Go implementation of the SQLite replication protocol, extracted from
[`sqlite-replication`](https://github.com/lucaswilliameufrasio/sqlite-replication)
at commit `7541689719b137db44504a505aad518b9e6f8b08`.

The module path is `github.com/lucaswilliameufrasio/sqlite-replication-go`.
The public APIs include the root package, `replicator`, `ltx`, `storagefs`, and
`storages3`. See `examples/` for usage.

Run `go vet ./...` and `go test -count=1 ./...` from the repository root.
The independent telemetry example is a nested module; run
`go test -race -count=3 ./...` in `examples/coordinator-telemetry`.

Fixtures under `conformance/fixtures` are copied unchanged from the source
commit. Their generation tools and detailed provenance remain in the original
repository at that commit. Cross-language and local S3 conformance are not
covered by the commands above.

The third-party notice and provenance review remains incomplete; see the
[source evidence](https://github.com/lucaswilliameufrasio/sqlite-replication/tree/7541689719b137db44504a505aad518b9e6f8b08/docs/sbom/2026-11-05).
No final license approval or package release is implied by this repository.
