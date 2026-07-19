# Versioning

`tether` follows [Semantic Versioning 2.0.0](https://semver.org/).

Current version: **0.1.0** (`tether.Version`, git tag `v0.1.0` when released).

## Format

```
MAJOR.MINOR.PATCH
```

Git tags and Go module versions use a leading `v`:

```bash
git tag v0.1.0
go get github.com/anuwatthisuka/tether@v0.1.0
```

## While MAJOR is 0 (initial development)

SemVer treats `0.y.z` as unstable: the public API may change. Until `1.0.0`:

| Change                                              | Bump   |
| --------------------------------------------------- | ------ |
| Bugfix, docs, tests, internal-only changes         | PATCH  |
| Backward-compatible feature in the root package     | MINOR  |
| Breaking change to the root (public) package        | MINOR  |
| First stable API commitment                         | MAJOR → `1.0.0` |

`internal/` is not a compatibility surface and may change in any release
without a bump by itself (only bump when a public release ships).

Wire protocol (`internal/proto.ProtocolVersion`) is versioned separately from
the library SemVer. A protocol break that affects embedders is still a
**breaking** library change and bumps MINOR while MAJOR is 0 (MAJOR after 1.0.0).

## From 1.0.0 onward

| Change                         | Bump  |
| ------------------------------ | ----- |
| Breaking public API / protocol | MAJOR |
| Backward-compatible feature    | MINOR |
| Backward-compatible fix        | PATCH |

## Release checklist

1. Update `Version` in `version.go`.
2. Move items under `## Unreleased` in `CHANGELOG.md` into
   `## [X.Y.Z] - YYYY-MM-DD`.
3. Commit on `main`.
4. Tag annotated: `git tag -a vX.Y.Z -m "vX.Y.Z"`.
5. Push commit and tag when publishing.

Do not retag or force-move a published tag.
