# Security policy

## Supported versions

Security fixes are applied to the latest released version and the current
`main` branch.

## Reporting a vulnerability

Please do not open a public issue for a suspected vulnerability. Use GitHub
private vulnerability reporting for this repository when available. If private
reporting is unavailable, contact the repository owner through the GitHub
profile before publishing details.

Include:

- the affected revision or release;
- a minimal reproduction;
- the expected security boundary;
- whether exploitation requires local shell access, root, or control of an
  authenticated worker account.

## Security model

`gemini-router` is a Linux process scheduler around the external `agy` CLI.
It is not a sandbox.

The supported deployment runs the router as root and uses `sudo -H -u` to
execute `agy` as configured worker users. Treat the router configuration and
the configured `agy` binaries as privileged inputs. Worker credentials stay
in worker home directories rather than root's home.

Runtime state is private by default:

- the CLI creates or tightens `data_dir` and its log directory to `0700`;
- the database is pre-created as `0600`, and WAL/SHM sidecars present after
  opening are tightened to `0600`;
- existing router log files are tightened to `0600` when opened;
- prompt previews are disabled by default;
- request-owned AGY logs are temporary and removed after the request;
- generated artifacts are placed in request-specific directories.

The generated sudoers rule is validated with `visudo -cf` before it is
installed. Installation uses an atomic rename so an invalid replacement does
not destroy the previous rule.

## Filesystem requirements

The SQLite database uses WAL mode and coordinates independent router
processes. Keep `data_dir` on a local filesystem with reliable POSIX locking.
Do not place the live database on Dropbox, NFS, SMB, FUSE mounts, or similar
network/synchronised filesystems.

## Sensitive data

Depending on the request, sensitive material can appear in:

- input files passed to AGY;
- AGY's own provider-side state;
- generated artifacts;
- error diagnostics;
- prompt previews, but only when explicitly enabled.

Review log retention and artifact retention for the host where the router is
deployed.

## Out of scope

The router cannot protect against:

- a malicious or compromised `agy` binary;
- compromise of the host root account;
- compromise of a configured worker account;
- provider-side retention or processing performed by AGY;
- arbitrary code intentionally supplied to external tools invoked by AGY.
