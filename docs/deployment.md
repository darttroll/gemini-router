# Deployment

## Build

Build the exact revision that will be deployed:

```bash
git switch --detach <tag-or-commit>
go mod tidy -diff
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
go build -trimpath -o /tmp/gemini-router-new ./cmd/gemini-router
/tmp/gemini-router-new --help
```

The repository requires the Go version declared by `go.mod`.

## Runtime layout

Recommended paths:

```text
/usr/local/bin/gemini-router
/etc/gemini-router/config.yaml
/var/lib/gemini-router/gemini-router.db
/var/lib/gemini-router/logs/
```

Keep `/var/lib/gemini-router` on local storage. SQLite WAL is the
cross-process coordination mechanism and should not live on Dropbox/NFS/FUSE.

The configuration should be owned by a trusted administrator. On a multi-user
host, mode `0600` is recommended.

## Initial setup

The supported deployment is root-managed: router commands run as root, while
AGY authentication and provider state live in dedicated worker home
directories.

1. Create worker accounts:

```bash
sudo useradd --create-home --shell /bin/bash gemini_a
sudo useradd --create-home --shell /bin/bash gemini_b
```

2. Install the tested Antigravity CLI **1.2.8** release as each worker:

```bash
for worker in gemini_a gemini_b; do
  sudo -H -u "$worker" bash <<'EOF'
set -euo pipefail
version=1.2.8
case "$(uname -m)" in
  x86_64)
    asset=agy_cli_linux_x64.tar.gz
    sha256=244752206d1f65c01aff489628f1df51f1a3fddacaa8ed74984661ebb6d09136
    ;;
  aarch64|arm64)
    asset=agy_cli_linux_arm64.tar.gz
    sha256=85ea71929436711e4b0332026508ee4e7ddb05192f24f1774dfbed879255fc6e
    ;;
  *)
    echo "unsupported architecture: $(uname -m)" >&2
    exit 1
    ;;
esac
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
curl -fL --retry 3 \
  -o "$tmp/agy.tar.gz" \
  "https://github.com/google-antigravity/antigravity-cli/releases/download/$version/$asset"
echo "$sha256  $tmp/agy.tar.gz" | sha256sum -c -
tar -xzf "$tmp/agy.tar.gz" -C "$tmp"
install -d -m 0755 "$HOME/.local/bin"
install -m 0755 "$tmp/antigravity" "$HOME/.local/bin/agy"
"$HOME/.local/bin/agy" --version
EOF
done
```

Public upstream: https://github.com/google-antigravity/antigravity-cli
Installation/authentication guide: https://antigravity.google/docs/cli/install/
This release was validated against AGY 1.2.8. The commands above intentionally
pin that release and verify the upstream SHA-256. If you choose a newer AGY
release, re-run the integration tests before deployment.

3. Authenticate each worker once:

```bash
sudo -H -u gemini_a /home/gemini_a/.local/bin/agy
sudo -H -u gemini_b /home/gemini_b/.local/bin/agy
```

On SSH, follow the authorization URL printed by AGY.

4. Copy `configs/config.example.yaml` to
   `/etc/gemini-router/config.yaml`, set worker usernames, and use each
   worker's installed AGY path.

5. Protect configuration and initialize the router:

```bash
sudo chown root:root /etc/gemini-router/config.yaml
sudo chmod 0600 /etc/gemini-router/config.yaml
sudo gemini-router setup
```

Setup creates or tightens private data/log directories, initializes SQLite,
and generates sudoers. The candidate rule is checked with `visudo -cf`;
failed validation leaves an existing installed rule untouched.

## Read-only smoke checks

```bash
sudo gemini-router --help
sudo gemini-router quota
sudo gemini-router quota --json
sudo gemini-router status
sudo gemini-router status --json
sudo gemini-router stats
sudo gemini-router health --verbose
```

Quota/status refreshes may execute AGY's read-only `/quota` command but do
not intentionally generate a model response.

## Upgrade

Before replacing a working binary, save both the binary and a consistent
SQLite snapshot.

```bash
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OLD="/usr/local/bin/gemini-router.rollback-$STAMP"
RBDIR="/var/lib/gemini-router/rollback-$STAMP"

cp -a /usr/local/bin/gemini-router "$OLD"
install -d -m 0700 "$RBDIR"

python3 - "$RBDIR/gemini-router.db" <<'PY'
import sqlite3
import sys

src = sqlite3.connect("/var/lib/gemini-router/gemini-router.db")
dst = sqlite3.connect(sys.argv[1])
with dst:
    src.backup(dst)
dst.close()
src.close()
PY
chmod 0600 "$RBDIR/gemini-router.db"
```

Install the tested binary atomically:

```bash
install -m 0755 /tmp/gemini-router-new /usr/local/bin/gemini-router.new
mv -f /usr/local/bin/gemini-router.new /usr/local/bin/gemini-router
```

Existing one-shot processes continue using their already-open executable
inode; new invocations use the replacement.

## Canary

When a provider with available quota exists, run one short generation canary
through a deliberately selected model:

```bash
sudo gemini-router --model <model> --timeout 2m -p 'Reply with exactly: ROUTER_OK'
```

Observe `status`, `stats`, and logs before treating the deployment as
complete.

## Rollback

Restore the previous binary atomically:

```bash
cp -a "$OLD" /usr/local/bin/gemini-router.rollback-tmp
mv -f /usr/local/bin/gemini-router.rollback-tmp /usr/local/bin/gemini-router
```

Prefer restoring only the binary when the old binary understands the current
database schema. Restore the SQLite snapshot only for an actual schema/data
incompatibility, and only after quiescing every process that can write the
database; otherwise newer queue/controller state would be discarded.
