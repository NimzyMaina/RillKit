#!/usr/bin/env bash
# Install the RillKit control plane without cloning the repository.
#
#   curl -fsSL https://raw.githubusercontent.com/NimzyMaina/RillKit/main/deploy/install.sh \
#     | bash -s -- --dir rillkit
#
# It downloads deploy/compose.yaml, writes a .env with random secrets, and starts the published
# image. With your own Postgres:
#
#   ... | bash -s -- --dir rillkit --database-url postgresql://user:pass@db.example.com:5432/rillkit
#
# Flags:
#   --dir DIR              directory to create (default: rillkit)
#   --database-url URL     use an existing Postgres instead of the bundled one
#   --public-url URL       how workers reach the server (default: http://localhost:PORT)
#   --port PORT            host port for the dashboard (default: 8080)
#   --version TAG          image tag, e.g. v0.4.0 (default: edge)
#   --ref REF              git ref for compose.yaml (default: the --version tag, else main)
#   --no-start             write the files but do not run docker compose
#   --force                overwrite compose.yaml and .env in an existing directory
#   --help
#
# Redis/Valkey is not configured here: the server does not connect to it. Workers point
# RILLKIT_URL at their own Redis or Valkey, and `valkey` control mode needs no server at all.

set -eu

REPO="${RILLKIT_REPO:-NimzyMaina/RillKit}"
REF="main"
REF_EXPLICIT="no"
DIR="rillkit"
DATABASE_URL=""
PUBLIC_URL=""
PORT="8080"
VERSION="edge"
START="yes"
FORCE="no"

die() { printf 'error: %s\n' "$*" >&2; exit 1; }
say() { printf '%s\n' "$*"; }

usage() {
  cat <<'EOF'
Install the RillKit control plane without cloning the repository.

  curl -fsSL https://raw.githubusercontent.com/NimzyMaina/RillKit/main/deploy/install.sh \
    | bash -s -- --dir rillkit

Flags:
  --dir DIR              directory to create (default: rillkit)
  --database-url URL     use an existing Postgres instead of the bundled one
  --public-url URL       how workers reach the server (default: http://localhost:PORT)
  --port PORT            host port for the dashboard (default: 8080)
  --version TAG          image tag, e.g. v0.4.0 (default: edge)
  --ref REF              git ref for compose.yaml (default: the --version tag, else main)
  --no-start             write the files but do not run docker compose
  --force                overwrite compose.yaml and .env in an existing directory
  --help

Redis/Valkey is not configured here: the server does not connect to it. Workers point
RILLKIT_URL at their own Redis or Valkey, and `valkey` control mode needs no server at all.
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --dir) DIR="${2:?--dir needs a value}"; shift 2 ;;
    --database-url) DATABASE_URL="${2:?--database-url needs a value}"; shift 2 ;;
    --public-url) PUBLIC_URL="${2:?--public-url needs a value}"; shift 2 ;;
    --port) PORT="${2:?--port needs a value}"; shift 2 ;;
    --version) VERSION="${2:?--version needs a value}"; shift 2 ;;
    --ref) REF="${2:?--ref needs a value}"; REF_EXPLICIT="yes"; shift 2 ;;
    --no-start) START="no"; shift ;;
    --force) FORCE="yes"; shift ;;
    --help) usage; exit 0 ;;
    *) die "unknown flag: $1 (try --help)" ;;
  esac
done

command -v curl >/dev/null 2>&1 || die "curl is required"
if [ "$REF_EXPLICIT" = "no" ]; then
  case "$VERSION" in
    edge|latest) ;;
    v*) REF="$VERSION" ;;
    [0-9]*) REF="v$VERSION" ;;
  esac
fi
if [ "$START" = "yes" ]; then
  command -v docker >/dev/null 2>&1 || die "Docker is required (or pass --no-start)"
  docker compose version >/dev/null 2>&1 || die "Docker Compose v2 is required"
fi

umask 077
mkdir -p "$DIR"
[ "$FORCE" = "yes" ] || {
  [ ! -e "$DIR/compose.yaml" ] || die "$DIR/compose.yaml already exists (use --force to replace it)"
  [ ! -e "$DIR/.env" ] || die "$DIR/.env already exists (use --force to replace it)"
}

random_hex() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex "$1"
  elif command -v python3 >/dev/null 2>&1; then
    python3 -c 'import secrets,sys; print(secrets.token_hex(int(sys.argv[1])))' "$1"
  else
    od -An -N "$1" -tx1 /dev/urandom | tr -d ' \n'
  fi
}

say "Downloading deploy/compose.yaml from $REPO@$REF"
RAW_BASE="${RILLKIT_RAW_BASE:-https://raw.githubusercontent.com/$REPO/$REF/deploy}"
curl -fsSL "$RAW_BASE/compose.yaml" -o "$DIR/compose.yaml" \
  || die "could not download compose.yaml"

ADMIN_TOKEN="$(random_hex 24)"
PROJECT="$(basename "$DIR" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9_-' '-' | sed 's/-*$//')"
[ -n "$PROJECT" ] || PROJECT="rillkit"

if [ -n "$DATABASE_URL" ]; then
  PROFILES=""
  POSTGRES_PASSWORD=""
  DB_URL="$DATABASE_URL"
  say "Using your Postgres; the bundled database will not start."
else
  PROFILES="local-db"
  POSTGRES_PASSWORD="$(random_hex 16)"
  DB_URL="postgresql://rillkit:${POSTGRES_PASSWORD}@postgres:5432/rillkit"
fi

[ -n "$PUBLIC_URL" ] || PUBLIC_URL="http://localhost:$PORT"

{
  printf 'COMPOSE_PROFILES=%s\n' "$PROFILES"
  printf 'RILLKIT_PROJECT_NAME=%s\n' "$PROJECT"
  printf 'RILLKIT_VERSION=%s\n' "$VERSION"
  printf 'RILLKIT_PORT=%s\n' "$PORT"
  printf 'RILLKIT_SERVER_PUBLIC_URL=%s\n' "$PUBLIC_URL"
  printf 'RILLKIT_SERVER_ADMIN_TOKEN=%s\n' "$ADMIN_TOKEN"
  [ -n "$POSTGRES_PASSWORD" ] && printf 'POSTGRES_PASSWORD=%s\n' "$POSTGRES_PASSWORD"
  printf 'RILLKIT_SERVER_DATABASE_URL=%s\n' "$DB_URL"
} > "$DIR/.env"
chmod 600 "$DIR/.env"

say ""
say "Wrote $DIR/compose.yaml and $DIR/.env (mode 600)."
say "  Admin token: $ADMIN_TOKEN"
say "  URL:         http://localhost:$PORT"

if [ "$START" = "no" ]; then
  say ""
  say "Start it with:"
  say "  cd $DIR && docker compose up -d"
  exit 0
fi

say ""
say "Starting the control plane..."
(cd "$DIR" && docker compose up -d)

if command -v curl >/dev/null 2>&1; then
  n=0
  until curl -fsS "http://localhost:$PORT/api/health" >/dev/null 2>&1; do
    n=$((n + 1))
    [ "$n" -lt 30 ] || { say "It is still starting; check: cd $DIR && docker compose logs -f server"; exit 0; }
    sleep 2
  done
fi

say ""
say "RillKit is up at http://localhost:$PORT"
say "Sign in with the admin token above, then create an API key under Settings."
say "Point workers at your Redis/Valkey with RILLKIT_URL and RILLKIT_CONTROL=relay;"
say "in valkey mode they use your Redis and need no server."
