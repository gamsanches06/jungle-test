#!/bin/sh
# Creates the least-privileged application role. Schema objects are owned by
# POSTGRES_USER (used only by migrations); the service connects as
# jungle-app, which cannot UPDATE/DELETE/TRUNCATE the ledger.
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'jungle-app') THEN
    CREATE ROLE "jungle-app" LOGIN PASSWORD '${APP_DB_PASSWORD:-jungle-app-1234}';
  END IF;
END
\$\$;
GRANT CONNECT ON DATABASE "$POSTGRES_DB" TO "jungle-app";
SQL
