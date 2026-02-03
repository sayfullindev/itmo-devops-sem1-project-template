#!/bin/bash
set -e
echo "⟦ starting preparation ⟧"
# step 1: check go and postgres

echo "➤ looking for golang and postgres..."
if ! command -v go &> /dev/null; then
    echo "✕ error: golang missing"
    exit 1
fi

if ! command -v psql &> /dev/null; then
    echo "✕ error: postgres missing"
    exit 1
fi
echo "✔ go and postres intalled"
# step 2: install dependencies
echo "➤ installing go dependencies..."
go mod tidy
echo "✔ go dependencies intalled"

# step 3: set up for postgres

echo "➤ waiting for postgres to be ready..."
for i in {1..30}; do
    if pg_isready -h localhost -p 5432 &> /dev/null; then
        echo "✔ postgres is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "✕ error: postgres is not available after 30 attempts"
        exit 1
    fi
    echo "➤ attempt $i/30: Postgres is not ready yet, waiting..."
    sleep 1
done

echo "➤ setting up database..."

PGPASSWORD=val1dat0r psql -h localhost -U validator -d project-sem-1 <<'SQL'
CREATE TABLE IF NOT EXISTS prices(
    id INTEGER PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(100),
    price NUMERIC(10,2),
    create_date VARCHAR(50)
);

TRUNCATE TABLE prices;
SQL

echo "✔ database is ready"
echo "⟦ preparation completed successfully ⟧"