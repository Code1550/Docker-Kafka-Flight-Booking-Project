#!/bin/sh
set -e

echo "Waiting for PostgreSQL..."
until python3 -c "
import asyncio, asyncpg, os, sys
async def check():
    try:
        url = os.environ['POSTGRES_URL'].replace('+asyncpg','')
        conn = await asyncpg.connect(url)
        await conn.close()
    except Exception as e:
        print(f'DB not ready: {e}', flush=True)
        sys.exit(1)
asyncio.run(check())
"; do
  echo "   PostgreSQL not ready ? retrying in 2s..."
  sleep 2
done
echo "PostgreSQL is ready"

echo "Running Alembic migrations..."
alembic upgrade head
echo "Migrations applied"

echo "Starting Payment Service consumer..."
exec python3 -m app.consumer