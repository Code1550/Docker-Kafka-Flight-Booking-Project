# seat-service/app/cache/__init__.py
#
# Marks the cache/ directory as a Python package so its modules can be
# imported using dot notation from anywhere inside the container:
#
#   from app.cache.redis_client      import create_redis_client, close_redis_client
#   from app.cache.seat_lock_manager import SeatLockManager
#
# This file is intentionally empty beyond this docstring.
#
# Why keep it empty?
#
# The cache/ package is unique to the Seat Service - no other service in
# this project has a dedicated cache layer because no other service uses
# Redis directly for application state. The Seat Service is Redis-first:
# Redis is not a cache sitting in front of a database, it IS the primary
# store for all seat lock state. This distinction is reflected in the
# package name - "cache" in the sense of a fast, ephemeral key-value
# store, not a cache that backs a slower persistent store.
#
# The two modules inside this package have a strict dependency order:
#
#   1. redis_client.py
#      → Defines create_redis_client() and close_redis_client() which
#        manage the lifecycle of the async Redis connection pool.
#
#        create_redis_client() is an async factory function rather than
#        a module-level singleton (unlike the SQLAlchemy engines in the
#        Booking and Payment Services). This is intentional:
#
#        The Redis connection pool must be created INSIDE the asyncio
#        event loop that will use it. redis-py's async client creates
#        internal asyncio.Queue objects for connection pooling during
#        initialisation - these Queue objects are bound to the event loop
#        that was running when they were created. Creating the pool at
#        module import time (before asyncio.run() starts the event loop)
#        would bind it to a temporary event loop that is immediately
#        discarded, causing "got Future attached to a different loop"
#        errors on the first async Redis operation.
#
#        By deferring pool creation to create_redis_client() which is
#        called inside run_consumer() (already inside the event loop),
#        the pool is always bound to the correct loop.
#
#   2. seat_lock_manager.py
#      → Defines SeatLockManager which wraps the Redis client with
#        seat-specific lock acquisition, ownership tracking, and release
#        logic. Must be imported after redis_client.py because it
#        type-hints the Redis client class defined there.
#
#        SeatLockManager contains NO module-level I/O - it is safe to
#        import in tests without a live Redis connection. The actual
#        Redis calls happen inside its async methods which are only
#        invoked after the client is connected and injected.
#
# Why is the Redis pool an async factory instead of a module singleton?
#
#   Compare with SQLAlchemy in the Booking/Payment Services:
#
#     SQLAlchemy (module-level singleton - correct):
#       engine = create_async_engine(url)   ← safe at module level
#       # create_async_engine() does NOT open connections or create
#       # asyncio objects at call time. Connections are opened lazily
#       # on the first async session.execute() call, at which point
#       # the event loop is already running.
#
#     redis-py (factory function - required):
#       _pool = None
#       async def create_redis_client():    ← must be called inside event loop
#           return redis.Redis(...)         # creates asyncio.Queue objects
#       # The Redis() constructor (or ConnectionPool()) creates internal
#       # asyncio primitives during __init__ in some redis-py versions,
#       # or on the first await call in others. Either way, deferring
#       # to an async factory is the safe, version-agnostic pattern.
#
#   This difference is subtle but causes real bugs in production when
#   developers assume all async clients follow the SQLAlchemy pattern.
#   The module docstring above exists specifically to document this so
#   future contributors do not "fix" the factory pattern back to a
#   module-level singleton thinking they are simplifying the code.
#
# Testing note:
#   Keeping this file empty means both modules are importable in tests
#   without a live Redis connection:
#
#       # Works - no Redis connection opened at import time
#       from app.cache.seat_lock_manager import SeatLockManager
#
#       # Test with fakeredis instead of real Redis
#       import fakeredis.aioredis
#       fake_redis = fakeredis.aioredis.FakeRedis()
#       lock_manager = SeatLockManager(redis_client=fake_redis, lock_ttl=300)
#
#   If __init__.py eagerly called create_redis_client() at import time,
#   every test file importing anything from app.cache would attempt a
#   real TCP connection to Redis - making tests impossible to run in CI
#   without a live Redis instance and adding ~100ms to every test run
#   just to establish and tear down the connection.
#
# Namespace note:
#   The cache/ package sits alongside app/ modules rather than inside
#   a generic utils/ or infrastructure/ package. This keeps the Seat
#   Service's directory structure flat and self-documenting:
#
#     app/
#       consumer.py          ← entry point and poll loop
#       service.py           ← seat lock domain logic
#       producer.py          ← Kafka event publishing
#       config.py            ← settings and environment variables
#       cache/
#         __init__.py        ← this file
#         redis_client.py    ← Redis connection pool lifecycle
#         seat_lock_manager.py  ← lock acquisition and release primitives
#
#   Anyone reading the directory tree immediately understands that Redis
#   is a first-class infrastructure concern of this service - not a
#   detail buried inside a generic utils module shared with other layers.