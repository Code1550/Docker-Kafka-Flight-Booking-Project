# seat-service/app/__init__.py
#
# Marks the app/ directory as a Python package so its modules can be
# imported using dot notation from anywhere inside the container:
#
#   from app.consumer import run_consumer
#   from app.producer import publish_seat_reserved, publish_seat_unavailable
#   from app.service  import SeatService
#   from app.cache    import redis_client, SeatLockManager
#
# This file is intentionally empty beyond this docstring.
#
# Why keep it empty?
#
# The Seat Service has a strict initialisation order that is more sensitive
# than the other services because it manages distributed state (Redis locks)
# that outlives any single process. Getting the order wrong can leave seats
# permanently locked in Redis with no Kafka event published downstream —
# the worst failure mode in the Seat Service.
#
# The correct initialisation order enforced by consumer.py is:
#
#   1. app/config.py
#      → Loaded first by every other module. Reads and validates all
#        environment variables via pydantic-settings including:
#          - KAFKA_BOOTSTRAP_SERVERS  (required for consumer and producer)
#          - REDIS_URL                (required for lock manager)
#          - REDIS_PASSWORD           (required for authenticated Redis)
#          - SEAT_LOCK_TTL_SECONDS    (required for lock acquisition)
#          - TOPIC_BOOKING_REQUESTED  (consumer subscription topic)
#          - TOPIC_PAYMENT_FAILED     (consumer subscription topic)
#          - TOPIC_SEAT_RESERVED      (producer publish topic)
#          - TOPIC_SEAT_UNAVAILABLE   (producer publish topic)
#        A missing or invalid variable raises a clear ValidationError here
#        rather than a cryptic KeyError inside a lock acquisition mid-event.
#
#   2. app/cache/redis_client.py
#      → Creates the async Redis connection pool using REDIS_URL from
#        config.py. Must come before app/service.py because SeatService
#        receives the Redis client as a dependency (injected by consumer.py)
#        rather than importing it directly — this keeps SeatService testable
#        with a fakeredis instance without patching module-level globals.
#
#        Unlike the DB engines in other services, the Redis client is NOT
#        a module-level singleton here — it is created inside run_consumer()
#        and passed into SeatService.__init__(). This makes the lifetime of
#        the Redis connection pool explicit and controllable: the pool opens
#        when the consumer starts and closes in the finally block on shutdown,
#        even if the consumer loop crashes mid-lock.
#
#   3. app/cache/seat_lock_manager.py
#      → Defines SeatLockManager, which wraps the Redis client with seat-
#        specific lock acquisition, extension, and release logic. Imported
#        after redis_client.py because it type-hints against the Redis client
#        class defined there. Contains no module-level I/O — safe to import
#        in tests without a live Redis connection.
#
#   4. app/service.py
#      → Defines SeatService, which holds the seat lock business logic.
#        Receives a SeatLockManager instance via __init__ (dependency
#        injection) rather than importing it at module level. This is the
#        key design decision that makes the Seat Service testable without
#        a running Redis instance — pass in a SeatLockManager backed by
#        fakeredis and the service behaves identically to production.
#
#   5. app/producer.py
#      → Creates the Kafka producer singleton using KAFKA_BOOTSTRAP_SERVERS
#        from config.py. Initialised after the Redis layer so that if the
#        Redis client fails to connect (wrong password, wrong host), no
#        Kafka producer is created and no partial events can be published
#        before the consumer is fully ready.
#
#   6. app/consumer.py
#      → The entry point. Orchestrates the full startup sequence in
#        run_consumer():
#          a. Start Prometheus metrics server on port 8003
#          b. Create async Redis client and verify connectivity
#          c. Instantiate SeatLockManager with the Redis client
#          d. Instantiate SeatService with the SeatLockManager
#          e. Build the Kafka consumer and subscribe to both topics
#          f. Create the initial heartbeat sentinel file
#          g. Run the poll loop until SIGTERM
#          h. Release all held locks and close Redis pool on shutdown
#
# Why is shutdown order especially critical for the Seat Service?
#
#   When SIGTERM fires (docker compose stop), the Seat Service may be
#   mid-way through a lock operation:
#
#   Scenario A — SIGTERM during lock acquisition (SET NX EX):
#     The Redis command is atomic — it either completes or does not.
#     If the process dies before the command completes, Redis never sets
#     the key and the seat remains available. Safe.
#
#   Scenario B — SIGTERM after lock acquisition, before Kafka publish:
#     The Redis lock is now held but no SeatReservedEvent has been
#     published. Booking Service never confirms the booking. The seat
#     lock will expire after SEAT_LOCK_TTL_SECONDS (default 300s),
#     releasing the seat automatically — eventually consistent but
#     the passenger experiences a delayed failure notification.
#     The SIGTERM handler in consumer.py prevents this by finishing
#     the current message before checking the shutdown flag.
#
#   Scenario C — SIGTERM after Kafka publish, before offset commit:
#     The SeatReservedEvent is published but the offset is not committed.
#     On restart, the same booking.requested message is re-delivered.
#     The idempotency check in SeatService checks whether a lock for
#     this booking_id already exists in Redis before attempting a second
#     acquisition — preventing a duplicate SeatReservedEvent.
#
# Testing note:
#   Keeping this file empty means each module can be imported in isolation:
#
#       from app.service import SeatService          # no Redis required
#       from app.cache.seat_lock_manager import SeatLockManager  # no Redis
#       from app.producer import publish_seat_reserved  # no Kafka required
#
#   If __init__.py imported app.cache.redis_client eagerly, every test
#   file would attempt to connect to Redis at import time — making tests
#   impossible to run in a CI environment without a live Redis instance.
#
#   The dependency injection pattern (Redis client passed into SeatService
#   rather than imported by it) means tests can pass a fakeredis instance
#   without patching any module-level state:
#
#       import fakeredis.aioredis
#       from app.cache.seat_lock_manager import SeatLockManager
#       from app.service import SeatService
#
#       redis = fakeredis.aioredis.FakeRedis()
#       lock_manager = SeatLockManager(redis_client=redis)
#       service = SeatService(lock_manager=lock_manager)
#       # Full service testable with no live infrastructure