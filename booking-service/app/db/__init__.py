# booking-service/app/db/__init__.py
#
# Marks the db/ directory as a Python package so its modules can be
# imported using dot notation from anywhere inside the container:
#
#   from app.db.connection import get_db_session, engine
#   from app.db.models     import Booking, Flight
#
# This file is intentionally empty beyond this docstring.
#
# Why keep it empty?
#
# The two modules inside this package have a strict initialisation order:
#
#   1. connection.py must be imported first — it creates the SQLAlchemy
#      async engine and sessionmaker using environment variables loaded
#      from .env via pydantic-settings.
#
#   2. models.py must be imported after connection.py — it defines the
#      ORM mapped classes (Booking, Flight) that reference the engine's
#      metadata object.
#
# Putting any import statements here (e.g. `from app.db.models import *`)
# would collapse that order into a single implicit import chain that is
# hard to reason about and even harder to mock in tests. Keeping this
# file empty forces callers to be explicit about which module they need:
#
#   # In service.py — needs the session factory, not the models directly
#   from app.db.connection import get_db_session
#
#   # In alembic/env.py — needs the metadata for autogenerate
#   from app.db.models import Base
#
# Explicit imports also make circular import errors easier to diagnose
# because the dependency graph stays visible at the call site rather than
# being hidden inside a package-level __init__.py.