# api-gateway/app/__init__.py
#
# Marks the app/ directory as a Python package so its modules can be
# imported using dot notation from anywhere inside the container:
#
#   from app.main     import app
#   from app.producer import publish_booking_event
#   from app.models   import BookingRequest
#
# This file is intentionally empty beyond this docstring — initialisation
# logic (database connections, Kafka producer setup, middleware registration)
# lives in main.py so it runs inside the FastAPI lifespan context manager
# rather than at import time. Putting startup logic here would cause it to
# execute whenever any module in the package is imported, including during
# testing, which makes tests slower and harder to isolate.