# notification-service/app/service.py
#
# Core business logic for the Notification Service.
#
# NotificationService is the single place where notification domain rules
# live. It is deliberately kept free of Kafka concerns — it receives a
# validated BookingConfirmedEvent, renders templates via Jinja2, dispatches
# notifications via SendGrid (email) and Twilio (SMS), and returns a result
# dict that consumer.py uses for metrics and offset commit decisions.
#
# Responsibilities:
#   - send_confirmation         → orchestrate email + SMS dispatch
#   - _send_email               → render HTML/text templates, call SendGrid
#   - _send_sms                 → render SMS template, call Twilio
#   - _render_email_template    → Jinja2 HTML + plain-text rendering
#   - _render_sms_template      → Jinja2 SMS message rendering
#   - _mock_email               → return synthetic result without SendGrid
#   - _mock_sms                 → return synthetic result without Twilio
#   - _build_jinja_env          → configure Jinja2 with custom filters
#   - _format_datetime          → locale-aware datetime Jinja2 filter
#   - _format_currency          → locale-aware currency Jinja2 filter
#
# Mock mode:
#   When SENDGRID_MOCK_ENABLED=true (default in .env), all email dispatch
#   is replaced with a log statement and a synthetic result. The full
#   template rendering pipeline still runs so template errors are caught
#   in local development before they reach production.
#   Same pattern for TWILIO_MOCK_ENABLED=true for SMS.
#
# Template structure (in templates/ directory):
#   templates/
#     email/
#       booking_confirmed.html    → HTML email body (Jinja2)
#       booking_confirmed.txt     → plain-text fallback (Jinja2)
#       subject.txt               → email subject line (Jinja2)
#     sms/
#       booking_confirmed.txt     → SMS message body (Jinja2, <160 chars)
#
# Jinja2 template context variables available in all templates:
#   {{ passenger.first_name }}        → "Jane"
#   {{ passenger.last_name }}         → "Doe"
#   {{ booking_id }}                  → "c3d4e5f6-..."
#   {{ flight.flight_number }}        → "AA123"
#   {{ flight.origin }}               → "JFK"
#   {{ flight.destination }}          → "LAX"
#   {{ flight.departure_time }}       → datetime object
#   {{ flight.arrival_time }}         → datetime object
#   {{ flight.seat_number }}          → "12A"
#   {{ flight.seat_class }}           → "economy"
#   {{ payment.amount }}              → Decimal("299.99")
#   {{ payment.currency }}            → "USD"
#   {{ payment.payment_method }}      → "stripe"
#   {{ payment.transaction_ref }}     → "pi_3OqrV2Lk..."

from __future__ import annotations

import logging
import os
from datetime import datetime
from decimal import Decimal
from typing import Any

import pytz
from babel import Locale
from babel.dates import format_datetime as babel_format_datetime
from babel.numbers import format_currency as babel_format_currency
from jinja2 import (
    Environment,
    FileSystemLoader,
    select_autoescape,
    TemplateNotFound,
    TemplateError,
)
from premailer import transform as premailer_transform
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail,
    Email,
    To,
    Content,
    MimeType,
)
from twilio.rest import Client as TwilioClient
from twilio.base.exceptions import TwilioRestException

from app.config import settings
from shared.schemas import BookingConfirmedEvent, NotificationType


logger = logging.getLogger("notification-service.service")


# ──────────────────────────────────────────────────────────────────────────────
# TEMPLATE DIRECTORY
# Resolved relative to this file's location so it works regardless of
# the working directory the container is started from.
# ──────────────────────────────────────────────────────────────────────────────

TEMPLATES_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "templates",
)


# ──────────────────────────────────────────────────────────────────────────────
# NOTIFICATION SERVICE
# ──────────────────────────────────────────────────────────────────────────────

class NotificationService:
    """
    Stateless service class containing all notification domain logic.

    Configures SendGrid, Twilio, and Jinja2 once on __init__ and reuses
    the same clients across all messages. All public methods are async
    because they perform I/O (SendGrid HTTPS calls, Twilio HTTPS calls).

    SendGrid and Twilio clients are initialised lazily inside __init__()
    rather than at module level so unit tests can instantiate
    NotificationService without valid API credentials — just pass
    mock=True or set SENDGRID_MOCK_ENABLED=true in the environment.
    """

    def __init__(self) -> None:
        """
        Initialise SendGrid client, Twilio client, and Jinja2 environment.

        SendGrid:
          Uses the official sendgrid-python SDK which wraps the SendGrid
          Web API v3. The SDK is synchronous — we wrap its calls in
          asyncio.run_in_executor() in _send_email() to avoid blocking
          the async event loop during the HTTPS request.

        Twilio:
          Uses the official twilio-python SDK. Also synchronous —
          same run_in_executor() pattern as SendGrid.

        Jinja2:
          FileSystemLoader reads templates from the templates/ directory.
          autoescape=True prevents XSS in HTML emails by escaping special
          characters in all template variables by default.
          Templates are compiled to bytecode on first use and cached in
          memory for the lifetime of the process — subsequent renders
          skip compilation and only perform variable substitution.
        """
        self._mock_email_enabled: bool = settings.SENDGRID_MOCK_ENABLED
        self._mock_sms_enabled:   bool = settings.TWILIO_MOCK_ENABLED

        # ── SENDGRID CLIENT ───────────────────────────────────────────────────
        if not self._mock_email_enabled:
            self._sendgrid = SendGridAPIClient(
                api_key=settings.SENDGRID_API_KEY,
            )
            logger.info(
                "SendGrid client initialised with real API key",
                extra={"key_prefix": settings.SENDGRID_API_KEY[:12] + "..."},
            )
        else:
            self._sendgrid = None
            logger.info(
                "SendGrid client NOT initialised — mock mode enabled. "
                "No real emails will be sent."
            )

        # ── TWILIO CLIENT ─────────────────────────────────────────────────────
        if not self._mock_sms_enabled:
            self._twilio = TwilioClient(
                username=settings.TWILIO_ACCOUNT_SID,
                password=settings.TWILIO_AUTH_TOKEN,
            )
            logger.info(
                "Twilio client initialised",
                extra={
                    "account_sid_prefix": settings.TWILIO_ACCOUNT_SID[:8] + "...",
                },
            )
        else:
            self._twilio = None
            logger.info(
                "Twilio client NOT initialised — mock mode enabled. "
                "No real SMS messages will be sent."
            )

        # ── JINJA2 ENVIRONMENT ────────────────────────────────────────────────
        # Build the Jinja2 environment regardless of mock mode —
        # templates are rendered even in mock mode so template errors
        # are caught during local development before reaching production.
        self._jinja_env: Environment = self._build_jinja_env()

        logger.info(
            "NotificationService initialised",
            extra={
                "templates_dir":      TEMPLATES_DIR,
                "mock_email_enabled": self._mock_email_enabled,
                "mock_sms_enabled":   self._mock_sms_enabled,
                "from_email":         settings.SENDGRID_FROM_EMAIL,
                "from_name":          settings.SENDGRID_FROM_NAME,
            },
        )


    # ── PUBLIC API ────────────────────────────────────────────────────────────

    async def send_confirmation(
        self,
        event: BookingConfirmedEvent,
    ) -> dict[str, Any]:
        """
        Dispatch a booking confirmation notification to the passenger.

        Dispatches email and/or SMS based on the notification_type field
        in the BookingConfirmedEvent:
          - NotificationType.EMAIL → email only
          - NotificationType.SMS   → SMS only (requires passenger.phone)
          - NotificationType.BOTH  → email + SMS

        Both email and SMS use the full Jinja2 template rendering pipeline —
        even in mock mode — so template errors surface in development.

        Args:
            event: The deserialized BookingConfirmedEvent from Kafka.
                   Contains passenger details, flight info, and payment
                   details needed to populate the confirmation templates.

        Returns:
            Result dict:
            {
              "email_sent":          bool,
              "sms_sent":            bool,
              "sendgrid_message_id": str | None,   # None in mock mode
              "twilio_sid":          str | None,   # None in mock/email-only mode
              "mock":                bool,         # True if any mock was used
              "notification_type":   str,          # "email" | "sms" | "both"
              "render_duration_ms":  float,        # Jinja2 rendering time
            }

        Raises:
            TemplateError:       If a Jinja2 template fails to render.
            Exception:           If SendGrid or Twilio API calls fail.
                                 consumer.py classifies these as permanent
                                 or transient and decides retry vs DLQ.
        """
        notification_type = event.notification_type

        logger.info(
            "Dispatching booking confirmation notification",
            extra={
                "booking_id":        event.booking_id,
                "correlation_id":    event.correlation_id,
                "notification_type": notification_type.value,
                "passenger_email":   event.passenger.email,
                "flight_number":     event.flight.flight_number,
            },
        )

        result: dict[str, Any] = {
            "email_sent":          False,
            "sms_sent":            False,
            "sendgrid_message_id": None,
            "twilio_sid":          None,
            "mock":                False,
            "notification_type":   notification_type.value,
            "render_duration_ms":  0.0,
        }

        # Build the Jinja2 template context — shared by email and SMS
        context = self._build_template_context(event)

        # ── EMAIL ─────────────────────────────────────────────────────────────
        if notification_type in (NotificationType.EMAIL, NotificationType.BOTH):
            email_result = await self._send_email(
                event=event,
                context=context,
            )
            result["email_sent"]          = email_result["sent"]
            result["sendgrid_message_id"] = email_result.get("message_id")
            result["render_duration_ms"]  = email_result.get("render_duration_ms", 0.0)
            if email_result.get("mock"):
                result["mock"] = True

        # ── SMS ───────────────────────────────────────────────────────────────
        if notification_type in (NotificationType.SMS, NotificationType.BOTH):
            if not event.passenger.phone:
                # Phone number missing — log warning but do not fail the
                # entire notification. Email was (hopefully) already sent.
                # consumer.py will see email_sent=True and succeed.
                logger.warning(
                    "SMS requested but passenger.phone is missing — skipping SMS",
                    extra={
                        "booking_id":        event.booking_id,
                        "notification_type": notification_type.value,
                    },
                )
            else:
                sms_result = await self._send_sms(
                    event=event,
                    context=context,
                )
                result["sms_sent"]   = sms_result["sent"]
                result["twilio_sid"] = sms_result.get("sid")
                if sms_result.get("mock"):
                    result["mock"] = True

        logger.info(
            "Notification dispatch complete",
            extra={
                "booking_id":          event.booking_id,
                "email_sent":          result["email_sent"],
                "sms_sent":            result["sms_sent"],
                "sendgrid_message_id": result["sendgrid_message_id"],
                "twilio_sid":          result["twilio_sid"],
                "mock":                result["mock"],
            },
        )

        return result


    # ── EMAIL DISPATCH ────────────────────────────────────────────────────────

    async def _send_email(
        self,
        event: BookingConfirmedEvent,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Render email templates and dispatch via SendGrid Web API v3.

        Renders three templates:
          1. email/subject.txt      → email subject line (single line)
          2. email/booking_confirmed.html → HTML body (with CSS inlined)
          3. email/booking_confirmed.txt  → plain-text fallback body

        Both HTML and plain-text bodies are sent so email clients that
        cannot render HTML (some corporate mail servers, screen readers)
        still receive a readable confirmation.

        CSS inlining via premailer:
          premailer.transform() converts all <style> block rules into
          inline style attributes on individual elements before sending.
          This ensures the email renders correctly in Gmail, Outlook,
          and Apple Mail which strip <style> blocks for security.

        Args:
            event:   The BookingConfirmedEvent with passenger/flight/payment.
            context: Pre-built Jinja2 template context dict.

        Returns:
            {
              "sent":             bool,
              "message_id":       str | None,   # SendGrid X-Message-Id
              "mock":             bool,
              "render_duration_ms": float,
            }

        Raises:
            TemplateNotFound: If email templates are missing from templates/
            TemplateError:    If Jinja2 rendering fails (syntax error, etc.)
            Exception:        If SendGrid API returns non-2xx response
        """
        import time
        render_start = time.monotonic()

        # ── RENDER TEMPLATES ──────────────────────────────────────────────────
        subject    = await self._render_email_template("subject.txt",                context)
        html_body  = await self._render_email_template("booking_confirmed.html",     context)
        text_body  = await self._render_email_template("booking_confirmed.txt",      context)

        render_duration_ms = (time.monotonic() - render_start) * 1000

        # Inline CSS into HTML body for email client compatibility.
        # premailer.transform() is CPU-bound (DOM traversal) but fast
        # enough (~5ms) that run_in_executor is not needed here.
        html_body_inlined = premailer_transform(
            html=html_body,
            # Remove <style> blocks after inlining — email clients
            # would otherwise apply styles twice (inline + block)
            remove_classes=False,
            # Preserve media queries (used for responsive email layout)
            # which cannot be inlined — leave them in <style> blocks
            cssutils_logging_level=logging.CRITICAL,  # silence cssutils warnings
        )

        logger.debug(
            "Email templates rendered",
            extra={
                "booking_id":        event.booking_id,
                "subject":           subject.strip(),
                "render_duration_ms": round(render_duration_ms, 2),
                "html_length":       len(html_body_inlined),
                "text_length":       len(text_body),
            },
        )

        # ── MOCK MODE ─────────────────────────────────────────────────────────
        if self._mock_email_enabled:
            return await self._mock_email(
                event=event,
                subject=subject.strip(),
                html_body=html_body_inlined,
                text_body=text_body,
                render_duration_ms=render_duration_ms,
            )

        # ── REAL SENDGRID DISPATCH ────────────────────────────────────────────
        return await self._dispatch_sendgrid(
            event=event,
            subject=subject.strip(),
            html_body=html_body_inlined,
            text_body=text_body,
            render_duration_ms=render_duration_ms,
        )


    async def _dispatch_sendgrid(
        self,
        event: BookingConfirmedEvent,
        subject: str,
        html_body: str,
        text_body: str,
        render_duration_ms: float,
    ) -> dict[str, Any]:
        """
        Send an email via the SendGrid Web API v3.

        Constructs a SendGrid Mail object with both HTML and plain-text
        content parts and dispatches via the sendgrid-python SDK.

        SendGrid returns HTTP 202 Accepted for successfully queued emails —
        not 200 OK. The 202 means SendGrid has accepted responsibility for
        delivery but the email has not yet been sent. Delivery status
        (delivered, bounced, spam, etc.) is available via SendGrid webhooks
        or the SendGrid Activity Feed API.

        Args:
            event:              The BookingConfirmedEvent.
            subject:            Rendered email subject line.
            html_body:          CSS-inlined HTML email body.
            text_body:          Plain-text email body.
            render_duration_ms: Time taken to render templates.

        Returns:
            {"sent": True, "message_id": str, "mock": False, ...}

        Raises:
            Exception: If SendGrid returns non-202 status or network error.
                       consumer.py classifies as permanent or transient.
        """
        import asyncio

        message = Mail(
            from_email=Email(
                email=settings.SENDGRID_FROM_EMAIL,
                name=settings.SENDGRID_FROM_NAME,
            ),
            to_emails=To(
                email=event.passenger.email,
                name=f"{event.passenger.first_name} {event.passenger.last_name}",
            ),
            subject=subject,
        )

        # Add both content types — HTML preferred, plain-text fallback
        message.content = [
            Content(MimeType.text, text_body),
            Content(MimeType.html, html_body),
        ]

        # Add custom headers for tracking and audit
        message.header = {
            # Allows correlation of SendGrid delivery events with internal logs
            "X-Booking-ID":     event.booking_id,
            "X-Correlation-ID": event.correlation_id,
            # Prevents SendGrid from treating this as a marketing email
            # and applying unsubscribe footers to transactional confirmations
            "X-SMTPAPI": '{"category": ["transactional", "booking_confirmation"]}',
        }

        # SendGrid SDK is synchronous — run in thread executor to avoid
        # blocking the asyncio event loop during the HTTPS request to
        # api.sendgrid.com which can take 200-500ms.
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: self._sendgrid.send(message),
        )

        # SendGrid returns 202 Accepted for successfully queued emails
        if response.status_code not in (200, 202):
            raise Exception(
                f"SendGrid API returned unexpected status {response.status_code}: "
                f"{response.body}"
            )

        # Extract the SendGrid message ID from the response headers.
        # Format: X-Message-Id: <uuid>
        # Used to correlate delivery events from SendGrid webhooks with
        # our internal logs — the primary audit trail for email delivery.
        message_id = response.headers.get("X-Message-Id", "unknown")

        logger.info(
            "Email sent via SendGrid",
            extra={
                "booking_id":        event.booking_id,
                "passenger_email":   event.passenger.email,
                "sendgrid_message_id": message_id,
                "status_code":       response.status_code,
                "render_duration_ms": round(render_duration_ms, 2),
            },
        )

        return {
            "sent":               True,
            "message_id":         message_id,
            "mock":               False,
            "render_duration_ms": render_duration_ms,
        }


    async def _mock_email(
        self,
        event: BookingConfirmedEvent,
        subject: str,
        html_body: str,
        text_body: str,
        render_duration_ms: float,
    ) -> dict[str, Any]:
        """
        Return a synthetic email result without calling SendGrid.

        Templates are fully rendered before this function is called so
        template errors are still caught in mock mode — local development
        exercises the same rendering code path as production.

        Logs the email subject and first 200 characters of the plain-text
        body so developers can verify template output during local testing
        without checking the SendGrid dashboard.

        Args:
            event:              The BookingConfirmedEvent.
            subject:            Rendered email subject line.
            html_body:          CSS-inlined HTML email body.
            text_body:          Plain-text email body.
            render_duration_ms: Time taken to render templates.

        Returns:
            {"sent": True, "message_id": "mock-...", "mock": True, ...}
        """
        import uuid
        mock_message_id = f"mock-{uuid.uuid4().hex[:16]}"

        logger.info(
            "MOCK EMAIL (not sent to SendGrid)",
            extra={
                "booking_id":          event.booking_id,
                "to":                  event.passenger.email,
                "subject":             subject,
                "text_preview":        text_body[:200],
                "sendgrid_message_id": mock_message_id,
                "render_duration_ms":  round(render_duration_ms, 2),
            },
        )

        return {
            "sent":               True,
            "message_id":         mock_message_id,
            "mock":               True,
            "render_duration_ms": render_duration_ms,
        }


    # ── SMS DISPATCH ──────────────────────────────────────────────────────────

    async def _send_sms(
        self,
        event: BookingConfirmedEvent,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Render SMS template and dispatch via Twilio REST API.

        SMS messages must be <= 160 characters for a single-segment message.
        The template sms/booking_confirmed.txt is designed to fit within
        this limit. Messages exceeding 160 characters are automatically
        split into multi-segment SMS by Twilio, which costs more and may
        arrive out of order on some carriers.

        Args:
            event:   The BookingConfirmedEvent with passenger.phone populated.
            context: Pre-built Jinja2 template context dict.

        Returns:
            {
              "sent": bool,
              "sid":  str | None,   # Twilio message SID
              "mock": bool,
            }

        Raises:
            TwilioRestException: If Twilio rejects the SMS (invalid number,
                                 opted-out recipient, etc.)
            Exception:           If Twilio API is unreachable.
        """
        # Render the SMS template
        sms_body = await self._render_sms_template(
            "booking_confirmed.txt",
            context,
        )

        # Warn if the SMS exceeds the single-segment limit
        if len(sms_body) > 160:
            logger.warning(
                "SMS body exceeds 160 characters — will be sent as multi-segment",
                extra={
                    "booking_id":   event.booking_id,
                    "sms_length":   len(sms_body),
                    "sms_segments": (len(sms_body) // 160) + 1,
                },
            )

        # ── MOCK MODE ─────────────────────────────────────────────────────────
        if self._mock_sms_enabled:
            return await self._mock_sms(
                event=event,
                sms_body=sms_body,
            )

        # ── REAL TWILIO DISPATCH ──────────────────────────────────────────────
        return await self._dispatch_twilio(
            event=event,
            sms_body=sms_body,
        )


    async def _dispatch_twilio(
        self,
        event: BookingConfirmedEvent,
        sms_body: str,
    ) -> dict[str, Any]:
        """
        Send an SMS via the Twilio REST API.

        Twilio's Python SDK is synchronous — wrapped in run_in_executor
        to avoid blocking the asyncio event loop during the HTTPS request.

        Twilio returns a message SID (e.g. SM1234...) for successfully
        queued SMS messages. The SID is the Twilio equivalent of SendGrid's
        message_id — used to track delivery status via Twilio webhooks.

        Args:
            event:    The BookingConfirmedEvent with passenger.phone.
            sms_body: The rendered SMS message text.

        Returns:
            {"sent": True, "sid": str, "mock": False}

        Raises:
            TwilioRestException: e.g. 21211 (invalid phone), 21610 (opted-out)
        """
        import asyncio

        loop = asyncio.get_event_loop()

        try:
            message = await loop.run_in_executor(
                None,
                lambda: self._twilio.messages.create(
                    body=sms_body,
                    from_=settings.TWILIO_FROM_NUMBER,
                    to=event.passenger.phone,
                ),
            )

            logger.info(
                "SMS sent via Twilio",
                extra={
                    "booking_id":    event.booking_id,
                    "to_number":     event.passenger.phone,
                    "twilio_sid":    message.sid,
                    "sms_status":    message.status,
                    "sms_length":    len(sms_body),
                },
            )

            return {
                "sent": True,
                "sid":  message.sid,
                "mock": False,
            }

        except TwilioRestException as exc:
            # Re-raise so consumer.py can classify as permanent or transient.
            # Twilio error codes 21211, 21610 etc. are classified as
            # permanent in consumer.py's _is_permanent_error().
            logger.warning(
                "Twilio SMS dispatch failed",
                extra={
                    "booking_id":  event.booking_id,
                    "to_number":   event.passenger.phone,
                    "twilio_code": exc.code,
                    "twilio_msg":  exc.msg,
                },
            )
            raise


    async def _mock_sms(
        self,
        event: BookingConfirmedEvent,
        sms_body: str,
    ) -> dict[str, Any]:
        """
        Return a synthetic SMS result without calling Twilio.

        Logs the SMS body so developers can verify template output during
        local testing without a real Twilio account or phone number.

        Args:
            event:    The BookingConfirmedEvent.
            sms_body: The rendered SMS message text.

        Returns:
            {"sent": True, "sid": "mock-...", "mock": True}
        """
        import uuid
        mock_sid = f"mock-SM{uuid.uuid4().hex[:16]}"

        logger.info(
            "MOCK SMS (not sent via Twilio)",
            extra={
                "booking_id": event.booking_id,
                "to_number":  event.passenger.phone,
                "sms_body":   sms_body,
                "sms_length": len(sms_body),
                "twilio_sid": mock_sid,
            },
        )

        return {
            "sent": True,
            "sid":  mock_sid,
            "mock": True,
        }


    # ── TEMPLATE RENDERING ────────────────────────────────────────────────────

    async def _render_email_template(
        self,
        template_name: str,
        context: dict[str, Any],
    ) -> str:
        """
        Render a Jinja2 email template with the given context.

        Loads templates from the templates/email/ subdirectory.
        Template compilation is cached by the Jinja2 environment —
        subsequent renders reuse the compiled bytecode.

        Args:
            template_name: Filename within templates/email/ e.g. "subject.txt"
            context:       Dict of variables available in the template.

        Returns:
            Rendered template string.

        Raises:
            TemplateNotFound: If the template file does not exist.
            TemplateError:    If the template has a syntax error or
                              references an undefined variable.
        """
        import asyncio

        loop = asyncio.get_event_loop()

        # Jinja2 template rendering is CPU-bound (bytecode execution).
        # For short templates it is fast enough to run in the event loop
        # directly. For large HTML templates with complex logic, wrapping
        # in run_in_executor prevents blocking during peak load.
        rendered = await loop.run_in_executor(
            None,
            lambda: self._jinja_env.get_template(
                f"email/{template_name}"
            ).render(context),
        )

        return rendered


    async def _render_sms_template(
        self,
        template_name: str,
        context: dict[str, Any],
    ) -> str:
        """
        Render a Jinja2 SMS template with the given context.

        Loads templates from the templates/sms/ subdirectory.
        SMS templates must produce output <= 160 characters — the
        single-segment SMS limit. The template should be kept short
        and include only the most essential booking details.

        Args:
            template_name: Filename within templates/sms/ e.g. "booking_confirmed.txt"
            context:       Dict of variables available in the template.

        Returns:
            Rendered SMS body string.

        Raises:
            TemplateNotFound: If the template file does not exist.
            TemplateError:    If the template has a syntax or variable error.
        """
        import asyncio

        loop = asyncio.get_event_loop()

        rendered = await loop.run_in_executor(
            None,
            lambda: self._jinja_env.get_template(
                f"sms/{template_name}"
            ).render(context),
        )

        # Strip leading/trailing whitespace — Jinja2 templates often have
        # trailing newlines that would count toward the 160-char limit.
        return rendered.strip()


    # ── TEMPLATE CONTEXT BUILDER ──────────────────────────────────────────────

    def _build_template_context(
        self,
        event: BookingConfirmedEvent,
    ) -> dict[str, Any]:
        """
        Build the Jinja2 template context dict from a BookingConfirmedEvent.

        Flattens the nested Pydantic event structure into a flat context dict
        with top-level keys that are more readable in templates:
          {{ passenger.first_name }} instead of {{ event.passenger.first_name }}

        Also pre-computes derived values (full name, formatted price, booking
        reference) so templates do not need to compute them inline — keeping
        templates simple and testable.

        Args:
            event: The BookingConfirmedEvent with all booking details.

        Returns:
            Dict of template variables ready to pass to Jinja2 render().
        """
        # Format the booking reference as an uppercase short form for
        # easy reading on the confirmation — last 8 chars of the UUID.
        booking_ref = event.booking_id.replace("-", "").upper()[-8:]

        return {
            # ── BOOKING METADATA ──────────────────────────────────────────────
            "booking_id":  event.booking_id,
            "booking_ref": booking_ref,   # e.g. "AB12CD34" — more readable
            "correlation_id": event.correlation_id,

            # ── PASSENGER ─────────────────────────────────────────────────────
            "passenger": event.passenger,
            "passenger_name": (
                f"{event.passenger.first_name} {event.passenger.last_name}"
            ),

            # ── FLIGHT ────────────────────────────────────────────────────────
            "flight": event.flight,

            # Pre-formatted departure and arrival for simple templates
            # Full locale-aware formatting available via Jinja2 filters:
            #   {{ flight.departure_time | format_datetime }}
            "departure_date": event.flight.departure_time.strftime("%B %d, %Y"),
            "departure_time": event.flight.departure_time.strftime("%I:%M %p"),
            "arrival_date":   event.flight.arrival_time.strftime("%B %d, %Y"),
            "arrival_time":   event.flight.arrival_time.strftime("%I:%M %p"),

            # ── PAYMENT ───────────────────────────────────────────────────────
            "payment": event.payment,

            # Pre-formatted price string for simple templates
            # Full locale-aware formatting available via Jinja2 filter:
            #   {{ payment.amount | format_currency(payment.currency) }}
            "formatted_price": (
                f"{event.payment.currency} "
                f"{float(event.payment.amount):,.2f}"
            ),

            # ── SUPPORT METADATA ──────────────────────────────────────────────
            # Included in the email footer for customer support reference
            "support_email":   settings.NOTIFICATION_FROM_EMAIL,
            "current_year":    datetime.utcnow().year,
        }


    # ── JINJA2 ENVIRONMENT ────────────────────────────────────────────────────

    def _build_jinja_env(self) -> Environment:
        """
        Configure and return the Jinja2 template environment.

        Sets up:
          - FileSystemLoader pointing at the templates/ directory
          - autoescape=True for HTML emails (prevents XSS from PII fields)
          - Custom filters: format_datetime, format_currency, pluralize
          - Trim blocks and lstrip blocks for cleaner template whitespace

        Returns:
            A configured Jinja2 Environment instance with compiled templates
            cached for the lifetime of the process.

        Raises:
            OSError: If the templates/ directory does not exist.
                     Caught during __init__ — a missing templates/ directory
                     is a deployment error that should fail fast.
        """
        env = Environment(
            # Load templates from the templates/ directory on disk
            loader=FileSystemLoader(TEMPLATES_DIR),

            # Enable autoescape for .html templates — escape special HTML
            # characters in all template variables by default.
            # Plain-text (.txt) templates do not need escaping.
            autoescape=select_autoescape(
                enabled_extensions=("html", "htm"),
                disabled_extensions=("txt",),
                default=False,
            ),

            # Remove the newline after block tags ({% if %}, {% for %})
            # Prevents extra blank lines in rendered HTML output from
            # Jinja2's block tag formatting.
            trim_blocks=True,

            # Strip leading whitespace from lines starting with a block tag.
            # Combined with trim_blocks, produces clean HTML output without
            # irregular indentation caused by Jinja2 template formatting.
            lstrip_blocks=True,
        )

        # ── CUSTOM FILTERS ────────────────────────────────────────────────────
        # Registered as Jinja2 filters so templates can call them directly:
        #   {{ flight.departure_time | format_datetime }}
        #   {{ payment.amount | format_currency(payment.currency) }}
        #   {{ seat_count | pluralize("seat", "seats") }}

        env.filters["format_datetime"]  = self._format_datetime
        env.filters["format_currency"]  = self._format_currency
        env.filters["pluralize"]        = self._pluralize

        logger.debug(
            "Jinja2 environment configured",
            extra={"templates_dir": TEMPLATES_DIR},
        )

        return env


    # ── JINJA2 CUSTOM FILTERS ─────────────────────────────────────────────────

    @staticmethod
    def _format_datetime(
        dt: datetime,
        timezone: str = "UTC",
        format: str = "medium",
        locale: str = "en_US",
    ) -> str:
        """
        Jinja2 filter: format a datetime object with locale-aware formatting.

        Uses Babel's format_datetime() for locale-aware output so passengers
        see dates and times in their preferred format:
          en_US: "Sep 1, 2024, 8:00:00 AM"
          en_GB: "1 Sep 2024, 08:00:00"
          de_DE: "01.09.2024, 08:00:00"

        Also converts to the passenger's timezone so departure times are
        displayed in the correct local time (e.g. "8:00 AM PST" for a
        Los Angeles departure, not "16:00 UTC").

        Usage in templates:
          {{ flight.departure_time | format_datetime(timezone="America/Los_Angeles") }}
          {{ flight.departure_time | format_datetime(format="short", locale="en_GB") }}

        Args:
            dt:       The datetime to format (assumed UTC if naive).
            timezone: IANA timezone name e.g. "America/New_York".
                      Defaults to "UTC".
            format:   Babel format style: "full", "long", "medium", "short".
                      Defaults to "medium".
            locale:   BCP 47 locale string e.g. "en_US", "en_GB", "de_DE".
                      Defaults to "en_US".

        Returns:
            Locale-aware formatted datetime string.
        """
        try:
            tz = pytz.timezone(timezone)
            if dt.tzinfo is None:
                # Assume UTC for naive datetimes
                dt = pytz.utc.localize(dt)
            dt_local = dt.astimezone(tz)
            return babel_format_datetime(
                dt_local,
                format=format,
                locale=Locale.parse(locale),
            )
        except Exception:
            # Fallback to simple strftime if Babel fails —
            # a notification with an imperfect date format is better
            # than a notification that fails to send entirely.
            return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


    @staticmethod
    def _format_currency(
        amount: Decimal,
        currency: str = "USD",
        locale: str = "en_US",
    ) -> str:
        """
        Jinja2 filter: format a Decimal amount as a locale-aware currency string.

        Uses Babel's format_currency() for correct symbol placement and
        decimal/thousands separators:
          en_US + USD: "$299.99"
          en_GB + GBP: "£235.50"
          de_DE + EUR: "299,99 €"

        Usage in templates:
          {{ payment.amount | format_currency(payment.currency) }}
          {{ payment.amount | format_currency("GBP", "en_GB") }}

        Args:
            amount:   The Decimal amount to format.
            currency: ISO 4217 currency code e.g. "USD", "EUR", "GBP".
            locale:   BCP 47 locale string.

        Returns:
            Locale-aware formatted currency string.
        """
        try:
            return babel_format_currency(
                number=float(amount),
                currency=currency,
                locale=Locale.parse(locale),
            )
        except Exception:
            # Fallback: plain decimal with currency code prefix
            return f"{currency} {float(amount):,.2f}"


    @staticmethod
    def _pluralize(
        count: int,
        singular: str,
        plural: str,
    ) -> str:
        """
        Jinja2 filter: return singular or plural form based on count.

        Usage in templates:
          You have {{ seat_count | pluralize("seat", "seats") }} reserved.
          → "You have 1 seat reserved."
          → "You have 3 seats reserved."

        Args:
            count:    The integer to base the pluralisation on.
            singular: Singular form of the word e.g. "seat".
            plural:   Plural form of the word e.g. "seats".

        Returns:
            singular if count == 1, else plural.
        """
        return singular if count == 1 else plural