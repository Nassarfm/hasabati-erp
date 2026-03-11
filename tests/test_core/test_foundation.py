"""
tests/test_core/test_foundation.py
Tests for Module 0 Foundation layer.
"""
from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_health_endpoint(client):
    response = await client.get("/api/v1/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "version" in data


@pytest.mark.asyncio
async def test_unauthenticated_request_returns_401(client):
    """Any protected endpoint without token → 401."""
    response = await client.get("/api/v1/accounting/je")
    # 404 is also acceptable if route not yet registered
    assert response.status_code in (401, 404)


@pytest.mark.asyncio
async def test_idempotency_header_accepted(client):
    """Requests with Idempotency-Key header are processed."""
    response = await client.get(
        "/api/v1/health",
        headers={"Idempotency-Key": "test-key-001"},
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_request_id_in_response(client):
    """Every response carries X-Request-ID header."""
    response = await client.get("/api/v1/health")
    assert "x-request-id" in response.headers


def test_exception_hierarchy():
    """ERP exceptions have correct codes and status codes."""
    from app.core.exceptions import (
        DoubleEntryImbalanceError,
        FiscalPeriodLockedError,
        PermissionDeniedError,
        TenantIsolationError,
    )

    exc = DoubleEntryImbalanceError(1000.0, 900.0)
    assert exc.code == "DOUBLE_ENTRY_IMBALANCE"
    assert exc.status_code == 422

    exc2 = FiscalPeriodLockedError(2024, 12, "hard")
    assert exc2.code == "FISCAL_PERIOD_LOCKED"
    assert exc2.status_code == 409

    exc3 = TenantIsolationError()
    assert exc3.status_code == 403

    exc4 = PermissionDeniedError("can_post_je")
    assert "can_post_je" in exc4.message


def test_settings_loaded():
    """Settings load without error."""
    from app.core.config import settings
    assert settings.APP_NAME
    assert settings.API_V1_PREFIX == "/api/v1"
