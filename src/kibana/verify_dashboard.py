"""Verify Kibana Stage 12 setup."""

from __future__ import annotations

from dataclasses import dataclass

import httpx

from src.config import KIBANA_URL
from src.kibana.setup_dashboard import (
    DASHBOARD_ID,
    DASHBOARD_TITLE,
    DATA_VIEWS,
    VISUALIZATIONS,
)


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


def _headers() -> dict[str, str]:
    return {"kbn-xsrf": "true"}


def _get_saved_object(object_type: str, object_id: str) -> bool:
    response = httpx.get(
        f"{KIBANA_URL.rstrip('/')}/api/saved_objects/{object_type}/{object_id}",
        headers=_headers(),
        timeout=30.0,
    )
    return response.status_code == 200


def _get_data_view(data_view_id: str) -> bool:
    response = httpx.get(
        f"{KIBANA_URL.rstrip('/')}/api/data_views/data_view/{data_view_id}",
        headers=_headers(),
        timeout=30.0,
    )
    return response.status_code == 200


def verify_kibana_setup() -> list[CheckResult]:
    results: list[CheckResult] = []

    for data_view_id, index_name, _label in DATA_VIEWS:
        passed = _get_data_view(data_view_id)
        results.append(
            CheckResult(
                name=f"data view: {index_name}",
                passed=passed,
                detail="ok" if passed else f"missing id `{data_view_id}`",
            )
        )

    for vis_id, title, _, _ in VISUALIZATIONS:
        passed = _get_saved_object("visualization", vis_id)
        results.append(
            CheckResult(
                name=f"visualization: {title}",
                passed=passed,
                detail="ok" if passed else f"missing id `{vis_id}`",
            )
        )

    dashboard_ok = _get_saved_object("dashboard", DASHBOARD_ID)
    results.append(
        CheckResult(
            name=f"dashboard: {DASHBOARD_TITLE}",
            passed=dashboard_ok,
            detail="ok" if dashboard_ok else f"missing id `{DASHBOARD_ID}`",
        )
    )

    return results
