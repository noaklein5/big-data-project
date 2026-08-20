"""Kibana dashboard setup for MovieLens analytics."""

from __future__ import annotations

import json
from typing import Any

import httpx

from src.config import (
    INDEX_MOVIE_RATINGS_BY_RATING_YEAR,
    INDEX_MOVIES,
    INDEX_MOVIES_BY_RELEASE_YEAR,
    KIBANA_URL,
)

DATA_VIEWS = (
    ("dv-movies", INDEX_MOVIES, "Movies"),
    ("dv-release-year", INDEX_MOVIES_BY_RELEASE_YEAR, "Movies by Release Year"),
    ("dv-rating-year", INDEX_MOVIE_RATINGS_BY_RATING_YEAR, "Movie Ratings by Rating Year"),
)

DASHBOARD_ID = "movielens-analytics"
DASHBOARD_TITLE = "MovieLens Analytics"


class KibanaSetupError(RuntimeError):
    """Raised when Kibana setup fails."""


def _headers() -> dict[str, str]:
    return {"kbn-xsrf": "true", "Content-Type": "application/json"}


def _base_url() -> str:
    return KIBANA_URL.rstrip("/")


def _histogram_vis(
    title: str,
    *,
    segment_field: str,
    metric_field: str,
    metric_type: str = "avg",
    size: int = 10,
) -> dict[str, Any]:
    metric_params: dict[str, Any] = (
        {} if metric_type == "count" else {"field": metric_field}
    )
    return {
        "title": title,
        "type": "histogram",
        "params": {
            "type": "histogram",
            "grid": {"categoryLines": False},
            "categoryAxes": [
                {
                    "id": "CategoryAxis-1",
                    "type": "category",
                    "position": "bottom",
                    "show": True,
                    "labels": {"show": True, "truncate": 100},
                }
            ],
            "valueAxes": [
                {
                    "id": "ValueAxis-1",
                    "name": "LeftAxis-1",
                    "type": "value",
                    "position": "left",
                    "show": True,
                    "labels": {"show": True},
                }
            ],
            "seriesParams": [
                {
                    "show": True,
                    "type": "histogram",
                    "mode": "normal",
                    "data": {"label": title, "id": "1"},
                    "valueAxis": "ValueAxis-1",
                }
            ],
            "addTooltip": True,
            "addLegend": True,
        },
        "aggs": [
            {
                "id": "1",
                "enabled": True,
                "type": metric_type,
                "schema": "metric",
                "params": metric_params,
            },
            {
                "id": "2",
                "enabled": True,
                "type": "terms",
                "schema": "segment",
                "params": {
                    "field": segment_field,
                    "size": size,
                    "order": {"type": "metric", "id": "1", "direction": "desc"},
                },
            },
        ],
    }


VISUALIZATIONS: tuple[tuple[str, str, str, dict[str, Any]], ...] = (
    (
        "vis-avg-rating-by-genre",
        "Average Rating by Genre",
        "dv-movies",
        _histogram_vis(
            "Average Rating by Genre",
            segment_field="genres",
            metric_field="average_rating",
            size=12,
        ),
    ),
    (
        "vis-movies-per-genre",
        "Movies per Genre",
        "dv-movies",
        _histogram_vis(
            "Movies per Genre",
            segment_field="genres",
            metric_field="",
            metric_type="count",
            size=12,
        ),
    ),
    (
        "vis-top-movies-by-rating-count",
        "Top Movies by Rating Count",
        "dv-movies",
        _histogram_vis(
            "Top Movies by Rating Count",
            segment_field="title.keyword",
            metric_field="rating_count",
            metric_type="max",
            size=10,
        ),
    ),
    (
        "vis-movies-by-release-year",
        "Movies Released per Year",
        "dv-release-year",
        _histogram_vis(
            "Movies Released per Year",
            segment_field="release_year",
            metric_field="movie_count",
            metric_type="max",
            size=20,
        ),
    ),
    (
        "vis-rating-activity-by-year",
        "Rating Activity by Year",
        "dv-rating-year",
        _histogram_vis(
            "Rating Activity by Year",
            segment_field="rating_year",
            metric_field="rating_count",
            metric_type="sum",
            size=20,
        ),
    ),
    (
        "vis-avg-rating-by-release-year",
        "Cohort Average Rating by Release Year",
        "dv-release-year",
        _histogram_vis(
            "Cohort Average Rating by Release Year",
            segment_field="release_year",
            metric_field="average_rating",
            metric_type="avg",
            size=20,
        ),
    ),
)


def _request(method: str, path: str, **kwargs: Any) -> httpx.Response:
    url = f"{_base_url()}{path}"
    try:
        response = httpx.request(method, url, headers=_headers(), timeout=60.0, **kwargs)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise KibanaSetupError(f"Kibana API error for {path}: {exc}") from exc
    return response


def create_data_views() -> list[str]:
    created: list[str] = []
    for data_view_id, title, name in DATA_VIEWS:
        existing = httpx.get(
            f"{_base_url()}/api/data_views/data_view/{data_view_id}",
            headers=_headers(),
            timeout=30.0,
        )
        if existing.status_code == 200:
            created.append(data_view_id)
            continue
        _request(
            "POST",
            "/api/data_views/data_view",
            json={"data_view": {"id": data_view_id, "title": title, "name": name}},
        )
        created.append(data_view_id)
    return created


def _create_visualization(
    vis_id: str,
    title: str,
    data_view_id: str,
    vis_state: dict[str, Any],
) -> str:
    body = {
        "attributes": {
            "title": title,
            "visState": json.dumps(vis_state),
            "uiStateJSON": "{}",
            "description": "",
            "version": 1,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps(
                    {
                        "query": {"language": "kuery", "query": ""},
                        "filter": [],
                        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
                    }
                ),
            },
        },
        "references": [
            {
                "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                "type": "index-pattern",
                "id": data_view_id,
            }
        ],
    }
    _request("POST", f"/api/saved_objects/visualization/{vis_id}?overwrite=true", json=body)
    return vis_id


def create_visualizations() -> list[str]:
    created: list[str] = []
    for vis_id, title, data_view_id, vis_state in VISUALIZATIONS:
        _create_visualization(vis_id, title, data_view_id, vis_state)
        created.append(vis_id)
    return created


def create_dashboard(visualization_ids: list[str]) -> str:
    panel_width = 8
    panel_height = 15
    cols = 3
    panels = []
    references = []
    for index, vis_id in enumerate(visualization_ids):
        row = index // cols
        col = index % cols
        panel_name = f"panel_{index + 1}"
        panels.append(
            {
                "version": "8.15.0",
                "type": "visualization",
                "gridData": {
                    "x": col * panel_width,
                    "y": row * panel_height,
                    "w": panel_width,
                    "h": panel_height,
                    "i": str(index + 1),
                },
                "panelIndex": str(index + 1),
                "embeddableConfig": {},
                "panelRefName": panel_name,
            }
        )
        references.append({"name": panel_name, "type": "visualization", "id": vis_id})

    body = {
        "attributes": {
            "title": DASHBOARD_TITLE,
            "panelsJSON": json.dumps(panels),
            "optionsJSON": json.dumps(
                {"useMargins": True, "syncColors": False, "hidePanelTitles": False}
            ),
            "version": 1,
            "timeRestore": False,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps(
                    {"query": {"language": "kuery", "query": ""}, "filter": []}
                ),
            },
        },
        "references": references,
    }
    _request(
        "POST",
        f"/api/saved_objects/dashboard/{DASHBOARD_ID}?overwrite=true",
        json=body,
    )
    return DASHBOARD_ID


def setup_kibana_dashboard() -> dict[str, Any]:
    """Create data views, visualizations, and dashboard in Kibana."""
    data_views = create_data_views()
    visualizations = create_visualizations()
    dashboard_id = create_dashboard(visualizations)
    return {
        "data_views": data_views,
        "visualizations": visualizations,
        "dashboard_id": dashboard_id,
        "dashboard_url": f"{_base_url()}/app/dashboards#/view/{dashboard_id}",
    }
