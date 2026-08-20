"""Kibana analytics setup for MovieLens."""

from src.kibana.insights import generate_insights, write_insights
from src.kibana.setup_dashboard import setup_kibana_dashboard
from src.kibana.verify_dashboard import verify_kibana_setup

__all__ = [
    "generate_insights",
    "setup_kibana_dashboard",
    "verify_kibana_setup",
    "write_insights",
]
