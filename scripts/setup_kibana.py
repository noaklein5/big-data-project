"""Create Kibana data views, dashboard, and insights document."""

from __future__ import annotations

import sys

from src.kibana.insights import write_insights
from src.kibana.setup_dashboard import KibanaSetupError, setup_kibana_dashboard


def main() -> int:
    print("Setting up Kibana dashboard (Stage 12)...\n")

    try:
        result = setup_kibana_dashboard()
    except KibanaSetupError as exc:
        print(f"ERROR: {exc}")
        print("Ensure Kibana is running: docker compose up -d kibana")
        return 1

    insights_path = write_insights()

    print("Created data views:")
    for data_view_id in result["data_views"]:
        print(f"  - {data_view_id}")

    print("\nCreated visualizations:")
    for vis_id in result["visualizations"]:
        print(f"  - {vis_id}")

    print(f"\nDashboard: {result['dashboard_id']}")
    print(f"Open: {result['dashboard_url']}")
    print(f"\nInsights written to: {insights_path}")
    print("\nKibana setup complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
