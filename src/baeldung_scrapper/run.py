from __future__ import annotations

import json
import sys

from baeldung_scrapper.config.logging_setup import configure_logging
from baeldung_scrapper.config.settings import AppSettings
from baeldung_scrapper.pipeline.daily_run import run_daily_scrape


def main() -> int:
    settings = AppSettings()
    configure_logging(level=settings.log_level)
    report = run_daily_scrape(settings)
    print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    return 1 if report.has_failures else 0


if __name__ == "__main__":
    sys.exit(main())
