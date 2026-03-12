from __future__ import annotations

import logging


def configure_logging(*, level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def format_log_fields(**fields: object) -> str:
    if not fields:
        return ""
    rendered = [f"{key}={fields[key]!r}" for key in sorted(fields)]
    return " " + " ".join(rendered)
