"""Small reporting primitives for non-fatal pipeline warnings."""

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineWarning:
    stage: str
    message: str
    detail: str = ""


def format_warning(warning):
    suffix = f" ({warning.detail})" if warning.detail else ""
    return f"{warning.stage}: {warning.message}{suffix}"
