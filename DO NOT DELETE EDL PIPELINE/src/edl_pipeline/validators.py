"""Artifact validation helpers used by the runner and tests."""

from dataclasses import asdict, dataclass
import csv
import gzip
import json
from pathlib import Path

from pipeline_utils import resolve_path


@dataclass(frozen=True)
class ArtifactSpec:
    path: str
    kind: str
    min_count: int = 1
    required_fields: tuple = ()


@dataclass
class ArtifactCheck:
    path: str
    kind: str
    ok: bool
    message: str = ""
    size_bytes: int = 0
    count: int = 0

    def to_dict(self):
        return asdict(self)


def _missing(path, kind):
    return ArtifactCheck(str(path), kind, False, "missing")


def _bad(path, kind, message, size=0, count=0):
    return ArtifactCheck(str(path), kind, False, message, size, count)


def _good(path, kind, message="ok", size=0, count=0):
    return ArtifactCheck(str(path), kind, True, message, size, count)


def _check_required_fields(rows, required_fields):
    if not required_fields:
        return ""
    if isinstance(rows, list):
        if not rows:
            return "empty list has no fields"
        sample = rows[0]
    elif isinstance(rows, dict):
        sample = rows
    else:
        return "unsupported JSON shape for required field check"
    missing = [field for field in required_fields if field not in sample]
    return f"missing fields: {', '.join(missing)}" if missing else ""


def validate_json(path, min_count=1, required_fields=()):
    resolved = resolve_path(path)
    if not resolved.exists():
        return _missing(resolved, "json")
    size = resolved.stat().st_size
    if size <= 0:
        return _bad(resolved, "json", "empty file", size)
    try:
        with resolved.open("r") as f:
            data = json.load(f)
    except Exception as e:
        return _bad(resolved, "json", f"invalid JSON: {e}", size)

    count = len(data) if isinstance(data, (list, dict)) else 0
    if count < min_count:
        return _bad(resolved, "json", f"count {count} < {min_count}", size, count)
    field_error = _check_required_fields(data, required_fields)
    if field_error:
        return _bad(resolved, "json", field_error, size, count)
    return _good(resolved, "json", size=size, count=count)


def validate_gzip_json(path, min_count=1, required_fields=()):
    resolved = resolve_path(path)
    if not resolved.exists():
        return _missing(resolved, "gzip_json")
    size = resolved.stat().st_size
    if size <= 0:
        return _bad(resolved, "gzip_json", "empty file", size)
    try:
        with gzip.open(resolved, "rt") as f:
            data = json.load(f)
    except Exception as e:
        return _bad(resolved, "gzip_json", f"invalid gzip JSON: {e}", size)

    count = len(data) if isinstance(data, (list, dict)) else 0
    if count < min_count:
        return _bad(resolved, "gzip_json", f"count {count} < {min_count}", size, count)
    field_error = _check_required_fields(data, required_fields)
    if field_error:
        return _bad(resolved, "gzip_json", field_error, size, count)
    return _good(resolved, "gzip_json", size=size, count=count)


def validate_csv(path, min_count=1):
    resolved = resolve_path(path)
    if not resolved.exists():
        return _missing(resolved, "csv")
    size = resolved.stat().st_size
    if size <= 0:
        return _bad(resolved, "csv", "empty file", size)
    try:
        with resolved.open("r") as f:
            rows = list(csv.reader(f))
    except Exception as e:
        return _bad(resolved, "csv", f"invalid CSV: {e}", size)
    count = len(rows)
    if count < min_count:
        return _bad(resolved, "csv", f"rows {count} < {min_count}", size, count)
    return _good(resolved, "csv", size=size, count=count)


def validate_gzip_csv(path, min_count=1):
    resolved = resolve_path(path)
    if not resolved.exists():
        return _missing(resolved, "gzip_csv")
    size = resolved.stat().st_size
    if size <= 0:
        return _bad(resolved, "gzip_csv", "empty file", size)
    try:
        with gzip.open(resolved, "rt") as f:
            rows = list(csv.reader(f))
    except Exception as e:
        return _bad(resolved, "gzip_csv", f"invalid gzip CSV: {e}", size)
    count = len(rows)
    if count < min_count:
        return _bad(resolved, "gzip_csv", f"rows {count} < {min_count}", size, count)
    return _good(resolved, "gzip_csv", size=size, count=count)


def validate_dir(path, min_count=1):
    resolved = resolve_path(path)
    if not resolved.exists():
        return _missing(resolved, "dir")
    if not resolved.is_dir():
        return _bad(resolved, "dir", "not a directory")
    count = sum(1 for item in resolved.iterdir() if item.is_file())
    if count < min_count:
        return _bad(resolved, "dir", f"files {count} < {min_count}", count=count)
    return _good(resolved, "dir", count=count)


def validate_file(path, min_count=1):
    resolved = resolve_path(path)
    if not resolved.exists():
        return _missing(resolved, "file")
    size = resolved.stat().st_size
    if size < min_count:
        return _bad(resolved, "file", f"size {size} < {min_count}", size)
    return _good(resolved, "file", size=size, count=1)


def validate_artifact(spec):
    if spec.kind == "json":
        return validate_json(spec.path, spec.min_count, spec.required_fields)
    if spec.kind == "gzip_json":
        return validate_gzip_json(spec.path, spec.min_count, spec.required_fields)
    if spec.kind == "csv":
        return validate_csv(spec.path, spec.min_count)
    if spec.kind == "gzip_csv":
        return validate_gzip_csv(spec.path, spec.min_count)
    if spec.kind == "dir":
        return validate_dir(spec.path, spec.min_count)
    if spec.kind == "file":
        return validate_file(spec.path, spec.min_count)
    return ArtifactCheck(spec.path, spec.kind, False, f"unknown kind: {spec.kind}")


def validate_many(specs):
    return [validate_artifact(spec) for spec in specs]
