"""Pure helpers for auditing a local screen against a dated reference list."""

from __future__ import annotations


def _symbols(values):
    return {
        str(value).strip().upper()
        for value in values
        if isinstance(value, str) and value.strip()
    }


def compare_symbol_sets(reference_symbols, local_symbols):
    """Return a deterministic, explainable comparison of two screen outputs."""
    reference = _symbols(reference_symbols)
    local = _symbols(local_symbols)
    shared = reference & local
    union = reference | local
    return {
        "reference_count": len(reference),
        "local_count": len(local),
        "shared_count": len(shared),
        "reference_only_count": len(reference - local),
        "local_only_count": len(local - reference),
        "jaccard": round(len(shared) / len(union), 6) if union else 1.0,
        "exact": reference == local,
        "reference_only": sorted(reference - local),
        "local_only": sorted(local - reference),
    }
