"""Stable scanner-native output schema for the generated stock artifact."""

REQUIRED_FINAL_FIELDS = [
    "schema_version",
    "symbol",
    "name",
    "sector",
    "industry",
    "market_cap_crore",
    "close",
    "event_markers",
    "recent_announcements",
    "news_feed",
]
