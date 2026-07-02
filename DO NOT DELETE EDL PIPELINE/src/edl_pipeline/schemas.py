"""Stable output field names for the generated stock artifact."""

IDENTITY_FIELDS = [
    "Symbol",
    "Name",
    "Listing Date",
    "Basic Industry",
    "Sector",
    "Index",
]

REQUIRED_FINAL_FIELDS = [
    "Symbol",
    "Name",
    "Basic Industry",
    "Sector",
    "Market Cap(Cr.)",
    "Stock Price(₹)",
    "P/E",
    "RS Rating",
    "Industry RS Rank",
    "Event Markers",
    "Recent Announcements",
    "News Feed",
]

EVENT_FIELDS = [
    "Event Markers",
    "Recent Announcements",
    "News Feed",
]

LEGACY_APPROXIMATE_FIELDS = [
    "Historical P/E 5",
    "Industry 1W Rank",
    "Industry 3W Rank",
]
