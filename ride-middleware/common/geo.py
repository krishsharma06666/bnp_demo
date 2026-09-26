"""The 18 Bengaluru service areas with coordinates. Used for validation, fares, surge and dispatch.
Owner: Person 1 (common/). Same list is seeded into the `locations` table by Person 2.
"""
from math import asin, cos, radians, sin, sqrt

AREAS: dict[str, tuple[float, float]] = {
    "Airport": (13.1986, 77.7066), "BTM Layout": (12.9166, 77.6101), "Banashankari": (12.9255, 77.5468),
    "Bellandur": (12.9304, 77.6784), "Electronic City": (12.8452, 77.6602), "HSR Layout": (12.9116, 77.6474),
    "Hebbal": (13.0354, 77.5970), "Indiranagar": (12.9719, 77.6412), "JP Nagar": (12.9063, 77.5857),
    "Jayanagar": (12.9308, 77.5838), "KR Puram": (13.0076, 77.6958), "Koramangala": (12.9352, 77.6245),
    "MG Road": (12.9756, 77.6050), "Majestic": (12.9767, 77.5713), "Marathahalli": (12.9591, 77.6974),
    "Rajajinagar": (12.9912, 77.5561), "Whitefield": (12.9698, 77.7500), "Yelahanka": (13.1007, 77.5963),
}
AREA_NAMES = sorted(AREAS)


def km_between(a: str, b: str) -> float:
    """Straight-line distance between two areas (haversine), in km."""
    (lat1, lon1), (lat2, lon2) = AREAS[a], AREAS[b]
    dlat, dlon = radians(lat2 - lat1), radians(lon2 - lon1)
    h = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * 6371 * asin(sqrt(h))


def road_km(a: str, b: str) -> float:
    return round(km_between(a, b) * 1.3, 1)          # roads are ~30% longer than a straight line


def base_fare(a: str, b: str) -> float:
    return round(max(80.0, 50 + 12 * road_km(a, b)), 2)
