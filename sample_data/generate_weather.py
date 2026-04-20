"""Generate a deterministic weather-sensor CSV for testing.

Intentionally very different from sales.csv:
 - scientific/sensor domain (not commercial)
 - timestamps with hours+minutes (not just dates)
 - geographic lat/long floats (exercises Grid File index)
 - negative numbers (temperature)
 - booleans (precipitation)
 - high-cardinality station IDs
 - many numeric measurements with different ranges/units
"""

import csv
import math
import os
import random
from datetime import datetime, timedelta

WIND_DIRECTIONS = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
CONDITIONS = ["Clear", "Cloudy", "PartlyCloudy", "Rain", "Snow", "Fog", "Storm"]
STATION_COUNT = 120  # high cardinality


def generate(output_path: str, n_rows: int = 50_000, seed: int = 7) -> str:
    random.seed(seed)
    start = datetime(2024, 1, 1, 0, 0)
    minute_range = 60 * 24 * 365  # one year in minutes

    stations = [f"WX-{i:04d}" for i in range(1, STATION_COUNT + 1)]
    station_coords = {
        s: (
            round(random.uniform(24.5, 49.0), 4),
            round(random.uniform(-124.0, -66.9), 4),
        )
        for s in stations
    }
    station_elev = {s: random.randint(0, 4000) for s in stations}

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    with open(output_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "reading_id", "timestamp", "station_id", "latitude", "longitude",
            "elevation_m", "temperature_c", "humidity_pct", "pressure_hpa",
            "wind_speed_kmh", "wind_direction", "precipitation", "condition",
        ])
        for i in range(1, n_rows + 1):
            ts = start + timedelta(minutes=random.randint(0, minute_range))
            station = random.choice(stations)
            lat, lon = station_coords[station]
            elev = station_elev[station]

            day_of_year = ts.timetuple().tm_yday
            seasonal = 15 * math.sin(2 * math.pi * (day_of_year - 80) / 365)
            base_temp = 12 + seasonal - elev / 200
            temperature = round(base_temp + random.gauss(0, 6), 2)
            humidity = round(min(100, max(5, random.gauss(65, 18))), 2)
            pressure = round(1013 - elev / 8 + random.gauss(0, 6), 2)
            wind_speed = round(max(0, random.gammavariate(2.0, 6.0)), 2)
            direction = random.choice(WIND_DIRECTIONS)
            condition = random.choices(
                CONDITIONS,
                weights=[30, 22, 18, 14, 6, 5, 5],
                k=1,
            )[0]
            precip = condition in ("Rain", "Snow", "Storm")

            w.writerow([
                i, ts.isoformat(sep=" ", timespec="minutes"), station,
                lat, lon, elev, temperature, humidity, pressure,
                wind_speed, direction, str(precip).lower(), condition,
            ])

    return output_path


if __name__ == "__main__":
    path = generate(os.path.join(os.path.dirname(__file__), "weather_data.csv"))
    print(f"Generated {path}")
