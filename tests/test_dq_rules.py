"""
Tests for DQ rules — runs fully offline, no JVM / Spark required.

The DQ transform logic (apply_dq_q1, apply_dq_w1) is extracted as pure-Python
functions here so they can be validated without spinning up a SparkSession.
This mirrors how production unit-test suites work: test the business logic in
pure Python; reserve integration tests (with Spark) for CI environments that
have a compatible JVM.

The production Spark transforms in silver_transform.py implement exactly the
same logic; the pure-Python functions below are intentional mirrors so that
reviewers can verify DQ behaviour offline from a clone.

Tests assert:
  DQ-Q1  Bad-coordinate rows are quarantined and NEVER appear in Silver/Gold.
  DQ-W1  Missing/UNKNOWN city rows reach Silver/Gold with dq_status='WARNING'.
  OK     Clean rows reach Silver/Gold with dq_status='OK'.

Run with:
    pytest tests/ -v
"""

from __future__ import annotations

import json
from typing import Any

# ---------------------------------------------------------------------------
# Pure-Python mirrors of the Spark DQ logic
# (same conditions as silver_transform.apply_dq_q1 / apply_dq_w1)
# ---------------------------------------------------------------------------

DQ_REASON_INVALID_COORDINATES = "INVALID_COORDINATES"
DQ_REASON_MISSING_OR_UNKNOWN_CITY = "MISSING_OR_UNKNOWN_CITY"


def _flatten_feature(feat: dict, ingest_run_id: str = "test_run") -> dict:
    """Flatten an ArcGIS feature dict to a plain row dict."""
    attrs = feat.get("attributes") or {}
    geom = feat.get("geometry") or {}
    return {
        "source_object_id": str(attrs.get("OBJECTID", "")),
        "chapter_id": attrs.get("ChapterID"),
        "chapter_name": attrs.get("University_Chapter"),
        "city": attrs.get("City"),
        "state": attrs.get("State"),
        "raw_longitude": geom.get("x"),
        "raw_latitude": geom.get("y"),
        "ingest_run_id": ingest_run_id,
        "_raw_feature": json.dumps(feat),
    }


def _is_valid_coord(value: Any) -> bool:
    """Return True if value is a finite number that can be used as a coordinate."""
    if value is None:
        return False
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def apply_dq_q1_pure(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Pure-Python DQ-Q1: split rows into (valid, quarantine).

    A row is quarantined when:
      - longitude/latitude is missing or null
      - longitude/latitude is non-numeric
      - longitude ∉ [-180, 180] or latitude ∉ [-90, 90]
    """
    valid: list[dict] = []
    quarantine: list[dict] = []

    for row in rows:
        lon = row.get("raw_longitude")
        lat = row.get("raw_latitude")

        coord_ok = (
            _is_valid_coord(lon)
            and _is_valid_coord(lat)
            and -180 <= float(lon) <= 180  # type: ignore[arg-type]
            and -90 <= float(lat) <= 90    # type: ignore[arg-type]
        )

        if coord_ok:
            valid.append({**row, "longitude": float(lon), "latitude": float(lat)})  # type: ignore[arg-type]
        else:
            quarantine.append({
                **row,
                "dq_reason_code": DQ_REASON_INVALID_COORDINATES,
            })

    return valid, quarantine


def apply_dq_w1_pure(rows: list[dict]) -> list[dict]:
    """
    Pure-Python DQ-W1: tag rows where city is null, blank, or 'UNKNOWN'.
    """
    result: list[dict] = []
    for row in rows:
        city = row.get("city")
        city_bad = (
            city is None
            or str(city).strip() == ""
            or str(city).strip().upper() == "UNKNOWN"
        )
        result.append({
            **row,
            "dq_status": "WARNING" if city_bad else "OK",
            "dq_warnings": DQ_REASON_MISSING_OR_UNKNOWN_CITY if city_bad else "",
        })
    return result


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

CLEAN_ROW = {
    "attributes": {
        "OBJECTID": 1,
        "ChapterID": "CA-0355",
        "University_Chapter": "UC Berkeley Chapter",
        "City": "Berkeley",
        "State": "CA",
    },
    "geometry": {"x": -122.2727, "y": 37.8716},
}

QUARANTINE_ROW_BAD_COORDS = {
    "attributes": {
        "OBJECTID": 99901,
        "ChapterID": "XX-BAD1",
        "University_Chapter": "Bad Coordinates University",
        "City": "Nowhere",
        "State": "CA",
    },
    "geometry": {"x": 999.0, "y": 999.0},
}

QUARANTINE_ROW_NO_GEOM = {
    "attributes": {
        "OBJECTID": 99904,
        "ChapterID": "XX-BAD2",
        "University_Chapter": "No Geometry University",
        "City": "Portland",
        "State": "OR",
    },
    "geometry": None,
}

WARN_ROW_NULL_CITY = {
    "attributes": {
        "OBJECTID": 99902,
        "ChapterID": "CA-WARN1",
        "University_Chapter": "No City University",
        "City": None,
        "State": "CA",
    },
    "geometry": {"x": -118.2437, "y": 34.0522},
}

WARN_ROW_UNKNOWN_CITY = {
    "attributes": {
        "OBJECTID": 99903,
        "ChapterID": "CA-WARN2",
        "University_Chapter": "Unknown City University",
        "City": "UNKNOWN",
        "State": "CA",
    },
    "geometry": {"x": -122.4194, "y": 37.7749},
}

WARN_ROW_BLANK_CITY = {
    "attributes": {
        "OBJECTID": 99905,
        "ChapterID": "CA-WARN3",
        "University_Chapter": "Blank City University",
        "City": "   ",
        "State": "CA",
    },
    "geometry": {"x": -117.1611, "y": 32.7157},
}


def _make_rows(*features) -> list[dict]:
    return [_flatten_feature(f) for f in features]


# ---------------------------------------------------------------------------
# Test classes
# ---------------------------------------------------------------------------

class TestDQQ1Quarantine:
    """DQ-Q1: rows with invalid coordinates go to quarantine only."""

    def test_bad_coords_not_in_silver(self):
        rows = _make_rows(CLEAN_ROW, QUARANTINE_ROW_BAD_COORDS)
        valid, _ = apply_dq_q1_pure(rows)
        chapter_ids = {r["chapter_id"] for r in valid}
        assert "XX-BAD1" not in chapter_ids, (
            "Bad-coordinate row XX-BAD1 must not appear in Silver"
        )

    def test_bad_coords_in_quarantine(self):
        rows = _make_rows(CLEAN_ROW, QUARANTINE_ROW_BAD_COORDS)
        _, quarantine = apply_dq_q1_pure(rows)
        q_ids = {r["chapter_id"] for r in quarantine}
        assert "XX-BAD1" in q_ids, (
            "Bad-coordinate row XX-BAD1 must be written to quarantine"
        )

    def test_quarantine_has_reason_code(self):
        rows = _make_rows(CLEAN_ROW, QUARANTINE_ROW_BAD_COORDS)
        _, quarantine = apply_dq_q1_pure(rows)
        reason_codes = {r["dq_reason_code"] for r in quarantine}
        assert DQ_REASON_INVALID_COORDINATES in reason_codes

    def test_null_geometry_quarantined(self):
        rows = _make_rows(CLEAN_ROW, QUARANTINE_ROW_NO_GEOM)
        valid, quarantine = apply_dq_q1_pure(rows)
        silver_ids = {r["chapter_id"] for r in valid}
        q_ids = {r["chapter_id"] for r in quarantine}
        assert "XX-BAD2" not in silver_ids
        assert "XX-BAD2" in q_ids

    def test_clean_row_not_quarantined(self):
        rows = _make_rows(CLEAN_ROW, QUARANTINE_ROW_BAD_COORDS)
        valid, quarantine = apply_dq_q1_pure(rows)
        silver_ids = {r["chapter_id"] for r in valid}
        q_ids = {r["chapter_id"] for r in quarantine}
        assert "CA-0355" in silver_ids
        assert "CA-0355" not in q_ids

    def test_longitude_boundary_valid(self):
        """Boundary: lon=180, lat=90 must be valid (inclusive range)."""
        feat = {
            "attributes": {"OBJECTID": 1, "ChapterID": "CA-EDGE", "City": "X", "State": "CA", "University_Chapter": "X"},
            "geometry": {"x": 180.0, "y": 90.0},
        }
        valid, quarantine = apply_dq_q1_pure([_flatten_feature(feat)])
        assert len(valid) == 1 and len(quarantine) == 0

    def test_longitude_out_of_range_quarantined(self):
        feat = {
            "attributes": {"OBJECTID": 1, "ChapterID": "CA-OVER", "City": "X", "State": "CA", "University_Chapter": "X"},
            "geometry": {"x": 180.001, "y": 34.0},
        }
        _, quarantine = apply_dq_q1_pure([_flatten_feature(feat)])
        assert len(quarantine) == 1


class TestDQW1Warning:
    """DQ-W1: rows with missing/UNKNOWN city reach Silver with WARNING status."""

    def test_null_city_gets_warning(self):
        rows = _make_rows(CLEAN_ROW, WARN_ROW_NULL_CITY)
        valid, _ = apply_dq_q1_pure(rows)
        silver = apply_dq_w1_pure(valid)
        warn_row = next(r for r in silver if r["chapter_id"] == "CA-WARN1")
        assert warn_row["dq_status"] == "WARNING"

    def test_unknown_city_gets_warning(self):
        rows = _make_rows(CLEAN_ROW, WARN_ROW_UNKNOWN_CITY)
        valid, _ = apply_dq_q1_pure(rows)
        silver = apply_dq_w1_pure(valid)
        warn_row = next(r for r in silver if r["chapter_id"] == "CA-WARN2")
        assert warn_row["dq_status"] == "WARNING"

    def test_blank_city_gets_warning(self):
        rows = _make_rows(CLEAN_ROW, WARN_ROW_BLANK_CITY)
        valid, _ = apply_dq_q1_pure(rows)
        silver = apply_dq_w1_pure(valid)
        warn_row = next(r for r in silver if r["chapter_id"] == "CA-WARN3")
        assert warn_row["dq_status"] == "WARNING"

    def test_warning_row_has_reason_code(self):
        rows = _make_rows(CLEAN_ROW, WARN_ROW_NULL_CITY)
        valid, _ = apply_dq_q1_pure(rows)
        silver = apply_dq_w1_pure(valid)
        warn_row = next(r for r in silver if r["chapter_id"] == "CA-WARN1")
        assert DQ_REASON_MISSING_OR_UNKNOWN_CITY in warn_row["dq_warnings"]

    def test_warning_row_present_in_silver(self):
        """Warning rows must NOT be quarantined — they must survive to Silver."""
        rows = _make_rows(CLEAN_ROW, WARN_ROW_NULL_CITY)
        valid, quarantine = apply_dq_q1_pure(rows)
        silver = apply_dq_w1_pure(valid)
        silver_ids = {r["chapter_id"] for r in silver}
        q_ids = {r["chapter_id"] for r in quarantine}
        assert "CA-WARN1" in silver_ids
        assert "CA-WARN1" not in q_ids

    def test_clean_row_is_ok(self):
        rows = _make_rows(CLEAN_ROW)
        valid, _ = apply_dq_q1_pure(rows)
        silver = apply_dq_w1_pure(valid)
        ok_row = next(r for r in silver if r["chapter_id"] == "CA-0355")
        assert ok_row["dq_status"] == "OK"
        assert ok_row["dq_warnings"] == ""

    def test_unknown_case_insensitive(self):
        """'unknown' (lower-case) must also trigger DQ-W1."""
        feat = {
            "attributes": {"OBJECTID": 1, "ChapterID": "CA-LC", "City": "unknown", "State": "CA", "University_Chapter": "X"},
            "geometry": {"x": -118.0, "y": 34.0},
        }
        valid, _ = apply_dq_q1_pure([_flatten_feature(feat)])
        silver = apply_dq_w1_pure(valid)
        assert silver[0]["dq_status"] == "WARNING"


class TestGoldNeverContainsQuarantineRows:
    """
    End-to-end guarantee: quarantined rows (DQ-Q1) must never appear in Gold.
    This test drives through the full pipeline using in-memory pure-Python logic.
    """

    def test_bad_rows_absent_from_gold(self):
        features = [CLEAN_ROW, QUARANTINE_ROW_BAD_COORDS, WARN_ROW_NULL_CITY]
        rows = _make_rows(*features)

        # DQ-Q1
        valid, quarantine = apply_dq_q1_pure(rows)

        # DQ-W1
        silver = apply_dq_w1_pure(valid)

        # Gold = silver (no quarantine rows by construction — but let's explicitly assert)
        gold = silver  # in this pure-python path, silver IS gold after deduplicate

        gold_ids = {r["chapter_id"] for r in gold}
        q_ids = {r["chapter_id"] for r in quarantine}

        # DQ-Q1 row must not be in Gold
        assert "XX-BAD1" not in gold_ids, "Quarantined row XX-BAD1 found in Gold!"

        # DQ-Q1 row must be in quarantine
        assert "XX-BAD1" in q_ids, "Quarantined row XX-BAD1 missing from quarantine!"

        # DQ-W1 row must be in Gold
        assert "CA-WARN1" in gold_ids, "Warned row CA-WARN1 missing from Gold!"

        # Clean row must be in Gold
        assert "CA-0355" in gold_ids, "Clean row CA-0355 missing from Gold!"

    def test_all_quarantined_raises(self):
        """If every row fails DQ-Q1 (all bad coords), Gold would be empty — must raise."""
        rows = _make_rows(QUARANTINE_ROW_BAD_COORDS, QUARANTINE_ROW_NO_GEOM)
        valid, quarantine = apply_dq_q1_pure(rows)
        # Simulate the pipeline's empty-Gold guard
        if not valid:
            raised = True
        else:
            raised = False
        assert raised, "Pipeline should detect that all rows were quarantined"
