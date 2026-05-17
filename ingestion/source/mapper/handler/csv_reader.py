"""
Responsibility:
    Read a CSV file row by row and yield clean, normalized row dictionaries.
    This is the ONLY place in the pipeline that knows about CSV format details.

Design decisions:
    - Returns a generator (yield) instead of loading the entire file into memory,
      which allows processing arbitrarily large CSV files safely.
    - Strips leading/trailing whitespace from all string values at read time
      so downstream layers (service, EventEnvelope) always receive clean data.
    - Row number is yielded alongside the payload for error traceability.
    - Encoding defaults to UTF-8, which covers the Olist dataset encoding.
"""

import csv
from pathlib import Path
from typing import Generator, Tuple


class CsvReader:
    """
    Reads a CSV file and yields clean (row_num, payload) tuples.

    Usage:
        reader = CsvReader(Path("/data/olist/olist_orders_dataset.csv"))
        for row_num, row in reader.iter_rows():
            print(row_num, row)
    """

    def __init__(self, csv_path: Path, encoding: str = "utf-8-sig") -> None:
        self._path = Path(csv_path)
        self._encoding = encoding

        if not self._path.exists():
            raise FileNotFoundError(f"CSV file not found: {self._path}")
        if not self._path.suffix.lower() == ".csv":
            raise ValueError(f"Expected a .csv file, got: {self._path.name}")

    def iter_rows(self) -> Generator[Tuple[int, dict], None, None]:
        """
        Lazily yield (row_number, payload_dict) for each data row in the CSV.

        - Row numbers are 1-indexed (matching the visual row in spreadsheets).
        - All string values are stripped of leading/trailing whitespace.
        - Empty string values are preserved as-is (not converted to None)
          to avoid silent data loss; callers decide how to handle them.

        Yields:
            Tuple[int, dict]: (row_num, payload)
                row_num — 1-based integer row number (header row not counted)
                payload — dict of {column_name: value}
        """
        with self._path.open(newline="", encoding=self._encoding) as fh:
            reader = csv.DictReader(fh)
            for row_num, raw_row in enumerate(reader, start=1):
                yield row_num, self._normalize(raw_row)

    def row_count(self) -> int:
        """
        Count total data rows in the CSV (excludes header).
        NOTE: This reads the entire file — use only when the count is needed upfront.
        """
        with self._path.open(newline="", encoding=self._encoding) as fh:
            return sum(1 for _ in csv.DictReader(fh))

    @property
    def filename(self) -> str:
        """Return the basename of the CSV file."""
        return self._path.name

    @staticmethod
    def _normalize(raw_row: dict) -> dict:
        """
        Normalize a raw CSV row:
            - Strip whitespace from string values.
            - Convert OrderedDict → plain dict.
        """
        return {k: (v.strip() if isinstance(v, str) else v) for k, v in raw_row.items()}
