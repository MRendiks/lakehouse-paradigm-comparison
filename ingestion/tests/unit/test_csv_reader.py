import tempfile
from pathlib import Path
import pytest
from source.mapper.handler.csv_reader import CsvReader


class TestCsvReader:
    """Test CSV reading and normalization."""

    @pytest.fixture
    def sample_csv(self):
        """Create a temporary CSV file for testing."""
        content = (
            "order_id,customer_id,order_status\n"
            "ORD-001,CUST-A,delivered\n"
            "ORD-002,CUST-B,shipped\n"
            "ORD-003,CUST-C,processing\n"
        )
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, encoding="utf-8"
        ) as f:
            f.write(content)
            path = f.name

        yield Path(path)
        # Cleanup
        Path(path).unlink(missing_ok=True)

    def test_iter_rows_yields_all_rows(self, sample_csv):
        reader = CsvReader(sample_csv)
        rows = list(reader.iter_rows())

        assert len(rows) == 3
        # First element is row_number, second is payload dict
        row_num, payload = rows[0]
        assert row_num == 1
        assert payload["order_id"] == "ORD-001"

    def test_filename_property(self, sample_csv):
        reader = CsvReader(sample_csv)
        assert reader.filename == sample_csv.name

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            CsvReader(Path("/nonexistent/file.csv"))