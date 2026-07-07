import tempfile
from pathlib import Path
import pytest
from source.core.contract_validator import ContractValidator, ContractValidationError

VALID_ORDER_CONTRACT = """
entity: order
version: "1.0.0"
owner: data-engineering-team
fields:
  - name: order_id
    type: string
    nullable: false
  - name: order_status
    type: string
    nullable: false
    accepted_values: [delivered, shipped, canceled]
  - name: optional_notes
    type: string
    nullable: true
"""


class TestContractValidator:

    @pytest.fixture
    def contract_file(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, encoding="utf-8"
        ) as f:
            f.write(VALID_ORDER_CONTRACT)
            path = f.name
        yield path
        Path(path).unlink(missing_ok=True)

    def test_valid_payload_passes(self, contract_file):
        validator = ContractValidator(contract_file)
        # Should not raise
        validator.validate({
            "order_id": "ORD-001",
            "order_status": "delivered",
        }, row_num=1)

    def test_null_required_field_fails(self, contract_file):
        validator = ContractValidator(contract_file)
        with pytest.raises(ContractValidationError) as exc:
            validator.validate({"order_id": None, "order_status": "delivered"}, row_num=2)
        assert "order_id" in str(exc.value)
        assert "Row #2" in str(exc.value)

    def test_invalid_accepted_value_fails(self, contract_file):
        validator = ContractValidator(contract_file)
        with pytest.raises(ContractValidationError) as exc:
            validator.validate({
                "order_id": "ORD-001",
                "order_status": "unknown_status",
            }, row_num=3)
        assert "order_status" in str(exc.value)
        assert "delivered" in str(exc.value)

    def test_optional_null_field_passes(self, contract_file):
        validator = ContractValidator(contract_file)
        # Should not raise — optional_notes is nullable
        validator.validate({
            "order_id": "ORD-001",
            "order_status": "shipped",
            "optional_notes": None,
        }, row_num=4)