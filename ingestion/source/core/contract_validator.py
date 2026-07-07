"""
contract_validator.py — Validates payloads against data contracts (YAML).

Used by the producer before sending messages to Kafka.
Route: Data Contract → ValidationError → DLQ (never enters Kafka).
"""

from pathlib import Path
from typing import Any

import yaml
from loguru import logger


class ContractValidationError(Exception):
    """Exception for data that violates the contract."""

    def __init__(self, field: str, expected: str, actual: Any, row_num: int):
        self.field = field
        self.expected = expected
        self.actual = actual
        self.row_num = row_num
        super().__init__(
            f"[Row #{row_num}] Field '{field}': expected {expected}, got {type(actual).__name__} = {actual}"
        )


class ContractValidator:
    """
    Loads a YAML contract and validates payloads row-by-row.

    Usage:
        validator = ContractValidator("contracts/order_contract.yaml")
        validator.validate(payload, row_num=42)
        # Raises ContractValidationError on failure
    """

    # Mapping: YAML type string → Python type
    TYPE_MAP = {
        "string": str,
        "integer": int,
        "float": float,
        "numeric": (int, float),
        "timestamp": str,  # ISO 8601 string
        "boolean": bool,
    }

    def __init__(self, contract_path: str | Path) -> None:
        self._contract_path = Path(contract_path)
        self._contract = self._load()
        self._field_rules = self._parse_fields()
        logger.info(
            f"ContractValidator loaded | entity={self._contract['entity']} "
            f"v{self._contract['version']} | {len(self._field_rules)} fields"
        )

    def _load(self) -> dict:
        if not self._contract_path.exists():
            raise FileNotFoundError(f"Contract file not found: {self._contract_path}")
        with open(self._contract_path, encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _parse_fields(self) -> dict:
        """Build dict: {field_name: {type, nullable, accepted_values}}"""
        rules = {}
        for field in self._contract.get("fields", []):
            rules[field["name"]] = {
                "type": field.get("type", "string"),
                "nullable": field.get("nullable", True),
                "accepted_values": field.get("accepted_values", None),
            }
        return rules

    def validate(self, payload: dict, row_num: int = 0) -> None:
        """
        Validate payload against the contract.

        Raises ContractValidationError on the first failure.
        Does not validate fields not present in the contract (forward-compatible).
        """
        for field_name, rules in self._field_rules.items():
            value = payload.get(field_name)

            # ── Nullability Check ──
            if value is None or value == "":
                if not rules["nullable"]:
                    raise ContractValidationError(
                        field=field_name,
                        expected="not null",
                        actual="null",
                        row_num=row_num,
                    )
                continue

            # ── Type Check ──
            expected_type = rules["type"]
            python_type = self.TYPE_MAP.get(expected_type)
            if python_type and not isinstance(value, python_type):
                raise ContractValidationError(
                    field=field_name,
                    expected=expected_type,
                    actual=value,
                    row_num=row_num,
                )

            # ── Accepted Values Check ──
            accepted = rules.get("accepted_values")
            if accepted and value not in accepted:
                raise ContractValidationError(
                    field=field_name,
                    expected=f"one of {accepted}",
                    actual=value,
                    row_num=row_num,
                )

    @property
    def entity(self) -> str:
        return self._contract["entity"]

    @property
    def version(self) -> str:
        return self._contract["version"]