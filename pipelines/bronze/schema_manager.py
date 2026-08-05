"""
schema_manager.py

Responsibilities:
1. Validate incoming schema against expected Bronze schema.
2. Detect new columns (Schema Evolution).
3. Detect missing mandatory columns.
4. Generate schema change reports.
"""

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from typing import List, Dict

class SchemaManager:
    def __init__(self, expected_schema: StructType):
        self.expected_schema = expected_schema

    # Expected Columns     
    def expected_columns(self) -> List[str]:
        return [field.name for field in self.expected_schema.fields]
     
    # Incoming Columns
    @staticmethod
    def incoming_columns(df: DataFrame) -> List[str]:
        return df.columns
     
    # Detect New Columns
    def detect_new_columns(self, df: DataFrame) -> List[str]:
        expected = set(self.expected_columns())
        incoming = set(self.incoming_columns(df))
        return sorted(list(incoming - expected))
     
    # Detect Missing Columns
    def detect_missing_columns(self, df: DataFrame) -> List[str]:
        expected = set(self.expected_columns())
        incoming = set(self.incoming_columns(df))
        return sorted(list(expected - incoming))

    # Detect Column Type Changes
    def detect_type_changes(self, df: DataFrame) -> Dict:
        expected = {
            f.name: f.dataType.simpleString()
            for f in self.expected_schema.fields
        }
        incoming = {
            f.name: f.dataType.simpleString()
            for f in df.schema.fields
        }
        changes = {}

        for column in expected:
            if column not in incoming:
                continue
            if expected[column] != incoming[column]:
                changes[column] = {
                    "expected": expected[column],
                    "incoming": incoming[column]
                }
        return changes

    # Schema Report
    def generate_report(self, df: DataFrame) -> Dict:
        return {
            "new_columns":
                self.detect_new_columns(df),
            "missing_columns":
                self.detect_missing_columns(df),
            "type_changes":
                self.detect_type_changes(df)
        }

    # Print Report     
    def print_report(self, report: Dict):
        print("\n========== Schema Report ==========\n")
        print("New Columns:")
        if report["new_columns"]:
            for c in report["new_columns"]:
                print(f"  + {c}")
        else:
            print("  None")

        print("\nMissing Columns:")
        if report["missing_columns"]:
            for c in report["missing_columns"]:
                print(f"  - {c}")
        else:
            print("  None")

        print("\nType Changes:")
        if report["type_changes"]:
            for col, change in report["type_changes"].items():
                print(
                    f"  {col}: "
                    f"{change['expected']} "
                    f"-> "
                    f"{change['incoming']}"
                )
        else:
            print("  None")
        print("\n===================================\n")