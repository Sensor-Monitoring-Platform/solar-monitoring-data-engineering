from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


class SchemaManager:
    def __init__(self, expected_schema: StructType):
        self.expected_schema = expected_schema

    def expected_columns(self) -> List[str]:
        return [field.name for field in self.expected_schema.fields]

    @staticmethod
    def incoming_columns(df: DataFrame) -> List[str]:
        return df.columns

    def detect_missing_columns(self, df: DataFrame) -> List[str]:
        expected = set(self.expected_columns())
        incoming = set(self.incoming_columns(df))
        return sorted(list(expected - incoming))

    def detect_type_changes(self, df: DataFrame) -> Dict:
        expected = {f.name: f.dataType.simpleString() for f in self.expected_schema.fields}
        incoming = {f.name: f.dataType.simpleString() for f in df.schema.fields}

        changes = {}
        for column in expected:
            if column not in incoming:
                continue
            if expected[column] != incoming[column]:
                changes[column] = {"expected": expected[column], "incoming": incoming[column]}
        return changes

    def generate_report(self, df: DataFrame) -> Dict:
        return {
            "missing_columns": self.detect_missing_columns(df),
            "type_changes": self.detect_type_changes(df),
        }
