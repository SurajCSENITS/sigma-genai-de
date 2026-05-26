from typing import Dict, List, Tuple, Any
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def detect_schema_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str]) -> Dict[str, Any]:
    new_columns = {k: v for k, v in actual_schema.items() if k not in expected_schema}
    removed_columns = {k: v for k, v in expected_schema.items() if k not in actual_schema}
    type_changes = {k: (expected_schema[k], actual_schema[k]) for k in expected_schema if expected_schema[k]!= actual_schema[k]}
    drift_severity = 'NONE'
    
    if new_columns:
        if any(actual_schema[col] not in ['string', 'float'] or 'null' not in actual_schema[col] for col in new_columns):
            drift_severity = 'HIGH'
        else:
            drift_severity = 'LOW'
    
    if removed_columns:
        drift_severity = 'BREAKING'
    
    return {
        "new_columns": new_columns,
        "removed_columns": removed_columns,
        "type_changes": type_changes,
        "drift_severity": drift_severity
    }

def decide_action(drift_report: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    decisions = {}
    for col_name, col_type in drift_report["new_columns"].items():
        if col_type == 'string':
            decisions[col_name] = {"action": "ADD_TO_SCHEMA", "reason": "New nullable string column", "risk_level": "LOW"}
        elif col_type in ['float', 'double']:
            decisions[col_name] = {"action": "FLAG_ANOMALY", "reason": "New numeric column that could affect calculations", "risk_level": "HIGH"}
    
    for col_name in drift_report["removed_columns"]:
        decisions[col_name] = {"action": "HALT", "reason": "Removed column that will break downstream queries", "risk_level": "BREAKING"}
    
    return decisions

def apply_schema_evolution(spark_df: DataFrame, decisions: Dict[str, Dict[str, str]], updated_schema: Dict[str, str]) -> Tuple[DataFrame, List[str]]:
    migration_notes = []
    for col_name, decision in decisions.items():
        if decision["action"] == "DROP_SILENTLY":
            spark_df = spark_df.drop(col_name)
        elif decision["action"] == "ADD_TO_SCHEMA":
            migration_notes.append(f"Added column {col_name} to schema registry.")
        elif decision["action"] == "FLAG_ANOMALY":
            spark_df = spark_df.withColumn(f"{col_name}_anomaly_flag", F.lit(True))
            migration_notes.append(f"Flagged column {col_name} as anomaly.")
    
    return spark_df, migration_notes

def handle_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str], spark_df: DataFrame = None) -> Dict[str, Any]:
    drift_report = detect_schema_drift(expected_schema, actual_schema)
    decisions = decide_action(drift_report)
    
    print(f"Drift Report: {drift_report}")
    print(f"Action Decisions: {decisions}")
    
    if spark_df is not None:
        evolved_df, migration_notes = apply_schema_evolution(spark_df, decisions, actual_schema)
        return {"drift_report": drift_report, "decisions": decisions, "migration_notes": migration_notes, "evolved_df": evolved_df}
    
    return {"drift_report": drift_report, "decisions": decisions}
