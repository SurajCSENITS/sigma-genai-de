from typing import Dict, List, Tuple, Union
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col

def detect_schema_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str]) -> Dict[str, Union[Dict[str, str], List[str], Dict[str, str], str]]:
    """
    Detects schema drift between expected and actual schema.

    Args:
        expected_schema (Dict[str, str]): The expected schema.
        actual_schema (Dict[str, str]): The actual schema.

    Returns:
        Dict[str, Union[Dict[str, str], List[str], Dict[str, str], str]]: A dictionary containing new columns, removed columns, type changes, and drift severity.
    """
    new_columns = {k: v for k, v in actual_schema.items() if k not in expected_schema}
    removed_columns = {k: v for k, v in expected_schema.items() if k not in actual_schema}
    type_changes = {k: (expected_schema[k], actual_schema[k]) for k in expected_schema if expected_schema[k]!= actual_schema[k]}
    drift_severity = 'NONE'
    if new_columns:
        if any(actual_schema[col] not in ['string', 'null'] for col in new_columns):
            drift_severity = 'HIGH'
        else:
            drift_severity = 'LOW'
    if removed_columns:
        drift_severity = 'BREAKING'
    return {
        'new_columns': new_columns,
       'removed_columns': list(removed_columns.keys()),
        'type_changes': type_changes,
        'drift_severity': drift_severity
    }

def decide_action(drift_report: Dict[str, Union[Dict[str, str], List[str], Dict[str, str], str]]) -> Dict[str, Dict[str, Union[str, str, int]]]:
    """
    Decides the action to take for each column based on the drift report.

    Args:
        drift_report (Dict[str, Union[Dict[str, str], List[str], Dict[str, str], str]]): The drift report.

    Returns:
        Dict[str, Dict[str, Union[str, str, int]]]: A dictionary containing the action, reason, and risk level for each column.
    """
    decisions = {}
    for col_name, col_type in drift_report['new_columns'].items():
        if col_type =='string':
            decisions[col_name] = {'action': 'ADD_TO_SCHEMA','reason': 'New nullable string column', 'risk_level': 0}
        elif col_type in ['float', 'double', 'decimal']:
            decisions[col_name] = {'action': 'FLAG_ANOMALY','reason': 'New numeric column affecting revenue', 'risk_level': 2}
    for col_name in drift_report['removed_columns']:
        decisions[col_name] = {'action': 'HALT','reason': 'Removed column', 'risk_level': 3}
    for col_name, (old_type, new_type) in drift_report['type_changes'].items():
        if new_type in ['float', 'double', 'decimal'] and old_type in ['int', 'long']:
            decisions[col_name] = {'action': 'ADD_TO_SCHEMA','reason': 'Type widening', 'risk_level': 1}
        elif new_type in ['int', 'long'] and old_type in ['float', 'double', 'decimal']:
            decisions[col_name] = {'action': 'FLAG_ANOMALY','reason': 'Type narrowing', 'risk_level': 2}
    return decisions

def apply_schema_evolution(spark_df: SparkDataFrame, decisions: Dict[str, Dict[str, Union[str, str, int]]], updated_schema: Dict[str, str]) -> Tuple[SparkDataFrame, List[str]]:
    """
    Applies the schema evolution decisions to the DataFrame.

    Args:
        spark_df (SparkDataFrame): The DataFrame to evolve.
        decisions (Dict[str, Dict[str, Union[str, str, int]]]): The decisions to apply.
        updated_schema (Dict[str, str]): The updated schema.

    Returns:
        Tuple[SparkDataFrame, List[str]]: The evolved DataFrame and a list of migration notes.
    """
    migration_notes = []
    for col_name, decision in decisions.items():
        if decision['action'] == 'DROP_SILENTLY':
            spark_df = spark_df.drop(col_name)
        elif decision['action'] == 'ADD_TO_SCHEMA':
            migration_notes.append(f"Added column '{col_name}' to schema.")
        elif decision['action'] == 'FLAG_ANOMALY':
            spark_df = spark_df.withColumn(f"{col_name}_anomaly_flag", col(col_name).isNull())
            migration_notes.append(f"Flagged anomalies in column '{col_name}'.")
    return spark_df, migration_notes

def handle_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str], spark_df: SparkDataFrame = None) -> Dict[str, Union[Dict[str, Union[Dict[str, Dict[str, Union[str, str, int]]], List[str]]], Dict[str, Union[Dict[str, str], List[str], Dict[str, str], str]]]]:
    """
    Handles schema drift by detecting, deciding, and applying schema evolution.

    Args:
        expected_schema (Dict[str, str]): The expected schema.
        actual_schema (Dict[str, str]): The actual schema.
        spark_df (SparkDataFrame, optional): The DataFrame to evolve. Defaults to None.

    Returns:
        Dict[str, Union[Dict[str, Union[Dict[str, Dict[str, Union[str, str, int]]], List[str]]], Dict[str, Union[Dict[str, str], List[str], Dict[str, str], str]]]]: The full evolution report.
    """
    drift_report = detect_schema_drift(expected_schema, actual_schema)
    decisions = decide_action(drift_report)
    if spark_df is not None:
        evolved_df, migration_notes = apply_schema_evolution(spark_df, decisions, {**expected_schema, **{k: v for k, v in actual_schema.items() if k not in expected_schema}})
        print(f"Drift Report: {drift_report}")
        print(f"Decisions: {decisions}")
        print(f"Migration Notes: {migration_notes}")
        return {'drift_report': drift_report, 'decisions': decisions,'migration_notes': migration_notes}
    else:
        print(f"Drift Report: {drift_report}")
        print(f"Decisions: {decisions}")
        return {'drift_report': drift_report, 'decisions': decisions}
