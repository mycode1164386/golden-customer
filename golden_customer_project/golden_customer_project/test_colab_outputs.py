import glob
from pathlib import Path

import pandas as pd
import yaml


CONFIG_PATH = "/content/colab_config.yaml"


def _load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _single_csv_from_dir(directory: str) -> Path:
    matches = glob.glob(f"{directory}/part-*.csv")
    assert matches, f"No Spark CSV part file found in: {directory}"
    return Path(matches[0])


def test_gold_output_exists():
    config = _load_config()
    output_dir = config["output"]["gold_dataset_dir"]
    csv_file = _single_csv_from_dir(output_dir)
    assert csv_file.exists()


def test_metrics_output_exists():
    config = _load_config()
    output_dir = config["output"]["metrics_dir"]
    csv_file = _single_csv_from_dir(output_dir)
    assert csv_file.exists()


def test_gold_dataset_not_empty():
    config = _load_config()
    gold_file = _single_csv_from_dir(config["output"]["gold_dataset_dir"])
    gold_df = pd.read_csv(gold_file)
    assert len(gold_df) > 0, "Gold dataset is empty"


def test_golden_customer_id_is_unique():
    config = _load_config()
    gold_file = _single_csv_from_dir(config["output"]["gold_dataset_dir"])
    gold_df = pd.read_csv(gold_file)
    assert gold_df["golden_customer_id"].is_unique, "golden_customer_id is not unique"


def test_match_rule_values_are_valid():
    config = _load_config()
    expected = set(config["validation"]["expected_match_rules"])
    gold_file = _single_csv_from_dir(config["output"]["gold_dataset_dir"])
    gold_df = pd.read_csv(gold_file)

    actual = set(gold_df["match_rule"].dropna().unique().tolist())
    assert actual.issubset(expected), f"Unexpected match_rule values found: {actual - expected}"


def test_required_metrics_exist():
    config = _load_config()
    required_metrics = set(config["validation"]["required_metrics"])

    metrics_file = _single_csv_from_dir(config["output"]["metrics_dir"])
    metrics_df = pd.read_csv(metrics_file)

    actual_metrics = set(metrics_df["metric_name"].tolist())
    missing = required_metrics - actual_metrics
    assert not missing, f"Missing required metrics: {missing}"


def test_gold_total_metric_matches_actual_row_count():
    config = _load_config()

    gold_file = _single_csv_from_dir(config["output"]["gold_dataset_dir"])
    metrics_file = _single_csv_from_dir(config["output"]["metrics_dir"])

    gold_df = pd.read_csv(gold_file)
    metrics_df = pd.read_csv(metrics_file)

    gold_total = metrics_df.loc[
        metrics_df["metric_name"] == "gold_total_records", "metric_value"
    ].iloc[0]

    assert int(gold_total) == len(gold_df), (
        f"gold_total_records metric ({gold_total}) does not match actual Gold row count ({len(gold_df)})"
    )
