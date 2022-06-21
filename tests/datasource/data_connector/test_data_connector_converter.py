from typing import List

import pandas as pd

from great_expectations import DataContext
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.batch import RuntimeBatchRequest


def test_retrieve_asset_from_validation_result(
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    """Investigatory happy path test retrieving asset info from a validation result."""

    # TODO: Use a more purpose built fixture
    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    batch_request: dict = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "Titanic_1911",
    }

    # RuntimeBatchRequest with a DataFrame
    runtime_batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "my_runtime_data_connector",
            "data_asset_name": "test_df",
            "batch_identifiers": {
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
            "runtime_parameters": {"batch_data": test_df},
        }
    )

    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template",
        expectation_suite_name="my_expectation_suite",
        action_list=[
            {
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreValidationResultAction",
                },
            },
            {
                "name": "store_evaluation_params",
                "action": {
                    "class_name": "StoreEvaluationParametersAction",
                },
            },
            {
                "name": "update_data_docs",
                "action": {
                    "class_name": "UpdateDataDocsAction",
                },
            },
        ],
    )

    context.create_expectation_suite("my_expectation_suite")
    # noinspection PyUnusedLocal
    result = checkpoint.run(
        validations=[
            {"batch_request": runtime_batch_request},
            {"batch_request": batch_request},
        ]
    )

    validation_keys: List[str] = context.validations_store.list_keys()

    assert len(validation_keys) == 2
    assert result["success"]

    print(context.validations_store.get(validation_keys[1]))


def test_retrieve_asset_from_checkpoint_configuration():
    pass
