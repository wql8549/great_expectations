from typing import List

import pandas as pd

from great_expectations import DataContext
from great_expectations.checkpoint import Checkpoint
from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationSuiteValidationResult,
)
from great_expectations.core.batch import RuntimeBatchRequest


def test_retrieve_asset_from_validation_result_file_based(
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
    suite = context.get_expectation_suite("my_expectation_suite")
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_table_columns_to_match_ordered_list",
        kwargs={
            "column_list": [
                "account_id",
                "user_id",
                "transaction_id",
                "transaction_type",
                "transaction_amt_usd",
            ]
        },
        meta={
            "notes": {
                "format": "markdown",
                "content": "Some clever comment about this expectation. **Markdown** `Supported`",
            }
        },
    )
    suite.add_expectation(expectation_configuration=expectation_configuration)
    context.save_expectation_suite(expectation_suite=suite)

    assert context.list_expectation_suite_names() == ["my_expectation_suite"]
    assert len(context.get_expectation_suite("my_expectation_suite").expectations) == 1

    result = checkpoint.run(
        validations=[
            {"batch_request": runtime_batch_request},
            {"batch_request": batch_request},
        ]
    )

    validation_keys: List[str] = context.validations_store.list_keys()

    assert len(validation_keys) == 2
    assert not result["success"]

    # print(context.validations_store.get(validation_keys[1]))

    validation_results: List[ExpectationSuiteValidationResult] = []

    for validation_key in validation_keys:
        validation_result = context.validations_store.get(validation_key)
        validation_results.append(validation_result)
        # print(type(validation_result))  # <class 'great_expectations.core.expectation_validation_result.ExpectationSuiteValidationResult'>
        print(validation_result)
        # print(validation_result["meta"])
        # print(validation_result["meta"]["active_batch_definition"])
        # print(validation_result["meta"]["batch_spec"])
        # print(validation_result["meta"]["great_expectations_version"])

    # For reference during development
    # my_basic_data_connector:
    # class_name: InferredAssetFilesystemDataConnector
    # base_directory: {data_path}
    # default_regex:
    # pattern: (.*)\\.csv
    # group_names:
    # - data_asset_name

    # For reference during development:
    # assert validation_results == [
    #     {
    #         "results": [
    #             {
    #                 "meta": {},
    #                 "result": {
    #                     "details": {
    #                         "mismatched": [
    #                             {
    #                                 "Expected": "account_id",
    #                                 "Expected Column Position": 0,
    #                                 "Found": "col1"
    #                             },
    #                             {
    #                                 "Expected": "user_id",
    #                                 "Expected Column Position": 1,
    #                                 "Found": "col2"
    #                             },
    #                             {
    #                                 "Expected": "transaction_id",
    #                                 "Expected Column Position": 2,
    #                                 "Found": None
    #                             },
    #                             {
    #                                 "Expected": "transaction_type",
    #                                 "Expected Column Position": 3,
    #                                 "Found": None
    #                             },
    #                             {
    #                                 "Expected": "transaction_amt_usd",
    #                                 "Expected Column Position": 4,
    #                                 "Found": None
    #                             }
    #                         ]
    #                     },
    #                     "observed_value": [
    #                         "col1",
    #                         "col2"
    #                     ]
    #                 },
    #                 "expectation_config": {
    #                     "meta": {
    #                         "notes": {
    #                             "content": "Some clever comment about this expectation. **Markdown** `Supported`",
    #                             "format": "markdown"
    #                         }
    #                     },
    #                     "kwargs": {
    #                         "batch_id": "1abaa2df537d732362c957bf9529da22",
    #                         "column_list": [
    #                             "account_id",
    #                             "user_id",
    #                             "transaction_id",
    #                             "transaction_type",
    #                             "transaction_amt_usd"
    #                         ]
    #                     },
    #                     "expectation_type": "expect_table_columns_to_match_ordered_list"
    #                 },
    #                 "exception_info": {
    #                     "exception_message": None,
    #                     "exception_traceback": None,
    #                     "raised_exception": False
    #                 },
    #                 "success": False
    #             }
    #         ],
    #         "meta": {
    #             "active_batch_definition": {
    #                 "batch_identifiers": {
    #                     "airflow_run_id": 1234567890,
    #                     "pipeline_stage_name": "core_processing"
    #                 },
    #                 "data_asset_name": "test_df",
    #                 "data_connector_name": "my_runtime_data_connector",
    #                 "datasource_name": "my_datasource"
    #             },
    #             "batch_markers": {
    #                 "ge_load_time": "20220622T181140.948376Z",
    #                 "pandas_data_fingerprint": "1e461a0df5fe0a6db2c3bc4ef88ef1f0"
    #             },
    #             "batch_spec": {
    #                 "batch_data": "PandasDataFrame",
    #                 "data_asset_name": "test_df"
    #             },
    #             "expectation_suite_name": "my_expectation_suite",
    #             "great_expectations_version": "0.15.10+23.g815099491.dirty",
    #             "run_id": {
    #                 "run_name": "2022-11-foo-bar-template",
    #                 "run_time": "2022-06-22T14:11:40.931109+00:00"
    #             },
    #             "validation_time": "20220622T181141.005537Z"
    #         },
    #         "statistics": {
    #             "evaluated_expectations": 1,
    #             "success_percent": 0.0,
    #             "successful_expectations": 0,
    #             "unsuccessful_expectations": 1
    #         },
    #         "evaluation_parameters": {},
    #         "success": False
    #     },
    #     {
    #         "results": [
    #             {
    #                 "meta": {},
    #                 "result": {
    #                     "details": {
    #                         "mismatched": [
    #                             {
    #                                 "Expected": "account_id",
    #                                 "Expected Column Position": 0,
    #                                 "Found": "Unnamed: 0"
    #                             },
    #                             {
    #                                 "Expected": "user_id",
    #                                 "Expected Column Position": 1,
    #                                 "Found": "Name"
    #                             },
    #                             {
    #                                 "Expected": "transaction_id",
    #                                 "Expected Column Position": 2,
    #                                 "Found": "PClass"
    #                             },
    #                             {
    #                                 "Expected": "transaction_type",
    #                                 "Expected Column Position": 3,
    #                                 "Found": "Age"
    #                             },
    #                             {
    #                                 "Expected": "transaction_amt_usd",
    #                                 "Expected Column Position": 4,
    #                                 "Found": "Sex"
    #                             },
    #                             {
    #                                 "Expected": None,
    #                                 "Expected Column Position": 5,
    #                                 "Found": "Survived"
    #                             },
    #                             {
    #                                 "Expected": None,
    #                                 "Expected Column Position": 6,
    #                                 "Found": "SexCode"
    #                             }
    #                         ]
    #                     },
    #                     "observed_value": [
    #                         "Unnamed: 0",
    #                         "Name",
    #                         "PClass",
    #                         "Age",
    #                         "Sex",
    #                         "Survived",
    #                         "SexCode"
    #                     ]
    #                 },
    #                 "expectation_config": {
    #                     "meta": {
    #                         "notes": {
    #                             "content": "Some clever comment about this expectation. **Markdown** `Supported`",
    #                             "format": "markdown"
    #                         }
    #                     },
    #                     "kwargs": {
    #                         "batch_id": "987634a27606ee9b395599d85743ee12",
    #                         "column_list": [
    #                             "account_id",
    #                             "user_id",
    #                             "transaction_id",
    #                             "transaction_type",
    #                             "transaction_amt_usd"
    #                         ]
    #                     },
    #                     "expectation_type": "expect_table_columns_to_match_ordered_list"
    #                 },
    #                 "exception_info": {
    #                     "exception_message": None,
    #                     "exception_traceback": None,
    #                     "raised_exception": False
    #                 },
    #                 "success": False
    #             }
    #         ],
    #         "meta": {
    #             "active_batch_definition": {
    #                 "batch_identifiers": {},
    #                 "data_asset_name": "Titanic_1911",
    #                 "data_connector_name": "my_basic_data_connector",
    #                 "datasource_name": "my_datasource"
    #             },
    #             "batch_markers": {
    #                 "ge_load_time": "20220622T181141.490854Z",
    #                 "pandas_data_fingerprint": "3aaabc12402f987ff006429a7756f5cf"
    #             },
    #             "batch_spec": {
    #                 "path": "/private/var/folders/ds/hn_qpp1n6y3fz28clrkfmpsr0000gn/T/pytest-of-anthonyburdi/pytest-13/titanic_data_context0/great_expectations/../data/titanic/Titanic_1911.csv"
    #             },
    #             "expectation_suite_name": "my_expectation_suite",
    #             "great_expectations_version": "0.15.10+23.g815099491.dirty",
    #             "run_id": {
    #                 "run_name": "2022-11-foo-bar-template",
    #                 "run_time": "2022-06-22T14:11:40.931109+00:00"
    #             },
    #             "validation_time": "20220622T181141.556709Z"
    #         },
    #         "statistics": {
    #             "evaluated_expectations": 1,
    #             "success_percent": 0.0,
    #             "successful_expectations": 0,
    #             "unsuccessful_expectations": 1
    #         },
    #         "evaluation_parameters": {},
    #         "success": False
    #     }
    # ]


def test_retrieve_asset_from_validation_result_sql_based():
    pass


def test_retrieve_asset_from_checkpoint_configuration():
    pass
