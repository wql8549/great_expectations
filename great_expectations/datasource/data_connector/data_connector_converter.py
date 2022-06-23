from typing import List, Optional, Union

from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.data_context.types.base import (
    DataConnectorConfig,
    dataConnectorConfigSchema,
)
from great_expectations.datasource.data_connector import (
    InferredAssetFilePathDataConnector,
    InferredAssetSqlDataConnector,
)


class InferredToConfiguredDataConnectorConverter:
    pass

    INFERRED_TO_CONFIGURED_MAPPING: dict = {
        "InferredAssetAzureDataConnector": "ConfiguredAssetAzureDataConnector",
        # TODO: AJB 20220623 Fill this in
    }

    def convert(
        self,
        inferred_data_connector: Union[
            InferredAssetFilePathDataConnector,
            InferredAssetSqlDataConnector,
            DataConnectorConfig,
        ],
        validation_results: Optional[List[ExpectationSuiteValidationResult]] = None,
    ):

        # TODO: AJB 20220623 Use the INFERRED_TO_CONFIGURED_MAPPING mapping here
        if isinstance(inferred_data_connector, InferredAssetSqlDataConnector):
            return self._convert_inferred_sql(
                inferred_data_connector, validation_results
            )
        """Placeholder method for testing conversion to ConfiguredAsset"""
        # TODO: AJB 20220623 This should be broken up into separate methods for data connector type
        # TODO: AJB 20220623 This or related methods should check to make sure the data connector is not a custom user created one
        pass

    # TODO: AJB 20220623 What methods are needed here?

    def _get_asset_names_from_validation_results(
        self,
        data_connector_name: str,
        validation_results: Optional[List[ExpectationSuiteValidationResult]] = None,
    ) -> List[str]:
        # TODO: AJB 20220623 Add docstring
        data_asset_names: List[str] = []

        # Skip if we can't get the data asset name
        for validation_result in validation_results:
            if "meta" in validation_result:
                if "active_batch_definition" in validation_result["meta"]:
                    if (
                        "data_asset_name"
                        in validation_result["meta"]["active_batch_definition"]
                    ):
                        # Only add for assets under this data connector
                        if (
                            validation_result["meta"]["active_batch_definition"][
                                "data_connector_name"
                            ]
                            == data_connector_name
                        ):
                            data_asset_names.append(
                                validation_result["meta"]["active_batch_definition"][
                                    "data_asset_name"
                                ]
                            )

        return data_asset_names

    def _convert_inferred_sql(
        self,
        inferred_data_connector: Union[
            InferredAssetFilePathDataConnector,
            InferredAssetSqlDataConnector,
            DataConnectorConfig,
        ],
        validation_results: Optional[List[ExpectationSuiteValidationResult]] = None,
    ):
        # TODO: AJB 20220623 Flesh this docstring out
        """Convert an InferredAssetSqlDataConnector to a ConfiguredAssetSqlDataConnector."""

        data_asset_names: List[str] = []
        # TODO: AJB 20220623 Do we try accessing data to get asset names if it is available? YES. If so split into separate method.
        # try:
        #     asset_names_from_db: List[str] = inferred_data_connector.get_available_data_asset_names()
        #     data_asset_names.extend(asset_names_from_db)
        # except: # TODO: AJB 20220623 What is the right exception type for this?
        #     pass

        data_asset_names.extend(
            self._get_asset_names_from_validation_results(
                inferred_data_connector.name, validation_results
            )
        )

        assets = {data_asset_name: {} for data_asset_name in data_asset_names}

        configured_asset_data_connector_config: DataConnectorConfig = (
            DataConnectorConfig(
                name=f"auto_generated_from_{inferred_data_connector.name}",
                class_name="ConfiguredAssetSqlDataConnector",
                module_name="great_expectations.datasource.data_connector",
                datasource_name=inferred_data_connector.datasource_name,
                assets=assets,
            )
        )
        # TODO: AJB 20220623 is a round trip through the schema necessary?:
        # validated_config: DataConnectorConfig = dataConnectorConfigSchema.load(configured_asset_data_connector_config.to_json_dict())
        # return validated_config

        return configured_asset_data_connector_config
