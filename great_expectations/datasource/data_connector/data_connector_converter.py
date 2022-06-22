class InferredToConfiguredDataConnectorConverter:
    pass

    INFERRED_TO_CONFIGURED_MAPPING: dict = {
        "InferredAssetAzureDataConnector": "ConfiguredAssetAzureDataConnector",
        # TODO: Fill this in
    }

    def convert(
        self,
        inferred_data_connector: Union[
            InferredAssetFilePathDataConnector,
            InferredAssetSqlDataConnector,
            DataConnectorConfig,
        ],
    ):
        """Placeholder method for testing conversion to ConfiguredAsset"""
        # TODO: This should be broken up into separate methods for data connector type
        # TODO: This or related methods should check to make sure the data connector is not a custom user created one
        pass

    # TODO: What methods are needed here?
