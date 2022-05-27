import configparser
import json
import logging
import os
from abc import ABC
from typing import Dict, Mapping, Optional, Union

from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.store import ExpectationsStore, Store
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DataContextConfigDefaults,
    anonymizedUsageStatisticsSchema,
)
from great_expectations.data_context.util import build_store_from_config

logger = logging.getLogger(__name__)

yaml: YAMLHandler = YAMLHandler()


class AbstractDataContext(ABC):
    """
    Base class for all DataContexts that contain all context-agnostic data context operations.

    The class encapsulates most store / core components and convenience methods used to access them, meaning the
    majority of DataContext functionality lives here.

    TODO: eventually the dependency on ConfigPeer will be removed and this will become a pure ABC.
    """

    PROFILING_ERROR_CODE_TOO_MANY_DATA_ASSETS = 2
    PROFILING_ERROR_CODE_SPECIFIED_DATA_ASSETS_NOT_FOUND = 3
    PROFILING_ERROR_CODE_NO_BATCH_KWARGS_GENERATORS_FOUND = 4
    PROFILING_ERROR_CODE_MULTIPLE_BATCH_KWARGS_GENERATORS_FOUND = 5

    DOLLAR_SIGN_ESCAPE_STRING = r"\$"
    FALSEY_STRINGS = ["FALSE", "false", "False", "f", "F", "0"]

    TEST_YAML_CONFIG_SUPPORTED_STORE_TYPES = [
        "ExpectationsStore",
        "ValidationsStore",
        "HtmlSiteStore",
        "EvaluationParameterStore",
        "MetricStore",
        "SqlAlchemyQueryStore",
        "CheckpointStore",
        "ProfilerStore",
    ]
    TEST_YAML_CONFIG_SUPPORTED_DATASOURCE_TYPES = [
        "Datasource",
        "SimpleSqlalchemyDatasource",
    ]
    TEST_YAML_CONFIG_SUPPORTED_DATA_CONNECTOR_TYPES = [
        "InferredAssetFilesystemDataConnector",
        "ConfiguredAssetFilesystemDataConnector",
        "InferredAssetS3DataConnector",
        "ConfiguredAssetS3DataConnector",
        "InferredAssetAzureDataConnector",
        "ConfiguredAssetAzureDataConnector",
        "InferredAssetGCSDataConnector",
        "ConfiguredAssetGCSDataConnector",
        "InferredAssetSqlDataConnector",
        "ConfiguredAssetSqlDataConnector",
    ]
    TEST_YAML_CONFIG_SUPPORTED_CHECKPOINT_TYPES = [
        "Checkpoint",
        "SimpleCheckpoint",
    ]
    TEST_YAML_CONFIG_SUPPORTED_PROFILER_TYPES = [
        "RuleBasedProfiler",
    ]
    ALL_TEST_YAML_CONFIG_DIAGNOSTIC_INFO_TYPES = [
        "__substitution_error__",
        "__yaml_parse_error__",
        "__custom_subclass_not_core_ge__",
        "__class_name_not_provided__",
    ]
    ALL_TEST_YAML_CONFIG_SUPPORTED_TYPES = (
        TEST_YAML_CONFIG_SUPPORTED_STORE_TYPES
        + TEST_YAML_CONFIG_SUPPORTED_DATASOURCE_TYPES
        + TEST_YAML_CONFIG_SUPPORTED_DATA_CONNECTOR_TYPES
        + TEST_YAML_CONFIG_SUPPORTED_CHECKPOINT_TYPES
        + TEST_YAML_CONFIG_SUPPORTED_PROFILER_TYPES
    )

    UNCOMMITTED_DIRECTORIES = ["data_docs", "validations"]
    GE_UNCOMMITTED_DIR = "uncommitted"
    BASE_DIRECTORIES = [
        DataContextConfigDefaults.CHECKPOINTS_BASE_DIRECTORY.value,
        DataContextConfigDefaults.EXPECTATIONS_BASE_DIRECTORY.value,
        DataContextConfigDefaults.PLUGINS_BASE_DIRECTORY.value,
        DataContextConfigDefaults.PROFILERS_BASE_DIRECTORY.value,
        GE_UNCOMMITTED_DIR,
    ]
    GE_DIR = "great_expectations"
    GE_YML = "great_expectations.yml"
    GE_EDIT_NOTEBOOK_DIR = GE_UNCOMMITTED_DIR

    GLOBAL_CONFIG_PATHS = [
        os.path.expanduser("~/.great_expectations/great_expectations.conf"),
        "/etc/great_expectations.conf",
    ]

    def __init__(
        self,
        project_config: Union[DataContextConfig, Mapping],
        runtime_environment: Optional[dict] = None
        # context_root_dir do we keep this?
    ):
        self._project_config = project_config
        self.runtime_environment = runtime_environment or {}

        self._apply_global_config_overrides()

        # We want to have directories set up before initializing usage statistics so that we can obtain a context instance id
        self._in_memory_instance_id = (
            None  # This variable *may* be used in case we cannot save an instance id
        )

        # Init stores
        self._stores = {}
        self._init_stores(self.project_config_with_variables_substituted.stores)

    ### properties
    @property
    def expectations_store_name(self) -> Optional[str]:
        return self.project_config_with_variables_substituted.expectations_store_name

    @property
    def expectations_store(self) -> ExpectationsStore:
        return self.stores[self.expectations_store_name]

    ### private methods
    def _apply_global_config_overrides(self) -> None:
        """
        Checking global settings like usage_statistics and environment variables and applying them to
        config associated with current BaseDataContext

        Run as part of BaseDataContext __init__()
        """
        # check for global usage statistics opt out
        validation_errors = {}

        if self._check_global_usage_statistics_opt_out():
            logger.info(
                "Usage statistics is disabled globally. Applying override to project_config."
            )
            self.config.anonymous_usage_statistics.enabled = False

        # check for global data_context_id
        global_data_context_id = AbstractDataContext._get_global_config_value(
            environment_variable="GE_DATA_CONTEXT_ID",
            conf_file_section="anonymous_usage_statistics",
            conf_file_option="data_context_id",
        )
        if global_data_context_id:
            data_context_id_errors = anonymizedUsageStatisticsSchema.validate(
                {"data_context_id": global_data_context_id}
            )
            if not data_context_id_errors:
                logger.info(
                    "data_context_id is defined globally. Applying override to project_config."
                )
                self.config.anonymous_usage_statistics.data_context_id = (
                    global_data_context_id
                )
            else:
                validation_errors.update(data_context_id_errors)
        # check for global usage_statistics url
        global_usage_statistics_url = AbstractDataContext._get_global_config_value(
            environment_variable="GE_USAGE_STATISTICS_URL",
            conf_file_section="anonymous_usage_statistics",
            conf_file_option="usage_statistics_url",
        )
        if global_usage_statistics_url:
            usage_statistics_url_errors = anonymizedUsageStatisticsSchema.validate(
                {"usage_statistics_url": global_usage_statistics_url}
            )
            if not usage_statistics_url_errors:
                logger.info(
                    "usage_statistics_url is defined globally. Applying override to project_config."
                )
                self.config.anonymous_usage_statistics.usage_statistics_url = (
                    global_usage_statistics_url
                )
            else:
                validation_errors.update(usage_statistics_url_errors)
        if validation_errors:
            logger.warning(
                "The following globally-defined config variables failed validation:\n{}\n\n"
                "Please fix the variables if you would like to apply global values to project_config.".format(
                    json.dumps(validation_errors, indent=2)
                )
            )

    @classmethod
    def _get_global_config_value(
        cls,
        environment_variable: Optional[str] = None,
        conf_file_section=None,
        conf_file_option=None,
    ) -> Optional[str]:
        """
        TODO: this needs to be split up or handled elsewhere
        Args:
            environment_variable ():
            conf_file_section ():
            conf_file_option ():

        Returns:

        """
        assert (conf_file_section and conf_file_option) or (
            not conf_file_section and not conf_file_option
        ), "Must pass both 'conf_file_section' and 'conf_file_option' or neither."
        # TODO : this would be under ephemeral
        if environment_variable and os.environ.get(environment_variable, False):
            return os.environ.get(environment_variable)
        # TODO : pull this out into something in the FileDataContext
        if conf_file_section and conf_file_option:
            for config_path in AbstractDataContext.GLOBAL_CONFIG_PATHS:
                config = configparser.ConfigParser()
                config.read(config_path)
                config_value = config.get(
                    conf_file_section, conf_file_option, fallback=None
                )
                if config_value:
                    return config_value
        return None

    @staticmethod
    def _check_global_usage_statistics_opt_out() -> bool:
        if os.environ.get("GE_USAGE_STATS", False):
            ge_usage_stats = os.environ.get("GE_USAGE_STATS")
            if ge_usage_stats in AbstractDataContext.FALSEY_STRINGS:
                return True
            else:
                logger.warning(
                    "GE_USAGE_STATS environment variable must be one of: {}".format(
                        AbstractDataContext.FALSEY_STRINGS
                    )
                )
        for config_path in AbstractDataContext.GLOBAL_CONFIG_PATHS:
            config = configparser.ConfigParser()
            states = config.BOOLEAN_STATES
            for falsey_string in AbstractDataContext.FALSEY_STRINGS:
                states[falsey_string] = False
            states["TRUE"] = True
            states["True"] = True
            config.BOOLEAN_STATES = states
            config.read(config_path)
            try:
                if config.getboolean("anonymous_usage_statistics", "enabled") is False:
                    # If stats are disabled, then opt out is true
                    return True
            except (ValueError, configparser.Error):
                pass
        return False

    def _build_store_from_config(
        self, store_name: str, store_config: dict
    ) -> Optional[Store]:
        module_name = "great_expectations.data_context.store"
        # Set expectations_store.store_backend_id to the data_context_id from the project_config if
        # the expectations_store does not yet exist by:
        # adding the data_context_id from the project_config
        # to the store_config under the key manually_initialize_store_backend_id
        if (store_name == self.expectations_store_name) and store_config.get(
            "store_backend"
        ):
            store_config["store_backend"].update(
                {
                    "manually_initialize_store_backend_id": self.project_config_with_variables_substituted.anonymous_usage_statistics.data_context_id
                }
            )

        # Set suppress_store_backend_id = True if store is inactive and has a store_backend.
        if (
            store_name not in [store["name"] for store in self.list_active_stores()]
            and store_config.get("store_backend") is not None
        ):
            store_config["store_backend"].update({"suppress_store_backend_id": True})

        new_store = build_store_from_config(
            store_name=store_name,
            store_config=store_config,
            module_name=module_name,
            runtime_environment={
                "root_directory": self.root_directory,
            },
        )
        self._stores[store_name] = new_store
        return new_store

    def _init_stores(self, store_configs: Dict[str, dict]) -> None:
        """Initialize all Stores for this DataContext.

        Stores are a good fit for reading/writing objects that:
            1. follow a clear key-value pattern, and
            2. are usually edited programmatically, using the Context

        Note that stores do NOT manage plugins.
        """
        for store_name, store_config in store_configs.items():
            self._build_store_from_config(store_name, store_config)
