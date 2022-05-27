import configparser
import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Dict, Mapping, Optional, Union

import great_expectations.exceptions.exceptions as ge_exceptions
from great_expectations.core.config_peer import ConfigPeer
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.types.base import (
    AnonymizedUsageStatisticsConfig,
    DataContextConfig,
    DataContextConfigDefaults,
    anonymizedUsageStatisticsSchema,
)
from great_expectations.data_context.util import (
    build_store_from_config,
    instantiate_class_from_config,
    substitute_all_config_variables,
)
from great_expectations.rule_based_profiler.data_assistant.data_assistant_dispatcher import (
    DataAssistantDispatcher,
)

from great_expectations.core.usage_statistics.usage_statistics import (  # isort: skip
    UsageStatisticsHandler,
)

#
from great_expectations.data_context.store import ExpectationsStore  # isort:skip
from great_expectations.data_context.store import Store  # isort:skip
from great_expectations.data_context.store import TupleStoreBackend  # isort:skip

# this is here because of circular imports
from great_expectations.rule_based_profiler import RuleBasedProfiler  # isort:skip
from great_expectations.rule_based_profiler import RuleBasedProfilerResult  # isort:skip


logger = logging.getLogger(__name__)

yaml: YAMLHandler = YAMLHandler()


class AbstractDataContext(ConfigPeer, ABC):
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

    _data_context = None

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

        # Init data_context_id
        self._data_context_id = self._construct_data_context_id()

        # Override the project_config data_context_id if an expectations_store was already set up
        self.config.anonymous_usage_statistics.data_context_id = self._data_context_id
        self._initialize_usage_statistics(
            self.project_config_with_variables_substituted.anonymous_usage_statistics
        )

        # Store cached datasources but don't init them
        self._cached_datasources = {}

        # Build the datasources we know about and have access to
        self._init_datasources(self.project_config_with_variables_substituted)

        # Init validation operators
        # NOTE - 20200522 - JPC - A consistent approach to lazy loading for plugins will be useful here, harmonizing
        # the way that execution environments (AKA datasources), validation operators, site builders and other
        # plugins are built.
        self.validation_operators = {}
        # NOTE - 20210112 - Alex Sherstinsky - Validation Operators are planned to be deprecated.
        if (
            "validation_operators" in self.get_config().commented_map
            and self.config.validation_operators
        ):
            for (
                validation_operator_name,
                validation_operator_config,
            ) in self.config.validation_operators.items():
                self.add_validation_operator(
                    validation_operator_name,
                    validation_operator_config,
                )

        self._evaluation_parameter_dependencies_compiled = False
        self._evaluation_parameter_dependencies = {}

        # is this something we need too?
        self._assistants = DataAssistantDispatcher(data_context=self)

    ### properties
    @property
    def expectations_store_name(self) -> Optional[str]:
        return self.project_config_with_variables_substituted.expectations_store_name

    @property
    def expectations_store(self) -> ExpectationsStore:
        return self.stores[self.expectations_store_name]

    @property
    def config(self) -> DataContextConfig:
        return self._project_config

    @property
    def project_config_with_variables_substituted(self) -> DataContextConfig:
        return self.get_config_with_variables_substituted()

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
        """
        Set expectations_store.store_backend_id to the data_context_id from the project_config if
        the expectations_store does not yet exist by:
        adding the data_context_id from the project_config
        to the store_config under the key manually_initialize_store_backend_id

        Args:
            store_name ():
            store_config ():

        Returns:
            TODO
        """
        module_name = "great_expectations.data_context.store"
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

    def _init_datasources(self, config: DataContextConfig) -> None:
        """
        Initializes datasources from config. Called by the __init__() stage.

        If configuration contains datasources that GE can no longer connect to.
        Raises a warning, because that won't break anything until we try to retrieve a batch with it.
        The error will be caught at the context.get_batch() step.

        Args:
            config (DataContextConfig): Config associated with current AbstractDataContext
        """
        if not config.datasources:
            return
        for datasource_name in config.datasources:
            try:
                self._cached_datasources[datasource_name] = self.get_datasource(
                    datasource_name=datasource_name
                )
            except ge_exceptions.DatasourceInitializationError as e:
                logger.warning(f"Cannot initialize datasource {datasource_name}: {e}")
                # this error will happen if our configuration contains datasources that GE can no longer connect to.
                # this is ok, as long as we don't use it to retrieve a batch. If we try to do that, the error will be
                # caught at the context.get_batch() step. So we just pass here.
                pass

    def _construct_data_context_id(self) -> str:
        """
        Choose the id of the currently-configured expectations store, if available and a persistent store.
        If not, it should choose the id stored in DataContextConfig.

        TODO: cloud is handled at the BaseDataContext level for now. It will need to be moved to CloudDataContext. Stub exists.
        ID is returned from :
            1) store
            2) config
        Returns:
            UUID to use as data_context_id
        """
        # Choose the id of the currently-configured expectations store, if it is a persistent store
        expectations_store = self._stores[
            self.project_config_with_variables_substituted.expectations_store_name
        ]
        if isinstance(expectations_store.store_backend, TupleStoreBackend):
            # suppress_warnings since a warning will already have been issued during the store creation if there was an invalid store config
            return expectations_store.store_backend_id_warnings_suppressed

        # Otherwise choose the id stored in the project_config
        else:
            return (
                self.project_config_with_variables_substituted.anonymous_usage_statistics.data_context_id
            )

    def _initialize_usage_statistics(
        self, usage_statistics_config: AnonymizedUsageStatisticsConfig
    ) -> None:
        """Initialize the usage statistics system."""
        if not usage_statistics_config.enabled:
            logger.info("Usage statistics is disabled; skipping initialization.")
            self._usage_statistics_handler = None
            return

        self._usage_statistics_handler = UsageStatisticsHandler(
            data_context=self,
            data_context_id=self._data_context_id,
            usage_statistics_url=usage_statistics_config.usage_statistics_url,
        )

    # def get_config_with_variables_substituted(self, config=None) -> DataContextConfig:
    #     """
    #     Substitute vars in config of form ${var} or $(var) with values found in the following places,
    #     in order of precedence: ge_cloud_config (for Data Contexts in GE Cloud mode), runtime_environment,
    #     environment variables, config_variables, or ge_cloud_config_variable_defaults (allows certain variables to
    #     be optional in GE Cloud mode).
    #     """
    #     if not config:
    #         config = self.config
    #
    #     substituted_config_variables = substitute_all_config_variables(
    #         self.config_variables,
    #         dict(os.environ),
    #         self.DOLLAR_SIGN_ESCAPE_STRING,
    #     )

    # Public method
    def add_validation_operator(
        self, validation_operator_name: str, validation_operator_config: dict
    ) -> "ValidationOperator":
        """Add a new ValidationOperator to the DataContext and (for convenience) return the instantiated object.

        Args:
            validation_operator_name (str): a key for the new ValidationOperator in in self._validation_operators
            validation_operator_config (dict): a config for the ValidationOperator to add

        Returns:
            validation_operator (ValidationOperator)
        """

        self.config["validation_operators"][
            validation_operator_name
        ] = validation_operator_config
        config = self.project_config_with_variables_substituted.validation_operators[
            validation_operator_name
        ]
        module_name = "great_expectations.validation_operators"
        new_validation_operator = instantiate_class_from_config(
            config=config,
            runtime_environment={
                "data_context": self,
                "name": validation_operator_name,
            },
            config_defaults={"module_name": module_name},
        )
        if not new_validation_operator:
            raise ge_exceptions.ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=config["class_name"],
            )
        self.validation_operators[validation_operator_name] = new_validation_operator
        return new_validation_operator
