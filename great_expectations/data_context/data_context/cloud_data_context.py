from typing import Mapping, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.types.base import DataContextConfig, GeCloudConfig


class CloudDataContext(AbstractDataContext):
    """
    Subclass of AbstractDataContext that contains functionality necessary to hydrate state from cloud
    """

    def __init__(
        self,
        project_config: Union[DataContextConfig, Mapping],
        context_root_dir: Optional[str] = None,
        runtime_environment: Optional[dict] = None,
        ge_cloud_mode: bool = False,
        ge_cloud_config: Optional[GeCloudConfig] = None,
    ) -> None:
        """DataContext constructor
        Args:
            context_root_dir: location to look for the ``great_expectations.yml`` file. If None, searches for the file \
            based on conventions for project subdirectories.
            runtime_environment: a dictionary of config variables that
            override both those set in config_variables.yml and the environment
        Returns:
            None
    """
        super().__init__(
            project_config=project_config, runtime_environment=runtime_environment
        )
        self._ge_cloud_mode = ge_cloud_mode
        self._ge_cloud_config = ge_cloud_config

    @property
    def ge_cloud_config(self) -> Optional[GeCloudConfig]:
        return self._ge_cloud_config

    @property
    def ge_cloud_mode(self) -> bool:
        return self._ge_cloud_mode

    def _construct_data_context_id(self) -> str:
        """
        Choose the id of the currently-configured expectations store, if available and a persistent store.
        If not, it should choose the id stored in DataContextConfig.
        Returns:
            UUID to use as the data_context_id
        """
        # if in ge_cloud_mode, use ge_cloud_organization_id
        if self.ge_cloud_mode:
            return self.ge_cloud_config.organization_id
        else:
            raise ge_exceptions.DataContextError(
                "You cannot return a data_context_id for CloudDataContext without enabling enable_cloud_mode."
            )
