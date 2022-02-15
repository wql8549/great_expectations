import warnings
from typing import Dict, Any

import numpy as np

from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import MetricPartialFunctionTypes, MetricDomainTypes
from great_expectations.expectations.metrics import metric_partial
from great_expectations.expectations.metrics.import_manager import F
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


class ColumnValuesInSet(ColumnMapMetricProvider):
    condition_metric_name = "column_values.in_set"
    condition_value_keys = ("value_set", "parse_strings_as_datetimes")

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        column,
        value_set,
        **kwargs,
    ):
        # no need to parse as datetime; just compare the strings as is
        parse_strings_as_datetimes: bool = (
            kwargs.get("parse_strings_as_datetimes") or False
        )
        if parse_strings_as_datetimes:
            warnings.warn(
                f"""The parameter "parse_strings_as_datetimes" is no longer supported and will be deprecated in a \
future release.  Please update code accordingly.  Moreover, in "{cls.__name__}._pandas()", it is not used.
""",
                DeprecationWarning,
            )

        if value_set is None:
            # Vacuously true
            return np.ones(len(column), dtype=np.bool_)

        return column.isin(value_set)

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        column,
        value_set,
        **kwargs,
    ):
        # no need to parse as datetime; just compare the strings as is
        parse_strings_as_datetimes: bool = (
            kwargs.get("parse_strings_as_datetimes") or False
        )
        if parse_strings_as_datetimes:
            warnings.warn(
                f"""The parameter "parse_strings_as_datetimes" is no longer supported and will be deprecated in a \
future release.  Please update code accordingly.  Moreover, in "{cls.__name__}._sqlalchemy()", it is not used.
""",
                DeprecationWarning,
            )

        if value_set is None:
            # vacuously true
            return True

        if len(value_set) == 0:
            return False

        return column.in_(value_set)


    @metric_partial(
        engine=SparkDFExecutionEngine,
        partial_fn_type=MetricPartialFunctionTypes.MAP_CONDITION_SERIES,
        domain_type=MetricDomainTypes.COLUMN
    )
    def _spark(
        cls,
        execution_engine: "SqlAlchemyExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[str, Any],
        runtime_configuration: Dict,
    ):
        # no need to parse as datetime; just compare the strings as is
        parse_strings_as_datetimes: bool = (
            metric_value_kwargs.get("parse_strings_as_datetimes") or False
        )
        if parse_strings_as_datetimes:
            warnings.warn(
                f"""The parameter "parse_strings_as_datetimes" is no longer supported and will be deprecated in a \
future release.  Please update code accordingly.  Moreover, in "{cls.__name__}._spark()", it is not used.
""",
                DeprecationWarning,
            )

        value_set = metric_value_kwargs.get("value_set")
        if value_set is None:
            # vacuously true
            return F.lit(True)

        import pyspark
        value_set_df = pyspark.sql.DataFrame({"value_set": value_set})
        filter_column_isnull = metric_value_kwargs.get(
            "filter_column_isnull", getattr(cls, "filter_column_isnull", True)
        )

        (
            data,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            domain_kwargs=metric_domain_kwargs, domain_type=MetricDomainTypes.COLUMN
        )
        if filter_column_isnull:
            data = data.filter(data[accessor_domain_kwargs["column"]].isNotNull())

        res = data.join(value_set_df, data[accessor_domain_kwargs["column"]] == value_set_df["value_set"], "left")
        # res = res.withColumn("__success", pyspark.sql.when(value_set_df["value_set"] == None, False).otherwise(True))
        return pyspark.sql.when(value_set_df["value_set"] == None, 0).otherwise(1)

        #  return column.isin(value_set)
