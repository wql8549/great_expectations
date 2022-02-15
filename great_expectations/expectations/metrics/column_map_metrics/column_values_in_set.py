import warnings
from typing import Any, Dict

import numpy as np

from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import (
    MetricDomainTypes,
    MetricFunctionTypes,
    MetricPartialFunctionTypes,
)
from great_expectations.expectations.metrics.import_manager import F, Row
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
    metric_partial,
)
from great_expectations.expectations.metrics.metric_provider import metric_value


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

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, value_set, **kwargs):
        execution_engine = cls._spark.metric_engine()
        selectable = kwargs["_table"]
        column_name = kwargs["_accessor_domain_kwargs"]["column"]

        join = _join_filter(selectable, execution_engine, value_set, column_name)
        result = selectable.join(join, selectable[column_name] == join["key"])

        return result["__success"]

    # @metric_partial(engine=SparkDFExecutionEngine,
    #                 partial_fn_type=MetricPartialFunctionTypes.MAP_CONDITION_FN,
    #                 domain_type=MetricDomainTypes.COLUMN)
    # def _spark(
    #         cls,
    #         execution_engine,
    #         metric_domain_kwargs,
    #         metric_value_kwargs,
    #         metrics,
    #         runtime_configuration,
    # ):
    #
    #     (
    #         df,
    #         compute_domain_kwargs,
    #         accessor_domain_kwargs,
    #     ) = execution_engine.get_compute_domain(
    #         metric_domain_kwargs, domain_type=MetricDomainTypes.COLUMN
    #     )
    #
    #     column_name = accessor_domain_kwargs["column"]
    #     value_set = metric_value_kwargs["value_set"]
    #
    #     join = _join_filter(df, execution_engine, value_set, column_name)
    #
    #     query = F.when(join["__success"] == True, F.lit(False)).otherwise(F.lit(True))
    #
    #     return (query, compute_domain_kwargs, accessor_domain_kwargs)


def _join_filter(selectable, execution_engine, value_set, column_name):
    value_set = execution_engine.spark.sparkContext.parallelize(value_set)
    value_set = value_set.map(Row("value_set")).toDF()

    join = (
        selectable.join(value_set, selectable[0] == value_set[0], "left")
        .withColumn("__success", F.when(value_set[0].isNull(), False).otherwise(True))
        .select(selectable[0], "__success")
        .withColumnRenamed(column_name, "key")
    )

    return join
