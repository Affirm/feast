import copy
import functools
import logging
import warnings
from datetime import datetime
from types import FunctionType
from typing import Any, Dict, List, Optional, Type, Union, cast

import dill
import pandas as pd
from typeguard import typechecked

from feast.base_feature_view import BaseFeatureView
from feast.batch_feature_view import BatchFeatureView
from feast.data_source import RequestSource
from feast.feature_view import FeatureView
from feast.feature_view_projection import FeatureViewProjection
from feast.field import Field, from_value_type
from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
    OnDemandFeatureView as OnDemandFeatureViewProto,
)
from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
    OnDemandFeatureViewMeta,
    OnDemandFeatureViewSpec,
    OnDemandSource,
)
from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
    UserDefinedFunction as UserDefinedFunctionProto,
)
from feast.type_map import (
    feast_value_type_to_pandas_type,
    python_type_to_feast_value_type,
)
from feast.usage import log_exceptions
from feast.value_type import ValueType

warnings.simplefilter("once", DeprecationWarning)


def _empty_odfv_udf_fn(x: Any):
    # RB: really just an identity mapping, otherwise we risk tripping some downstream tests
    return x

@typechecked
class OnDemandFeatureView(BaseFeatureView):
    """
    [Experimental] An OnDemandFeatureView defines a logical group of features that are
    generated by applying a transformation on a set of input sources, such as feature
    views and request data sources.

    Attributes:
        name: The unique name of the on demand feature view.
        features: The list of features in the output of the on demand feature view.
        source_feature_view_projections: A map from input source names to actual input
            sources with type FeatureViewProjection.
        source_request_sources: A map from input source names to the actual input
            sources with type RequestSource.
        udf: The user defined transformation function, which must take pandas dataframes
            as inputs.
        description: A human-readable description.
        tags: A dictionary of key-value pairs to store arbitrary metadata.
        owner: The owner of the on demand feature view, typically the email of the primary
            maintainer.
    """

    name: str
    features: List[Field]
    source_feature_view_projections: Dict[str, FeatureViewProjection]
    source_request_sources: Dict[str, RequestSource]
    udf: FunctionType
    udf_string: str
    mode: str
    description: str
    tags: Dict[str, str]
    owner: str

    @log_exceptions  # noqa: C901
    def __init__(  # noqa: C901
        self,
        *,
        name: str,
        schema: List[Field],
        mode: str,
        sources: List[
            Union[
                FeatureView,
                RequestSource,
                FeatureViewProjection,
            ]
        ],
        udf: FunctionType,
        udf_string: str = "",
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        owner: str = "",
    ):
        """
        Creates an OnDemandFeatureView object.

        Args:
            name: The unique name of the on demand feature view.
            schema: The list of features in the output of the on demand feature view, after
                the transformation has been applied.
            sources: A map from input source names to the actual input sources, which may be
                feature views, or request data sources. These sources serve as inputs to the udf,
                which will refer to them by name.
            udf: The user defined transformation function, which must take pandas
                dataframes as inputs.
            udf_string: The source code version of the udf (for diffing and displaying in Web UI)
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the on demand feature view, typically the email
                of the primary maintainer.
        """
        super().__init__(
            name=name,
            features=schema,
            description=description,
            tags=tags,
            owner=owner,
        )

        self.source_feature_view_projections: Dict[str, FeatureViewProjection] = {}
        self.source_request_sources: Dict[str, RequestSource] = {}
        for odfv_source in sources:
            if isinstance(odfv_source, RequestSource):
                self.source_request_sources[odfv_source.name] = odfv_source
            elif isinstance(odfv_source, FeatureViewProjection):
                self.source_feature_view_projections[odfv_source.name] = odfv_source
            else:
                self.source_feature_view_projections[
                    odfv_source.name
                ] = odfv_source.projection

        self.udf = udf  # type: ignore
        self.udf_string = udf_string

        if mode not in {"python", "pandas"}:
            raise Exception(f"Unknown mode {mode}. OnDemandFeatureView only supports python or pandas UDFs.")
        self.mode = mode

    @property
    def proto_class(self) -> Type[OnDemandFeatureViewProto]:
        return OnDemandFeatureViewProto

    def __copy__(self):
        fv = OnDemandFeatureView(
            name=self.name,
            schema=self.features,
            sources=list(self.source_feature_view_projections.values())
            + list(self.source_request_sources.values()),
            udf=self.udf,
            udf_string=self.udf_string,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )
        fv.projection = copy.copy(self.projection)
        return fv

    def __eq__(self, other):
        if not isinstance(other, OnDemandFeatureView):
            raise TypeError(
                "Comparisons should only involve OnDemandFeatureView class objects."
            )

        if not super().__eq__(other):
            return False

        if (
            self.source_feature_view_projections
            != other.source_feature_view_projections
            or self.source_request_sources != other.source_request_sources
            or self.udf_string != other.udf_string
            or self.udf.__code__.co_code != other.udf.__code__.co_code
        ):
            return False

        return True

    def __hash__(self):
        return super().__hash__()

    def to_proto(self) -> OnDemandFeatureViewProto:
        """
        Converts an on demand feature view object to its protobuf representation.

        Returns:
            A OnDemandFeatureViewProto protobuf.
        """
        meta = OnDemandFeatureViewMeta()
        if self.created_timestamp:
            meta.created_timestamp.FromDatetime(self.created_timestamp)
        else:
            meta.created_timestamp.FromDatetime(datetime.now())
        if self.last_updated_timestamp:
            meta.last_updated_timestamp.FromDatetime(self.last_updated_timestamp)
        else:
            meta.last_updated_timestamp.FromDatetime(datetime.now())
        sources = {}
        for source_name, fv_projection in self.source_feature_view_projections.items():
            sources[source_name] = OnDemandSource(
                feature_view_projection=fv_projection.to_proto()
            )
        for (
            source_name,
            request_sources,
        ) in self.source_request_sources.items():
            sources[source_name] = OnDemandSource(
                request_data_source=request_sources.to_proto()
            )

        spec = OnDemandFeatureViewSpec(
            name=self.name,
            features=[feature.to_proto() for feature in self.features],
            sources=sources,
            user_defined_function=UserDefinedFunctionProto(
                name=self.udf.__name__,
                body=dill.dumps(self.udf, recurse=True),
                body_text=self.udf_string,
            ),
            description=self.description,
            mode=self.mode,
            tags=self.tags,
            owner=self.owner,
        )

        return OnDemandFeatureViewProto(spec=spec, meta=meta)

    @classmethod
    def from_proto(cls, on_demand_feature_view_proto: OnDemandFeatureViewProto, skip_udf: bool = False):
        """
        Creates an on demand feature view from a protobuf representation.

        Args:
            on_demand_feature_view_proto: A protobuf representation of an on-demand feature view.

        Returns:
            A OnDemandFeatureView object based on the on-demand feature view protobuf.
        """
        sources = []
        for (
            _,
            on_demand_source,
        ) in on_demand_feature_view_proto.spec.sources.items():
            if on_demand_source.WhichOneof("source") == "feature_view":
                sources.append(
                    FeatureView.from_proto(on_demand_source.feature_view).projection
                )
            elif on_demand_source.WhichOneof("source") == "feature_view_projection":
                sources.append(
                    FeatureViewProjection.from_proto(
                        on_demand_source.feature_view_projection
                    )
                )
            else:
                sources.append(
                    RequestSource.from_proto(on_demand_source.request_data_source)
                )

        udf = (
            _empty_odfv_udf_fn
            if skip_udf
            else dill.loads(on_demand_feature_view_proto.spec.user_defined_function.body)
        )

        on_demand_feature_view_obj = cls(
            name=on_demand_feature_view_proto.spec.name,
            schema=[
                Field(
                    name=feature.name,
                    dtype=from_value_type(ValueType(feature.value_type)),
                )
                for feature in on_demand_feature_view_proto.spec.features
            ],
            sources=sources,
            udf=udf,
            udf_string=on_demand_feature_view_proto.spec.user_defined_function.body_text,
            mode=on_demand_feature_view_proto.spec.mode,
            description=on_demand_feature_view_proto.spec.description,
            tags=dict(on_demand_feature_view_proto.spec.tags),
            owner=on_demand_feature_view_proto.spec.owner,
        )

        # FeatureViewProjections are not saved in the OnDemandFeatureView proto.
        # Create the default projection.
        on_demand_feature_view_obj.projection = FeatureViewProjection.from_definition(
            on_demand_feature_view_obj
        )

        if on_demand_feature_view_proto.meta.HasField("created_timestamp"):
            on_demand_feature_view_obj.created_timestamp = (
                on_demand_feature_view_proto.meta.created_timestamp.ToDatetime()
            )
        if on_demand_feature_view_proto.meta.HasField("last_updated_timestamp"):
            on_demand_feature_view_obj.last_updated_timestamp = (
                on_demand_feature_view_proto.meta.last_updated_timestamp.ToDatetime()
            )

        return on_demand_feature_view_obj

    def get_request_data_schema(self) -> Dict[str, ValueType]:
        schema: Dict[str, ValueType] = {}
        for request_source in self.source_request_sources.values():
            if isinstance(request_source.schema, List):
                new_schema = {}
                for field in request_source.schema:
                    new_schema[field.name] = field.dtype.to_value_type()
                schema.update(new_schema)
            elif isinstance(request_source.schema, Dict):
                schema.update(request_source.schema)
            else:
                raise Exception(
                    f"Request source schema is not correct type: ${str(type(request_source.schema))}"
                )
        return schema

    def _get_projected_feature_name(
        self,
        feature: str
    ) -> str:
        return f"{self.projection.name_to_use()}__{feature}"

    def _get_transformed_features_df(
        self,
        df_with_features: pd.DataFrame,
        full_feature_names: bool = False,
    ) -> pd.DataFrame:
        # Apply on demand transformations
        columns_to_cleanup = []
        dict_with_features = df_with_features.to_dict()
        for source_fv_projection in self.source_feature_view_projections.values():
            for feature in source_fv_projection.features:
                full_feature_ref = f"{source_fv_projection.name}__{feature.name}"
                if full_feature_ref in dict_with_features.keys():
                    # Make sure the partial feature name is always present
                    dict_with_features[feature.name] = dict_with_features[full_feature_ref]
                    columns_to_cleanup.append(feature.name)
                elif feature.name in dict_with_features.keys():
                    # Make sure the full feature name is always present
                    dict_with_features[full_feature_ref] = dict_with_features[feature.name]
                    columns_to_cleanup.append(full_feature_ref)
        df_with_features = pd.DataFrame.from_dict(dict_with_features)
        # Compute transformed values and apply to each result row
        df_with_transformed_features = self.udf.__call__(df_with_features)

        # Work out whether the correct columns names are used.
        rename_columns: Dict[str, str] = {}
        for feature in self.features:
            short_name = feature.name
            long_name = self._get_projected_feature_name(feature.name)
            if (
                short_name in df_with_transformed_features.columns
                and full_feature_names
            ):
                rename_columns[short_name] = long_name
            elif not full_feature_names:
                # Long name must be in dataframe.
                rename_columns[long_name] = short_name

        # Cleanup extra columns used for transformation. Ensure they were not removed in the udf
        df_with_features.drop(columns=[c for c in columns_to_cleanup if c in df_with_features])
        new_columns = []
        for n in df_with_transformed_features.columns:
            new_columns.append(rename_columns[n] if n in rename_columns else n)
        df_with_transformed_features.columns = new_columns
        return df_with_transformed_features

    def _get_transformed_features_dict(
        self,
        feature_dict: Dict[str, List[Any]],
        full_feature_names: bool = False,
    ) -> Dict[str, Any]:
        # generates a mapping between feature names and fv__feature names (and vice versa)
        name_map: Dict[str, str] = {}
        for source_fv_projection in self.source_feature_view_projections.values():
            for feature in source_fv_projection.features:
                full_feature_ref = f"{source_fv_projection.name}__{feature.name}"
                if full_feature_ref in feature_dict:
                    name_map[full_feature_ref] = feature.name
                elif feature.name in feature_dict:
                    name_map[feature.name] = name_map[full_feature_ref]

        rows = []
        # this doesn't actually require 2 x |key_space| space; k and name_map[k] point to the same object in memory
        for values in zip(*feature_dict.values()):
            rows.append({
                **{k: v for k, v in zip(feature_dict.keys(), values)},
                **{name_map[k]: v for k, v in zip(feature_dict.keys(), values)},
            })

        # construct output dictionary and mapping from expected feature names to alternative feature names
        output_dict: Dict[str, List[Any]] = {}
        correct_feature_name_to_alias: Dict[str, str] = {}
        for feature in self.features:
            long_name = self._get_projected_feature_name(feature.name)
            correct_name = long_name if full_feature_names else feature.name
            correct_feature_name_to_alias[correct_name] = feature.name if full_feature_names else long_name
            output_dict[correct_name] = [None] * len(rows)

        # populate output dictionary per row
        for i, row in enumerate(rows):
            row_output = self.udf.__call__(row)
            for feature in output_dict:
                output_dict[feature][i] = (
                    row_output.get(feature, row_output[correct_feature_name_to_alias[feature]])
                )
        return output_dict

    def get_transformed_features(
            self,
            features: Union[Dict[str, List[Any]], pd.DataFrame],
            full_feature_names: bool = False
    ) -> Union[Dict[str, List[Any]], pd.DataFrame]:
        # RB / TODO: classic inheritance pattern....maybe fix this
        if self.mode == "python":
            assert isinstance(features, dict)
            return self._get_transformed_features_dict(
                feature_dict=cast(features, Dict[str, List[Any]]),
                full_feature_names=full_feature_names
            )
        elif self.mode == "pandas":
            assert isinstance(features, pd.DataFrame)
            return self._get_transformed_features_df(
                df_with_features=cast(features, pd.DataFrame),
                full_feature_names=full_feature_names
            )
        else:
            raise Exception(f'Invalid OnDemandFeatureMode: {self.mode}. Expected one of "pandas" or "python".')

    def infer_features(self):
        """
        Infers the set of features associated to this feature view from the input source.

        Raises:
            RegistryInferenceFailure: The set of features could not be inferred.
        """
        """
        rand_df_value: Dict[str, Any] = {
            "float": 1.0,
            "int": 1,
            "str": "hello world",
            "bytes": str.encode("hello world"),
            "bool": True,
            "datetime64[ns]": datetime.utcnow(),
        }

        df = pd.DataFrame()
        for feature_view_projection in self.source_feature_view_projections.values():
            for feature in feature_view_projection.features:
                dtype = feast_value_type_to_pandas_type(feature.dtype.to_value_type())
                df[f"{feature_view_projection.name}__{feature.name}"] = pd.Series(
                    dtype=dtype
                )
                sample_val = rand_df_value[dtype] if dtype in rand_df_value else None
                df[f"{feature.name}"] = pd.Series(data=sample_val, dtype=dtype)
        for request_data in self.source_request_sources.values():
            for field in request_data.schema:
                dtype = feast_value_type_to_pandas_type(field.dtype.to_value_type())
                sample_val = rand_df_value[dtype] if dtype in rand_df_value else None
                df[f"{field.name}"] = pd.Series(sample_val, dtype=dtype)
        output_df: pd.DataFrame = self.udf.__call__(df)
        inferred_features = []
        for f, dt in zip(output_df.columns, output_df.dtypes):
            inferred_features.append(
                Field(
                    name=f,
                    dtype=from_value_type(
                        python_type_to_feast_value_type(f, type_name=str(dt))
                    ),
                )
            )

        if self.features:
            missing_features = []
            for specified_features in self.features:
                if specified_features not in inferred_features:
                    missing_features.append(specified_features)
            if missing_features:
                raise SpecifiedFeaturesNotPresentError(
                    missing_features, inferred_features, self.name
                )
        else:
            self.features = inferred_features

        if not self.features:
            raise RegistryInferenceFailure(
                "OnDemandFeatureView",
                f"Could not infer Features for the feature view '{self.name}'.",
            )
        """
        pass 

    @staticmethod
    def get_requested_odfvs(feature_refs, project, registry):
        all_on_demand_feature_views = registry.list_on_demand_feature_views(
            project, allow_cache=True
        )
        requested_on_demand_feature_views: List[OnDemandFeatureView] = []
        for odfv in all_on_demand_feature_views:
            for feature in odfv.features:
                if f"{odfv.name}:{feature.name}" in feature_refs:
                    requested_on_demand_feature_views.append(odfv)
                    break
        return requested_on_demand_feature_views


def on_demand_feature_view(
    *,
    schema: List[Field],
    sources: List[
        Union[
            FeatureView,
            RequestSource,
            FeatureViewProjection,
        ]
    ],
    mode: str = "pandas",
    description: str = "",
    tags: Optional[Dict[str, str]] = None,
    owner: str = "",
):
    """
    Creates an OnDemandFeatureView object with the given user function as udf.

    Args:
        schema: The list of features in the output of the on demand feature view, after
            the transformation has been applied.
        sources: A map from input source names to the actual input sources, which may be
            feature views, or request data sources. These sources serve as inputs to the udf,
            which will refer to them by name.
        mode (optional): Backend used to compute on-demand transforms. Must be one of "python" or "pandas"
        description (optional): A human-readable description.
        tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
        owner (optional): The owner of the on demand feature view, typically the email
            of the primary maintainer.
    """

    def mainify(obj):
        # Needed to allow dill to properly serialize the udf. Otherwise, clients will need to have a file with the same
        # name as the original file defining the ODFV.
        if obj.__module__ != "__main__":
            obj.__module__ = "__main__"

    def decorator(user_function):
        udf_string = dill.source.getsource(user_function)
        mainify(user_function)
        on_demand_feature_view_obj = OnDemandFeatureView(
            name=user_function.__name__,
            sources=sources,
            schema=schema,
            udf=user_function,
            description=description,
            tags=tags,
            owner=owner,
            udf_string=udf_string,
        )
        functools.update_wrapper(
            wrapper=on_demand_feature_view_obj, wrapped=user_function
        )
        return on_demand_feature_view_obj

    return decorator


def feature_view_to_batch_feature_view(fv: FeatureView) -> BatchFeatureView:
    bfv = BatchFeatureView(
        name=fv.name,
        entities=fv.entities,
        ttl=fv.ttl,
        tags=fv.tags,
        online=fv.online,
        owner=fv.owner,
        schema=fv.schema,
        source=fv.batch_source,
    )

    bfv.features = copy.copy(fv.features)
    bfv.entities = copy.copy(fv.entities)
    return bfv
