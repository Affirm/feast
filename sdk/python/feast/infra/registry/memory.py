import copy

from typing import Optional, Dict, List, Any, Tuple, Union
from pathlib import Path
from datetime import datetime

from feast.base_feature_view import BaseFeatureView
from feast.data_source import DataSource
from feast.entity import Entity
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView, BaseFeatureView
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.stream_feature_view import StreamFeatureView
from feast.request_feature_view import RequestFeatureView
from feast.repo_config import RegistryConfig

from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.infra.registry.base_registry import BaseRegistry
from feast.infra.registry.exceptions import RegistryError
from feast.errors import (
    ConflictingFeatureViewNames,
    DataSourceNotFoundException,
    EntityNotFoundException,
    FeatureServiceNotFoundException,
    FeatureViewNotFoundException,
    ValidationReferenceNotFound,
)
from feast.saved_dataset import SavedDataset, ValidationReference


def project_key(project: str, key: str) -> str:
    return f"{project}:{key}"

def invert_projected_key(projected_key: str) -> Tuple[str, str]:
    s = projected_key.split(":")
    if len(s) != 2:
        raise RegistryError(f"Invalid projected key {projected_key}.")
    return s[0], s[1]


def list_registry_dict(project: str, registry: Dict[str, Any]) -> List[Any]:
    return [
        copy.copy(v) for k, v in registry.items() if invert_projected_key(k)[0] == project
    ]


class MemoryRegistry(BaseRegistry):
    def __init__(
            self,
            x: Optional[RegistryConfig],
            y: Optional[Path],
            z: bool = False
    ) -> None:
        self.entities: Dict[str, Entity] = {}

        self.data_sources: Dict[str, DataSource] = {}
        self.feature_services: Dict[str, FeatureService] = {}

        self.stream_feature_views: Dict[str, StreamFeatureView] = {}
        self.feature_views: Dict[str, FeatureView] = {}
        self.on_demand_feature_views: Dict[str, OnDemandFeatureView] = {}
        self.request_feature_views: Dict[str, RequestFeatureView] = {}

        self.saved_datasets: Dict[str, SavedDataset] = {}

        self.fv_registries = [
            self.stream_feature_views,
            self.feature_views,
            self.on_demand_feature_views,
            self.request_feature_views
        ]

        # TODO: call `feast apply` here? may cause infinite loop

    def _fv_registry(self, feature_view: BaseFeatureView) -> Dict[str, BaseFeatureView]:
        if isinstance(feature_view, StreamFeatureView):
            return self.stream_feature_views
        if isinstance(feature_view, FeatureView):
            return self.feature_views
        if isinstance(feature_view, OnDemandFeatureView):
            return self.on_demand_feature_views
        if isinstance(feature_view, RequestFeatureView):
            return self.request_feature_views
        raise RegistryError(f"Unknown feature view type {feature_view}")

    def enter_apply_context(self):
        pass

    def exit_apply_context(self):
        pass

    def apply_entity(self, entity: Entity, project: str, commit: bool = True):
        """
        Registers a single entity with Feast

        Args:
            entity: Entity that will be registered
            project: Feast project that this entity belongs to
            commit: Whether the change should be persisted immediately
        """
        entity.is_valid()
        now = datetime.utcnow()
        if not entity.created_timestamp:
            entity.created_timestamp = now
        entity.last_updated_timestamp = now

        key = project_key(project, entity.name)
        if key in self.entities:
            raise RegistryError(f"Duplicate entity {entity.name} for project {project}.")
        self.entities[key] = entity

    def delete_entity(self, name: str, project: str, commit: bool = True):
        """
        Deletes an entity or raises an exception if not found.

        Args:
            name: Name of entity
            project: Feast project that this entity belongs to
            commit: Whether the change should be persisted immediately
        """
        key = project_key(project, name)
        if key not in self.entities:
            raise RegistryError(f"Cannot delete unknown entity {name} for project {project}.")
        del self.entities[key]

    def get_entity(self, name: str, project: str, allow_cache: bool = False) -> Entity:
        """
        Retrieves an entity.

        Args:
            name: Name of entity
            project: Feast project that this entity belongs to
            allow_cache: Whether to allow returning this entity from a cached registry

        Returns:
            Returns either the specified entity, or raises an exception if
            none is found
        """
        key = project_key(project, name)
        if key not in self.entities:
            raise RegistryError(f"Cannot retrieve unknown entity {name} for project {project}.")
        return copy.copy(self.entities[key])

    def list_entities(self, project: str, allow_cache: bool = False) -> List[Entity]:
        """
        Retrieve a list of entities from the registry

        Args:
            allow_cache: Whether to allow returning entities from a cached registry
            project: Filter entities based on project name

        Returns:
            List of entities
        """
        return list_registry_dict(project, self.entities)

    # Data source operations
    def apply_data_source(
            self, data_source: DataSource, project: str, commit: bool = True
    ):
        """
        Registers a single data source with Feast

        Args:
            data_source: A data source that will be registered
            project: Feast project that this data source belongs to
            commit: Whether to immediately commit to the registry
        """
        key = project_key(project, data_source.name)
        if key in self.data_sources:
            raise RegistryError(f"Duplicate data source {data_source.name} for project {project}.")
        self.data_sources[key] = data_source

    def delete_data_source(self, name: str, project: str, commit: bool = True):
        """
        Deletes a data source or raises an exception if not found.

        Args:
            name: Name of data source
            project: Feast project that this data source belongs to
            commit: Whether the change should be persisted immediately
        """
        key = project_key(project, name)
        if key not in self.data_sources:
            raise RegistryError(f"Cannot delete unknown data source {name} for project {project}.")
        del self.data_sources[key]

    def get_data_source(
            self, name: str, project: str, allow_cache: bool = False
    ) -> DataSource:
        """
        Retrieves a data source.

        Args:
            name: Name of data source
            project: Feast project that this data source belongs to
            allow_cache: Whether to allow returning this data source from a cached registry

        Returns:
            Returns either the specified data source, or raises an exception if none is found
        """
        key = project_key(project, name)
        if key not in self.data_sources:
            raise RegistryError(f"Cannot retrieve unknown data source {name} for project {project}.")
        return copy.copy(self.data_sources[key])

    def list_data_sources(
            self, project: str, allow_cache: bool = False
    ) -> List[DataSource]:
        """
        Retrieve a list of data sources from the registry

        Args:
            project: Filter data source based on project name
            allow_cache: Whether to allow returning data sources from a cached registry

        Returns:
            List of data sources
        """
        return list_registry_dict(project, self.data_sources)

    # Feature service operations
    def apply_feature_service(
            self, feature_service: FeatureService, project: str, commit: bool = True
    ):
        """
        Registers a single feature service with Feast

        Args:
            feature_service: A feature service that will be registered
            project: Feast project that this entity belongs to
        """
        key = project_key(project, feature_service.name)
        if key in self.feature_services:
            raise RegistryError(f"Duplicate feature service {feature_service.name} for project {project}.")
        self.feature_services[key] = feature_service

    def delete_feature_service(self, name: str, project: str, commit: bool = True):
        """
        Deletes a feature service or raises an exception if not found.

        Args:
            name: Name of feature service
            project: Feast project that this feature service belongs to
            commit: Whether the change should be persisted immediately
        """
        key = project_key(project, name)
        if key not in self.feature_services:
            raise RegistryError(f"Cannot delete unknown feature service {name} for project {project}.")
        del self.feature_services[key]

    def get_feature_service(
            self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureService:
        """
        Retrieves a feature service.

        Args:
            name: Name of feature service
            project: Feast project that this feature service belongs to
            allow_cache: Whether to allow returning this feature service from a cached registry

        Returns:
            Returns either the specified feature service, or raises an exception if
            none is found
        """
        key = project_key(project, name)
        if key not in self.feature_services:
            raise RegistryError(f"Cannot retrieve unknown feature service {name} for project {project}.")
        return copy.copy(self.feature_services[key])

    def list_feature_services(
            self, project: str, allow_cache: bool = False
    ) -> List[FeatureService]:
        """
        Retrieve a list of feature services from the registry

        Args:
            allow_cache: Whether to allow returning entities from a cached registry
            project: Filter entities based on project name

        Returns:
            List of feature services
        """
        return list_registry_dict(project, self.feature_services)

    def apply_feature_view(
            self, feature_view: BaseFeatureView, project: str, commit: bool = True
    ):
        """
        Registers a single feature view with Feast

        Args:
            feature_view: Feature view that will be registered
            project: Feast project that this feature view belongs to
            commit: Whether the change should be persisted immediately
        """
        registry = self._fv_registry(feature_view)
        key = project_key(project, feature_view.name)
        if key in registry:
            raise RegistryError(f"Duplicate feature view {feature_view.name} for project {project}.")
        registry[key] = feature_view

    def delete_feature_view(self, name: str, project: str, commit: bool = True):
        """
        Deletes a feature view or raises an exception if not found.

        Args:
            name: Name of feature view
            project: Feast project that this feature view belongs to
            commit: Whether the change should be persisted immediately
        """
        key = project_key(project, name)
        for registry in self.fv_registries:
            if key in registry:
                del registry[key]
                return
        raise RegistryError(f"Cannot delete unknown feature view {name} for project {project}.")

    def get_stream_feature_view(
            self, name: str, project: str, allow_cache: bool = False
    ):
        """
        Retrieves a stream feature view.

        Args:
            name: Name of stream feature view
            project: Feast project that this feature view belongs to
            allow_cache: Allow returning feature view from the cached registry

        Returns:
            Returns either the specified feature view, or raises an exception if
            none is found
        """
        key = project_key(project, name)
        if key not in self.stream_feature_views:
            raise RegistryError(f"Cannot retrieve unknown stream feature view {name} for project {project}")
        return copy.copy(self.stream_feature_views[key])

    def list_stream_feature_views(
            self, project: str, allow_cache: bool = False, ignore_udfs: bool = False,
    ) -> List[StreamFeatureView]:
        """
        Retrieve a list of stream feature views from the registry

        Args:
            project: Filter stream feature views based on project name
            allow_cache: Whether to allow returning stream feature views from a cached registry
            ignore_udfs: Whether a feast apply operation is being executed. Determines whether environment
                sensitive commands, such as dill.loads(), are skipped and 'None' is set as their results.
        Returns:
            List of stream feature views
        """
        return list_registry_dict(project, self.stream_feature_views)

    def get_on_demand_feature_view(
            self, name: str, project: str, allow_cache: bool = False
    ) -> OnDemandFeatureView:
        """
        Retrieves an on demand feature view.

        Args:
            name: Name of on demand feature view
            project: Feast project that this on demand feature view belongs to
            allow_cache: Whether to allow returning this on demand feature view from a cached registry

        Returns:
            Returns either the specified on demand feature view, or raises an exception if
            none is found
        """
        key = project_key(project, name)
        if key not in self.on_demand_feature_views:
            raise RegistryError(f"Cannot retrieve unknown on demand feature view {name} for project {project}")
        return copy.copy(self.on_demand_feature_views[key])

    def list_on_demand_feature_views(
            self, project: str, allow_cache: bool = False, ignore_udfs: bool = False
    ) -> List[OnDemandFeatureView]:
        """
        Retrieve a list of on demand feature views from the registry

        Args:
            project: Filter on demand feature views based on project name
            allow_cache: Whether to allow returning on demand feature views from a cached registry
            ignore_udfs: Whether a feast apply operation is being executed. Determines whether environment
                         sensitive commands, such as dill.loads(), are skipped and 'None' is set as their results.
        Returns:
            List of on demand feature views
        """
        return list_registry_dict(project, self.on_demand_feature_views)

    def get_feature_view(
            self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureView:
        """
        Retrieves a feature view.

        Args:
            name: Name of feature view
            project: Feast project that this feature view belongs to
            allow_cache: Allow returning feature view from the cached registry

        Returns:
            Returns either the specified feature view, or raises an exception if
            none is found
        """
        key = project_key(project, name)
        if key not in self.feature_views:
            raise RegistryError(f"Cannot retrieve unknown feature view {name} for project {project}")
        return copy.copy(self.feature_views[key])

    def list_feature_views(
            self, project: str, allow_cache: bool = False
    ) -> List[FeatureView]:
        """
        Retrieve a list of feature views from the registry

        Args:
            allow_cache: Allow returning feature views from the cached registry
            project: Filter feature views based on project name

        Returns:
            List of feature views
        """
        return list_registry_dict(project, self.feature_views)

    def get_request_feature_view(self, name: str, project: str) -> RequestFeatureView:
        """
        Retrieves a request feature view.

        Args:
            name: Name of request feature view
            project: Feast project that this feature view belongs to
            allow_cache: Allow returning feature view from the cached registry

        Returns:
            Returns either the specified feature view, or raises an exception if
            none is found
        """
        key = project_key(project, name)
        if key not in self.request_feature_views:
            raise RegistryError(f"Cannot retrieve unknown request feature view {name} for project {project}")
        return copy.copy(self.request_feature_views[key])

    def list_request_feature_views(
            self, project: str, allow_cache: bool = False
    ) -> List[RequestFeatureView]:
        """
        Retrieve a list of request feature views from the registry

        Args:
            allow_cache: Allow returning feature views from the cached registry
            project: Filter feature views based on project name

        Returns:
            List of request feature views
        """
        return list_registry_dict(project, self.request_feature_views)

    def apply_materialization(
            self,
            feature_view: FeatureView,
            project: str,
            start_date: datetime,
            end_date: datetime,
            commit: bool = True,
    ):
        key = project_key(project, feature_view.name)
        for registry in [self.feature_views, self.stream_feature_views]:
            if key in registry:
                fv = registry[key]
                fv.materialization_intervals.append((start_date, end_date))
                fv.last_updated_timestamp = datetime.utcnow()
                return
        raise FeatureViewNotFoundException(feature_view.name, project)

    def apply_saved_dataset(
            self,
            saved_dataset: SavedDataset,
            project: str,
            commit: bool = True,
    ):
        """
        Stores a saved dataset metadata with Feast

        Args:
            saved_dataset: SavedDataset that will be added / updated to registry
            project: Feast project that this dataset belongs to
            commit: Whether the change should be persisted immediately
        """
        key = project_key(project, saved_dataset.name)
        if key in self.saved_datasets:
            raise RegistryError(f"Duplicate saved dataset {saved_dataset.name} for project {project}.")
        self.saved_datasets[key] = saved_dataset

    def get_saved_dataset(
            self, name: str, project: str, allow_cache: bool = False
    ) -> SavedDataset:
        """
        Retrieves a saved dataset.

        Args:
            name: Name of dataset
            project: Feast project that this dataset belongs to
            allow_cache: Whether to allow returning this dataset from a cached registry

        Returns:
            Returns either the specified SavedDataset, or raises an exception if
            none is found
        """
        pass

    def delete_saved_dataset(self, name: str, project: str, allow_cache: bool = False):
        """
        Delete a saved dataset.

        Args:
            name: Name of dataset
            project: Feast project that this dataset belongs to
            allow_cache: Whether to allow returning this dataset from a cached registry

        Returns:
            Returns either the specified SavedDataset, or raises an exception if
            none is found
        """

    def list_saved_datasets(
            self, project: str, allow_cache: bool = False
    ) -> List[SavedDataset]:
        """
        Retrieves a list of all saved datasets in specified project

        Args:
            project: Feast project
            allow_cache: Whether to allow returning this dataset from a cached registry

        Returns:
            Returns the list of SavedDatasets
        """
        pass

    def apply_validation_reference(
            self,
            validation_reference: ValidationReference,
            project: str,
            commit: bool = True,
    ):
        """
        Persist a validation reference

        Args:
            validation_reference: ValidationReference that will be added / updated to registry
            project: Feast project that this dataset belongs to
            commit: Whether the change should be persisted immediately
        """
        pass

    def delete_validation_reference(self, name: str, project: str, commit: bool = True):
        """
        Deletes a validation reference or raises an exception if not found.

        Args:
            name: Name of validation reference
            project: Feast project that this object belongs to
            commit: Whether the change should be persisted immediately
        """
        pass

    def get_validation_reference(
            self, name: str, project: str, allow_cache: bool = False
    ) -> ValidationReference:
        """
        Retrieves a validation reference.

        Args:
            name: Name of dataset
            project: Feast project that this dataset belongs to
            allow_cache: Whether to allow returning this dataset from a cached registry

        Returns:
            Returns either the specified ValidationReference, or raises an exception if
            none is found
        """
        pass

    def list_validation_references(
            self, project: str, allow_cache: bool = False
    ) -> List[ValidationReference]:
        """
        Retrieve a list of validation references from the registry

        Args:
            allow_cache: Allow returning feature views from the cached registry
            project: Filter feature views based on project name

        Returns:
            List of request feature views
        """
        pass

    def list_project_metadata(
            self, project: str, allow_cache: bool = False
    ) -> List[ProjectMetadata]:
        """
        Retrieves project metadata

        Args:
            project: Filter metadata based on project name
            allow_cache: Allow returning feature views from the cached registry

        Returns:
            List of project metadata
        """
        pass

    def update_infra(self, infra: Infra, project: str, commit: bool = True):
        """
        Updates the stored Infra object.

        Args:
            infra: The new Infra object to be stored.
            project: Feast project that the Infra object refers to
            commit: Whether the change should be persisted immediately
        """
        pass

    def get_infra(self, project: str, allow_cache: bool = False) -> Infra:
        """
        Retrieves the stored Infra object.

        Args:
            project: Feast project that the Infra object refers to
            allow_cache: Whether to allow returning this entity from a cached registry

        Returns:
            The stored Infra object.
        """
        pass

    def apply_user_metadata(
            self,
            project: str,
            feature_view: BaseFeatureView,
            metadata_bytes: Optional[bytes],
    ):
        pass

    def get_user_metadata(
            self, project: str, feature_view: BaseFeatureView
    ) -> Optional[bytes]:
        pass

    def proto(self) -> RegistryProto:
        pass

    def commit(self):
        pass

    def refresh(self, project: Optional[str] = None):
        """Refreshes the state of the registry cache by fetching the registry state from the remote registry store."""
        pass
