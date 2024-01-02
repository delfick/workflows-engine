from __future__ import annotations

import abc
import contextlib
import threading
from collections import defaultdict
from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING, Generic, cast

from . import errors, protocols, state


class StorageBase(Generic[protocols.T_CO_Data], abc.ABC):
    @abc.abstractmethod
    def _new_identifier_str(self) -> str:
        """
        Return a new globally unique identifier string
        """

    @abc.abstractmethod
    def hold_workflow_lock(
        self, identifier: protocols.WorkflowIdentifier, /
    ) -> contextlib.AbstractContextManager[None]:
        """
        A context manager for holding a lock for a specific workflow
        """

    def store_new_workflow(
        self, workflow_saver: protocols.NewWorkflowSaver
    ) -> protocols.WorkflowIdentifier:
        """
        Put into storage a workflow and return a new identifier
        """
        identifier = state.WorkflowIdentifier(identifier=self._new_identifier_str())
        self.upsert_workflow_information(
            identifier=identifier, workflow_information=workflow_saver.for_storage(identifier)
        )
        return identifier

    @abc.abstractmethod
    def retrieve_workflow_information(
        self, *, identifier: protocols.WorkflowIdentifier
    ) -> protocols.WorkflowInformation:
        """
        Retrieve information from storage for the provided workflow identifier
        """

    @abc.abstractmethod
    def upsert_workflow_information(
        self,
        *,
        identifier: protocols.WorkflowIdentifier,
        workflow_information: protocols.WorkflowInformation,
    ) -> None:
        """
        Update existing information for this workflow
        """

    @abc.abstractmethod
    def retrieve_computations(
        self, *, identifier: protocols.WorkflowIdentifier
    ) -> Mapping[protocols.Path, protocols.StoredInfo[protocols.T_CO_Data]]:
        """
        Retrieve all stored information by path for this identifier
        """

    @abc.abstractmethod
    def upsert_computations(
        self,
        *,
        identifier: protocols.WorkflowIdentifier,
        stored_infos: Mapping[protocols.Path, protocols.StoredInfo[protocols.T_CO_Data]],
    ) -> None:
        """
        Upsert all the computations for this identifier
        """


class MemoryStorage(StorageBase[protocols.T_CO_Data], abc.ABC):
    def __init__(self) -> None:
        self._computations: dict[
            protocols.WorkflowIdentifier,
            dict[protocols.Path, protocols.StoredInfo[protocols.T_CO_Data]],
        ] = defaultdict(dict)

        self._workflows: dict[protocols.WorkflowIdentifier, protocols.WorkflowInformation] = {}

        self._locks: dict[protocols.WorkflowIdentifier, threading.Lock] = {}

    def _new_identifier_str(self) -> str:
        return state.make_identifier()

    def retrieve_workflow_information(
        self, *, identifier: protocols.WorkflowIdentifier
    ) -> protocols.WorkflowInformation:
        if (workflow := self._workflows.get(identifier)) is None:
            raise errors.WorkflowNotFound(identifier=identifier.identifier)

        return workflow

    @contextlib.contextmanager
    def hold_workflow_lock(self, identifier: protocols.WorkflowIdentifier, /) -> Iterator[None]:
        if identifier not in self._locks:
            self._locks[identifier] = threading.Lock()

        with self._locks[identifier]:
            yield

        if (lock := self._locks.get(identifier)) is not None:
            if not lock.locked():
                del self._locks[identifier]

    def upsert_workflow_information(
        self,
        *,
        identifier: protocols.WorkflowIdentifier,
        workflow_information: protocols.WorkflowInformation,
    ) -> None:
        """
        Update existing information for this workflow
        """
        self._workflows[identifier] = workflow_information

    def retrieve_computations(
        self, *, identifier: protocols.WorkflowIdentifier
    ) -> Mapping[protocols.Path, protocols.StoredInfo[protocols.T_CO_Data]]:
        """
        Retrieve all stored information by path for this identifier
        """
        if identifier not in self._workflows:
            raise errors.WorkflowNotFound(identifier=identifier.identifier)
        return dict(self._computations.get(identifier) or {})

    def upsert_computations(
        self,
        *,
        identifier: protocols.WorkflowIdentifier,
        stored_infos: Mapping[protocols.Path, protocols.StoredInfo[protocols.T_CO_Data]],
    ) -> None:
        if identifier not in self._workflows:
            raise errors.WorkflowNotFound(identifier=identifier.identifier)

        if identifier not in self._computations:
            self._computations[identifier] = {}

        for path, stored_info in stored_infos.items():
            self._computations[identifier][path] = stored_info


if TYPE_CHECKING:
    A_Storage = StorageBase[protocols.P_StoredInfo]

    C_Storage = StorageBase[state.C_StoredInfo]
    C_MemoryStorage = MemoryStorage[state.C_StoredInfo]

    _SB: protocols.P_Storage = cast(A_Storage, None)
    _SCB: protocols.P_Storage = cast(C_Storage, None)

    _MSC: protocols.P_Storage = cast(C_MemoryStorage, None)
