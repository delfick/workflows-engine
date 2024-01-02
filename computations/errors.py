import attrs

from . import protocols


@attrs.frozen
class ComputationError(Exception):
    pass


@attrs.frozen
class WorkflowNotFound(ComputationError):
    identifier: str


@attrs.frozen
class ComputationAlreadyExists(ComputationError):
    identifier: str
    path: protocols.Path


@attrs.frozen
class ValidationError(ValueError):
    pass


@attrs.frozen
class InvalidJobName(ValidationError):
    wanted: str


@attrs.frozen
class InvalidExternalInputName(ValidationError):
    wanted: str


@attrs.frozen
class StateError(ComputationError):
    pass


@attrs.frozen
class ComputationCancelled(StateError):
    identifier: str
    path: protocols.Path


@attrs.frozen
class ComputationErrored(StateError):
    identifier: str
    path: protocols.Path
    error: protocols.SimpleJSON
