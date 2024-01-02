from __future__ import annotations

import abc
import datetime
import re
import uuid
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, ClassVar, Generic, Self, cast

import attrs

from . import errors, protocols

make_identifier: Callable[[], str]

try:
    import ulid

    def make_identifier() -> str:
        return str(ulid.ULID())

except ImportError:

    def make_identifier() -> str:
        return str(uuid.uuid4())


regexes = {"non_empty_path_element": re.compile(r"^(?!.*[.\s])[\w-]+$", re.ASCII)}


class WorkflowIdentifierBase(abc.ABC):
    """
    Workflow Identifier is used to represent a workflow

    It is a container so it's harder to pass around the wrong strings.
    """

    @property
    @abc.abstractproperty
    def identifier(self) -> str:
        ...

    def __hash__(self) -> int:
        return hash(self.identifier)


@attrs.frozen
class WorkflowIdentifier(WorkflowIdentifierBase):
    """
    Workflow Identifier is used to represent a workflow

    It is a container so it's harder to pass around the wrong strings.
    """

    identifier: str


class WorkflowInformationBase(abc.ABC):
    """
    This represents the information used to hydrate a workflow.
    """

    @property
    @abc.abstractproperty
    def workflow_code(self) -> str:
        ...

    @property
    @abc.abstractproperty
    def workflow_version(self) -> int:
        ...

    @property
    @abc.abstractproperty
    def information(self) -> protocols.SimpleJSON:
        ...

    @property
    @abc.abstractproperty
    def tags(self) -> Iterable[str]:
        ...

    @property
    @abc.abstractproperty
    def earliest_due_at(self) -> datetime.datetime | None:
        ...

    @property
    @abc.abstractproperty
    def earliest_next_schedule_at(self) -> datetime.datetime | None:
        ...


@attrs.frozen
class WorkflowInformation(WorkflowInformationBase):
    """
    This represents the information used to hydrate a workflow.
    """

    workflow_code: str
    workflow_version: int
    information: protocols.SimpleJSON
    tags: Iterable[str] = attrs.field(factory=list)
    earliest_due_at: datetime.datetime | None = None
    earliest_next_schedule_at: datetime.datetime | None = None


class ErrorRawBase(abc.ABC):
    @property
    @abc.abstractproperty
    def format_code(self) -> str:
        ...

    @property
    @abc.abstractproperty
    def format_version(self) -> int:
        ...

    @property
    @abc.abstractproperty
    def serialized(self) -> protocols.SimpleJSON:
        ...


@attrs.frozen
class ErrorRaw(ErrorRawBase):
    format_code: str
    format_version: int
    serialized: protocols.SimpleJSON


class ErrorBase(abc.ABC):
    """
    Represents an exception from a computation
    """

    format_code: ClassVar[str]
    format_version: ClassVar[int]

    @property
    @abc.abstractproperty
    def serialized(self) -> protocols.SimpleJSON:
        ...

    @classmethod
    @abc.abstractmethod
    def serialize(cls, exception: Exception) -> Self:
        """
        Return an instance of this class representing the provided exception
        """

    @abc.abstractmethod
    def as_exception(
        self, *, identifier: protocols.WorkflowIdentifier, path: protocols.Path
    ) -> Exception:
        """
        Return a concrete exception for this error code
        """


@attrs.frozen
class Error(ErrorBase, abc.ABC):
    serialized: protocols.SimpleJSON
    format_code: ClassVar[str]
    format_version: ClassVar[int]


@attrs.frozen
class SimpleError(Error):
    format_code: ClassVar[str] = "simple"
    format_version: ClassVar[int] = 1

    @classmethod
    def serialize(cls, exception: Exception) -> Self:
        return cls(serialized=str(exception))

    def as_exception(
        self, *, identifier: protocols.WorkflowIdentifier, path: protocols.Path
    ) -> Exception:
        return errors.ComputationErrored(
            identifier=identifier.identifier, path=path, error=self.serialized
        )


class JobPathBase(abc.ABC):
    """
    Keeps track of the name of the next job and the path leading up to it
    """

    @property
    @abc.abstractproperty
    def identifier(self) -> protocols.WorkflowIdentifier:
        ...

    @property
    @abc.abstractproperty
    def prefix(self) -> protocols.Path:
        ...

    @property
    @abc.abstractproperty
    def job_name(self) -> str:
        ...

    @property
    def path(self) -> protocols.Path:
        return (*self.prefix, self.job_name)


@attrs.frozen
class JobPath(JobPathBase):
    identifier: protocols.WorkflowIdentifier
    prefix: protocols.Path
    job_name: str = attrs.field()

    @job_name.validator
    def _validate_job_name(self, attribute: str, value: str) -> None:
        if not regexes["non_empty_path_element"].match(value):
            raise errors.InvalidJobName(wanted=value)


class ExternalInputPathBase(abc.ABC):
    """
    Used to represent a path to external input
    """

    @property
    @abc.abstractproperty
    def identifier(self) -> protocols.WorkflowIdentifier:
        ...

    @property
    @abc.abstractproperty
    def external_input_name(self) -> str:
        ...


@attrs.frozen
class ExternalInputPath(ExternalInputPathBase):
    identifier: protocols.WorkflowIdentifier
    external_input_name: str = attrs.field()

    @external_input_name.validator
    def _validate_external_input_name(self, attribute: str, value: str) -> None:
        if not regexes["non_empty_path_element"].match(value):
            raise errors.InvalidExternalInputName(wanted=value)


class StateBase(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def fresh(cls) -> Self:
        ...

    @property
    @abc.abstractproperty
    def error(self) -> protocols.ErrorRaw | None:
        ...

    @property
    @abc.abstractproperty
    def execution_state(self) -> protocols.ExecutionState:
        ...

    @property
    @abc.abstractproperty
    def result_state(self) -> protocols.ResultState:
        ...

    @property
    @abc.abstractproperty
    def created_at(self) -> datetime.datetime:
        ...

    @property
    @abc.abstractproperty
    def due_at(self) -> datetime.datetime | datetime.timedelta | None:
        ...

    @property
    @abc.abstractproperty
    def schedule_next_latest_at(self) -> datetime.datetime | datetime.timedelta | None:
        ...

    @abc.abstractmethod
    def clone(
        self,
        error: (
            protocols.ErrorRaw | protocols.Error | None | protocols.NotGiven
        ) = protocols._NotGiven,
        execution_state: (protocols.ExecutionState | protocols.NotGiven) = protocols._NotGiven,
        result_state: protocols.ResultState | protocols.NotGiven = protocols._NotGiven,
        due_at: protocols.ScheduleBy = protocols._NotGiven,
        schedule_next_latest_at: protocols.ScheduleBy = protocols._NotGiven,
    ) -> Self:
        ...


@attrs.frozen
class State(StateBase):
    """
    Represents the current state of a computation.
    """

    error: protocols.ErrorRaw | None
    execution_state: protocols.ExecutionState
    result_state: protocols.ResultState

    created_at: datetime.datetime
    due_at: datetime.datetime | datetime.timedelta | None
    schedule_next_latest_at: datetime.datetime | datetime.timedelta | None

    @classmethod
    def fresh(cls) -> Self:
        return cls(
            error=None,
            execution_state=protocols.ExecutionState.PENDING,
            result_state=protocols.ResultState.ABSENT,
            created_at=datetime.datetime.utcnow(),
            due_at=None,
            schedule_next_latest_at=None,
        )

    def clone(
        self,
        error: (
            protocols.ErrorRaw | protocols.Error | None | protocols.NotGiven
        ) = protocols._NotGiven,
        execution_state: (protocols.ExecutionState | protocols.NotGiven) = protocols._NotGiven,
        result_state: protocols.ResultState | protocols.NotGiven = protocols._NotGiven,
        due_at: protocols.ScheduleBy = protocols._NotGiven,
        schedule_next_latest_at: protocols.ScheduleBy = protocols._NotGiven,
    ) -> Self:
        if isinstance(error, protocols.NotGiven):
            error = self.error

        if isinstance(execution_state, protocols.NotGiven):
            execution_state = self.execution_state

        if isinstance(result_state, protocols.NotGiven):
            result_state = self.result_state

        if isinstance(due_at, protocols.NotGiven):
            due_at = self.due_at

        if isinstance(schedule_next_latest_at, protocols.NotGiven):
            schedule_next_latest_at = self.schedule_next_latest_at

        return self.__class__(
            error=error,
            execution_state=execution_state,
            result_state=result_state,
            created_at=self.created_at,
            due_at=due_at,
            schedule_next_latest_at=schedule_next_latest_at,
        )


class ComputationStateBase(Generic[protocols.T_CO_Data], abc.ABC):
    @property
    @abc.abstractproperty
    def _original_state(self) -> protocols.T_CO_Data:
        ...

    @property
    @abc.abstractproperty
    def identifier(self) -> protocols.WorkflowIdentifier:
        ...

    @property
    @abc.abstractproperty
    def path(self) -> protocols.Path:
        ...

    @property
    @abc.abstractproperty
    def exception(self) -> Exception | None:
        ...

    @property
    @abc.abstractproperty
    def error(self) -> protocols.Error | None:
        ...

    @property
    @abc.abstractproperty
    def execution_state(self) -> protocols.ExecutionState:
        ...

    @property
    @abc.abstractproperty
    def result_state(self) -> protocols.ResultState:
        ...

    @property
    @abc.abstractproperty
    def due_at(self) -> datetime.datetime | datetime.timedelta | None:
        ...

    @property
    @abc.abstractproperty
    def logging_context(self) -> protocols.LoggingContext:
        ...

    @abc.abstractmethod
    def job_path(self, job_name: str, /) -> protocols.JobPath:
        """
        Create an object representing the path to a job.
        """

    @abc.abstractmethod
    def external_input_path(self, external_input_name: str, /) -> protocols.ExternalInputPath:
        """
        Create an object representing the path to a external input.
        """


@attrs.frozen
class ComputationState(ComputationStateBase[protocols.T_CO_Data], abc.ABC):
    _original_state: protocols.T_CO_Data

    identifier: protocols.WorkflowIdentifier
    path: protocols.Path
    error: protocols.Error | None

    execution_state: protocols.ExecutionState = attrs.field(
        default=attrs.Factory(lambda s: s._original_state.execution_state, takes_self=True),
    )
    result_state: protocols.ResultState = attrs.field(
        default=attrs.Factory(lambda s: s._original_state.result_state, takes_self=True),
    )
    due_at: datetime.datetime | datetime.timedelta | None = attrs.field(
        default=attrs.Factory(lambda s: s._original_state.due_at, takes_self=True)
    )

    @property
    def logging_context(self) -> protocols.LoggingContext:
        return {
            "workflow_identifier": self.identifier.identifier,
            "computation_path": ".".join(self.path),
        }

    @property
    def exception(self) -> Exception | None:
        if self.error is None and self.result_state is protocols.ResultState.CANCELLED:
            return errors.ComputationCancelled(
                identifier=self.identifier.identifier, path=self.path
            )

        if self.result_state not in (
            protocols.ResultState.HANDLED_FAILURE,
            protocols.ResultState.UNHANDLED_FAILURE,
            protocols.ResultState.CANCELLED,
        ):
            return None

        if self.error is None:
            return errors.ComputationErrored(
                identifier=self.identifier.identifier,
                path=self.path,
                error=repr(self.result_state),
            )

        return self.error.as_exception(identifier=self.identifier, path=self.path)

    def job_path(self, job_name: str, /) -> protocols.JobPath:
        return JobPath(
            identifier=self.identifier,
            prefix=self.path,
            job_name=job_name,
        )

    def external_input_path(self, external_input_name: str, /) -> protocols.ExternalInputPath:
        return ExternalInputPath(
            identifier=self.identifier,
            external_input_name=external_input_name,
        )


class ResultBase(Generic[protocols.T_CO_Data], abc.ABC):
    """
    Returned from a computation to represent the state the computation is now in
    """

    @property
    @abc.abstractproperty
    def state(self) -> protocols.T_CO_Data:
        ...

    @property
    @abc.abstractproperty
    def audit_message(self) -> str:
        ...

    @property
    @abc.abstractproperty
    def due_at(self) -> protocols.ScheduleBy:
        ...

    @property
    @abc.abstractproperty
    def schedule_next_latest_at(self) -> protocols.ScheduleBy:
        ...


@attrs.frozen
class Result(ResultBase[protocols.T_CO_Data]):
    """
    Returned from a computation to represent the state the computation is now in
    """

    state: protocols.T_CO_Data
    audit_message: str
    due_at: protocols.ScheduleBy = protocols._NotGiven
    schedule_next_latest_at: protocols.ScheduleBy = protocols._NotGiven


class ResultsBase(Generic[protocols.T_CO_Data], abc.ABC):
    """
    Object used to return a new state
    """

    @abc.abstractmethod
    def no_change(
        self,
        *,
        audit_message: str = "",
        due_at: protocols.ScheduleBy = protocols._NotGiven,
        schedule_next_latest_at: protocols.ScheduleBy = protocols._NotGiven,
    ) -> protocols.Result[protocols.T_CO_Data]:
        ...

    @abc.abstractmethod
    def pending(
        self,
        *,
        audit_message: str,
        due_at: protocols.ScheduleBy = protocols._NotGiven,
        schedule_next_latest_at: protocols.ScheduleBy = protocols._NotGiven,
    ) -> protocols.Result[protocols.T_CO_Data]:
        ...

    @abc.abstractmethod
    def progressing(
        self,
        *,
        audit_message: str,
        due_at: protocols.ScheduleBy = protocols._NotGiven,
        schedule_next_latest_at: protocols.ScheduleBy = protocols._NotGiven,
    ) -> protocols.Result[protocols.T_CO_Data]:
        ...

    @abc.abstractmethod
    def success(
        self,
        *,
        audit_message: str,
        due_at: protocols.ScheduleBy = protocols._NotGiven,
        schedule_next_latest_at: protocols.ScheduleBy = protocols._NotGiven,
    ) -> protocols.Result[protocols.T_CO_Data]:
        ...

    @abc.abstractmethod
    def paused(
        self,
        *,
        audit_message: str,
        due_at: protocols.ScheduleBy = protocols._NotGiven,
        schedule_next_latest_at: protocols.ScheduleBy = protocols._NotGiven,
    ) -> protocols.Result[protocols.T_CO_Data]:
        ...

    @abc.abstractmethod
    def cancelled(
        self,
        *,
        audit_message: str,
        due_at: protocols.ScheduleBy = protocols._NotGiven,
        schedule_next_latest_at: protocols.ScheduleBy = protocols._NotGiven,
    ) -> protocols.Result[protocols.T_CO_Data]:
        ...

    @abc.abstractmethod
    def cancelling(
        self,
        *,
        audit_message: str,
        due_at: protocols.ScheduleBy = protocols._NotGiven,
        schedule_next_latest_at: protocols.ScheduleBy = protocols._NotGiven,
    ) -> protocols.Result[protocols.T_CO_Data]:
        ...

    @abc.abstractmethod
    def handled_failure(
        self,
        *,
        error: protocols.Error,
        audit_message: str,
        due_at: protocols.ScheduleBy = protocols._NotGiven,
        schedule_next_latest_at: protocols.ScheduleBy = protocols._NotGiven,
    ) -> protocols.Result[protocols.T_CO_Data]:
        ...

    @abc.abstractmethod
    def unhandled_failure(
        self,
        *,
        exc: Exception,
        audit_message: str,
        due_at: protocols.ScheduleBy = protocols._NotGiven,
        exception_serializer: protocols.ExceptionSerializer,
        schedule_next_latest_at: protocols.ScheduleBy = protocols._NotGiven,
    ) -> protocols.Result[protocols.T_CO_Data]:
        ...


@attrs.frozen
class Results(ResultsBase[protocols.T_CO_State]):
    """
    Object used to return a new state
    """

    _original_state: protocols.T_CO_State

    @classmethod
    def using(
        cls,
        computation_state: protocols.ComputationState[protocols.T_CO_State],
        /,
    ) -> Self:
        return cls(original_state=computation_state._original_state)

    def no_change(
        self,
        *,
        audit_message: str = "",
        due_at: protocols.ScheduleBy = protocols._NotGiven,
        schedule_next_latest_at: protocols.ScheduleBy = protocols._NotGiven,
    ) -> protocols.Result[protocols.T_CO_State]:
        if isinstance(due_at, protocols.NotGiven) and self._original_state.due_at:
            due_at = self._original_state.due_at

        if (
            isinstance(schedule_next_latest_at, protocols.NotGiven)
            and self._original_state.schedule_next_latest_at
        ):
            schedule_next_latest_at = self._original_state.schedule_next_latest_at
        return Result(
            state=self._original_state.clone(),
            audit_message=audit_message,
            due_at=due_at,
            schedule_next_latest_at=schedule_next_latest_at,
        )

    def pending(
        self,
        *,
        audit_message: str,
        due_at: protocols.ScheduleBy = protocols._NotGiven,
        schedule_next_latest_at: protocols.ScheduleBy = protocols._NotGiven,
    ) -> protocols.Result[protocols.T_CO_State]:
        return Result(
            state=self._original_state.clone(
                error=None,
                execution_state=protocols.ExecutionState.PENDING,
                result_state=protocols.ResultState.ABSENT,
            ),
            audit_message=audit_message,
            due_at=due_at,
            schedule_next_latest_at=schedule_next_latest_at,
        )

    def progressing(
        self,
        *,
        audit_message: str,
        due_at: protocols.ScheduleBy = protocols._NotGiven,
        schedule_next_latest_at: protocols.ScheduleBy = protocols._NotGiven,
    ) -> protocols.Result[protocols.T_CO_State]:
        return Result(
            state=self._original_state.clone(
                error=None,
                execution_state=protocols.ExecutionState.PROGRESSING,
                result_state=protocols.ResultState.ABSENT,
            ),
            audit_message=audit_message,
            due_at=due_at,
            schedule_next_latest_at=schedule_next_latest_at,
        )

    def success(
        self,
        *,
        audit_message: str,
        due_at: protocols.ScheduleBy = protocols._NotGiven,
        schedule_next_latest_at: protocols.ScheduleBy = protocols._NotGiven,
    ) -> protocols.Result[protocols.T_CO_State]:
        return Result(
            state=self._original_state.clone(
                error=None,
                execution_state=protocols.ExecutionState.STOPPED,
                result_state=protocols.ResultState.SUCCESS,
            ),
            audit_message=audit_message,
            due_at=due_at,
            schedule_next_latest_at=schedule_next_latest_at,
        )

    def paused(
        self,
        *,
        audit_message: str,
        due_at: protocols.ScheduleBy = protocols._NotGiven,
        schedule_next_latest_at: protocols.ScheduleBy = protocols._NotGiven,
    ) -> protocols.Result[protocols.T_CO_State]:
        return Result(
            state=self._original_state.clone(
                error=None,
                execution_state=protocols.ExecutionState.PAUSED,
                result_state=protocols.ResultState.ABSENT,
            ),
            audit_message=audit_message,
            due_at=due_at,
            schedule_next_latest_at=schedule_next_latest_at,
        )

    def cancelled(
        self,
        *,
        audit_message: str,
        due_at: protocols.ScheduleBy = protocols._NotGiven,
        schedule_next_latest_at: protocols.ScheduleBy = protocols._NotGiven,
    ) -> protocols.Result[protocols.T_CO_State]:
        return Result(
            state=self._original_state.clone(
                error=None,
                execution_state=protocols.ExecutionState.STOPPED,
                result_state=protocols.ResultState.CANCELLED,
            ),
            audit_message=audit_message,
            due_at=due_at,
            schedule_next_latest_at=schedule_next_latest_at,
        )

    def cancelling(
        self,
        *,
        audit_message: str,
        due_at: protocols.ScheduleBy = protocols._NotGiven,
        schedule_next_latest_at: protocols.ScheduleBy = protocols._NotGiven,
    ) -> protocols.Result[protocols.T_CO_State]:
        return Result(
            state=self._original_state.clone(
                error=None,
                execution_state=protocols.ExecutionState.CANCELLING,
                result_state=protocols.ResultState.ABSENT,
            ),
            audit_message=audit_message,
            due_at=due_at,
            schedule_next_latest_at=schedule_next_latest_at,
        )

    def handled_failure(
        self,
        *,
        error: protocols.Error,
        audit_message: str,
        due_at: protocols.ScheduleBy = protocols._NotGiven,
        schedule_next_latest_at: protocols.ScheduleBy = protocols._NotGiven,
    ) -> protocols.Result[protocols.T_CO_State]:
        return Result(
            state=self._original_state.clone(
                error=error,
                execution_state=protocols.ExecutionState.STOPPED,
                result_state=protocols.ResultState.HANDLED_FAILURE,
            ),
            audit_message=audit_message,
            due_at=due_at,
            schedule_next_latest_at=schedule_next_latest_at,
        )

    def unhandled_failure(
        self,
        *,
        exc: Exception,
        audit_message: str,
        due_at: protocols.ScheduleBy = protocols._NotGiven,
        exception_serializer: protocols.ExceptionSerializer,
        schedule_next_latest_at: protocols.ScheduleBy = protocols._NotGiven,
    ) -> protocols.Result[protocols.T_CO_State]:
        return Result(
            state=self._original_state.clone(
                error=exception_serializer.serialize_exception(exc),
                execution_state=protocols.ExecutionState.STOPPED,
                result_state=protocols.ResultState.UNHANDLED_FAILURE,
            ),
            audit_message=audit_message,
            due_at=due_at,
            schedule_next_latest_at=schedule_next_latest_at,
        )


class StoredInfoBase(Generic[protocols.T_CO_Data], abc.ABC):
    """
    Information stored for a computation
    """

    @property
    @abc.abstractproperty
    def state(self) -> protocols.T_CO_Data | None:
        ...

    @abc.abstractmethod
    def merge(self, result: protocols.Result[protocols.T_CO_Data]) -> Self:
        ...


@attrs.frozen
class StoredInfo(StoredInfoBase[protocols.T_CO_State], abc.ABC):
    """
    Information stored for a computation
    """

    state: protocols.T_CO_State

    def merge(self, result: protocols.Result[protocols.T_CO_State]) -> Self:
        return self.__class__(
            state=self.state.clone(
                error=result.state.error,
                execution_state=result.state.execution_state,
                result_state=result.state.result_state,
                due_at=result.due_at,
                schedule_next_latest_at=result.schedule_next_latest_at,
            )
        )


if TYPE_CHECKING:
    A_WorkflowIdentifier = WorkflowIdentifierBase
    A_Error = ErrorBase
    A_ErrorRaw = ErrorRawBase
    A_JobPath = JobPathBase
    A_ExternalInputPath = ExternalInputPathBase
    A_State = StateBase
    A_ComputationState = ComputationStateBase[protocols.P_State]
    A_Result = ResultBase[protocols.P_State]
    A_Results = ResultsBase[protocols.P_State]
    A_StoredInfo = StoredInfoBase[protocols.P_State]

    C_WorkflowIdentifier = WorkflowIdentifier
    C_Error = Error
    C_ErrorRaw = ErrorRaw
    C_JobPath = JobPath
    C_ExternalInputPath = ExternalInputPath
    C_State = State
    C_ComputationState = ComputationState[C_State]
    C_Result = Result[C_State]
    C_Results = Results[C_State]
    C_StoredInfo = StoredInfo[C_State]

    _WI: protocols.P_WorkflowIdentifier = cast(A_WorkflowIdentifier, None)
    _WIC: protocols.P_WorkflowIdentifier = cast(C_WorkflowIdentifier, None)

    _E: protocols.P_Error = cast(A_Error, None)
    _EC: protocols.P_Error = cast(C_Error, None)
    _SE: protocols.P_Error = cast(SimpleError, None)

    _JP: protocols.P_JobPath = cast(A_JobPath, None)
    _JPC: protocols.P_JobPath = cast(C_JobPath, None)

    _EI: protocols.P_ExternalInputPath = cast(A_ExternalInputPath, None)
    _EIC: protocols.P_ExternalInputPath = cast(C_ExternalInputPath, None)

    _S: protocols.State = cast(A_State, None)
    _SC: protocols.State = cast(C_State, None)

    _CS: protocols.P_ComputationState = cast(A_ComputationState, None)
    _CSC: protocols.P_ComputationState = cast(C_ComputationState, None)

    _R: protocols.P_Result = cast(A_Result, None)
    _RC: protocols.P_Result = cast(C_Result, None)

    _RS: protocols.P_Results = cast(A_Results, None)
    _RSC: protocols.P_Results = cast(C_Results, None)

    _SI: protocols.P_StoredInfo = cast(A_StoredInfo, None)
    _SIC: protocols.P_StoredInfo = cast(C_StoredInfo, None)
