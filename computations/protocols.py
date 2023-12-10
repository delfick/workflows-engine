"""
A computation is a function attached to an entity that may run zero or more jobs
where each job represents another computation that has stored state.

A workflow represents the first computation and may store extra information in
the database. All computations are then defined relative to the workflow.

All computations have an execution state, a result state and optionally an error.

There exists a registry for creating and running workflows. The registry is
responsible for assigning a code to each workflow class for serialization and
de-serialization.

The registry is used in conjunction with an engine to find, hydrate and run all
computations. Keeping this as a separate object facilitates easily running
computations in a testing environment with the same API used in production.

There is also the concept of ``ExternalInput`` which can be used to
get input from outside of the workflow.
"""
from __future__ import annotations

import contextlib
import datetime
import enum
from collections.abc import Hashable, Iterable, Mapping, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Protocol,
    Self,
    TypeVar,
    overload,
    runtime_checkable,
)

T_Job = TypeVar("T_Job", bound="P_Job")
T_Data = TypeVar("T_Data")
T_Storage = TypeVar("T_Storage", bound="P_Storage")
T_Computation = TypeVar("T_Computation", bound="P_Computation")
T_ExternalData = TypeVar("T_ExternalData")
T_ComputationState = TypeVar("T_ComputationState", bound="P_ComputationState")

T_CO_Job = TypeVar("T_CO_Job", bound="P_Job", covariant=True)
T_CO_Data = TypeVar("T_CO_Data", covariant=True)
T_CO_State = TypeVar("T_CO_State", bound="P_State", covariant=True)
T_CO_Result = TypeVar("T_CO_Result", bound="P_Result", covariant=True)
T_CO_JobStatus = TypeVar("T_CO_JobStatus", bound="P_JobStatus", covariant=True)
T_CO_Computation = TypeVar("T_CO_Computation", bound="P_Computation", covariant=True)
T_CO_ExternalData = TypeVar("T_CO_ExternalData", covariant=True)
T_CO_ComputationState = TypeVar(
    "T_CO_ComputationState", bound="P_ComputationState", covariant=True
)


class NotGiven(type):
    pass


class _NotGiven(metaclass=NotGiven):
    """
    Used to represent an argument not being provided a value
    """


ScheduleBy = datetime.datetime | datetime.timedelta | None | NotGiven

LoggingContext = Mapping[str, str | int | bool | None]

if TYPE_CHECKING:
    SimpleJSON = None | int | str | bool | Iterable[SimpleJSON] | Mapping[str, SimpleJSON]
else:
    SimpleJSON = None | int | str | bool | Iterable[object] | Mapping[str, object]

Path = tuple[str, ...]


class ExecutionState(enum.IntEnum):
    """
    Execution state represents the motion of a computation.
    """

    PENDING = 0
    PROGRESSING = 1
    CANCELLING = 2
    PAUSED = 3
    STOPPED = 4


class ResultState(enum.IntEnum):
    """
    Result state represents the type of result on the computation.
    """

    ABSENT = 0
    SUCCESS = 1
    CANCELLED = 2
    HANDLED_FAILURE = 3
    UNHANDLED_FAILURE = 4


@runtime_checkable
class ExternalInputResolver(Protocol[T_CO_ExternalData]):
    """
    A class for resolving external data
    """

    def resolve(self) -> T_CO_ExternalData:
        """
        Resolve the external input
        """


@runtime_checkable
class ExceptionSerializer(Protocol):
    """
    Object that can serialize an exception into an Error
    """

    def serialize_exception(self, exc: Exception, /) -> ErrorRaw:
        ...


@runtime_checkable
class ErrorResolver(Protocol):
    """
    Object that can resolve an error code into a concrete class
    """

    def resolve_error(self, error: ErrorRaw | Error, /) -> Error:
        ...


class WorkflowIdentifier(Protocol, Hashable):
    """
    Workflow Identifier is used to represent a workflow

    It is a container so it's harder to pass around the wrong strings.
    """

    @property
    def identifier(self) -> str:
        ...


class WorkflowInformation(Protocol):
    """
    This represents the information used to hydrate a workflow.
    """

    @property
    def workflow_code(self) -> str:
        ...

    @property
    def workflow_version(self) -> int:
        ...

    @property
    def information(self) -> SimpleJSON:
        ...

    @property
    def tags(self) -> Iterable[str]:
        ...

    @property
    def earliest_due_at(self) -> datetime.datetime | None:
        ...

    @property
    def earliest_next_schedule_at(self) -> datetime.datetime | None:
        ...


@runtime_checkable
class ErrorRaw(Protocol):
    """
    ErrorRaw represents the type of error on the computation.

    Without the ability to turn that into a concrete exception.
    """

    @property
    def format_code(self) -> str:
        ...

    @property
    def format_version(self) -> int:
        ...

    @property
    def serialized(self) -> SimpleJSON:
        ...


@runtime_checkable
class Error(Protocol):
    """
    Error represents the type of error on the computation.

    With the ability to turn that into a concrete exception.
    """

    @property
    def format_code(self) -> str:
        ...

    @property
    def format_version(self) -> int:
        ...

    @property
    def serialized(self) -> SimpleJSON:
        ...

    def as_exception(self, *, identifier: WorkflowIdentifier, path: Path) -> Exception:
        """
        Return a concrete exception for this error code
        """


@runtime_checkable
class JobPath(Protocol):
    """
    Used to represent a path to a job
    """

    @property
    def identifier(self) -> WorkflowIdentifier:
        """
        Identifier for the workflow itself
        """

    @property
    def prefix(self) -> Path:
        """
        The path up to the current computation
        """

    @property
    def job_name(self) -> str:
        """
        The name of the job the computation wants to get state for.
        """

    @property
    def path(self) -> Path:
        """
        The full path including this job.
        """


@runtime_checkable
class ExternalInputPath(Protocol):
    """
    Used to represent a path to external input
    """

    @property
    def identifier(self) -> WorkflowIdentifier:
        """
        Identifier for the workflow itself
        """

    @property
    def external_input_name(self) -> str:
        """
        The name of the external input the computation wants to get information for.
        """


class State(Protocol):
    """
    Represents the current state of a computation.
    """

    @property
    def error(self) -> ErrorRaw | None:
        ...

    @property
    def execution_state(self) -> ExecutionState:
        ...

    @property
    def result_state(self) -> ResultState:
        ...

    @property
    def created_at(self) -> datetime.datetime:
        ...

    @property
    def due_at(self) -> datetime.datetime | datetime.timedelta | None:
        ...

    @property
    def schedule_next_latest_at(self) -> ScheduleBy:
        ...

    def clone(
        self,
        error: ErrorRaw | Error | None | NotGiven = _NotGiven,
        execution_state: ExecutionState | NotGiven = _NotGiven,
        result_state: ResultState | NotGiven = _NotGiven,
        due_at: ScheduleBy = _NotGiven,
        schedule_next_latest_at: ScheduleBy = _NotGiven,
    ) -> Self:
        ...


class ComputationState(Protocol[T_CO_Data]):
    """
    A version of the state that all computations must be able to take in
    """

    @property
    def _original_state(self) -> T_CO_Data:
        ...

    @property
    def identifier(self) -> WorkflowIdentifier:
        ...

    @property
    def path(self) -> Path:
        ...

    @property
    def exception(self) -> Exception | None:
        ...

    @property
    def error(self) -> Error | None:
        ...

    @property
    def execution_state(self) -> ExecutionState:
        ...

    @property
    def result_state(self) -> ResultState:
        ...

    @property
    def due_at(self) -> datetime.datetime | datetime.timedelta | None:
        ...

    @property
    def logging_context(self) -> LoggingContext:
        """
        Return a map of information that can be used for logging context
        """

    def job_path(self, job_name: str, /) -> JobPath:
        """
        Create an object representing the path to a job.
        """

    def external_input_path(self, external_input_name: str, /) -> ExternalInputPath:
        """
        Create an object representing the path to a external input.
        """


class Result(Protocol[T_CO_Data]):
    """
    Returned from a computation to represent the state the computation is now in
    """

    @property
    def state(self) -> T_CO_Data:
        ...

    @property
    def audit_message(self) -> str:
        ...

    @property
    def due_at(self) -> ScheduleBy:
        ...

    @property
    def schedule_next_latest_at(self) -> ScheduleBy:
        ...


class Results(Protocol[T_CO_Data]):
    """
    Object used to return a new state
    """

    def no_change(
        self,
        *,
        audit_message: str = "",
        due_at: ScheduleBy = _NotGiven,
        schedule_next_latest_at: ScheduleBy = _NotGiven,
    ) -> Result[T_CO_Data]:
        """
        Return a result representing no change
        """

    def pending(
        self,
        *,
        audit_message: str,
        due_at: ScheduleBy = _NotGiven,
        schedule_next_latest_at: ScheduleBy = _NotGiven,
    ) -> Result[T_CO_Data]:
        """
        Return a result representing a change to pending
        """

    def progressing(
        self,
        *,
        audit_message: str,
        due_at: ScheduleBy = _NotGiven,
        schedule_next_latest_at: ScheduleBy = _NotGiven,
    ) -> Result[T_CO_Data]:
        """
        Return a result representing a change to progressing
        """

    def success(
        self,
        *,
        audit_message: str,
        due_at: ScheduleBy = _NotGiven,
        schedule_next_latest_at: ScheduleBy = _NotGiven,
    ) -> Result[T_CO_Data]:
        """
        Return a result representing a change to success
        """

    def paused(
        self,
        *,
        audit_message: str,
        due_at: ScheduleBy = _NotGiven,
        schedule_next_latest_at: ScheduleBy = _NotGiven,
    ) -> Result[T_CO_Data]:
        """
        Return a result representing a change to paused
        """

    def cancelled(
        self,
        *,
        audit_message: str,
        due_at: ScheduleBy = _NotGiven,
        schedule_next_latest_at: ScheduleBy = _NotGiven,
    ) -> Result[T_CO_Data]:
        """
        Return a result representing a change to cancelled
        """

    def cancelling(
        self,
        *,
        audit_message: str,
        due_at: ScheduleBy = _NotGiven,
        schedule_next_latest_at: ScheduleBy = _NotGiven,
    ) -> Result[T_CO_Data]:
        """
        Return a result representing a change to cancelling
        """

    def handled_failure(
        self,
        *,
        error: Error,
        audit_message: str,
        due_at: ScheduleBy = _NotGiven,
        schedule_next_latest_at: ScheduleBy = _NotGiven,
    ) -> Result[T_CO_Data]:
        """
        Return a result representing a change to a handled error
        """

    def unhandled_failure(
        self,
        *,
        exc: Exception,
        audit_message: str,
        due_at: ScheduleBy = _NotGiven,
        exception_serializer: ExceptionSerializer,
        schedule_next_latest_at: ScheduleBy = _NotGiven,
    ) -> Result[T_CO_Data]:
        """
        Return a result representing a change to a unhandled error
        """


class ComputationExecutor(Protocol[T_Data]):
    @overload
    def __call__(
        self,
        path: JobPath,
        intention: Computation[T_Data],
        /,
        *,
        override_execute: Computation[T_Data] | Literal[True] | None = None,
    ) -> Job[ComputationState[T_Data], Computation[T_Data], Result[T_Data]]:
        ...

    @overload
    def __call__(
        self,
        path: ExternalInputPath,
        intention: ExternalInputResolver[T_ExternalData],
        /,
        *,
        override_execute: None = None,
    ) -> T_ExternalData:
        ...

    def __call__(
        self,
        path: JobPath | ExternalInputPath,
        intention: Computation[T_Data] | ExternalInputResolver[T_ExternalData],
        /,
        *,
        override_execute: Computation[T_Data] | Literal[True] | None = None,
    ) -> Job[ComputationState[T_Data], Computation[T_Data], Result[T_Data]] | T_ExternalData:
        ...

    def get_without_executing(
        self, path: JobPath, intention: Computation[T_Data], /
    ) -> Job[ComputationState[T_Data], Computation[T_Data], Result[T_Data]]:
        ...


@runtime_checkable
class Computation(Protocol[T_Data]):
    """
    Computation represents some logic that should be run.
    """

    def execute(
        self, state: ComputationState[T_Data], execute: ComputationExecutor[T_Data], /
    ) -> Result[T_Data]:
        """
        Given an engine and an existing state, perform some logic and return a
        new result.
        """


class Job(Protocol[T_CO_ComputationState, T_CO_Computation, T_CO_Result]):
    """
    A job represents the state of a computation. It does not store the computation
    itself, only the state associated with the job name it was made with.
    """

    @property
    def _result(self) -> T_CO_Result:
        """
        The result object that created this job
        """

    @property
    def name(self) -> str:
        """
        The name of the job
        """

    @property
    def state(self) -> T_CO_ComputationState:
        """
        The current state of the computation for this job
        """

    @property
    def computation(self) -> T_CO_Computation:
        """
        The specific computation that this job is running
        """

    @property
    def done(self) -> bool:
        """
        Return whether this job is done
        """

    @property
    def success(self) -> bool:
        """
        Return whether this job is done and successful
        """

    @property
    def cancelled(self) -> bool:
        """
        Return whether this job was cancelled
        """

    @property
    def exception(self) -> Exception | None:
        """
        Return an exception if this job has one
        """


class JobStatus(Protocol[T_Data]):
    @property
    def name(self) -> str:
        ...

    @property
    def job_before(
        self,
    ) -> Job[ComputationState[T_Data], Computation[T_Data], Result[T_Data]] | None:
        ...

    @property
    def job_executions(
        self,
    ) -> Sequence[Job[ComputationState[T_Data], Computation[T_Data], Result[T_Data]]]:
        ...

    def clone(self) -> Self:
        ...

    def add_execution(
        self, job: Job[ComputationState[T_Data], Computation[T_Data], Result[T_Data]]
    ) -> None:
        ...

    @property
    def latest_execution(
        self,
    ) -> Job[ComputationState[T_Data], Computation[T_Data], Result[T_Data]] | None:
        ...

    def earliest_due_at(
        self, *, delta_base_from: datetime.datetime, must_be_greater_than: datetime.datetime
    ) -> datetime.datetime | None:
        ...

    def earliest_next_schedule_at(
        self, *, delta_base_from: datetime.datetime, must_be_greater_than: datetime.datetime
    ) -> datetime.datetime | None:
        ...


class JobTracker(Protocol[T_CO_JobStatus]):
    def jobs(self, *, path: Path = (), max_levels: int = 1) -> Mapping[Path, T_CO_JobStatus]:
        ...

    def job_status(self, job_path: JobPath, /) -> T_CO_JobStatus:
        ...

    def earliest_due_at(
        self, *, delta_base_from: datetime.datetime, must_be_greater_than: datetime.datetime
    ) -> datetime.datetime | None:
        ...

    def earliest_next_schedule_at(
        self, delta_base_from: datetime.datetime, must_be_greater_than: datetime.datetime
    ) -> datetime.datetime | None:
        ...


class WorkflowSaver(Protocol[T_Data]):
    def for_storage(
        self,
        identifier: WorkflowIdentifier,
        /,
        *,
        workflow_job: Job[ComputationState[T_Data], Computation[T_Data], Result[T_Data]],
        job_tracker: JobTracker[JobStatus[T_Data]],
        original_workflow_information: WorkflowInformation,
    ) -> WorkflowInformation:
        """
        Return information to be stored for this Workflow.
        """


class NewWorkflowSaver(Protocol):
    def for_storage(self, identifier: WorkflowIdentifier, /) -> WorkflowInformation:
        """
        Return information to be stored for this new Workflow.
        """


class WorkflowLoader(Protocol[T_Data, T_CO_Computation]):
    """
    The starting point for executing a chain of computations.
    """

    @classmethod
    def from_storage(
        cls, identifier: WorkflowIdentifier, information: SimpleJSON, /
    ) -> tuple[WorkflowSaver[T_Data], T_CO_Computation]:
        """
        Given the stored data for this workflow, return a computation to run.
        """


class StoredInfo(Protocol[T_Data]):
    """
    Information representing a computation.
    """

    @property
    def state(self) -> T_Data:
        ...

    def merge(self, result: Result[T_Data]) -> Self:
        ...


class Storage(Protocol[T_Data]):
    """
    An object that knows how to operate on information with some storage.
    """

    def hold_workflow_lock(
        self, identifier: WorkflowIdentifier, /
    ) -> contextlib.AbstractContextManager[None]:
        """
        A context manager for holding a lock for a specific workflow
        """

    def retrieve_workflow_information(
        self, *, identifier: WorkflowIdentifier
    ) -> WorkflowInformation:
        """
        Retrieve information from storage for the provided workflow identifier
        """

    def upsert_workflow_information(
        self, *, identifier: WorkflowIdentifier, workflow_information: WorkflowInformation
    ) -> None:
        """
        Store information for this workflow
        """

    def retrieve_computations(
        self, *, identifier: WorkflowIdentifier
    ) -> Mapping[Path, StoredInfo[T_Data]]:
        """
        Retrieve all stored information by path for this identifier
        """

    def upsert_computations(
        self,
        *,
        identifier: WorkflowIdentifier,
        stored_infos: Mapping[Path, StoredInfo[T_Data]],
    ) -> None:
        """
        Upsert all the computations for this identifier
        """


class Engine(Protocol[T_Data]):
    """
    Used by a computation to run other jobs and get external input
    """

    def run(
        self,
        *,
        job_path: JobPath,
        job_tracker: JobTracker[JobStatus[T_Data]],
        computation: T_Computation,
        override_execute: Computation[T_Data] | Literal[True] | None = None,
    ) -> Job[ComputationState[T_Data], T_Computation, Result[T_Data]]:
        """
        Execute the computation and add to the job_tracker
        """

    def external_input(
        self,
        *,
        external_input_path: ExternalInputPath,
        external_input_resolver: ExternalInputResolver[T_ExternalData],
    ) -> T_ExternalData:
        """
        Get some external input.
        """


class WorkflowRunner(Protocol[T_Data]):
    """
    Object that represents the current state of a workflow and the ability to execute it.
    """

    @property
    def state(self) -> ComputationState[T_Data]:
        """
        The current state of the workflow.
        """

    def run(
        self, *, override_execute: Computation[T_Data] | None = None
    ) -> Job[ComputationState[T_Data], Computation[T_Data], Result[T_Data]]:
        """
        Start running the workflow.
        """


class ComputationRegistry(Protocol[T_Data]):
    """
    A registry that is the entry point for workflows. Used to create and retrieve
    workflows.
    """

    def create_workflow(self, workflow: NewWorkflowSaver, /) -> WorkflowIdentifier:
        """
        Create a workflow and return the identifier that can be used for retrieving
        it later.
        """

    def retrieve_workflow(self, identifier: WorkflowIdentifier, /) -> WorkflowRunner[T_Data]:
        """
        Get a desired workflow and return an object for interacting with it.
        """


if TYPE_CHECKING:
    P_ExternalInputResolver = ExternalInputResolver[Any]
    P_ExceptionSerializer = ExceptionSerializer
    P_ErrorResolver = ErrorResolver
    P_WorkflowIdentifier = WorkflowIdentifier
    P_WorkflowInformation = WorkflowInformation
    P_ErrorRaw = ErrorRaw
    P_Error = Error
    P_JobPath = JobPath
    P_ExternalInputPath = ExternalInputPath
    P_State = State
    P_ComputationState = ComputationState[Any]
    P_Result = Result[Any]
    P_Results = Results[Any]
    P_ComputationExecutor = ComputationExecutor[Any]
    P_Computation = Computation[Any]
    P_Job = Job[P_ComputationState, P_Computation, P_Result]
    P_JobStatus = JobStatus[Any]
    P_JobTracker = JobTracker[P_JobStatus]
    P_WorkflowSaver = WorkflowSaver[Any]
    P_NewWorkflowSaver = NewWorkflowSaver
    P_WorkflowLoader = WorkflowLoader[Any, P_Computation]
    P_StoredInfo = StoredInfo[Any]
    P_Storage = Storage[Any]
    P_Engine = Engine[Any]
    P_WorkflowRunner = WorkflowRunner[Any]
    P_ComputationRegistry = ComputationRegistry[Any]
