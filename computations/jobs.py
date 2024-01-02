from __future__ import annotations

import abc
import datetime
from collections.abc import Callable, Iterator, Mapping, MutableMapping, Sequence
from typing import TYPE_CHECKING, Generic, Protocol, Self, assert_never, cast

import attrs

from . import protocols, state

_P_Job = protocols.Job[
    protocols.ComputationState[protocols.T_CO_Data],
    protocols.Computation[protocols.T_CO_Data],
    protocols.Result[protocols.T_CO_Data],
]


class _DateGetter(Protocol):
    def __call__(
        self, *, delta_base_from: datetime.datetime, must_be_greater_than: datetime.datetime
    ) -> datetime.datetime | None:
        ...


class JobBase(
    Generic[protocols.T_CO_ComputationState, protocols.T_CO_Computation, protocols.T_CO_Result],
    abc.ABC,
):
    """
    A job represents the state of a computation. It does not store the computation
    itself, only the state associated with the job name it was made with.
    """

    @property
    @abc.abstractproperty
    def _result(self) -> protocols.T_CO_Result:
        """
        The result used to populate this job.
        """

    @property
    @abc.abstractproperty
    def name(self) -> str:
        """
        The name of the job
        """

    @property
    @abc.abstractproperty
    def state(self) -> protocols.T_CO_ComputationState:
        """
        The current state of the computation for this job
        """

    @property
    @abc.abstractproperty
    def computation(self) -> protocols.T_CO_Computation:
        """
        The specific computation that this job is running
        """

    @property
    @abc.abstractproperty
    def done(self) -> bool:
        """
        Return whether this job is done
        """

    @property
    @abc.abstractproperty
    def success(self) -> bool:
        """
        Return whether this job is done and successful
        """

    @property
    @abc.abstractproperty
    def cancelled(self) -> bool:
        """
        Return whether this job was cancelled
        """

    @property
    @abc.abstractproperty
    def exception(self) -> Exception | None:
        """
        Return an exception if this job has one
        """


@attrs.frozen
class Job(
    JobBase[protocols.T_CO_ComputationState, protocols.T_CO_Computation, protocols.T_CO_Result]
):
    """
    A job is an object that is used to manage the state of some computation.

    It is up to the computation to decide how to proceed with both action and updating that state.
    """

    _result: protocols.T_CO_Result

    name: str
    computation: protocols.T_CO_Computation
    state: protocols.T_CO_ComputationState

    @property
    def done(self) -> bool:
        match execution_state := self.state.execution_state:
            case protocols.ExecutionState.PENDING:
                return False
            case protocols.ExecutionState.PROGRESSING:
                return False
            case protocols.ExecutionState.CANCELLING:
                return False
            case protocols.ExecutionState.PAUSED:
                return False
            case protocols.ExecutionState.STOPPED:
                return True
            case _:
                assert_never(execution_state)

    @property
    def success(self) -> bool:
        match result_state := self.state.result_state:
            case protocols.ResultState.ABSENT:
                return False
            case protocols.ResultState.SUCCESS:
                return True
            case protocols.ResultState.CANCELLED:
                return False
            case protocols.ResultState.HANDLED_FAILURE:
                return False
            case protocols.ResultState.UNHANDLED_FAILURE:
                return False
            case _:
                assert_never(result_state)

    @property
    def cancelled(self) -> bool:
        match result_state := self.state.result_state:
            case protocols.ResultState.ABSENT:
                return False
            case protocols.ResultState.SUCCESS:
                return False
            case protocols.ResultState.CANCELLED:
                return True
            case protocols.ResultState.HANDLED_FAILURE:
                return False
            case protocols.ResultState.UNHANDLED_FAILURE:
                return False
            case _:
                assert_never(result_state)

    @property
    def exception(self) -> Exception | None:
        match result_state := self.state.result_state:
            case protocols.ResultState.ABSENT:
                return None
            case protocols.ResultState.SUCCESS:
                return None
            case protocols.ResultState.CANCELLED:
                return self.state.exception
            case protocols.ResultState.HANDLED_FAILURE:
                return self.state.exception
            case protocols.ResultState.UNHANDLED_FAILURE:
                return self.state.exception
            case _:
                assert_never(result_state)


class JobStatusBase(Generic[protocols.T_CO_Data], abc.ABC):
    @property
    @abc.abstractproperty
    def name(self) -> str:
        ...

    @property
    @abc.abstractproperty
    def job_before(
        self,
    ) -> _P_Job[protocols.T_CO_Data] | None:
        ...

    @property
    @abc.abstractproperty
    def job_executions(
        self,
    ) -> Sequence[_P_Job[protocols.T_CO_Data]]:
        ...

    @abc.abstractmethod
    def clone(self) -> Self:
        ...

    @abc.abstractmethod
    def add_execution(self, job: _P_Job[protocols.T_CO_Data]) -> None:
        ...

    @property
    @abc.abstractproperty
    def latest_execution(
        self,
    ) -> _P_Job[protocols.T_CO_Data] | None:
        ...

    @abc.abstractmethod
    def earliest_due_at(
        self, *, delta_base_from: datetime.datetime, must_be_greater_than: datetime.datetime
    ) -> datetime.datetime | None:
        ...

    @abc.abstractmethod
    def earliest_next_schedule_at(
        self, *, delta_base_from: datetime.datetime, must_be_greater_than: datetime.datetime
    ) -> datetime.datetime | None:
        ...


@attrs.frozen
class JobStatus(JobStatusBase[protocols.T_CO_Data]):
    name: str

    job_before: _P_Job[protocols.T_CO_Data] | None = None

    _job_executions: list[_P_Job[protocols.T_CO_Data]] = attrs.field(init=False, factory=list)

    def clone(self) -> Self:
        job_status = self.__class__(name=self.name, job_before=self.job_before)
        for job in self.job_executions:
            job_status.add_execution(job)
        return job_status

    @property
    def job_executions(
        self,
    ) -> Sequence[_P_Job[protocols.T_CO_Data]]:
        return list(self._job_executions)

    def add_execution(self, job: _P_Job[protocols.T_CO_Data]) -> None:
        self._job_executions.append(job)

    @property
    def latest_execution(
        self,
    ) -> _P_Job[protocols.T_CO_Data] | None:
        if not self._job_executions:
            return None
        else:
            return self._job_executions[-1]

    def earliest_due_at(
        self, *, delta_base_from: datetime.datetime, must_be_greater_than: datetime.datetime
    ) -> datetime.datetime | None:
        latest_execution = self.latest_execution
        if (
            latest_execution is None
            or isinstance(value := latest_execution._result.due_at, protocols.NotGiven)
            or value is None
        ):
            return None

        if isinstance(value, datetime.timedelta):
            value = delta_base_from + value

        if value < must_be_greater_than:
            return None
        else:
            return value

    def earliest_next_schedule_at(
        self, *, delta_base_from: datetime.datetime, must_be_greater_than: datetime.datetime
    ) -> datetime.datetime | None:
        latest_execution = self.latest_execution
        if (
            latest_execution is None
            or isinstance(
                value := latest_execution._result.schedule_next_latest_at, protocols.NotGiven
            )
            or value is None
        ):
            return None

        if isinstance(value, datetime.timedelta):
            value = delta_base_from + value

        if value < must_be_greater_than:
            return None
        else:
            return value


class JobTrackerBase(Generic[protocols.T_CO_JobStatus], abc.ABC):
    @abc.abstractmethod
    def jobs(
        self, *, path: protocols.Path = (), max_levels: int = 1
    ) -> Mapping[protocols.Path, protocols.T_CO_JobStatus]:
        ...

    @abc.abstractmethod
    def job_status(self, job_path: protocols.JobPath, /) -> protocols.T_CO_JobStatus:
        ...

    @abc.abstractmethod
    def earliest_due_at(
        self, *, delta_base_from: datetime.datetime, must_be_greater_than: datetime.datetime
    ) -> datetime.datetime | None:
        ...

    @abc.abstractmethod
    def earliest_next_schedule_at(
        self, delta_base_from: datetime.datetime, must_be_greater_than: datetime.datetime
    ) -> datetime.datetime | None:
        ...


@attrs.frozen
class JobTracker(JobTrackerBase[protocols.T_CO_JobStatus], abc.ABC):
    _start_jobs: Mapping[protocols.Path, protocols.T_CO_JobStatus]
    _added_jobs: MutableMapping[protocols.Path, protocols.T_CO_JobStatus] = attrs.field(
        init=False, factory=dict
    )

    @abc.abstractmethod
    def _fresh_job_status(self, name: str, /) -> protocols.T_CO_JobStatus:
        ...

    def jobs(
        self, *, path: protocols.Path = (), max_levels: int = 1
    ) -> Mapping[protocols.Path, protocols.T_CO_JobStatus]:
        jobs = {**self._start_jobs, **self._added_jobs}
        result: dict[protocols.Path, protocols.T_CO_JobStatus] = {}

        for k, job in jobs.items():
            if k[: len(path)] == path:
                remainder = k[len(path) :]
                if max_levels == -1 or 0 < len(remainder) <= max_levels:
                    result[k] = job

        return result

    def job_status(self, job_path: protocols.JobPath, /) -> protocols.T_CO_JobStatus:
        if status := self._added_jobs.get(job_path.path):
            return status

        if status := self._start_jobs.get(job_path.path):
            clone = status.clone()
            self._added_jobs[job_path.path] = clone
            return clone

        new_status = self._fresh_job_status(job_path.job_name)
        self._added_jobs[job_path.path] = new_status
        return new_status

    def _get_earliest_date(
        self,
        *,
        delta_base_from: datetime.datetime,
        must_be_greater_than: datetime.datetime,
        get_date_getters: Callable[[], Iterator[_DateGetter]],
    ) -> datetime.datetime | None:
        found = [
            val
            for val in [
                date_getter(
                    delta_base_from=delta_base_from, must_be_greater_than=must_be_greater_than
                )
                for date_getter in get_date_getters()
            ]
            if val is not None
        ]
        if not found:
            return None
        else:
            return min(found)

    def earliest_due_at(
        self, *, delta_base_from: datetime.datetime, must_be_greater_than: datetime.datetime
    ) -> datetime.datetime | None:
        def get_date_getters() -> Iterator[_DateGetter]:
            for job_status in self.jobs(max_levels=-1).values():
                yield job_status.earliest_due_at

        return self._get_earliest_date(
            delta_base_from=delta_base_from,
            must_be_greater_than=must_be_greater_than,
            get_date_getters=get_date_getters,
        )

    def earliest_next_schedule_at(
        self, delta_base_from: datetime.datetime, must_be_greater_than: datetime.datetime
    ) -> datetime.datetime | None:
        def get_date_getters() -> Iterator[_DateGetter]:
            for job_status in self.jobs(max_levels=-1).values():
                yield job_status.earliest_next_schedule_at

        return self._get_earliest_date(
            delta_base_from=delta_base_from,
            must_be_greater_than=must_be_greater_than,
            get_date_getters=get_date_getters,
        )


class ComputationBase(Generic[protocols.T_CO_Data], abc.ABC):
    """
    Represents a piece of logic
    """

    @abc.abstractmethod
    def execute(
        self,
        state: protocols.ComputationState[protocols.T_CO_Data],
        execute: protocols.ComputationExecutor[protocols.T_CO_Data],
        /,
    ) -> protocols.Result[protocols.T_CO_Data]:
        """
        Perform the logic of the computation.
        """


class WorkflowBase(Generic[protocols.T_Data, protocols.T_CO_Computation], abc.ABC):
    """
    The starting point for executing a chain of computations.
    """

    @classmethod
    @abc.abstractmethod
    def from_storage(
        cls, identifier: protocols.WorkflowIdentifier, information: protocols.SimpleJSON, /
    ) -> tuple[protocols.WorkflowSaver[protocols.T_Data], protocols.T_CO_Computation]:
        """
        Given the stored data for this workflow, return a computation to run.
        """

    @abc.abstractmethod
    def for_storage(
        self,
        identifier: protocols.WorkflowIdentifier,
        /,
        *,
        workflow_job: _P_Job[protocols.T_Data] | None = None,
        original_workflow_information: protocols.WorkflowInformation | None = None,
        job_tracker: protocols.JobTracker[protocols.JobStatus[protocols.T_Data]] | None = None,
    ) -> protocols.WorkflowInformation:
        """
        Return information to be stored for this Workflow.
        """


if TYPE_CHECKING:
    A_Job = JobBase[protocols.P_ComputationState, protocols.P_Computation, protocols.P_Result]
    A_JobStatus = JobStatusBase[protocols.P_State]
    A_JobTracker = JobTrackerBase[A_JobStatus]
    A_Computation = ComputationBase[protocols.P_State]
    A_Workflow = WorkflowBase[protocols.P_State, protocols.P_Computation]

    C_Computation = ComputationBase[state.C_State]
    C_Job = Job[state.C_ComputationState, C_Computation, state.C_Result]
    C_JobStatus = JobStatus[state.C_State]
    C_JobTracker = JobTracker[C_JobStatus]
    C_Workflow = WorkflowBase[state.C_State, C_Computation]

    _J: protocols.P_Job = cast(A_Job, None)
    _JC: protocols.P_Job = cast(C_Job, None)

    _JS: protocols.P_JobStatus = cast(A_JobStatus, None)
    _JSC: protocols.P_JobStatus = cast(C_JobStatus, None)

    _JT: protocols.P_JobTracker = cast(A_JobTracker, None)
    _JTC: protocols.P_JobTracker = cast(C_JobTracker, None)

    _C: protocols.P_Computation = cast(A_Computation, None)
    _CC: protocols.P_Computation = cast(C_Computation, None)

    _WH: protocols.P_WorkflowLoader = cast(A_Workflow, None)
    _WHC: protocols.P_WorkflowLoader = cast(C_Workflow, None)

    _WS: protocols.P_WorkflowSaver = cast(A_Workflow, None)
    _WSC: protocols.P_WorkflowSaver = cast(C_Workflow, None)

    _WSN: protocols.P_NewWorkflowSaver = cast(A_Workflow, None)
    _WSNC: protocols.P_NewWorkflowSaver = cast(C_Workflow, None)
