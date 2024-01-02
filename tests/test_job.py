import abc
import datetime
from collections.abc import Mapping, MutableMapping, Sequence
from typing import Protocol, Self, cast

import attrs
import pytest

from computations import engine, errors, jobs, protocols, state, storage


class M_State(state.State):
    pass


B_Result = protocols.Result[M_State]
B_Computation = protocols.Computation[M_State]
B_ComputationState = protocols.ComputationState[M_State]
B_Job = protocols.Job[B_ComputationState, B_Computation, B_Result]
B_ComputationExecutor = protocols.ComputationExecutor[M_State]
B_Engine = protocols.Engine[M_State]
B_JobStatus = protocols.JobStatus[M_State]
B_JobTracker = protocols.JobTracker[B_JobStatus]

M_Result = state.Result[M_State]
M_Results = state.Results[M_State]
M_StoredInfo = state.StoredInfo[M_State]
M_Computation = jobs.ComputationBase[M_State]
M_ComputationState = state.ComputationState[M_State]
M_Storage = storage.MemoryStorage[M_State]
M_JobPath = state.JobPath
M_Job = jobs.Job[B_ComputationState, B_Computation, B_Result]
M_JobStatus = jobs.JobStatus[M_State]


class M_JobTracker(jobs.JobTracker[M_JobStatus]):
    def _fresh_job_status(self, name: str, /) -> M_JobStatus:
        return M_JobStatus(name=name)


class DefaultErrorResolver:
    def resolve_error(self, error: protocols.ErrorRaw | protocols.Error, /) -> protocols.Error:
        assert isinstance(error, protocols.Error)
        return error


@attrs.frozen
class M_Engine(engine.Engine[M_State, M_Storage]):
    store: M_Storage

    @abc.abstractmethod
    def _make_job(
        self,
        *,
        job_path: protocols.JobPath,
        result: protocols.Result[M_State] | None,
        error: protocols.Error | None,
        computation: protocols.T_Computation,
    ) -> protocols.Job[B_ComputationState, protocols.T_Computation, B_Result]:
        if result is None:
            result = M_Result(state=M_State.fresh(), audit_message="")

        computation_state = M_ComputationState(
            identifier=job_path.identifier,
            path=job_path.path,
            error=error,
            original_state=result.state,
        )

        return jobs.Job(
            name=job_path.job_name, state=computation_state, computation=computation, result=result
        )


class ConcreteComputation(M_Computation):
    def execute(self, state: B_ComputationState, execute: B_ComputationExecutor, /) -> B_Result:
        return M_Results.using(state).no_change()


## FIXTURES


class JobMaker(Protocol):
    def __call__(
        self,
        *,
        comp: B_Computation | None = None,
        state: M_State | None = None,
        result: B_Result | None = None,
    ) -> B_Job:
        ...


class JobMakerState(Protocol):
    def __call__(self, state: M_State, /) -> B_Job:
        ...


@pytest.fixture
def identifier() -> protocols.WorkflowIdentifier:
    return state.WorkflowIdentifier(identifier="__IDENTIFIER__")


@pytest.fixture
def fresh(identifier: protocols.WorkflowIdentifier) -> M_State:
    return M_State.fresh()


@pytest.fixture
def memory_store() -> M_Storage:
    return M_Storage()


@pytest.fixture
def make_job(identifier: protocols.WorkflowIdentifier) -> JobMaker:
    def make_job(
        *,
        comp: B_Computation | None = None,
        state: M_State | None = None,
        result: B_Result | None = None,
    ) -> B_Job:
        if comp is None:
            comp = ConcreteComputation()

        if result is None:
            assert state is not None
            result = M_Result(state=state, audit_message="")
        else:
            assert state is None

        return M_Job(
            name="j1",
            result=result,
            state=M_ComputationState(
                original_state=result.state,
                identifier=identifier,
                path=(".j1",),
                error=(
                    None
                    if result.state.error is None
                    else DefaultErrorResolver().resolve_error(result.state.error)
                ),
            ),
            computation=comp,
        )

    return make_job


## TESTS


class TestJob:
    @pytest.fixture
    def make_simple_job(self, memory_store: M_Storage, make_job: JobMaker) -> JobMakerState:
        def make_simple_job(
            state: M_State,
            /,
        ) -> B_Job:
            return make_job(state=state, comp=ConcreteComputation())

        return make_simple_job

    def test_it_knows_only_stopped_is_done(
        self, fresh: M_State, make_simple_job: JobMakerState
    ) -> None:
        for rval in protocols.ResultState:
            if rval is not protocols.ResultState.ABSENT:
                assert make_simple_job(
                    fresh.clone(
                        execution_state=protocols.ExecutionState.STOPPED, result_state=rval
                    )
                ).done

        for val in protocols.ExecutionState:
            if val is not protocols.ExecutionState.STOPPED:
                assert not make_simple_job(fresh.clone(execution_state=val)).done

    def test_it_knows_only_success_result_state_is_success(
        self, fresh: M_State, make_simple_job: JobMakerState
    ) -> None:
        assert make_simple_job(
            fresh.clone(
                execution_state=protocols.ExecutionState.STOPPED,
                result_state=protocols.ResultState.SUCCESS,
            )
        ).success

        for val in protocols.ResultState:
            if val is not protocols.ResultState.SUCCESS:
                assert not make_simple_job(fresh.clone(result_state=val)).success

    def test_it_knows_only_failure_and_cancel_has_exception(
        self,
        identifier: protocols.WorkflowIdentifier,
        fresh: M_State,
        make_simple_job: JobMakerState,
    ) -> None:
        exc = make_simple_job(
            fresh.clone(
                error=state.SimpleError(serialized="nup"),
                execution_state=protocols.ExecutionState.STOPPED,
                result_state=protocols.ResultState.HANDLED_FAILURE,
            )
        ).exception
        assert isinstance(exc, errors.ComputationErrored)
        assert exc.identifier == identifier.identifier
        assert exc.path == (".j1",)
        assert exc.error == "nup"

        exc = make_simple_job(
            fresh.clone(
                error=state.SimpleError(serialized="nope"),
                result_state=protocols.ResultState.UNHANDLED_FAILURE,
            )
        ).exception
        assert isinstance(exc, errors.ComputationErrored)
        assert exc.identifier == identifier.identifier
        assert exc.path == (".j1",)
        assert exc.error == "nope"

        exc = make_simple_job(
            fresh.clone(
                execution_state=protocols.ExecutionState.STOPPED,
                result_state=protocols.ResultState.CANCELLED,
            )
        ).exception
        assert isinstance(exc, errors.ComputationCancelled)
        assert exc.identifier == identifier.identifier
        assert exc.path == (".j1",)

        for val in protocols.ResultState:
            if val not in (
                protocols.ResultState.HANDLED_FAILURE,
                protocols.ResultState.UNHANDLED_FAILURE,
                protocols.ResultState.CANCELLED,
            ):
                assert make_simple_job(fresh.clone(result_state=val)).exception is None

    def test_it_knows_only_cancelled_result_state_is_cancelled(
        self, fresh: M_State, make_simple_job: JobMakerState
    ) -> None:
        assert make_simple_job(
            fresh.clone(
                execution_state=protocols.ExecutionState.STOPPED,
                result_state=protocols.ResultState.CANCELLED,
            )
        ).cancelled

        for val in protocols.ResultState:
            if val is not protocols.ResultState.CANCELLED:
                assert not make_simple_job(fresh.clone(result_state=val)).cancelled


class TestJobStatus:
    def test_it_can_be_made_without_previous_data(self) -> None:
        job_status = M_JobStatus(name="one")
        assert job_status.name == "one"
        assert job_status.job_before is None
        assert job_status.job_executions == []

    def test_it_can_be_made_with_previous_data(
        self, make_job: JobMaker, identifier: protocols.WorkflowIdentifier
    ) -> None:
        job_before = make_job(state=M_State.fresh())
        job_status = M_JobStatus(name="one", job_before=job_before)
        assert job_status.name == "one"
        assert job_status.job_before is job_before
        assert job_status.job_executions == []
        assert job_status.latest_execution is None

    def test_it_can_clone(
        self, make_job: JobMaker, identifier: protocols.WorkflowIdentifier
    ) -> None:
        job_before = make_job(state=M_State.fresh())
        job_status = M_JobStatus(name="one", job_before=job_before)

        clone = job_status.clone()
        assert clone.name == "one"
        assert clone.job_before is job_before
        assert clone.job_executions == []
        assert clone.latest_execution is None

        exec1 = make_job(state=M_State.fresh())
        job_status.add_execution(exec1)

        assert job_status.job_executions == [exec1]
        assert job_status.latest_execution is exec1

        assert clone.job_executions == []
        assert clone.latest_execution is None

        clone2 = job_status.clone()
        assert clone2.job_executions == [exec1]
        assert clone2.latest_execution is exec1

    def test_it_can_given_executions(
        self, make_job: JobMaker, identifier: protocols.WorkflowIdentifier
    ) -> None:
        job_before = make_job(state=M_State.fresh())
        job_status = M_JobStatus(name="one", job_before=job_before)

        exec1 = make_job(state=M_Results.using(job_before.state).no_change().state)
        exec2 = make_job(state=M_Results.using(exec1.state).cancelled(audit_message="stuff").state)

        assert job_status.job_executions == []
        assert job_status.latest_execution is None

        job_status.add_execution(exec1)
        assert job_status.job_executions == [exec1]
        assert job_status.latest_execution is exec1

        job_status.add_execution(exec2)
        assert job_status.job_executions == [exec1, exec2]
        assert job_status.latest_execution is exec2

    def test_it_can_get_earliest_dates_from_latest_execution(self, make_job: JobMaker) -> None:
        d = datetime.datetime(2000, 1, 1, 1, 1, 1, 1)
        dates = [d + datetime.timedelta(hours=20 * i) for i in range(40)]

        last = make_job(state=M_State.fresh())
        job_status = M_JobStatus(name="one", job_before=last)

        assert job_status.earliest_due_at(delta_base_from=d, must_be_greater_than=d) is None
        assert (
            job_status.earliest_next_schedule_at(delta_base_from=d, must_be_greater_than=d) is None
        )

        job_status.add_execution(
            last := make_job(
                result=M_Results.using(last.state).progressing(
                    audit_message="", due_at=dates[0], schedule_next_latest_at=dates[5]
                )
            )
        )

        assert job_status.earliest_due_at(delta_base_from=d, must_be_greater_than=d) == dates[0]
        assert job_status.earliest_due_at(delta_base_from=d, must_be_greater_than=dates[1]) is None

        assert (
            job_status.earliest_next_schedule_at(delta_base_from=d, must_be_greater_than=d)
            == dates[5]
        )
        assert (
            job_status.earliest_next_schedule_at(delta_base_from=d, must_be_greater_than=dates[6])
            is None
        )

        job_status.add_execution(
            last := make_job(
                result=M_Results.using(last.state).progressing(
                    audit_message="", due_at=dates[10], schedule_next_latest_at=dates[2]
                )
            )
        )

        assert job_status.earliest_due_at(delta_base_from=d, must_be_greater_than=d) == dates[10]
        assert (
            job_status.earliest_next_schedule_at(delta_base_from=d, must_be_greater_than=d)
            == dates[2]
        )

        job_status.add_execution(
            last := make_job(result=M_Results.using(last.state).progressing(audit_message=""))
        )

        assert job_status.earliest_due_at(delta_base_from=d, must_be_greater_than=d) is None
        assert (
            job_status.earliest_next_schedule_at(delta_base_from=d, must_be_greater_than=d) is None
        )

        job_status.add_execution(
            last := make_job(
                result=M_Results.using(last.state).progressing(
                    audit_message="",
                    due_at=datetime.timedelta(hours=20 * 5),
                    schedule_next_latest_at=datetime.timedelta(hours=20 * 7),
                )
            )
        )

        assert job_status.earliest_due_at(
            delta_base_from=d, must_be_greater_than=d
        ) == d + datetime.timedelta(hours=20 * 5)
        assert job_status.earliest_next_schedule_at(
            delta_base_from=d, must_be_greater_than=d
        ) == d + datetime.timedelta(hours=20 * 7)

        assert job_status.earliest_due_at(
            delta_base_from=dates[9], must_be_greater_than=d
        ) == dates[9] + datetime.timedelta(hours=20 * 5)
        assert job_status.earliest_next_schedule_at(
            delta_base_from=dates[15], must_be_greater_than=d
        ) == dates[15] + datetime.timedelta(hours=20 * 7)


class TestJobTracker:
    def test_it_can_add_and_get_job_status_when_none_there(
        self, identifier: protocols.WorkflowIdentifier
    ) -> None:
        job_tracker = M_JobTracker(start_jobs={})
        assert job_tracker.jobs(max_levels=-1) == {}

        job_status = job_tracker.job_status(
            M_JobPath(identifier=identifier, prefix=("one", "two"), job_name="three")
        )
        assert isinstance(job_status, jobs.JobStatus)
        assert job_status.name == "three"
        assert job_status.job_before is None

        assert job_tracker.jobs(max_levels=-1) == {("one", "two", "three"): job_status}
        assert (
            job_tracker.job_status(
                M_JobPath(identifier=identifier, prefix=("one", "two"), job_name="three")
            )
            is job_status
        )
        assert job_tracker.jobs(max_levels=-1) == {("one", "two", "three"): job_status}
        assert job_tracker.jobs(path=("one",), max_levels=-1) == {
            ("one", "two", "three"): job_status
        }

    def test_it_defaults_to_only_getting_one_level_of_jobs(
        self, identifier: protocols.WorkflowIdentifier
    ) -> None:
        job_tracker = M_JobTracker(start_jobs={})
        assert job_tracker.jobs(max_levels=-1) == {}

        job_status1 = job_tracker.job_status(
            M_JobPath(identifier=identifier, prefix=("one", "two"), job_name="three")
        )
        job_status2 = job_tracker.job_status(
            M_JobPath(identifier=identifier, prefix=("one", "two"), job_name="four")
        )
        job_status3 = job_tracker.job_status(
            M_JobPath(identifier=identifier, prefix=("five",), job_name="six")
        )
        job_status4 = job_tracker.job_status(
            M_JobPath(identifier=identifier, prefix=("five", "six"), job_name="seven")
        )
        job_status5 = job_tracker.job_status(
            M_JobPath(identifier=identifier, prefix=("five", "nine"), job_name="ten")
        )

        assert job_tracker.jobs() == {}
        assert job_tracker.jobs(path=("one",)) == {}
        assert job_tracker.jobs(path=("one", "two")) == {
            ("one", "two", "three"): job_status1,
            ("one", "two", "four"): job_status2,
        }
        assert job_tracker.jobs(path=("one",), max_levels=2) == {
            ("one", "two", "three"): job_status1,
            ("one", "two", "four"): job_status2,
        }
        assert job_tracker.jobs(path=("five", "six")) == {("five", "six", "seven"): job_status4}

        assert job_tracker.jobs(path=("five",)) == {("five", "six"): job_status3}
        assert job_tracker.jobs(path=("five",), max_levels=2) == {
            ("five", "six"): job_status3,
            ("five", "six", "seven"): job_status4,
            ("five", "nine", "ten"): job_status5,
        }

    def test_it_copies_existing_job_status_when_accessed(
        self, identifier: protocols.WorkflowIdentifier, make_job: JobMaker
    ) -> None:
        original_job = make_job(state=M_State.fresh())
        job_status = M_JobStatus(name="blah", job_before=original_job)

        job_tracker = M_JobTracker(start_jobs={("blah",): job_status})
        assert job_tracker.jobs(max_levels=-1) == {("blah",): job_status}

        job_path = M_JobPath(identifier=identifier, prefix=(), job_name="blah")
        status = job_tracker.job_status(job_path)

        assert status is not job_status
        assert status.job_before == job_status.job_before
        assert status.name == "blah"

        job = make_job(result=M_Results.using(original_job.state).success(audit_message=""))
        status.add_execution(job)

        assert job_status.job_executions == []
        assert status.job_executions == [job]

    class TestGettingDates:
        @attrs.frozen
        class Dates:
            earliest_due_at: datetime.datetime | None = None
            earliest_next_schedule_at: datetime.datetime | None = None

        def make_job_tracker(
            self,
            *,
            start_jobs: dict[protocols.Path, Dates],
            added_jobs: dict[protocols.Path, Dates],
            delta_base_from: datetime.datetime,
            must_be_greater_than: datetime.datetime,
        ) -> jobs.JobTracker[B_JobStatus]:
            @attrs.frozen
            class JobStatus(jobs.JobStatusBase[M_State]):
                _earliest_due_at: datetime.datetime | None = None
                _earliest_next_schedule_at: datetime.datetime | None = None

                name: str = "irrelevant"
                job_before: jobs._P_Job[M_State] | None = None
                job_executions: Sequence[jobs._P_Job[M_State]] = attrs.field(factory=list)

                def clone(self) -> Self:
                    return self

                def add_execution(self, job: jobs._P_Job[protocols.T_CO_Data]) -> None:
                    pass

                @property
                def latest_execution(
                    self,
                ) -> jobs._P_Job[M_State] | None:
                    return None

                def earliest_due_at(
                    self,
                    *,
                    delta_base_from: datetime.datetime,
                    must_be_greater_than: datetime.datetime,
                ) -> datetime.datetime | None:
                    assert delta_base_from is delta_base_from
                    assert must_be_greater_than is must_be_greater_than
                    return self._earliest_due_at

                def earliest_next_schedule_at(
                    self,
                    *,
                    delta_base_from: datetime.datetime,
                    must_be_greater_than: datetime.datetime,
                ) -> datetime.datetime | None:
                    assert delta_base_from is delta_base_from
                    assert must_be_greater_than is must_be_greater_than
                    return self._earliest_next_schedule_at

            _JS: protocols.P_JobStatus = cast(JobStatus, None)

            @attrs.frozen
            class JobTracker(jobs.JobTracker[JobStatus]):
                _start_jobs: Mapping[protocols.Path, JobStatus] = attrs.field(
                    default={
                        k: JobStatus(
                            earliest_due_at=dates.earliest_due_at,
                            earliest_next_schedule_at=dates.earliest_next_schedule_at,
                        )
                        for k, dates in start_jobs.items()
                    }
                )
                _added_jobs: MutableMapping[protocols.Path, JobStatus] = attrs.field(
                    default={
                        k: JobStatus(
                            earliest_due_at=dates.earliest_due_at,
                            earliest_next_schedule_at=dates.earliest_next_schedule_at,
                        )
                        for k, dates in added_jobs.items()
                    }
                )

                def _fresh_job_status(self, name: str) -> JobStatus:
                    return JobStatus()

            return JobTracker()

        @pytest.fixture
        def date_relative_to(self) -> datetime.datetime:
            return datetime.datetime(2000, 1, 1, 1, 1, 1, 1)

        @pytest.fixture
        def dates(self, date_relative_to: datetime.datetime) -> list[datetime.datetime]:
            return [
                date_relative_to + datetime.timedelta(hours=-2000 + (250 * i)) for i in range(100)
            ]

        def test_it_can_get_earliest_dates_as_none_when_is_none(
            self, dates: list[datetime.datetime], date_relative_to: datetime.datetime
        ) -> None:
            must_be_greater_than = dates[1]

            job_tracker = self.make_job_tracker(
                start_jobs={(): self.Dates(), ("one",): self.Dates()},
                added_jobs={
                    ("one", "two"): self.Dates(),
                    ("one", "two", "three"): self.Dates(),
                    ("one", "four"): self.Dates(),
                    ("one", "five"): self.Dates(),
                },
                delta_base_from=date_relative_to,
                must_be_greater_than=must_be_greater_than,
            )

            assert (
                job_tracker.earliest_due_at(
                    delta_base_from=date_relative_to, must_be_greater_than=must_be_greater_than
                )
                is None
            )
            assert (
                job_tracker.earliest_next_schedule_at(
                    delta_base_from=date_relative_to, must_be_greater_than=must_be_greater_than
                )
                is None
            )

        def test_it_can_get_earliest_dates_when_they_are_in_start_jobs(
            self, dates: list[datetime.datetime], date_relative_to: datetime.datetime
        ) -> None:
            must_be_greater_than = dates[1]

            job_tracker = self.make_job_tracker(
                start_jobs={
                    (): self.Dates(earliest_due_at=dates[4], earliest_next_schedule_at=dates[10]),
                    ("one",): self.Dates(
                        earliest_due_at=dates[6], earliest_next_schedule_at=dates[9]
                    ),
                },
                added_jobs={
                    ("one", "two"): self.Dates(
                        earliest_due_at=dates[10], earliest_next_schedule_at=dates[11]
                    ),
                    ("one", "five"): self.Dates(),
                },
                delta_base_from=date_relative_to,
                must_be_greater_than=must_be_greater_than,
            )

            assert (
                job_tracker.earliest_due_at(
                    delta_base_from=date_relative_to, must_be_greater_than=must_be_greater_than
                )
                is dates[4]
            )
            assert (
                job_tracker.earliest_next_schedule_at(
                    delta_base_from=date_relative_to, must_be_greater_than=must_be_greater_than
                )
                is dates[9]
            )

        def test_it_can_get_earliest_dates_when_they_are_in_added_jobs(
            self, dates: list[datetime.datetime], date_relative_to: datetime.datetime
        ) -> None:
            must_be_greater_than = dates[1]

            job_tracker = self.make_job_tracker(
                start_jobs={
                    (): self.Dates(earliest_due_at=dates[4], earliest_next_schedule_at=dates[10]),
                    ("one",): self.Dates(
                        earliest_due_at=dates[6], earliest_next_schedule_at=dates[9]
                    ),
                },
                added_jobs={
                    ("one", "two"): self.Dates(
                        earliest_due_at=dates[2], earliest_next_schedule_at=dates[11]
                    ),
                    ("one", "five"): self.Dates(),
                    ("one", "six"): self.Dates(
                        earliest_due_at=dates[3], earliest_next_schedule_at=dates[8]
                    ),
                },
                delta_base_from=date_relative_to,
                must_be_greater_than=must_be_greater_than,
            )

            assert (
                job_tracker.earliest_due_at(
                    delta_base_from=date_relative_to, must_be_greater_than=must_be_greater_than
                )
                is dates[2]
            )
            assert (
                job_tracker.earliest_next_schedule_at(
                    delta_base_from=date_relative_to, must_be_greater_than=must_be_greater_than
                )
                is dates[8]
            )

        def test_it_can_get_earliest_dates_when_they_are_in_added_jobs_that_override_earlier_in_start_jobs(
            self, dates: list[datetime.datetime], date_relative_to: datetime.datetime
        ) -> None:
            must_be_greater_than = dates[1]

            job_tracker = self.make_job_tracker(
                start_jobs={
                    (): self.Dates(earliest_due_at=dates[20], earliest_next_schedule_at=dates[30]),
                    ("one",): self.Dates(
                        earliest_due_at=dates[40], earliest_next_schedule_at=dates[50]
                    ),
                },
                added_jobs={
                    (): self.Dates(earliest_due_at=None, earliest_next_schedule_at=None),
                    ("one", "five"): self.Dates(),
                    ("one", "six"): self.Dates(
                        earliest_due_at=dates[30], earliest_next_schedule_at=dates[60]
                    ),
                },
                delta_base_from=date_relative_to,
                must_be_greater_than=must_be_greater_than,
            )

            assert (
                job_tracker.earliest_due_at(
                    delta_base_from=date_relative_to, must_be_greater_than=must_be_greater_than
                )
                is dates[30]
            )
            assert (
                job_tracker.earliest_next_schedule_at(
                    delta_base_from=date_relative_to, must_be_greater_than=must_be_greater_than
                )
                is dates[50]
            )
