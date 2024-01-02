import datetime
from typing import assert_never

import attrs
import pytest
import time_machine

from computations import errors, protocols, state

M_State = state.State
M_Result = state.Result[M_State]
M_StoredInfo = state.StoredInfo[M_State]
M_Results = state.Results[M_State]
M_ComputationState = state.ComputationState[M_State]


@pytest.fixture
def identifier() -> state.WorkflowIdentifier:
    return state.WorkflowIdentifier(identifier="__IDENTIFIER__")


class NameElements:
    valid_names: tuple[object, ...] = (
        pytest.param("one", id="only_alpha"),
        pytest.param("1", id="only_numberic"),
        pytest.param("one_2", id="alphanumberic_and_underscore"),
        pytest.param("one_two", id="alpha_and_underscore"),
        pytest.param("one_two-three", id="with_underscore_and_dash"),
        pytest.param("three-four-five", id="with_dashes"),
    )

    invalid_names: tuple[object, ...] = (
        pytest.param("", id="empty_string"),
        pytest.param("hi\tthere", id="tab"),
        pytest.param("hi\nthere", id="newline"),
        pytest.param("  ", id="only_spaces"),
        pytest.param(".one", id="contains_dot"),
        pytest.param("one.two", id="dot_isnt_at_start_of_string"),
        pytest.param("three four five", id="contains_space"),
    )


class TestJobPath:
    PathClass = state.JobPath

    @pytest.mark.parametrize("valid_name", NameElements.valid_names)
    def test_it_can_be_made_with_a_valid_name(
        self, identifier: protocols.WorkflowIdentifier, valid_name: str
    ) -> None:
        path = self.PathClass(identifier=identifier, prefix=("asdf",), job_name=valid_name)
        assert path.identifier is identifier
        assert path.prefix == ("asdf",)
        assert path.job_name == valid_name
        assert path.path == ("asdf", valid_name)

    @pytest.mark.parametrize("invalid_name", NameElements.invalid_names)
    def test_it_complains_if_name_is_invalid(
        self, identifier: protocols.WorkflowIdentifier, invalid_name: str
    ) -> None:
        with pytest.raises(errors.InvalidJobName):
            self.PathClass(identifier=identifier, prefix=(), job_name=invalid_name)


class TestExternalInputPath:
    PathClass = state.ExternalInputPath

    @pytest.mark.parametrize("valid_name", NameElements.valid_names)
    def test_it_can_be_made_with_a_valid_name(
        self, identifier: protocols.WorkflowIdentifier, valid_name: str
    ) -> None:
        path = self.PathClass(identifier=identifier, external_input_name=valid_name)
        assert path.identifier is identifier
        assert path.external_input_name == valid_name

    @pytest.mark.parametrize("invalid_name", NameElements.invalid_names)
    def test_it_complains_if_name_is_invalid(
        self, identifier: protocols.WorkflowIdentifier, invalid_name: str
    ) -> None:
        with pytest.raises(errors.InvalidExternalInputName):
            self.PathClass(identifier=identifier, external_input_name=invalid_name)


class TestState:
    def test_it_can_be_constructed(self) -> None:
        date1 = datetime.datetime(2000, 1, 1, 1, 1, 1)
        date2 = datetime.datetime(2003, 3, 3, 3, 3, 3)
        date3 = datetime.datetime(2005, 5, 5, 5, 5, 5)

        instance = M_State(
            created_at=date1,
            due_at=date2,
            error=state.SimpleError("Computer says no"),
            execution_state=protocols.ExecutionState.STOPPED,
            result_state=protocols.ResultState.HANDLED_FAILURE,
            schedule_next_latest_at=date3,
        )

        assert isinstance(instance, M_State)
        assert instance.error == state.SimpleError("Computer says no")
        assert instance.execution_state is protocols.ExecutionState.STOPPED
        assert instance.result_state is protocols.ResultState.HANDLED_FAILURE
        assert instance.created_at == date1
        assert instance.due_at == date2
        assert instance.schedule_next_latest_at == date3

    def test_it_can_make_a_fresh_instance(
        self, time_machine: time_machine.TimeMachineFixture
    ) -> None:
        date1 = datetime.datetime(2004, 4, 4, 4, 4, 4)
        time_machine.move_to(date1, tick=False)

        instance = M_State.fresh()

        assert isinstance(instance, M_State)
        assert instance.error is None
        assert instance.execution_state is protocols.ExecutionState.PENDING
        assert instance.result_state is protocols.ResultState.ABSENT
        assert instance.created_at == date1
        assert instance.due_at is None
        assert instance.schedule_next_latest_at is None

    def test_it_can_clone(self, time_machine: time_machine.TimeMachineFixture) -> None:
        date1 = datetime.datetime(2000, 1, 1, 1, 1, 1)
        date2 = datetime.datetime(2040, 4, 4, 4, 4, 4)
        date3 = datetime.datetime(2060, 6, 6, 6, 6, 6)
        time_machine.move_to(date1, tick=False)

        instance = M_State.fresh()
        assert isinstance(instance, M_State)
        assert instance.error is None
        assert instance.execution_state is protocols.ExecutionState.PENDING
        assert instance.result_state is protocols.ResultState.ABSENT
        assert instance.created_at == date1
        assert instance.due_at is None

        error = state.ErrorRaw(format_code="test", format_version=1, serialized="nup")
        clone = instance.clone(error=error)
        assert clone is not instance
        assert isinstance(clone, M_State)
        assert clone.error is error
        assert clone.execution_state is protocols.ExecutionState.PENDING
        assert clone.result_state is protocols.ResultState.ABSENT
        assert clone.created_at == date1
        assert clone.due_at is None
        assert clone.schedule_next_latest_at is None

        clone2 = clone.clone(result_state=protocols.ResultState.HANDLED_FAILURE)
        assert clone2 is not clone
        assert clone2 is not instance
        clone = clone2
        assert isinstance(clone, M_State)
        assert clone.error is error
        assert clone.execution_state is protocols.ExecutionState.PENDING
        assert clone.result_state is protocols.ResultState.HANDLED_FAILURE
        assert clone.created_at == date1
        assert clone.due_at is None
        assert clone.schedule_next_latest_at is None

        clone2 = clone.clone(execution_state=protocols.ExecutionState.STOPPED)
        assert clone2 is not clone
        assert clone2 is not instance
        clone = clone2
        assert isinstance(clone, M_State)
        assert clone.error is error
        assert clone.execution_state is protocols.ExecutionState.STOPPED
        assert clone.result_state is protocols.ResultState.HANDLED_FAILURE
        assert clone.created_at == date1
        assert clone.due_at is None
        assert clone.schedule_next_latest_at is None

        clone2 = clone.clone(
            error=None,
            execution_state=protocols.ExecutionState.PENDING,
            result_state=protocols.ResultState.ABSENT,
            due_at=date2,
            schedule_next_latest_at=date3,
        )
        assert clone2 is not clone
        assert clone2 is not instance
        clone = clone2
        assert isinstance(clone, M_State)
        assert clone.error is None
        assert clone.execution_state is protocols.ExecutionState.PENDING
        assert clone.result_state is protocols.ResultState.ABSENT
        assert clone.created_at == date1
        assert clone.due_at == date2
        assert clone.schedule_next_latest_at == date3


class TestComputationState:
    def test_it_can_be_constructed(self, identifier: protocols.WorkflowIdentifier) -> None:
        original = M_State.fresh()
        computation_state = M_ComputationState(
            original_state=original, error=None, identifier=identifier, path=("j1",)
        )
        assert computation_state._original_state is original
        assert computation_state.identifier is identifier
        assert computation_state.path == ("j1",)
        assert computation_state.error is None
        assert computation_state.execution_state is original.execution_state
        assert computation_state.result_state is original.result_state
        assert computation_state.due_at is original.due_at

        error = state.SimpleError(serialized="asdf")
        computation_state_with_error = M_ComputationState(
            original_state=original, error=error, identifier=identifier, path=("j1",)
        )
        assert computation_state_with_error.error is error

    def test_it_can_produce_logging_context(
        self, identifier: protocols.WorkflowIdentifier
    ) -> None:
        original = M_State.fresh()
        computation_state = M_ComputationState(
            original_state=original, error=None, identifier=identifier, path=("j1", "blah")
        )
        assert computation_state.logging_context == {
            "workflow_identifier": identifier.identifier,
            "computation_path": "j1.blah",
        }

    def test_it_has_no_exception_if_no_error_result_state_even_if_error_registerd(
        self, identifier: protocols.WorkflowIdentifier
    ) -> None:
        result_states: list[protocols.ResultState] = []
        for result_state in protocols.ResultState:
            if (
                result_state is protocols.ResultState.ABSENT
                or result_state is protocols.ResultState.SUCCESS
            ):
                result_states.append(result_state)
            elif (
                result_state is protocols.ResultState.CANCELLED
                or result_state is protocols.ResultState.HANDLED_FAILURE
                or result_state is protocols.ResultState.UNHANDLED_FAILURE
            ):
                pass
            else:
                assert_never(result_state)

        for result_state in result_states:
            original = M_State.fresh().clone(result_state=result_state)
            computation_state = M_ComputationState(
                original_state=original,
                error=state.SimpleError(serialized="nup"),
                identifier=identifier,
                path=("j1",),
            )

            assert computation_state.exception is None

    def test_it_turns_registered_error_into_exception_for_error_states(
        self, identifier: protocols.WorkflowIdentifier
    ) -> None:
        result_states: list[protocols.ResultState] = []
        for result_state in protocols.ResultState:
            if (
                result_state is protocols.ResultState.ABSENT
                or result_state is protocols.ResultState.SUCCESS
            ):
                pass
            elif (
                result_state is protocols.ResultState.CANCELLED
                or result_state is protocols.ResultState.HANDLED_FAILURE
                or result_state is protocols.ResultState.UNHANDLED_FAILURE
            ):
                result_states.append(result_state)
            else:
                assert_never(result_state)

        @attrs.frozen
        class ComputerSaysNo(Exception):
            message: str

        @attrs.frozen
        class ErrorCode:
            message: str

            format_code: str = "error_code"
            format_version: int = 1
            serialized: str = ""

            def as_exception(
                self, identifier: protocols.WorkflowIdentifier, path: protocols.Path
            ) -> Exception:
                return ComputerSaysNo(message=self.message)

        for result_state in result_states:
            original = M_State.fresh().clone(result_state=result_state)
            computation_state = M_ComputationState(
                original_state=original,
                error=ErrorCode(message=str(result_state)),
                identifier=identifier,
                path=("j1",),
            )

            exc = computation_state.exception
            assert isinstance(exc, ComputerSaysNo)
            assert exc.message == str(result_state)

    def test_it_still_has_error_for_cancelled_with_no_provided_error(
        self, identifier: protocols.WorkflowIdentifier
    ) -> None:
        original = M_State.fresh().clone(result_state=protocols.ResultState.CANCELLED)
        computation_state = M_ComputationState(
            original_state=original, error=None, identifier=identifier, path=("j1",)
        )

        exc = computation_state.exception
        assert isinstance(exc, errors.ComputationCancelled)
        assert exc.identifier == identifier.identifier
        assert exc.path == ("j1",)

    def test_it_still_has_error_for_failures_with_no_provided_error(
        self, identifier: protocols.WorkflowIdentifier
    ) -> None:
        for result_state in (
            protocols.ResultState.HANDLED_FAILURE,
            protocols.ResultState.UNHANDLED_FAILURE,
        ):
            original = M_State.fresh().clone(result_state=result_state)
            computation_state = M_ComputationState(
                original_state=original, error=None, identifier=identifier, path=("j1",)
            )

            exc = computation_state.exception
            assert isinstance(exc, errors.ComputationErrored)
            assert exc.identifier == identifier.identifier
            assert exc.path == ("j1",)
            assert exc.error == repr(result_state)

    def test_it_can_make_a_job_path(self, identifier: protocols.WorkflowIdentifier) -> None:
        original = M_State.fresh()
        computation_state = M_ComputationState(
            original_state=original, identifier=identifier, path=("path", "one"), error=None
        )

        assert computation_state.job_path("three") == state.JobPath(
            identifier=identifier, prefix=("path", "one"), job_name="three"
        )
        assert computation_state.job_path("four") == state.JobPath(
            identifier=identifier, prefix=("path", "one"), job_name="four"
        )

        with pytest.raises(errors.InvalidJobName):
            computation_state.job_path("invalid.with.dots")

    def test_it_can_make_an_external_input_path(
        self, identifier: protocols.WorkflowIdentifier
    ) -> None:
        original = M_State.fresh()
        computation_state = M_ComputationState(
            original_state=original, identifier=identifier, path=("path", "one"), error=None
        )

        assert computation_state.external_input_path("four") == state.ExternalInputPath(
            identifier=identifier, external_input_name="four"
        )
        assert computation_state.external_input_path("five") == state.ExternalInputPath(
            identifier=identifier, external_input_name="five"
        )

        with pytest.raises(errors.InvalidExternalInputName):
            computation_state.external_input_path("invalid.with.dots")


class TestSimpleError:
    def test_it_serializes_str_of_exception(self) -> None:
        class MyException(Exception):
            def __str__(self) -> str:
                return "Computer says no"

        error = state.SimpleError.serialize(MyException())
        assert error.format_code == "simple"
        assert error.format_version == 1
        assert error.serialized == "Computer says no"

    def test_it_creates_ComputationErrored(self, identifier: state.WorkflowIdentifier) -> None:
        error = state.SimpleError(serialized="Computer says no")
        exc = error.as_exception(identifier=identifier, path=("job0",))
        assert isinstance(exc, errors.ComputationErrored)
        assert exc.identifier == identifier.identifier
        assert exc.path == ("job0",)
        assert exc.error == "Computer says no"


class TestResults:
    @pytest.fixture
    def fresh(self) -> M_State:
        return M_State.fresh()

    @pytest.fixture
    def exception_serializer(self) -> protocols.ExceptionSerializer:
        class Serializer:
            def serialize_exception(self, exc: Exception) -> protocols.ErrorRaw:
                return state.ErrorRaw(format_code="in_test", format_version=1, serialized=str(exc))

        return Serializer()

    @pytest.fixture
    def results(self, fresh: M_State) -> M_Results:
        return M_Results(original_state=fresh)

    def test_it_only_changes_errorcode_and_result_and_execution(
        self, exception_serializer: protocols.ExceptionSerializer
    ) -> None:
        fresh = M_State.fresh()
        results = M_Results(original_state=fresh)

        audit_message = "Change is good for you"

        new_results = [
            results.no_change(audit_message=audit_message),
            results.pending(audit_message=audit_message),
            results.progressing(audit_message=audit_message),
            results.success(audit_message=audit_message),
            results.paused(audit_message=audit_message),
            results.cancelled(audit_message=audit_message),
            results.cancelling(audit_message=audit_message),
            results.handled_failure(
                audit_message=audit_message, error=state.SimpleError(serialized="yeah nah")
            ),
            results.unhandled_failure(
                audit_message=audit_message,
                exc=Exception("yeah nah"),
                exception_serializer=exception_serializer,
            ),
        ]

        for result in new_results:
            assert isinstance(result, state.Result)
            assert result.audit_message == audit_message

            assert result.state is not fresh
            assert result.state.due_at == fresh.due_at
            assert result.state.created_at == fresh.created_at

    def test_it_has_for_no_changes(self, fresh: M_State, results: M_Results) -> None:
        result = results.no_change()
        assert result.audit_message == ""
        assert result.schedule_next_latest_at is protocols._NotGiven
        assert attrs.asdict(result.state) == attrs.asdict(fresh)

    def test_it_has_for_no_changes_that_carries_over_dates(
        self, fresh: M_State, results: M_Results, identifier: protocols.WorkflowIdentifier
    ) -> None:
        date1 = datetime.datetime(2000, 1, 1, 1, 1, 1)
        date2 = datetime.datetime(2000, 2, 2, 2, 2, 2)
        state = fresh.clone(due_at=date1, schedule_next_latest_at=date2)
        computation_state = M_ComputationState(
            original_state=state, identifier=identifier, path=(), error=None
        )

        result = M_Results.using(computation_state).no_change()
        assert result.audit_message == ""
        assert result.due_at is date1
        assert result.schedule_next_latest_at is date2
        assert attrs.asdict(result.state) == attrs.asdict(state)

        date3 = datetime.datetime(2000, 3, 3, 3, 3, 3)
        date4 = datetime.datetime(2000, 4, 4, 4, 4, 4)
        result = M_Results.using(computation_state).no_change(
            due_at=date3, schedule_next_latest_at=date4
        )
        assert result.audit_message == ""
        assert result.due_at is date3
        assert result.schedule_next_latest_at is date4
        assert attrs.asdict(result.state) == attrs.asdict(state)

    def test_it_has_for_pending(self, fresh: M_State, results: M_Results) -> None:
        result = results.pending(audit_message="p")
        assert result.audit_message == "p"
        assert result.schedule_next_latest_at is protocols._NotGiven
        assert result.state.error is None
        assert result.state.execution_state is protocols.ExecutionState.PENDING
        assert result.state.result_state is protocols.ResultState.ABSENT

    def test_it_has_for_progressing(self, fresh: M_State, results: M_Results) -> None:
        result = results.progressing(audit_message="p")
        assert result.audit_message == "p"
        assert result.schedule_next_latest_at is protocols._NotGiven
        assert result.state.error is None
        assert result.state.execution_state is protocols.ExecutionState.PROGRESSING
        assert result.state.result_state is protocols.ResultState.ABSENT

    def test_it_has_for_success(self, fresh: M_State, results: M_Results) -> None:
        result = results.success(audit_message="p")
        assert result.audit_message == "p"
        assert result.schedule_next_latest_at is protocols._NotGiven
        assert result.state.error is None
        assert result.state.execution_state is protocols.ExecutionState.STOPPED
        assert result.state.result_state is protocols.ResultState.SUCCESS

    def test_it_has_for_paused(self, fresh: M_State, results: M_Results) -> None:
        result = results.paused(audit_message="p")
        assert result.audit_message == "p"
        assert result.schedule_next_latest_at is protocols._NotGiven
        assert result.state.error is None
        assert result.state.execution_state is protocols.ExecutionState.PAUSED
        assert result.state.result_state is protocols.ResultState.ABSENT

    def test_it_has_for_cancelled(self, fresh: M_State, results: M_Results) -> None:
        result = results.cancelled(audit_message="p")
        assert result.audit_message == "p"
        assert result.schedule_next_latest_at is protocols._NotGiven
        assert result.state.error is None
        assert result.state.execution_state is protocols.ExecutionState.STOPPED
        assert result.state.result_state is protocols.ResultState.CANCELLED

    def test_it_has_for_cancelling(self, fresh: M_State, results: M_Results) -> None:
        result = results.cancelling(audit_message="p")
        assert result.audit_message == "p"
        assert result.schedule_next_latest_at is protocols._NotGiven
        assert result.state.error is None
        assert result.state.execution_state is protocols.ExecutionState.CANCELLING
        assert result.state.result_state is protocols.ResultState.ABSENT

    def test_it_has_for_handled_failure(self, fresh: M_State, results: M_Results) -> None:
        result = results.handled_failure(
            audit_message="p", error=state.SimpleError(serialized="Computer says no")
        )
        assert result.audit_message == "p"
        assert result.schedule_next_latest_at is protocols._NotGiven
        assert result.state.error is not None
        assert result.state.error.format_code == "simple"
        assert result.state.error.format_version == 1
        assert result.state.error.serialized == "Computer says no"
        assert result.state.execution_state is protocols.ExecutionState.STOPPED
        assert result.state.result_state is protocols.ResultState.HANDLED_FAILURE

    def test_it_has_for_unhandled_failure(
        self,
        fresh: M_State,
        results: M_Results,
        exception_serializer: protocols.ExceptionSerializer,
    ) -> None:
        result = results.unhandled_failure(
            audit_message="p",
            exc=Exception("Computer says no"),
            exception_serializer=exception_serializer,
        )
        assert result.audit_message == "p"
        assert result.schedule_next_latest_at is protocols._NotGiven
        assert result.state.error is not None
        assert result.state.error.format_code == "in_test"
        assert result.state.error.format_version == 1
        assert result.state.error.serialized == "Computer says no"
        assert result.state.execution_state is protocols.ExecutionState.STOPPED
        assert result.state.result_state is protocols.ResultState.UNHANDLED_FAILURE

    def test_it_can_pass_in_schedule_next_latest_at(
        self,
        fresh: M_State,
        results: M_Results,
        exception_serializer: protocols.ExceptionSerializer,
    ) -> None:
        as_date = datetime.datetime(2000, 1, 1, 1, 1, 1)
        as_timedelta = datetime.timedelta(hours=9001)
        choices: list[datetime.datetime | datetime.timedelta | None] = [
            as_date,
            as_timedelta,
            None,
        ]
        for schedule_next_latest_at in choices:
            result = results.no_change(schedule_next_latest_at=schedule_next_latest_at)
            assert result.schedule_next_latest_at is schedule_next_latest_at

            result = results.pending(
                audit_message="p", schedule_next_latest_at=schedule_next_latest_at
            )
            assert result.schedule_next_latest_at is schedule_next_latest_at

            result = results.progressing(
                audit_message="p", schedule_next_latest_at=schedule_next_latest_at
            )
            assert result.schedule_next_latest_at is schedule_next_latest_at

            result = results.success(
                audit_message="p", schedule_next_latest_at=schedule_next_latest_at
            )
            assert result.schedule_next_latest_at is schedule_next_latest_at

            result = results.paused(
                audit_message="p", schedule_next_latest_at=schedule_next_latest_at
            )
            assert result.schedule_next_latest_at is schedule_next_latest_at

            result = results.cancelled(
                audit_message="p", schedule_next_latest_at=schedule_next_latest_at
            )
            assert result.schedule_next_latest_at is schedule_next_latest_at

            result = results.cancelling(
                audit_message="p", schedule_next_latest_at=schedule_next_latest_at
            )
            assert result.schedule_next_latest_at is schedule_next_latest_at

            result = results.handled_failure(
                audit_message="p",
                error=state.SimpleError(serialized="Computer says no"),
                schedule_next_latest_at=schedule_next_latest_at,
            )
            assert result.schedule_next_latest_at is schedule_next_latest_at

            result = results.unhandled_failure(
                audit_message="p",
                exc=Exception("Computer says no"),
                exception_serializer=exception_serializer,
                schedule_next_latest_at=schedule_next_latest_at,
            )
            assert result.schedule_next_latest_at is schedule_next_latest_at

    def test_it_can_pass_in_due_at(
        self,
        fresh: M_State,
        results: M_Results,
        exception_serializer: protocols.ExceptionSerializer,
    ) -> None:
        as_date = datetime.datetime(2000, 1, 1, 1, 1, 1)
        as_timedelta = datetime.timedelta(hours=9001)
        choices: list[datetime.datetime | datetime.timedelta | None] = [
            as_date,
            as_timedelta,
            None,
        ]
        for schedule_next_latest_at in choices:
            result = results.no_change(schedule_next_latest_at=schedule_next_latest_at)
            assert result.schedule_next_latest_at is schedule_next_latest_at

            result = results.pending(
                audit_message="p", schedule_next_latest_at=schedule_next_latest_at
            )
            assert result.schedule_next_latest_at is schedule_next_latest_at

            result = results.progressing(
                audit_message="p", schedule_next_latest_at=schedule_next_latest_at
            )
            assert result.schedule_next_latest_at is schedule_next_latest_at

            result = results.success(
                audit_message="p", schedule_next_latest_at=schedule_next_latest_at
            )
            assert result.schedule_next_latest_at is schedule_next_latest_at

            result = results.paused(
                audit_message="p", schedule_next_latest_at=schedule_next_latest_at
            )
            assert result.schedule_next_latest_at is schedule_next_latest_at

            result = results.cancelled(
                audit_message="p", schedule_next_latest_at=schedule_next_latest_at
            )
            assert result.schedule_next_latest_at is schedule_next_latest_at

            result = results.cancelling(
                audit_message="p", schedule_next_latest_at=schedule_next_latest_at
            )
            assert result.schedule_next_latest_at is schedule_next_latest_at

            result = results.handled_failure(
                audit_message="p",
                error=state.SimpleError(serialized="Computer says no"),
                schedule_next_latest_at=schedule_next_latest_at,
            )
            assert result.schedule_next_latest_at is schedule_next_latest_at

            result = results.unhandled_failure(
                audit_message="p",
                exc=Exception("Computer says no"),
                exception_serializer=exception_serializer,
                schedule_next_latest_at=schedule_next_latest_at,
            )
            assert result.schedule_next_latest_at is schedule_next_latest_at

    def test_progression(
        self, fresh: M_State, exception_serializer: protocols.ExceptionSerializer
    ) -> None:
        results = M_Results(original_state=fresh)
        result = results.no_change()
        assert result.state.error is None
        assert result.state.execution_state is protocols.ExecutionState.PENDING
        assert result.state.result_state is protocols.ResultState.ABSENT

        results = M_Results(original_state=result.state)
        result = results.handled_failure(
            audit_message="p", error=state.SimpleError(serialized="nope")
        )
        assert result.state.error is not None
        assert result.state.error.format_code == "simple"
        assert result.state.error.format_version == 1
        assert result.state.error.serialized == "nope"
        assert result.state.execution_state is protocols.ExecutionState.STOPPED
        assert result.state.result_state is protocols.ResultState.HANDLED_FAILURE

        results = M_Results(original_state=result.state)
        result = results.progressing(audit_message="p")
        assert result.state.error is None
        assert result.state.execution_state is protocols.ExecutionState.PROGRESSING
        assert result.state.result_state is protocols.ResultState.ABSENT

        results = M_Results(original_state=result.state)
        result = results.unhandled_failure(
            audit_message="p",
            exc=Exception("nope more"),
            exception_serializer=exception_serializer,
        )
        assert result.state.error is not None
        assert result.state.error.format_code == "in_test"
        assert result.state.error.format_version == 1
        assert result.state.error.serialized == "nope more"
        assert result.state.execution_state is protocols.ExecutionState.STOPPED
        assert result.state.result_state is protocols.ResultState.UNHANDLED_FAILURE

        results = M_Results(original_state=result.state)
        result = results.pending(audit_message="p")
        assert result.state.error is None
        assert result.state.execution_state is protocols.ExecutionState.PENDING
        assert result.state.result_state is protocols.ResultState.ABSENT

        results = M_Results(original_state=result.state)
        result = results.unhandled_failure(
            audit_message="p",
            exc=Exception("nope more"),
            exception_serializer=exception_serializer,
        )
        assert result.state.error is not None
        assert result.state.execution_state is protocols.ExecutionState.STOPPED
        assert result.state.result_state is protocols.ResultState.UNHANDLED_FAILURE

        results = M_Results(original_state=result.state)
        result = results.cancelling(audit_message="p")
        assert result.state.error is None
        assert result.state.execution_state is protocols.ExecutionState.CANCELLING
        assert result.state.result_state is protocols.ResultState.ABSENT

        results = M_Results(original_state=result.state)
        result = results.unhandled_failure(
            audit_message="p",
            exc=Exception("nope more"),
            exception_serializer=exception_serializer,
        )
        assert result.state.error is not None
        assert result.state.execution_state is protocols.ExecutionState.STOPPED
        assert result.state.result_state is protocols.ResultState.UNHANDLED_FAILURE

        results = M_Results(original_state=result.state)
        result = results.cancelled(audit_message="p")
        assert result.state.error is None
        assert result.state.execution_state is protocols.ExecutionState.STOPPED
        assert result.state.result_state is protocols.ResultState.CANCELLED


class TestStoredInfo:
    def test_it_can_merge_in_state_from_a_result(
        self, identifier: protocols.WorkflowIdentifier
    ) -> None:
        date1 = datetime.datetime(2000, 1, 1, 1, 1, 1)
        date2 = datetime.datetime(2200, 2, 2, 2, 2, 2)
        with time_machine.travel(date1, tick=False):
            fresh = M_State.fresh().clone(due_at=date2)

        date3 = datetime.datetime(2900, 9, 9, 9, 9, 9)
        date4 = datetime.datetime(2800, 8, 8, 8, 8, 8)
        with time_machine.travel(date3, tick=False):
            result = M_Results.using(
                M_ComputationState(
                    original_state=M_State.fresh(),
                    error=None,
                    identifier=identifier,
                    path=("j1",),
                )
            ).cancelled(audit_message="stuff", schedule_next_latest_at=date4)

        assert isinstance(result.state, M_State)
        assert result.state.error is None
        assert result.state.execution_state is protocols.ExecutionState.STOPPED
        assert result.state.result_state is protocols.ResultState.CANCELLED
        assert result.state.created_at == date3
        assert result.due_at is protocols._NotGiven
        assert result.schedule_next_latest_at == date4

        assert isinstance(fresh, M_State)
        assert fresh.error is None
        assert fresh.execution_state is protocols.ExecutionState.PENDING
        assert fresh.result_state is protocols.ResultState.ABSENT
        assert fresh.created_at == date1
        assert fresh.due_at == date2
        assert fresh.schedule_next_latest_at is None

        stored_info = M_StoredInfo(state=fresh).merge(result)

        assert isinstance(stored_info.state, M_State)
        assert stored_info.state.error is None
        assert stored_info.state.execution_state is protocols.ExecutionState.STOPPED
        assert stored_info.state.result_state is protocols.ResultState.CANCELLED
        assert stored_info.state.created_at == date1
        assert stored_info.state.due_at == date2
        assert stored_info.state.schedule_next_latest_at == date4
