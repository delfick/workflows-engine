import datetime
import enum
import typing as tp
from collections.abc import Iterator


class Data(tp.Protocol):
    def summary_for_audit_trail(self) -> str:
        ...


CoDataType = tp.TypeVar("CoDataType", covariant=True, bound=Data)
DataType = tp.TypeVar("DataType", bound=Data)

Instructions: tp.TypeAlias = Iterator[object]


class ExecutionState(enum.IntEnum):
    SKIPPED = 0
    COMPLETED = 1
    PROGRESSING = 2
    STALLED = 3


class ResultState(enum.IntEnum):
    ABSENT = 0
    SUCCESS = 1
    CANCELLED = 2
    BAD_DEFINTION = 3
    EXPECTED_FAILURE = 3
    UNEXPECTED_FAILURE = 4


class ComputationState(tp.Protocol):
    began: datetime.datetime
    ended: datetime.datetime | None
    result_state: ResultState
    execution_state: ExecutionState
    parent_state: tp.Union["ComputationState", None]


class ComputationStateWithValues(ComputationState, tp.Protocol):
    raw_value: dict[str, object] | None
    raw_error: dict[str, object] | None


class ExceptionMaker(tp.Protocol):
    def __call__(self, raw_value: object) -> Exception:
        ...


class Result(tp.Generic[CoDataType], tp.Protocol):
    def has_value(self) -> bool:
        ...

    def get_value(self) -> CoDataType | Exception:
        ...


class Execute(tp.Protocol):
    def __call__(self) -> "ComputationResponse":
        ...


class FutureEngine(tp.Protocol):
    def resolve_state(
        self, name: str, computation: "ComputationAction[DataType]"
    ) -> ComputationStateWithValues:
        ...


class ComputationFuture(tp.Generic[CoDataType], tp.Protocol):
    def initialise_value(self, engine: FutureEngine) -> None:
        ...

    def resolve(self) -> Instructions:
        ...

    def done(self) -> bool:
        ...

    def result(self) -> CoDataType:
        ...

    def exception(self) -> Exception | None:
        ...

    def cancelled(self) -> bool:
        ...

    def cancel(self) -> None:
        ...


ComputationResponse: tp.TypeAlias = Iterator[ComputationFuture[Data]]


class ComputationAction(tp.Generic[DataType], tp.Protocol):
    def make_result_object(self, current_state: ComputationStateWithValues) -> Result[DataType]:
        ...

    def execute(
        self, current_state: ComputationState, result: Result[DataType]
    ) -> ComputationResponse:
        ...

    def future(self, name: str) -> ComputationFuture[DataType]:
        ...
