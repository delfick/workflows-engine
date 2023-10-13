import abc
import typing as tp

import attrs

from . import errors, futures, protocols


class NoValue:
    pass


@attrs.define(frozen=True)
class Result(tp.Generic[protocols.DataType]):
    value: protocols.DataType | Exception | NoValue

    def has_value(self) -> bool:
        return self.value is not NoValue

    def get_value(self) -> protocols.DataType | Exception:
        value = self.value
        if isinstance(value, NoValue):
            raise errors.ResultHasNoValue()
        else:
            return value


@attrs.define(frozen=True)
class Done:
    def future(self) -> futures.StaticValue[str]:
        return futures.StaticValue(name="result", value="done")


@attrs.define(frozen=True)
class Computation(tp.Generic[protocols.DataType], abc.ABC):
    exception_map: dict[str, protocols.ExceptionMaker] = attrs.field(factory=lambda: {})

    def make_result_object(
        self, current_state: protocols.ComputationStateWithValues
    ) -> protocols.Result[protocols.DataType]:
        if current_state.result_state is protocols.ResultState.ABSENT:
            return Result[protocols.DataType](NoValue())
        elif current_state.result_state is protocols.ResultState.SUCCESS:
            return Result(self.interpret_stored_raw_value(current_state.raw_value))
        else:
            return Result(self.interpret_stored_raw_error(current_state.raw_error))

    def interpret_stored_raw_error(self, raw_error: object) -> Exception:
        if isinstance(raw_error, dict) and "error_type" in raw_error:
            if raw_error["error_type"] == "cancellation":
                return errors.CancelledFuture()
            else:
                if exception_maker := self.exception_map.get(raw_error["error_type"]):
                    return exception_maker(raw_error)

        return errors.ArbitraryFutureException(raw_error)

    def future(self, name: str) -> protocols.ComputationFuture[protocols.DataType]:
        return futures.ComputationFuture[protocols.DataType](name=name, computation=self)

    @abc.abstractmethod
    def interpret_stored_raw_value(self, raw_value: object) -> protocols.DataType:
        ...

    @abc.abstractmethod
    def execute(
        self,
        current_state: protocols.ComputationState,
        result: protocols.Result[protocols.DataType],
    ) -> protocols.ComputationResponse:
        ...
