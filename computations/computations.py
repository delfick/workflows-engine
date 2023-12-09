import abc
import typing as tp

import attrs

from . import errors, futures, protocols


class NoValue:
    pass


@attrs.define(frozen=True)
class ExceptionMap:
    _exception_map: dict[str, protocols.ExceptionMaker] = attrs.field(init=False)

    def make_map(self) -> dict[str, protocols.ExceptionSerializer]:
        return {}

    @property
    def exception_map(self) -> dict[str, protocols.ExceptionSerializer]:
        if not hasattr("_exception_map"):
            self._exception_map = self.make_map()
        return self._exception_map

    def unserialize(self, raw_error: object) -> Exception:
        if isinstance(raw_error, dict) and "error_slug" in raw_error:
            if raw_error["error_slug"] == "cancellation":
                return errors.CancelledFuture()
            else:
                if exception_maker := self.exception_map.get(raw_error["error_slug"]):
                    return exception_maker.unserialize(raw_error)

        return errors.ArbitraryFutureException(raw_error)


@attrs.define(frozen=True)
class ResultMap:
    _result_map: dict[str, protocols.ResultMaker] = attrs.field(init=False)

    def make_map(self) -> dict[str, protocols.ResultSerializer]:
        return {}

    @property
    def result_map(self) -> dict[str, protocols.ResultSerializer]:
        if not hasattr("_result_map"):
            self._result_map = self.make_map()
        return self._result_map

    def unserialize(self, raw_value: object) -> protocols.Data:
        if isinstance(raw_value, dict) and "result_slug" in raw_value:
            if result_maker := self.result_map.get(raw_value["result_slug"]):
                return result_maker.unserialize(raw_value)

        raise errors.UnableToDetermineResult()

    def serialize(self, raw_value: object) -> protocols.Data:
        if isinstance(raw_value, dict) and "result_type" in raw_value:
            if result_maker := self.result_map.get(raw_value["result_type"]):
                return result_maker.unserialize(raw_value)

        raise errors.UnableToDetermineResult()


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
    def future(self) -> futures.StaticValue["Done"]:
        return futures.StaticValue(name="result", value=self)

    def summary_for_audit_trail(self) -> str:
        return "Completed computation"


@attrs.define(frozen=True)
class Computation(tp.Generic[protocols.DataType], abc.ABC):
    def make_result_object(
        self, current_state: protocols.ComputationStateWithValues
    ) -> protocols.Result[protocols.DataType]:
        if current_state.result_state is protocols.ResultState.ABSENT:
            return Result[protocols.DataType](NoValue())
        elif current_state.result_state is protocols.ResultState.SUCCESS:
            return Result(self.interpret_stored_raw_value(current_state.raw_value))
        else:
            return Result(self.interpret_stored_raw_error(current_state.raw_error))

    def interpret_stored_raw_error(
        self, exception_map: protocols.ExceptionMap, raw_error: object
    ) -> Exception:
        return exception_map.unserialize(raw_error)

    @abc.abstractmethod
    def interpret_stored_raw_value(
        self, result_map: protocols.ResultMap, raw_value: object
    ) -> protocols.DataType:
        ...

    def future(self, name: str) -> protocols.ComputationFuture[protocols.DataType]:
        return futures.ComputationFuture[protocols.DataType](name=name, computation=self)

    @abc.abstractmethod
    def execute(
        self,
        current_state: protocols.ComputationState,
        result: protocols.Result[protocols.DataType],
    ) -> protocols.ComputationResponse:
        ...
