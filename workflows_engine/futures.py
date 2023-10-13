import typing as tp

from . import errors, instructions, protocols


class StaticValue(tp.Generic[protocols.DataType]):
    def __init__(self, name: str, value: protocols.DataType) -> None:
        self.name = name
        self.value = value

    def initialise_value(self, engine: protocols.FutureEngine) -> None:
        pass

    def resolve(self) -> protocols.Instructions:
        yield from []

    def done(self) -> bool:
        return True

    def result(self) -> protocols.DataType:
        return self.value

    def exception(self) -> Exception | None:
        return None

    def cancelled(self) -> bool:
        return False

    def cancel(self) -> None:
        return None


class ComputationFuture(tp.Generic[protocols.DataType]):
    _result: protocols.Result[protocols.DataType]

    def __init__(
        self, name: str, computation: protocols.ComputationAction[protocols.DataType]
    ) -> None:
        self.name = name
        self.computation = computation
        self.pending_cancel = False

    def initialise_value(self, engine: protocols.FutureEngine) -> None:
        current_state = engine.resolve_state(self.name, self.computation)
        self._result = self.computation.make_result_object(current_state)

    def resolve(self) -> protocols.Instructions:
        if self.pending_cancel:
            yield instructions.CancelComputation(name=self.name, computation=self.computation)
        elif not self.done():
            yield instructions.RunComputation(name=self.name, computation=self.computation)

    def _ensure_result(self) -> protocols.Result[protocols.DataType]:
        if not hasattr(self, "_result"):
            raise errors.UnitiatedFuture()
        else:
            return self._result

    def done(self) -> bool:
        result = self._ensure_result()
        return result.has_value()

    def result(self) -> protocols.DataType:
        result = self._ensure_result()
        if not result.has_value():
            raise errors.PendingFuture()
        value = result.get_value()
        if isinstance(value, Exception):
            raise value
        else:
            return value

    def exception(self) -> Exception | None:
        result = self._ensure_result()
        if not result.has_value():
            raise errors.PendingFuture()
        value = result.get_value()
        if isinstance(value, Exception):
            return value
        else:
            return None

    def cancelled(self) -> bool:
        result = self._ensure_result()
        if not result.has_value():
            return False
        value = result.get_value()
        return isinstance(value, errors.CancelledFuture)

    def cancel(self) -> None:
        self.pending_cancel = True
