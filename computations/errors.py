import attrs


@attrs.define(frozen=True)
class ComputationsException(Exception):
    pass


@attrs.define(frozen=True)
class UnitiatedFuture(ComputationsException):
    pass


@attrs.define(frozen=True)
class PendingFuture(ComputationsException):
    pass


@attrs.define(frozen=True)
class CancelledFuture(ComputationsException):
    pass


@attrs.define(frozen=True)
class ResultHasNoValue(ComputationsException):
    pass


@attrs.define(frozen=True)
class ArbitraryFutureException(ComputationsException):
    value: object
