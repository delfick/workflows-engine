import attrs


@attrs.define(frozen=True)
class WorkflowsEngineException(Exception):
    pass


@attrs.define(frozen=True)
class UnitiatedFuture(WorkflowsEngineException):
    pass


@attrs.define(frozen=True)
class PendingFuture(WorkflowsEngineException):
    pass


@attrs.define(frozen=True)
class CancelledFuture(WorkflowsEngineException):
    pass


@attrs.define(frozen=True)
class ResultHasNoValue(WorkflowsEngineException):
    pass


@attrs.define(frozen=True)
class ArbitraryFutureException(WorkflowsEngineException):
    value: object
