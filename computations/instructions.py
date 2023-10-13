import typing as tp

import attrs

from . import protocols


@attrs.define(frozen=True)
class RunComputation(tp.Generic[protocols.DataType]):
    name: str
    computation: protocols.ComputationAction[protocols.DataType]


@attrs.define(frozen=True)
class CancelComputation(tp.Generic[protocols.DataType]):
    name: str
    computation: protocols.ComputationAction[protocols.DataType]
