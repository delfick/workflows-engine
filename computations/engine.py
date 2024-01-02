from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Generic, Literal, cast, overload

import attrs

from . import jobs, protocols, state, storage


class ComputationExecutorBase(Generic[protocols.T_CO_Data]):
    @property
    @abc.abstractproperty
    def _original_engine(
        self,
    ) -> protocols.Engine[protocols.T_CO_Data]:
        ...

    @property
    @abc.abstractproperty
    def _job_tracker(
        self,
    ) -> protocols.JobTracker[protocols.JobStatus[protocols.T_CO_Data]]:
        ...

    @overload
    def __call__(
        self,
        path: protocols.JobPath,
        intention: protocols.Computation[protocols.T_CO_Data],
        /,
        *,
        override_execute: (
            protocols.Computation[protocols.T_CO_Data] | Literal[True] | None
        ) = None,
    ) -> jobs._P_Job[protocols.T_CO_Data]:
        ...

    @overload
    def __call__(
        self,
        path: protocols.ExternalInputPath,
        intention: protocols.ExternalInputResolver[protocols.T_ExternalData],
        /,
        *,
        override_execute: None = None,
    ) -> protocols.T_ExternalData:
        ...

    def __call__(
        self,
        path: protocols.JobPath | protocols.ExternalInputPath,
        intention: (
            protocols.Computation[protocols.T_CO_Data]
            | protocols.ExternalInputResolver[protocols.T_ExternalData]
        ),
        /,
        *,
        override_execute: (
            protocols.Computation[protocols.T_CO_Data] | Literal[True] | None
        ) = None,
    ) -> jobs._P_Job[protocols.T_CO_Data] | protocols.T_ExternalData:
        if isinstance(intention, protocols.ExternalInputResolver):
            if isinstance(path, protocols.JobPath):
                raise RuntimeError("May only execute external inputs with external input paths")

            return self._original_engine.external_input(
                external_input_path=path, external_input_resolver=intention
            )
        else:
            if isinstance(path, protocols.ExternalInputPath):
                raise RuntimeError("May only execute computations with job paths")

            return self._original_engine.run(
                job_path=path,
                computation=intention,
                override_execute=override_execute,
                job_tracker=self._job_tracker,
            )

    def get_without_executing(
        self, path: protocols.JobPath, intention: protocols.Computation[protocols.T_CO_Data], /
    ) -> jobs._P_Job[protocols.T_CO_Data]:
        return self._original_engine.run(
            job_path=path,
            computation=intention,
            override_execute=True,
            job_tracker=self._job_tracker,
        )


@attrs.frozen
class ComputationExecutor(ComputationExecutorBase[protocols.T_CO_Data]):
    _original_engine: protocols.Engine[protocols.T_CO_Data]
    _job_tracker: protocols.JobTracker[protocols.JobStatus[protocols.T_CO_Data]]


class EngineBase(
    Generic[protocols.T_CO_Data, protocols.T_Storage],
    abc.ABC,
):
    """
    Used by a computation to run other jobs and get external input
    """

    @property
    @abc.abstractproperty
    def store(self) -> protocols.T_Storage:
        ...

    @abc.abstractmethod
    def run(
        self,
        *,
        job_path: protocols.JobPath,
        job_tracker: protocols.JobTracker[protocols.JobStatus[protocols.T_CO_Data]],
        computation: protocols.T_Computation,
        override_execute: (
            protocols.Computation[protocols.T_CO_Data] | Literal[True] | None
        ) = None,
    ) -> protocols.Job[
        protocols.ComputationState[protocols.T_CO_Data],
        protocols.T_Computation,
        protocols.Result[protocols.T_CO_Data],
    ]:
        """
        Execute the computation, store the result and return the job object
        """

    @abc.abstractmethod
    def external_input(
        self,
        *,
        external_input_path: protocols.ExternalInputPath,
        external_input_resolver: protocols.ExternalInputResolver[protocols.T_ExternalData],
    ) -> protocols.T_ExternalData:
        """
        Get some external input.
        """


@attrs.frozen
class Engine(EngineBase[protocols.T_CO_State, protocols.T_Storage], abc.ABC):
    """
    Used by a computation to run other jobs and get external input
    """

    store: protocols.T_Storage
    default_error_resolver: protocols.ErrorResolver
    default_exception_serializer: protocols.ExceptionSerializer

    def _make_results(
        self, computation_state: protocols.ComputationState[protocols.T_CO_State], /
    ) -> protocols.Results[protocols.T_CO_State]:
        return state.Results.using(computation_state)

    def _make_error(
        self,
        *,
        computation: protocols.Computation[protocols.T_CO_State],
        result: protocols.Result[protocols.T_CO_State] | None,
    ) -> protocols.Error | None:
        if result is None or result.state.error is None:
            return None

        error_resolver = self.default_error_resolver
        if isinstance(computation, protocols.ErrorResolver):
            error_resolver = computation

        return error_resolver.resolve_error(result.state.error)

    @abc.abstractmethod
    def _make_job(
        self,
        *,
        job_path: protocols.JobPath,
        result: protocols.Result[protocols.T_CO_State] | None,
        error: protocols.Error | None,
        computation: protocols.T_Computation,
    ) -> protocols.Job[
        protocols.ComputationState[protocols.T_CO_State],
        protocols.T_Computation,
        protocols.Result[protocols.T_CO_State],
    ]:
        ...

    def run(
        self,
        *,
        job_path: protocols.JobPath,
        job_tracker: protocols.JobTracker[protocols.JobStatus[protocols.T_CO_State]],
        computation: protocols.T_Computation,
        override_execute: (
            protocols.Computation[protocols.T_CO_State] | Literal[True] | None
        ) = None,
    ) -> protocols.Job[
        protocols.ComputationState[protocols.T_CO_State],
        protocols.T_Computation,
        protocols.Result[protocols.T_CO_State],
    ]:
        """
        Execute the computation, store the result and return the job object
        """
        job_status = job_tracker.job_status(job_path)

        if job_status.job_before is not None:
            result_before = job_status.job_before._result
        else:
            result_before = None

        job = self._make_job(
            job_path=job_path,
            result=result_before,
            error=self._make_error(computation=computation, result=result_before),
            computation=computation,
        )

        if override_execute is True:
            return job

        intention: protocols.Computation[protocols.T_CO_State]
        if override_execute is None:
            intention = computation
        else:
            intention = override_execute

        try:
            result = intention.execute(
                job.state, ComputationExecutor(original_engine=self, job_tracker=job_tracker)
            )
        except Exception as error:
            exception_serializer = self.default_exception_serializer
            if isinstance(computation, protocols.ExceptionSerializer):
                exception_serializer = computation

            result = self._make_results(job.state).unhandled_failure(
                exc=error,
                audit_message="unhandled exception caught by internal logic",
                exception_serializer=exception_serializer,
            )

        job_after = self._make_job(
            job_path=job_path,
            result=result,
            error=self._make_error(computation=computation, result=result),
            computation=computation,
        )
        job_status.add_execution(job_after)
        return job_after

    def external_input(
        self,
        *,
        external_input_path: protocols.ExternalInputPath,
        external_input_resolver: protocols.ExternalInputResolver[protocols.T_ExternalData],
    ) -> protocols.T_ExternalData:
        """
        Get some external input.
        """
        return external_input_resolver.resolve()


if TYPE_CHECKING:
    A_ComputationExecutor = ComputationExecutorBase[protocols.P_State]
    A_Engine = EngineBase[protocols.P_State, protocols.P_Storage]

    C_ComputationExecutor = ComputationExecutorBase[state.C_State]
    C_Engine = Engine[state.C_State, storage.C_Storage]

    _E: protocols.P_Engine = cast(A_Engine, None)
    _EC: protocols.P_Engine = cast(C_Engine, None)

    _CE: protocols.P_ComputationExecutor = cast(A_ComputationExecutor, None)
    _CEC: protocols.P_ComputationExecutor = cast(C_ComputationExecutor, None)
