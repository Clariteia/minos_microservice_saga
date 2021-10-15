from __future__ import (
    annotations,
)

from typing import (
    Optional,
)

from minos.common import (
    CommandReply,
    CommandStatus,
)

from ...context import (
    SagaContext,
)
from ...definitions import (
    RemoteSagaStep,
)
from ...exceptions import (
    CommandReplyFailedException,
    SagaFailedExecutionStepException,
    SagaPausedExecutionStepException,
    SagaRollbackExecutionStepException,
)
from ..executors import (
    RequestExecutor,
    ResponseExecutor,
)
from ..status import (
    SagaStepStatus,
)
from .abc import (
    SagaStepExecution,
)


class RemoteSagaStepExecution(SagaStepExecution):
    """Saga Execution Step class."""

    definition: RemoteSagaStep

    async def execute(self, context: SagaContext, reply: Optional[CommandReply] = None, *args, **kwargs) -> SagaContext:
        """Execution the step.

        :param context: The execution context to be used during the execution.
        :param reply: An optional command reply instance (to be consumed by the on_success method).
        :return: The updated context.
        """

        await self._execute_on_execute(context, *args, **kwargs)

        if reply is None:
            self.status = SagaStepStatus.PausedByOnExecute
            raise SagaPausedExecutionStepException()

        if reply.status == CommandStatus.SYSTEM_ERROR:
            self.status = SagaStepStatus.ErroredByOnExecute
            exc = CommandReplyFailedException(f"CommandReply failed with {reply.status!s} status: {reply.data!s}")
            raise SagaFailedExecutionStepException(exc)

        if reply.status == CommandStatus.SUCCESS:
            context = await self._execute_on_success(context, reply, *args, **kwargs)
        else:
            context = await self._execute_on_error(context, reply, *args, **kwargs)

        self.status = SagaStepStatus.Finished
        return context

    async def _execute_on_execute(self, context: SagaContext, *args, **kwargs) -> None:
        if self.status != SagaStepStatus.Created:
            return

        self.status = SagaStepStatus.RunningOnExecute
        executor = RequestExecutor(*args, **kwargs)
        try:
            await executor.exec(self.definition.on_execute_operation, context)
        except SagaFailedExecutionStepException as exc:
            self.status = SagaStepStatus.ErroredOnExecute
            raise exc
        self.status = SagaStepStatus.FinishedOnExecute

    async def _execute_on_success(self, context: SagaContext, reply: CommandReply, *args, **kwargs) -> SagaContext:
        self.status = SagaStepStatus.RunningOnSuccess
        executor = ResponseExecutor(*args, **kwargs)

        try:
            context = await executor.exec(self.definition.on_success_operation, context, reply)
        except SagaFailedExecutionStepException as exc:
            self.status = SagaStepStatus.ErroredOnSuccess
            await self.rollback(context, *args, **kwargs)
            raise exc

        return context

    async def _execute_on_error(self, context: SagaContext, reply: CommandReply, *args, **kwargs) -> SagaContext:
        self.status = SagaStepStatus.RunningOnError
        executor = ResponseExecutor(*args, **kwargs)

        try:
            context = await executor.exec(self.definition.on_error_operation, context, reply)
        except SagaFailedExecutionStepException as exc:
            self.status = SagaStepStatus.ErroredOnError
            await self.rollback(context, *args, **kwargs)
            raise exc

        return context

    async def rollback(self, context: SagaContext, *args, **kwargs) -> SagaContext:
        """Revert the executed operation with a compensatory operation.

        :param context: Execution context.
        :return: The updated execution context.
        """
        if self.status == SagaStepStatus.Created:
            raise SagaRollbackExecutionStepException("There is nothing to rollback.")

        if self.already_rollback:
            raise SagaRollbackExecutionStepException("The step was already rollbacked.")

        executor = RequestExecutor(*args, **kwargs)
        await executor.exec(self.definition.on_failure_operation, context)

        self.already_rollback = True
        return context
