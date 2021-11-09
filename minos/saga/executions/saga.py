from __future__ import (
    annotations,
)

import logging
import warnings
from typing import (
    Any,
    Iterable,
    Optional,
    Union,
)
from uuid import (
    UUID,
)

from ..context import (
    SagaContext,
)
from ..definitions import (
    Saga,
    SagaStep,
)
from ..exceptions import (
    SagaExecutionAlreadyExecutedException,
    SagaFailedCommitCallbackException,
    SagaFailedExecutionStepException,
    SagaPausedExecutionStepException,
    SagaRollbackExecutionException,
    SagaStepExecutionException,
)
from ..messages import (
    SagaResponse,
)
from .commit import (
    TransactionManager,
)
from .executors import (
    LocalExecutor,
)
from .status import (
    SagaStatus,
)
from .steps import (
    RemoteSagaStepExecution,
    SagaStepExecution,
)

logger = logging.getLogger(__name__)


class SagaExecution:
    """Saga Execution class."""

    # noinspection PyUnusedLocal
    def __init__(
        self,
        definition: Saga,
        uuid: UUID,
        context: SagaContext,
        status: SagaStatus = SagaStatus.Created,
        steps: list[SagaStepExecution] = None,
        paused_step: SagaStepExecution = None,
        already_rollback: bool = False,
        user: Optional[UUID] = None,
        *args,
        **kwargs,
    ):
        definition.validate()  # If not valid, raises an exception.

        if steps is None:
            steps = list()

        self.uuid = uuid
        self.definition = definition
        self.executed_steps = steps
        self.context = context
        self.status = status
        self.already_rollback = already_rollback
        self.paused_step = paused_step
        self.user = user

    @classmethod
    def from_raw(cls, raw: Union[dict[str, Any], SagaExecution], **kwargs) -> SagaExecution:
        """Build a new instance from a raw representation.

        :param raw: The raw representation of the instance.
        :param kwargs: Additional named arguments.
        :return: A ``SagaExecution`` instance.
        """
        if isinstance(raw, cls):
            return raw

        current = raw | kwargs
        current["definition"] = Saga.from_raw(current["definition"])
        current["status"] = SagaStatus.from_raw(current["status"])
        current["context"] = SagaContext.from_avro_str(current["context"])
        current["paused_step"] = (
            None if current["paused_step"] is None else SagaStepExecution.from_raw(current["paused_step"])
        )

        if isinstance(current["uuid"], str):
            current["uuid"] = UUID(current["uuid"])
        if isinstance(current["user"], str):
            current["user"] = UUID(current["user"])

        instance = cls(**current)

        executed_steps = (
            SagaStepExecution.from_raw(executed_step, definition=step)
            for step, executed_step in zip(instance.definition.steps, raw.pop("executed_steps"))
        )
        for executed_step in executed_steps:
            instance._add_executed(executed_step)

        return instance

    @classmethod
    def from_saga(cls, definition: Saga, context: Optional[SagaContext] = None, *args, **kwargs) -> SagaExecution:
        """Build a new instance from a ``Saga`` object.

        :param definition: The definition of the saga.
        :param context: Initial saga execution context. If not provided, then a new empty context is created.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A new ``SagaExecution`` instance.
        """
        warnings.warn(
            "from_saga() method is deprecated by from_definition() and will be removed soon.", DeprecationWarning
        )

        return cls.from_definition(definition, context, *args, **kwargs)

    @classmethod
    def from_definition(
        cls, definition: Saga, context: Optional[SagaContext] = None, uuid: Optional[UUID] = None, *args, **kwargs
    ) -> SagaExecution:
        """Build a new instance from a ``Saga`` object.

        :param definition: The definition of the saga.
        :param context: Initial saga execution context. If not provided, then a new empty context is created.
        :param uuid: The identifier of the execution. If ``None`` is provided, then a new one will be generated.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A new ``SagaExecution`` instance.
        """
        if uuid is None:
            from uuid import (
                uuid4,
            )

            uuid = uuid4()

        if context is None:
            context = SagaContext()

        return cls(definition, uuid, context, *args, **kwargs)

    async def execute(self, response: Optional[SagaResponse] = None, *args, **kwargs) -> SagaContext:
        """Execute the ``Saga`` definition.

        :param response: An optional ``SagaResponse`` to be consumed by the immediately next executed step.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A ``SagaContext instance.
        """
        if self.status == SagaStatus.Finished:
            raise SagaExecutionAlreadyExecutedException(
                f"The {self.uuid!s} execution cannot be executed because is in {self.status!r} status."
            )
        if self.status == SagaStatus.Errored:
            if response is None:
                raise SagaExecutionAlreadyExecutedException(
                    f"The {self.uuid!s} execution cannot be executed because is in {self.status!r} status."
                )

            logger.info(f"Received 'on_failure': {response!s}")
            return self.context

        self.status = SagaStatus.Running
        if self.paused_step is not None:
            try:
                await self._execute_one(self.paused_step, response=response, *args, **kwargs)
            finally:
                if self.status != SagaStatus.Paused:
                    self.paused_step = None

        for step in self._pending_steps:
            execution_step = SagaStepExecution.from_definition(step)
            await self._execute_one(execution_step, *args, **kwargs)

        await self._execute_commit(*args, **kwargs)
        self.status = SagaStatus.Finished
        return self.context

    async def _execute_one(self, execution_step: SagaStepExecution, *args, **kwargs) -> None:
        try:
            self.context = await execution_step.execute(
                self.context, execution_uuid=self.uuid, user=self.user, *args, **kwargs
            )
            self._add_executed(execution_step)
        except SagaFailedExecutionStepException as exc:
            await self.rollback(*args, **kwargs)
            self.status = SagaStatus.Errored
            raise exc
        except SagaPausedExecutionStepException as exc:
            self.paused_step = execution_step
            self.status = SagaStatus.Paused
            raise exc

    async def _execute_commit(self, *args, **kwargs) -> None:
        executor = LocalExecutor(execution_uuid=self.uuid, *args, **kwargs)

        try:
            self.context = await executor.exec(self.definition.commit_operation, self.context)
        except SagaFailedExecutionStepException as exc:
            await self.rollback(*args, **kwargs)
            self.status = SagaStatus.Errored
            raise SagaFailedCommitCallbackException(exc.exception)

        count = sum(isinstance(executed_step, RemoteSagaStepExecution) for executed_step in self.executed_steps)

        await TransactionManager(count=count, execution_uuid=self.uuid, *args, **kwargs).commit()

    async def rollback(self, *args, **kwargs) -> None:
        """Revert the executed operation with a compensatory operation.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: The updated execution context.
        """

        if self.already_rollback:
            raise SagaRollbackExecutionException("The saga was already rollbacked.")

        raised_exception = False
        for execution_step in reversed(self.executed_steps):
            try:
                self.context = await execution_step.rollback(
                    self.context, user=self.user, execution_uuid=self.uuid, *args, **kwargs
                )
            except SagaStepExecutionException as exc:
                logger.warning(f"There was an exception on {type(execution_step).__name__!r} rollback: {exc!r}")
                raised_exception = True

        await TransactionManager(count=0, execution_uuid=self.uuid, *args, **kwargs).reject()

        if raised_exception:
            raise SagaRollbackExecutionException("Some execution steps failed to rollback.")

        self.already_rollback = True

    @property
    def _pending_steps(self) -> list[SagaStep]:
        offset = len(self.executed_steps)
        return self.definition.steps[offset:]

    def _add_executed(self, executed_step: SagaStepExecution) -> None:
        self.executed_steps.append(executed_step)

    @property
    def raw(self) -> dict[str, Any]:
        """Compute a raw representation of the instance.

        :return: A ``dict`` instance.
        """
        return {
            "definition": self.definition.raw,
            "uuid": str(self.uuid),
            "status": self.status.raw,
            "executed_steps": [step.raw for step in self.executed_steps],
            "paused_step": None if self.paused_step is None else self.paused_step.raw,
            "context": self.context.avro_str,
            "already_rollback": self.already_rollback,
            "user": None if self.user is None else str(self.user),
        }

    def __eq__(self, other: SagaStep) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        yield from (
            self.definition,
            self.uuid,
            self.status,
            self.executed_steps,
            self.context,
            self.already_rollback,
            self.user,
        )
