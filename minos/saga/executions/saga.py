"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    Any,
    Iterable,
    NoReturn,
    Optional,
    Union,
)
from uuid import (
    UUID,
)

from minos.common import (
    CommandReply,
)

from ..definitions import (
    Saga,
    SagaStep,
)
from ..exceptions import (
    MinosSagaFailedExecutionStepException,
    MinosSagaPausedExecutionStepException,
    MinosSagaRollbackExecutionException,
)
from .context import (
    SagaContext,
)
from .status import (
    SagaStatus,
)
from .step import (
    SagaExecutionStep,
)


class SagaExecution(object):
    """TODO"""

    # noinspection PyUnusedLocal
    def __init__(
        self,
        definition: Saga,
        uuid: UUID,
        context: SagaContext,
        status: SagaStatus = SagaStatus.Created,
        steps: [SagaExecutionStep] = None,
        already_rollback: bool = False,
        *args,
        **kwargs
    ):
        if steps is None:
            steps = list()

        self.uuid = uuid
        self.definition = definition
        self.executed_steps = steps
        self.context = context
        self.status = status
        self.already_rollback = already_rollback

    @classmethod
    def from_raw(cls, raw: Union[dict[str, Any], SagaExecution], **kwargs) -> SagaExecution:
        """TODO

        :param raw: TODO
        :param kwargs: TODO
        :return: TODO
        """
        if isinstance(raw, cls):
            return raw

        current = raw | kwargs
        current["definition"] = Saga.from_raw(current["definition"])
        current["status"] = SagaStatus.from_raw(current["status"])
        current["context"] = SagaContext.from_avro_bytes(current["context"])

        if isinstance(current["uuid"], str):
            current["uuid"] = UUID(current["uuid"])

        instance = cls(**current)

        executed_steps = (
            SagaExecutionStep.from_raw(executed_step, definition=step)
            for step, executed_step in zip(instance.definition.steps, raw.pop("executed_steps"))
        )
        for executed_step in executed_steps:
            instance._add_executed(executed_step)

        return instance

    @classmethod
    def from_saga(cls, definition: Saga, *args, **kwargs):
        """TODO

        :param definition: TODO
        :return: TODO
        """
        from uuid import (
            uuid4,
        )

        return cls(definition, uuid4(), SagaContext(), *args, **kwargs)

    def execute(self, reply: Optional[CommandReply] = None):
        """TODO

        :param reply: TODO
        :return: TODO
        """
        self.status = SagaStatus.Running
        for step in self.pending_steps:
            execution_step = SagaExecutionStep(step)
            try:
                self.context = execution_step.execute(self.context, reply)
                self._add_executed(execution_step)
            except MinosSagaFailedExecutionStepException as exc:
                self.rollback()
                self.status = SagaStatus.Errored
                raise exc
            except MinosSagaPausedExecutionStepException as exc:
                self.status = SagaStatus.Paused
                raise exc

            reply = None  # Response is consumed

        self.status = SagaStatus.Finished
        return self.context

    def rollback(self, *args, **kwargs) -> NoReturn:
        """TODO

        :return: TODO
        """

        if self.already_rollback:
            raise MinosSagaRollbackExecutionException("The saga was already rollbacked.")

        for execution_step in reversed(self.executed_steps):
            self.context = execution_step.rollback(self.context, *args, **kwargs)

        self.already_rollback = True

    @property
    def pending_steps(self) -> [SagaStep]:
        """TODO

        :return: TODO
        """
        offset = len(self.executed_steps)
        return self.definition.steps[offset:]

    def _add_executed(self, executed_step: SagaExecutionStep):
        """TODO

        :param executed_step: TODO
        :return: TODO
        """
        self.executed_steps.append(executed_step)

    @property
    def raw(self) -> dict[str, Any]:
        """TODO

        :return: TODO
        """
        return {
            "definition": self.definition.raw,
            "uuid": str(self.uuid),
            "status": self.status.raw,
            "executed_steps": [step.raw for step in self.executed_steps],
            "context": self.context.avro_bytes,
            "already_rollback": self.already_rollback,
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
        )
