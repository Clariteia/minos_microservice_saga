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

from minos.common import (
    CommandReply,
)

from ..definitions import (
    SagaStep,
)
from ..exceptions import (
    MinosSagaException,
    MinosSagaFailedExecutionStepException,
    MinosSagaPausedExecutionStepException,
)
from .context import (
    SagaContext,
)
from .executors import (
    InvokeParticipantExecutor,
    OnReplyExecutor,
    WithCompensationExecutor,
)
from .status import (
    SagaStepStatus,
)


class SagaExecutionStep(object):
    """TODO"""

    def __init__(
        self, definition: SagaStep, status: SagaStepStatus = SagaStepStatus.Created, already_rollback: bool = False,
    ):

        self.definition = definition
        self.status = status
        self.already_rollback = already_rollback

    @classmethod
    def from_raw(cls, raw: Union[dict[str, Any], SagaExecutionStep], **kwargs) -> SagaExecutionStep:
        """TODO

        :param raw: TODO
        :param kwargs: TODO
        :return: TODO
        """
        if isinstance(raw, cls):
            return raw

        current = raw | kwargs
        current["definition"] = SagaStep.from_raw(current["definition"])
        current["status"] = SagaStepStatus.from_raw(current["status"])
        return cls(**current)

    def execute(self, context: SagaContext, reply: Optional[CommandReply] = None, *args, **kwargs) -> SagaContext:
        """TODO

        :param context: TODO
        :param reply: TODO
        :return: TODO
        """

        self._execute_invoke_participant(context, *args, **kwargs)
        context = self._execute_on_reply(context, reply, *args, **kwargs)

        self.status = SagaStepStatus.Finished
        return context

    def _execute_invoke_participant(self, context: SagaContext, *args, **kwargs) -> NoReturn:
        if self.status != SagaStepStatus.Created:
            return

        self.status = SagaStepStatus.RunningInvokeParticipant
        executor = InvokeParticipantExecutor(*args, **kwargs)
        try:
            executor.exec(self.definition.raw_invoke_participant, context)
        except MinosSagaException:
            self.status = SagaStepStatus.ErroredInvokeParticipant
            raise MinosSagaFailedExecutionStepException()
        self.status = SagaStepStatus.FinishedInvokeParticipant

    def _execute_on_reply(
        self, context: SagaContext, reply: Optional[CommandReply] = None, *args, **kwargs
    ) -> SagaContext:
        self.status = SagaStepStatus.RunningOnReply
        executor = OnReplyExecutor(*args, **kwargs)
        # noinspection PyBroadException
        try:
            context = executor.exec(self.definition.raw_on_reply, context, reply)
        except MinosSagaPausedExecutionStepException as exc:
            self.status = SagaStepStatus.PausedOnReply
            raise exc
        except Exception:
            self.status = SagaStepStatus.ErroredOnReply
            self.rollback(context, *args, **kwargs)
            raise MinosSagaFailedExecutionStepException()
        return context

    def rollback(self, context: SagaContext, *args, **kwargs) -> SagaContext:
        """TODO

        :param context: TODO
        :return: TODO
        """
        if self.status == SagaStepStatus.Created or self.already_rollback:
            return context

        executor = WithCompensationExecutor(*args, **kwargs)
        executor.exec(self.definition.raw_with_compensation, context)

        self.already_rollback = True
        return context

    @property
    def raw(self) -> dict[str, Any]:
        """TODO

        :return: TODO
        """
        return {
            "definition": self.definition.raw,
            "status": self.status.raw,
            "already_rollback": self.already_rollback,
        }

    def __eq__(self, other: SagaStep) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        yield from (
            self.definition,
            self.status,
            self.already_rollback,
        )
