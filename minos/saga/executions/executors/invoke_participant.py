"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import uuid
from typing import (
    Any,
)

from ...exceptions import (
    MinosSagaException,
    MinosSagaPausedExecutionStepException,
)
from ..context import (
    SagaContext,
)
from .local import (
    LocalExecutor,
)


class InvokeParticipantExecutor(LocalExecutor):
    """TODO"""

    def exec(self, operation: dict[str, Any], context: SagaContext):
        """TODO

        :param operation: TODO
        :param context: TODO
        :return: TODO
        """
        if operation is None:
            return context

        self.storage.create_operation(operation)
        try:
            context = self.launch_command(operation["name"])
        except MinosSagaException as error:
            self.storage.operation_error_db(operation["id"], error)
            raise error
        self.storage.store_operation_response(operation["id"], context)

        if operation["callback"] is None:
            return context

        callback_operation = {
            "id": str(uuid.uuid4()),
            "type": "invokeParticipant_callback",
            "name": operation["name"],
            "callback": operation["callback"],
        }
        context = super().exec(callback_operation, context)

        return context

    @staticmethod
    def launch_command(*args, **kwargs) -> SagaContext:
        """TODO

        :param args: TODO
        :param kwargs: TODO
        :return: TODO
        """
        # FIXME: This would be replaced with the corresponding broker call
        raise MinosSagaPausedExecutionStepException()
