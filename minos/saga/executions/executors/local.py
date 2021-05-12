"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import asyncio
import inspect
from abc import (
    ABC,
)
from asyncio import (
    AbstractEventLoop,
)
from collections import (
    Callable,
)
from typing import (
    Any,
    Optional,
)

from ...definitions import (
    SagaStepOperation,
)
from ..context import (
    SagaContext,
)


class LocalExecutor(ABC):
    """TODO"""

    def __init__(self, loop: Optional[AbstractEventLoop] = None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self.loop = loop

    def exec_one(self, operation: SagaStepOperation, *args, **kwargs) -> Any:
        """TODO

        :param operation: TODO
        :param response: TODO
        :return: TODO
        """

        return self._exec_function(operation.callback, *args, **kwargs)

    def _exec_function(self, func: Callable, *args, **kwargs) -> SagaContext:
        """TODO

        :param func: TODO
        :param request: TODO
        :return: TODO
        """
        result = func(*args, **kwargs)
        if inspect.isawaitable(result):
            result = self.loop.run_until_complete(result)
            return result
        return result
