from collections.abc import Callable
from typing import (
    Awaitable,
    Optional,
    Union,
)

from ..context import SagaContext
from ..messages import (
    SagaRequest,
    SagaResponse,
)

RequestCallBack = Callable[[SagaContext, ...], Union[SagaRequest, Awaitable[SagaRequest]]]
ResponseCallBack = Callable[[SagaContext, SagaResponse, ...], Union[SagaContext, Awaitable[SagaContext]]]
CommitCallback = Callable[[SagaContext, ...], Union[Optional[SagaContext], Awaitable[Optional[SagaContext]]]]
