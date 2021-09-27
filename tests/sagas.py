from minos.saga import (
    Saga,
)
from tests.utils import (
    foo_fn,
    foo_fn_raises,
)

ADD_ORDER = (
    Saga()
    .step()
    .invoke_participant("CreateProduct", foo_fn)
    .with_compensation("DeleteProduct", foo_fn)
    .on_reply("order1")
    .step()
    .invoke_participant("CreateTicket", foo_fn)
    .with_compensation("DeleteOrder", foo_fn)
    .on_reply("order2", foo_fn)
    .commit()
)

DELETE_ORDER = (
    Saga()
    .step()
    .invoke_participant("DeleteProduct", foo_fn)
    .on_reply("order1")
    .step()
    .invoke_participant("DeleteTicket", foo_fn)
    .on_reply("order2", foo_fn_raises)
    .commit()
)