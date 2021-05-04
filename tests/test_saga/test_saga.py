# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.
from tempfile import TemporaryDirectory

import pytest
from minos.saga import Saga


def create_ticket_on_reply_callback(response):
    treated_response = "async create_ticket_on_reply_callback response!!!!"
    # log.debug("---> create_ticket_on_reply_callback")
    return treated_response


async def create_order_callback(response):
    treated_response = "create_order_callback response!!!!"
    # log.debug("---> create_order_callback")
    return treated_response


def create_ticket_callback(response):
    treated_response = "create_ticket_callback response!!!!"
    # log.debug("---> create_ticket_callback")
    return treated_response


async def create_order_callback2(response):
    treated_response = "create_order_callback response!!!!"
    # log.debug("---> create_order_callback")
    return treated_response


async def delete_order_callback(response):
    treated_response = "async delete_order_callback response!!!!"
    # log.debug("---> delete_order_callback")
    return treated_response


def shipping_callback(response):
    treated_response = "async shipping_callback response!!!!"
    # log.debug("---> shipping_callback")
    return treated_response


async def a(response):
    treated_response = "create_order_callback response!!!!"
    # log.debug("---> create_order_callback")
    return treated_response


async def b(response):
    treated_response = "create_order_callback response!!!!"
    # log.debug("---> create_order_callback")
    return treated_response


async def c(response):
    treated_response = "async create_ticket_on_reply_callback response!!!!"
    # log.debug("---> create_ticket_on_reply_callback")
    return treated_response


def d(response):
    treated_response = "create_order_callback response!!!!"
    # log.debug("---> create_order_callback")
    return treated_response


def e(response):
    treated_response = "create_order_callback response!!!!"
    # log.debug("---> create_order_callback")
    return treated_response


def f(response):
    treated_response = "async create_ticket_on_reply_callback response!!!!"
    # log.debug("---> create_ticket_on_reply_callback")
    return treated_response


def test_saga_async_callbacks_ok():
    with TemporaryDirectory() as db_name:
        s = (
            Saga("OrdersAdd", db_name=db_name)
            .start()
            .step()
            .invokeParticipant("CreateOrder", a)
            .withCompensation("DeleteOrder", b)
            .onReply(c)
            .execute()
        )

        assert s.get_db_state() is None


def test_saga_sync_callbacks_ok():
    with TemporaryDirectory() as db_name:
        s = (
            Saga("OrdersAdd", db_name=db_name)
            .start()
            .step()
            .invokeParticipant("CreateOrder", d)
            .withCompensation("DeleteOrder", e)
            .onReply(f)
            .execute()
        )

        assert s.get_db_state() is None


def test_saga_async_callbacks_ko():
    with TemporaryDirectory() as db_name:
        s = (
            Saga("OrdersAdd", db_name=db_name)
            .start()
            .step()
            .invokeParticipant("Shipping", a)
            .withCompensation("DeleteOrder", b)
            .onReply(c)
            .execute()
        )

        state = s.get_db_state()

        assert state is not None
        assert list(state["operations"].values())[0]["error"] == "invokeParticipantTest exception"


def test_saga_sync_callbacks_ko():
    with TemporaryDirectory() as db_name:
        s = (
            Saga("OrdersAdd", db_name=db_name)
            .start()
            .step()
            .invokeParticipant("Shipping", d)
            .withCompensation("DeleteOrder", e)
            .onReply(f)
            .execute()
        )

        state = s.get_db_state()

        assert state is not None
        assert list(state["operations"].values())[0]["error"] == "invokeParticipantTest exception"


def test_saga_correct():
    with TemporaryDirectory() as db_name:
        (
            Saga("OrdersAdd", db_name=db_name)
            .start()
            .step()
            .invokeParticipant("CreateOrder", create_order_callback)
            .withCompensation("DeleteOrder", delete_order_callback)
            .onReply(create_ticket_on_reply_callback)
            .step()
            .invokeParticipant("CreateTicket", create_ticket_callback)
            .onReply(create_ticket_on_reply_callback)
            .step()
            .invokeParticipant("Shopping")
            .withCompensation(["Failed", "BlockOrder"], shipping_callback)
            .execute()
        )


def test_saga_execute_all_compensations():
    with TemporaryDirectory() as db_name:
        (
            Saga("ItemsAdd", db_name=db_name)
            .start()
            .step()
            .invokeParticipant("CreateOrder", create_order_callback)
            .withCompensation("DeleteOrder", delete_order_callback)
            .onReply(create_ticket_on_reply_callback)
            .step()
            .invokeParticipant("CreateTicket")
            .onReply(create_ticket_on_reply_callback)
            .step()
            .invokeParticipant("Shipping")
            .withCompensation(["Failed", "BlockOrder"], shipping_callback)
            .execute()
        )


def test_saga_empty_step_must_throw_exception():
    with TemporaryDirectory() as db_name:
        with pytest.raises(Exception) as exc:
            (
                Saga("OrdersAdd2", db_name=db_name)
                .start()
                .step()
                .invokeParticipant("CreateOrder")
                .withCompensation("DeleteOrder")
                .withCompensation("DeleteOrder2")
                .step()
                .step()
                .invokeParticipant("CreateTicket")
                .onReply(create_ticket_on_reply_callback)
                .step()
                .invokeParticipant("VerifyConsumer")
                .execute()
            )

        assert "The step() cannot be empty." in str(exc.value)


def test_saga_wrong_step_action_must_throw_exception():
    with TemporaryDirectory() as db_name:
        with pytest.raises(Exception) as exc:
            (
                Saga("OrdersAdd3", db_name=db_name)
                .start()
                .step()
                .invokeParticipant("CreateOrder")
                .withCompensation("DeleteOrder")
                .withCompensation("DeleteOrder2")
                .step()
                .onReply(create_ticket_on_reply_callback)
                .step()
                .invokeParticipant("VerifyConsumer")
                .execute()
            )

        assert "The first method of the step must be .invokeParticipant(name, callback (optional))." in str(exc.value)
