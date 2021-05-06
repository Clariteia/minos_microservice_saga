# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.

import unittest
from shutil import (
    rmtree,
)

from minos.saga import (
    MinosSagaException,
    Saga,
    SagaExecution,
)
from tests.callbacks import (
    create_ticket_on_reply_callback,
)
from tests.utils import (
    BASE_PATH,
)


class TestSaga(unittest.TestCase):
    DB_PATH = BASE_PATH / "test_db.lmdb"

    def tearDown(self) -> None:
        rmtree(self.DB_PATH, ignore_errors=True)

    def test_empty_step_must_throw_exception(self):
        with self.assertRaises(MinosSagaException) as exc:
            (
                Saga("OrdersAdd2", self.DB_PATH)
                .step()
                .invoke_participant("CreateOrder")
                .with_compensation("DeleteOrder")
                .with_compensation("DeleteOrder2")
                .step()
                .step()
                .invoke_participant("CreateTicket")
                .on_reply(create_ticket_on_reply_callback)
                .step()
                .invoke_participant("VerifyConsumer")
                .submit()
            )

            self.assertEqual("A 'SagaStep' can only define one 'with_compensation' method.", str(exc))

    def test_wrong_step_action_must_throw_exception(self):
        with self.assertRaises(MinosSagaException) as exc:
            (
                Saga("OrdersAdd3", self.DB_PATH)
                .step()
                .invoke_participant("CreateOrder")
                .with_compensation("DeleteOrder")
                .with_compensation("DeleteOrder2")
                .step()
                .on_reply(create_ticket_on_reply_callback)
                .step()
                .invoke_participant("VerifyConsumer")
                .submit()
            )

            self.assertEqual("A 'SagaStep' can only define one 'with_compensation' method.", str(exc))

    def test_build_execution(self):
        saga = (
            Saga("OrdersAdd3", self.DB_PATH)
            .step()
            .invoke_participant("CreateOrder")
            .with_compensation("DeleteOrder")
            .submit()
        )
        execution = saga.build_execution()
        self.assertIsInstance(execution, SagaExecution)


if __name__ == "__main__":
    unittest.main()
