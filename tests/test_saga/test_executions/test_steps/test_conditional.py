import unittest
from contextlib import (
    suppress,
)
from unittest.mock import (
    patch,
)

from minos.saga import (
    ConditionalSagaStep,
    ConditionalSagaStepExecution,
    ElseThenAlternative,
    IfThenAlternative,
    Saga,
    SagaContext,
    SagaFailedExecutionStepException,
    SagaPausedExecutionStepException,
    SagaRollbackExecutionStepException,
    SagaStepExecution,
    SagaStepStatus,
)
from tests.utils import (
    Foo,
    NaiveBroker,
    commit_callback_raises,
    fake_reply,
    handle_order_success,
    handle_ticket_success,
    handle_ticket_success_raises,
    send_create_order,
    send_create_ticket,
    send_delete_ticket,
)


def _is_one(context):
    return context["option"] == 1


def _is_two(context):
    return context["option"] == 2


class TestConditionalSageStepExecution(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.broker = NaiveBroker()

        self.definition = ConditionalSagaStep(
            [
                IfThenAlternative(
                    _is_one, Saga().remote_step(send_create_order).on_success(handle_order_success).commit()
                ),
                IfThenAlternative(
                    _is_two,
                    Saga()
                    .remote_step(send_create_order)
                    .on_success(handle_ticket_success_raises)
                    .on_failure(send_delete_ticket)
                    .commit(),
                ),
            ],
            ElseThenAlternative(
                (
                    Saga()
                    .remote_step(send_create_ticket)
                    .on_success(handle_ticket_success)
                    .on_failure(send_delete_ticket)
                    .commit(commit_callback_raises)
                )
            ),
        )
        # noinspection PyTypeChecker
        self.execution: ConditionalSagaStepExecution = SagaStepExecution.from_definition(self.definition)

    async def test_execute(self):
        context = SagaContext(option=1)

        with self.assertRaises(SagaPausedExecutionStepException):
            context = await self.execution.execute(context, broker=self.broker)
        self.assertEqual(SagaStepStatus.PausedByOnExecute, self.execution.status)
        self.assertEqual(SagaContext(option=1), context)

        reply = fake_reply(Foo("order"))
        context = await self.execution.execute(context, reply=reply, broker=self.broker)
        self.assertEqual(SagaStepStatus.Finished, self.execution.status)
        self.assertEqual(SagaContext(option=1, order=Foo("order")), context)

    async def test_execute_raises_step(self):
        context = SagaContext(option=2)

        with self.assertRaises(SagaPausedExecutionStepException):
            context = await self.execution.execute(context, broker=self.broker)
        self.assertEqual(SagaStepStatus.PausedByOnExecute, self.execution.status)
        self.assertEqual(SagaContext(option=2), context)

        reply = fake_reply(Foo("ticket"))
        with patch("minos.saga.SagaExecution.rollback") as mock:
            with self.assertRaises(SagaFailedExecutionStepException):
                context = await self.execution.execute(context, reply=reply, broker=self.broker)
            self.assertEqual(SagaStepStatus.ErroredByOnExecute, self.execution.status)
            self.assertEqual(SagaContext(option=2), context)
            self.assertEqual(1, mock.call_count)

    async def test_execute_raises_commit(self):
        context = SagaContext(option=3)

        with self.assertRaises(SagaPausedExecutionStepException):
            context = await self.execution.execute(context, broker=self.broker)
        self.assertEqual(SagaStepStatus.PausedByOnExecute, self.execution.status)
        self.assertEqual(SagaContext(option=3), context)

        reply = fake_reply(Foo("ticket"))
        with patch("minos.saga.SagaExecution.rollback") as mock:
            with self.assertRaises(SagaFailedExecutionStepException):
                context = await self.execution.execute(context, reply=reply, broker=self.broker)
            self.assertEqual(SagaStepStatus.ErroredByOnExecute, self.execution.status)
            self.assertEqual(SagaContext(option=3), context)
        self.assertEqual(1, mock.call_count)

    async def test_execute_empty(self):
        execution = ConditionalSagaStepExecution(ConditionalSagaStep())
        context = await execution.execute(SagaContext(one=1))
        self.assertEqual(SagaContext(one=1), context)
        self.assertEqual(SagaStepStatus.Finished, execution.status)

    async def test_rollback(self):
        with suppress(SagaPausedExecutionStepException):
            await self.execution.execute(SagaContext(option=1), broker=self.broker)
        await self.execution.execute(SagaContext(), reply=fake_reply(Foo("order")), broker=self.broker)
        with patch("minos.saga.SagaExecution.rollback") as mock:
            await self.execution.rollback(SagaContext(), broker=self.broker)
        self.assertEqual(1, mock.call_count)

    async def test_rollback_raises_create(self):
        with self.assertRaises(SagaRollbackExecutionStepException):
            await self.execution.rollback(SagaContext())

    async def test_rollback_raises_already(self):
        with suppress(SagaPausedExecutionStepException):
            await self.execution.execute(SagaContext(option=1), broker=self.broker)
        await self.execution.execute(SagaContext(), reply=fake_reply(Foo("order")), broker=self.broker)
        await self.execution.rollback(SagaContext(), broker=self.broker)
        with self.assertRaises(SagaRollbackExecutionStepException):
            await self.execution.rollback(SagaContext(), broker=self.broker)

    def test_raw_created(self):
        expected = {
            "already_rollback": False,
            "cls": "minos.saga.executions.steps.conditional.ConditionalSagaStepExecution",
            "definition": self.definition.raw,
            "inner": None,
            "status": "created",
        }
        self.assertEqual(expected, self.execution.raw)

    async def test_raw_paused(self):
        with suppress(SagaPausedExecutionStepException):
            await self.execution.execute(SagaContext(option=1), broker=self.broker)

        expected = {
            "already_rollback": False,
            "cls": "minos.saga.executions.steps.conditional.ConditionalSagaStepExecution",
            "definition": self.definition.raw,
            "inner": {
                "context": SagaContext(option=1).avro_str,
                "already_rollback": False,
                "definition": self.execution.inner.definition.raw,
                "executed_steps": [],
                "paused_step": {
                    "already_rollback": False,
                    "cls": "minos.saga.executions.steps.remote.RemoteSagaStepExecution",
                    "definition": {
                        "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                        "on_error": None,
                        "on_execute": {"callback": "tests.utils.send_create_order"},
                        "on_failure": None,
                        "on_success": {"callback": "tests.utils.handle_order_success"},
                    },
                    "status": "paused-by-on-execute",
                },
                "status": "paused",
                "uuid": str(self.execution.inner.uuid),
            },
            "status": "paused-by-on-execute",
        }
        observed = self.execution.raw

        self.assertEqual(
            SagaContext.from_avro_str(expected["inner"].pop("context")),
            SagaContext.from_avro_str(observed["inner"].pop("context")),
        )
        self.assertEqual(expected, observed)

    async def test_raw_finished(self):
        with suppress(SagaPausedExecutionStepException):
            await self.execution.execute(SagaContext(option=1), broker=self.broker)
        await self.execution.execute(SagaContext(), reply=fake_reply(Foo("order")), broker=self.broker)

        expected = {
            "already_rollback": False,
            "cls": "minos.saga.executions.steps.conditional.ConditionalSagaStepExecution",
            "definition": self.definition.raw,
            "inner": {
                "context": SagaContext(option=1, order=Foo(foo="order")).avro_str,
                "already_rollback": False,
                "definition": self.execution.inner.definition.raw,
                "executed_steps": [
                    {
                        "already_rollback": False,
                        "cls": "minos.saga.executions.steps.remote.RemoteSagaStepExecution",
                        "definition": {
                            "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                            "on_error": None,
                            "on_execute": {"callback": "tests.utils.send_create_order"},
                            "on_failure": None,
                            "on_success": {"callback": "tests.utils.handle_order_success"},
                        },
                        "status": "finished",
                    }
                ],
                "paused_step": None,
                "status": "finished",
                "uuid": str(self.execution.inner.uuid),
            },
            "status": "finished",
        }
        observed = self.execution.raw

        self.assertEqual(
            SagaContext.from_avro_str(expected["inner"].pop("context")),
            SagaContext.from_avro_str(observed["inner"].pop("context")),
        )
        self.assertEqual(expected, observed)

    async def test_raw_from_raw(self):
        with suppress(SagaPausedExecutionStepException):
            await self.execution.execute(SagaContext(option=1), broker=self.broker)

        another = SagaStepExecution.from_raw(self.execution.raw)
        self.assertEqual(self.execution, another)


if __name__ == "__main__":
    unittest.main()