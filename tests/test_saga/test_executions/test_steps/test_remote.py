import unittest
from unittest.mock import (
    AsyncMock,
)
from uuid import (
    uuid4,
)

from minos.common import (
    CommandStatus,
    MinosConfig,
)
from minos.saga import (
    RemoteSagaStep,
    RemoteSagaStepExecution,
    SagaContext,
    SagaFailedExecutionStepException,
    SagaPausedExecutionStepException,
    SagaRollbackExecutionStepException,
    SagaStepExecution,
    SagaStepStatus,
)
from tests.utils import (
    BASE_PATH,
    Foo,
    NaiveBroker,
    fake_reply,
    handle_ticket_error,
    handle_ticket_error_raises,
    handle_ticket_success,
    handle_ticket_success_raises,
    send_create_ticket,
    send_create_ticket_raises,
    send_delete_ticket,
)


class TestRemoteSagaStepExecution(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.config = MinosConfig(path=BASE_PATH / "config.yml")
        self.broker = NaiveBroker()
        self.execute_kwargs = {
            "definition_name": "FoodAdd",
            "execution_uuid": uuid4(),
            "broker": self.broker,
            "reply_topic": "FooAdd",
            "user": uuid4(),
        }

        self.publish_mock = AsyncMock()
        self.broker.send = self.publish_mock

    async def test_on_execute(self):
        step = RemoteSagaStep(send_create_ticket)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step)
        rollback_mock = AsyncMock()
        execution.rollback = rollback_mock

        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute(context, **self.execute_kwargs)

        self.assertEqual(1, self.publish_mock.call_count)
        self.assertEqual(SagaStepStatus.PausedByOnExecute, execution.status)
        self.assertEqual(0, rollback_mock.call_count)

    async def test_on_execute_raises(self):
        step = RemoteSagaStep(send_create_ticket_raises).on_failure(send_delete_ticket)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step)
        rollback_mock = AsyncMock()
        execution.rollback = rollback_mock

        with self.assertRaises(SagaFailedExecutionStepException):
            await execution.execute(context, **self.execute_kwargs)

        self.assertEqual(0, self.publish_mock.call_count)
        self.assertEqual(SagaStepStatus.ErroredOnExecute, execution.status)
        self.assertEqual(0, rollback_mock.call_count)

    async def test_errored_reply(self):
        step = RemoteSagaStep(send_create_ticket)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step, status=SagaStepStatus.PausedByOnExecute)
        rollback_mock = AsyncMock()
        execution.rollback = rollback_mock
        reply = fake_reply(status=CommandStatus.SYSTEM_ERROR)

        with self.assertRaises(SagaFailedExecutionStepException):
            await execution.execute(context, reply=reply, **self.execute_kwargs)

        self.assertEqual(SagaStepStatus.ErroredByOnExecute, execution.status)
        self.assertEqual(0, rollback_mock.call_count)

    async def test_on_success(self):
        step = RemoteSagaStep(send_create_ticket).on_success(handle_ticket_success)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step, status=SagaStepStatus.PausedByOnExecute)
        rollback_mock = AsyncMock()
        execution.rollback = rollback_mock
        reply = fake_reply(Foo("foo"), status=CommandStatus.SUCCESS)

        await execution.execute(context, reply=reply, **self.execute_kwargs)

        self.assertEqual(SagaStepStatus.Finished, execution.status)

    async def test_on_success_not_defined(self):
        step = RemoteSagaStep(send_create_ticket)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step, status=SagaStepStatus.PausedByOnExecute)
        rollback_mock = AsyncMock()
        execution.rollback = rollback_mock
        reply = fake_reply(Foo("foo"), status=CommandStatus.SUCCESS)

        await execution.execute(context, reply=reply, **self.execute_kwargs)

        self.assertEqual(SagaStepStatus.Finished, execution.status)
        self.assertEqual(0, rollback_mock.call_count)

    async def test_on_success_raises(self):
        step = RemoteSagaStep(send_create_ticket).on_success(handle_ticket_success_raises)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step, status=SagaStepStatus.PausedByOnExecute)
        rollback_mock = AsyncMock()
        execution.rollback = rollback_mock
        reply = fake_reply(Foo("foo"), status=CommandStatus.SUCCESS)

        with self.assertRaises(SagaFailedExecutionStepException):
            await execution.execute(context, reply=reply, **self.execute_kwargs)

        self.assertEqual(SagaStepStatus.ErroredOnSuccess, execution.status)
        self.assertEqual(1, rollback_mock.call_count)

    async def test_on_error(self):
        step = RemoteSagaStep(send_create_ticket).on_error(handle_ticket_error)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step, status=SagaStepStatus.PausedByOnExecute)
        rollback_mock = AsyncMock()
        execution.rollback = rollback_mock
        reply = fake_reply(Foo("foo"), status=CommandStatus.ERROR)

        await execution.execute(context, reply=reply, **self.execute_kwargs)

        self.assertEqual(SagaStepStatus.Finished, execution.status)
        self.assertEqual(0, rollback_mock.call_count)

    async def test_on_error_not_defined(self):
        step = RemoteSagaStep(send_create_ticket)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step, status=SagaStepStatus.PausedByOnExecute)
        rollback_mock = AsyncMock()
        execution.rollback = rollback_mock
        reply = fake_reply(Foo("foo"), status=CommandStatus.ERROR)

        await execution.execute(context, reply=reply, **self.execute_kwargs)

        self.assertEqual(SagaStepStatus.Finished, execution.status)
        self.assertEqual(0, rollback_mock.call_count)

    async def test_on_error_raises(self):
        step = RemoteSagaStep(send_create_ticket).on_error(handle_ticket_error_raises)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step, status=SagaStepStatus.PausedByOnExecute)
        rollback_mock = AsyncMock()
        execution.rollback = rollback_mock
        reply = fake_reply(Foo("foo"), status=CommandStatus.ERROR)

        with self.assertRaises(SagaFailedExecutionStepException):
            await execution.execute(context, reply=reply, **self.execute_kwargs)

        self.assertEqual(SagaStepStatus.ErroredOnError, execution.status)
        self.assertEqual(1, rollback_mock.call_count)

    async def test_rollback(self):
        step = RemoteSagaStep(send_create_ticket).on_failure(send_delete_ticket)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step)

        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute(context, **self.execute_kwargs)

        self.publish_mock.reset_mock()
        await execution.rollback(context, **self.execute_kwargs)
        self.assertEqual(1, self.publish_mock.call_count)

        self.publish_mock.reset_mock()
        with self.assertRaises(SagaRollbackExecutionStepException):
            await execution.rollback(context, **self.execute_kwargs)
        self.assertEqual(0, self.publish_mock.call_count)

    async def test_rollback_raises(self):
        step = RemoteSagaStep(send_create_ticket).on_failure(send_delete_ticket)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step)

        with self.assertRaises(SagaRollbackExecutionStepException):
            await execution.rollback(context, **self.execute_kwargs)

    def test_raw(self):
        definition = (
            RemoteSagaStep(send_create_ticket)
            .on_success(handle_ticket_success)
            .on_error(handle_ticket_error)
            .on_failure(send_delete_ticket)
        )
        execution = RemoteSagaStepExecution(definition)

        expected = {
            "already_rollback": False,
            "cls": "minos.saga.executions.steps.remote.RemoteSagaStepExecution",
            "definition": {
                "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                "on_execute": {"callback": "tests.utils.send_create_ticket"},
                "on_success": {"callback": "tests.utils.handle_ticket_success"},
                "on_error": {"callback": "tests.utils.handle_ticket_error"},
                "on_failure": {"callback": "tests.utils.send_delete_ticket"},
            },
            "status": "created",
        }
        self.assertEqual(expected, execution.raw)

    def test_from_raw(self):
        raw = {
            "already_rollback": False,
            "cls": "minos.saga.executions.steps.remote.RemoteSagaStepExecution",
            "definition": {
                "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                "on_execute": {"callback": "tests.utils.send_create_ticket"},
                "on_success": {"callback": "tests.utils.handle_ticket_success"},
                "on_error": {"callback": "tests.utils.handle_ticket_error"},
                "on_failure": {"callback": "tests.utils.send_delete_ticket"},
            },
            "status": "created",
        }
        expected = RemoteSagaStepExecution(
            RemoteSagaStep(send_create_ticket)
            .on_success(handle_ticket_success)
            .on_error(handle_ticket_error)
            .on_failure(send_delete_ticket)
        )
        observed = SagaStepExecution.from_raw(raw)
        self.assertEqual(expected, observed)

    def test_from_raw_already(self):
        expected = RemoteSagaStepExecution(
            RemoteSagaStep(send_create_ticket)
            .on_success(handle_ticket_success)
            .on_error(handle_ticket_error)
            .on_failure(send_delete_ticket)
        )
        observed = SagaStepExecution.from_raw(expected)
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()