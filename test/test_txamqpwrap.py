from txamqpwrap import AMQPClient, AMQPConfig
from txamqpwrap import NotConnectedException, QueueNotFoundException, ConfigException
from twisted.trial import unittest
from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks
import os

spec = os.path.dirname(os.path.realpath(__file__)) + "/amqp0-9-1.xml"


class AMQPSendTestCase(unittest.TestCase):
    def _receive(self, message):
        self.assertEqual(message.content.body, self.message_text)
        self.message_received = True
        self.d.callback(0)

    @inlineCallbacks
    def _send_messages(self):
        self.send_client.send_message(self.exchange_name, self.routing_key, self.message_text)
        self.d = defer.Deferred()
        yield self.d

    @inlineCallbacks
    def setUp(self):
        self.exchange_name = "common_amqp_test_exchange"
        self.queue_name = "common_amqp_test_queue"
        self.routing_key = "common_amqp_test_rk"
        self.consumer_tag = "common_amqp_test_consumer"
        self.message_text = "test_message"
        self.channel_num = 1
        self.d = None
        self.message_received = False

        self.recv_client = AMQPClient(AMQPConfig(spec=spec), self.channel_num, self.consumer_tag)
        self.send_client = AMQPClient(AMQPConfig(spec=spec), self.channel_num)

        yield self.recv_client.connect()
        yield self.recv_client.create_queue(self.queue_name)
        yield self.recv_client.create_exchange(self.exchange_name)
        yield self.recv_client.bind(self.queue_name, self.exchange_name, self.routing_key)
        yield self.recv_client.listen(self.queue_name, self._receive)
        yield self.send_client.connect()

    @inlineCallbacks
    def tearDown(self):
        self.assertTrue(self.message_received)
        yield self.send_client.disconnect()
        yield self.recv_client.disconnect()

    @inlineCallbacks
    def test_send(self):
        yield self._send_messages()


class AMQPExceptionsTestCase(unittest.TestCase):
    def setUp(self):
        self.exchange_name = "common_amqp_test_exchange"
        self.queue_name = "common_amqp_test_queue"
        self.routing_key = "common_amqp_test_rk"
        self.consumer_tag = "common_amqp_test_consumer"
        self.message_text = "test_message"
        self.channel_num = 2
        self.client = AMQPClient(AMQPConfig(spec=spec), self.channel_num, self.consumer_tag)

    @inlineCallbacks
    def tearDown(self):
        if self.client.is_connected():
            yield self.client.disconnect()

    @inlineCallbacks
    def test_not_connected_exceptions(self):
        yield self.assertFailure(self.client.create_exchange(self.exchange_name),
                                 NotConnectedException)
        yield self.assertFailure(self.client.create_queue(self.queue_name),
                                 NotConnectedException)
        yield self.assertFailure(self.client.bind(self.queue_name, self.exchange_name, self.routing_key),
                                 NotConnectedException)
        yield self.assertFailure(self.client.unbind(self.queue_name, self.exchange_name, self.routing_key),
                                 NotConnectedException)
        yield self.assertFailure(self.client.listen(self.queue_name, self._receive),
                                 NotConnectedException)
        yield self.assertRaises(NotConnectedException, self.client.send_message, self.exchange_name,
                                self.routing_key, self.message_text)

    @inlineCallbacks
    def test_queue_not_found_exceptions(self):
        yield self.client.connect()
        yield self.assertFailure(self.client.bind(self.queue_name, self.exchange_name, self.routing_key),
                                 QueueNotFoundException)
        yield self.assertFailure(self.client.unbind(self.queue_name, self.exchange_name, self.routing_key),
                                 QueueNotFoundException)
        yield self.assertFailure(self.client.listen(self.queue_name, self._receive),
                                 QueueNotFoundException)

    def test_config_exception(self):
        self.assertRaises(ConfigException, AMQPClient, None, self.channel_num)

    def _receive(self, message):
        pass