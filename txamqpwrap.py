from twisted.internet.defer import inlineCallbacks, CancelledError
from twisted.internet import reactor, defer
from twisted.internet.protocol import ClientCreator

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
from txamqp.content import Content
import txamqp.spec


NON_PERSISTENT = 1
PERSISTENT = 2


class AMQPConfig:
    def __init__(self, spec, vhost="/", host="localhost", port=5672, username="guest", password="guest"):
        self._spec = spec
        self._vhost = vhost
        self._host = host
        self._port = port
        self._username = username
        self._password = password

    def spec(self):
        return self._spec

    def vhost(self):
        return self._vhost

    def host(self):
        return self._host

    def port(self):
        return self._port

    def username(self):
        return self._username

    def password(self):
        return self._password


class AMQPClient:
    def __init__(self, config, channel_num, consumer_tag=None):
        if not isinstance(config, AMQPConfig):
            raise ConfigException("config should be instance of AMQPConfig")

        self.conn = None
        self.d = None
        self.callback = None
        self.channel_num = channel_num
        self.channel = None
        self.consumer_tag = consumer_tag
        self.queues = {}
        self.connected = False
        self.config = config

    @inlineCallbacks
    def connect(self):
        spec = txamqp.spec.load(self.config.spec())
        delegate = TwistedDelegate()
        d = ClientCreator(reactor, AMQClient, delegate=delegate, vhost=self.config.vhost(), spec=spec)\
            .connectTCP(self.config.host(), self.config.port())
        d.addCallback(self.got_connection)
        self.d = defer.Deferred()
        yield self.d

    @inlineCallbacks
    def got_connection(self, conn):
        self.conn = conn
        yield conn.authenticate(self.config.username(), self.config.password())
        self.channel = yield self.conn.channel(self.channel_num)
        yield self.channel.channel_open()
        self.connected = True
        self.d.callback(0)

    @inlineCallbacks
    def create_queue(self, name, durable=True, exclusive=False, auto_delete=False):
        if not self.connected:
            raise NotConnectedException("AMQP client is not connected")

        yield self.channel.queue_declare(queue=name, durable=durable, exclusive=exclusive, auto_delete=auto_delete)
        self.queues[name] = Queue(name, None)

    @inlineCallbacks
    def create_exchange(self, name, type="direct", durable=True):
        if not self.connected:
            raise NotConnectedException("AMQP client is not connected")

        yield self.channel.exchange_declare(exchange=name, type=type, durable=durable)

    @inlineCallbacks
    def bind(self, queue, exchange, routing_key):
        if not self.connected:
            raise NotConnectedException("AMQP client is not connected")
        if queue not in self.queues:
            raise QueueNotFoundException("Couldn't find queue: " + queue)

        yield self.channel.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key)

    @inlineCallbacks
    def unbind(self, queue, exchange, routing_key):
        if not self.connected:
            raise NotConnectedException("AMQP client is not connected")
        if queue not in self.queues:
            raise QueueNotFoundException("Couldn't find queue: " + queue)

        yield self.channel.queue_unbind(queue=queue, exchange=exchange, routing_key=routing_key)

    @inlineCallbacks
    def listen(self, queue, callback, no_ack=True):
        if not self.connected:
            raise NotConnectedException("AMQP client is not connected")

        if queue not in self.queues:
            raise QueueNotFoundException("Couldn't find queue: " + queue)

        yield self.channel.basic_consume(queue=queue, no_ack=no_ack, consumer_tag=self.consumer_tag)
        q = yield self.conn.queue(self.consumer_tag)
        self.queues[queue].set_queue(q)
        self.queues[queue].listen(callback)

    def is_listening(self, queue):
        if queue in self.queues:
            return self.queues[queue].is_listening()
        else:
            return False

    def is_connected(self):
        return self.connected

    def stop_listen(self, queue=None):
        if queue is None:
            for queue in self.queues.keys():
                self.queues[queue].stop_listen()
        else:
            if queue in self.queues:
                self.queues[queue].stop_listen()

    def send_message(self, exchange, routing_key, message, delivery_mode=PERSISTENT):
        if not self.connected:
            raise NotConnectedException("AMQP client is not connected")

        amqp_message = Content(message)
        amqp_message["delivery_mode"] = delivery_mode
        self.channel.basic_publish(exchange=exchange, content=amqp_message, routing_key=routing_key)

    @inlineCallbacks
    def disconnect(self):
        self.stop_listen()
        if self.consumer_tag is not None:
            yield self.channel.basic_cancel(self.consumer_tag)

        yield self.channel.channel_close()
        chan0 = yield self.conn.channel(0)
        yield chan0.connection_close()
        self.conn.transport.loseConnection()


class Queue:
    def __init__(self, name, queue):
        self.name = name
        self.callback = None
        self.listening = False
        self.queue = queue
        self.d = None

    def listen(self, callback):
        self.listening = True
        self.callback = callback
        self.d = self.queue.get()
        self.d.addCallbacks(self._on_message, self.canceled)

    def name(self):
        return self.name

    def queue(self):
        return self.queue

    def set_queue(self, queue):
        self.queue = queue

    def is_listening(self):
        return self.listening

    def canceled(self, error):
        error.trap(CancelledError)

    def stop_listen(self):
        if self.listening:
            self.listening = False
            self.d.cancel()

    def _on_message(self, msg):
        self.callback(msg)
        if self.listening:
            self.d = self.queue.get()
            self.d.addCallbacks(self._on_message, self.canceled)


class TxAmqpWrapException(Exception):
    pass


class ConfigException(TxAmqpWrapException):
    pass


class QueueNotFoundException(TxAmqpWrapException):
    pass


class NotConnectedException(TxAmqpWrapException):
    pass