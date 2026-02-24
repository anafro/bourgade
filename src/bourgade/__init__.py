from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import partial
import logging
from pika import BlockingConnection, ConnectionParameters, PlainCredentials
from pika.adapters.blocking_connection import BlockingChannel
from pika.exchange_type import ExchangeType
from pika.spec import Basic, BasicProperties
from typing import Any, cast
import json

from reification import Reified


logger: logging.Logger = logging.getLogger(__name__)


class EventHandler[E: Event = Event](ABC, Reified):
    """
    A base of all classes handling events.
    Specify event type in generic type (see Event class docs).
    Write what your app should do on event in overriden `handle` method.
    After creating a handler, add it to the event bus.
    """

    @classmethod
    def get_event_type(cls) -> type[E]:
        """
        Returns a type of the event this handler handles.
        Used for automagical event creation keeping event hydration
        outside Event class.

        :returns: The exact event type.
        """
        return cast(type[E], cls.targ)

    def trigger(
        self, event_bus: "EventBus", deliver: Basic.Deliver, message: bytes
    ) -> None:
        """
        Builds, and hydrates a new event object from RabbitMQ deliver,
        and triggers `handle` within this handler.

        :param EventBus event_bus: The event bus containing this handler
        :param Basic.Deliver deliver: The RabbitMQ deliver object considered to be an event for the handler
        :param bytes message: The RabbitMQ message bytes for event hydration
        """
        event_class: type[E] = self.get_event_type()
        event = event_class(event_bus, deliver=deliver, message=message)
        event.hydrate()
        self.handle(event=event)

    @abstractmethod
    def handle(self, event: E) -> None:
        """
        Handles the event when triggered.
        Override this method to define handling logic.

        :param E event: The event to handle
        """
        ...


class EventBus:
    """
    Connects to RabbitMQ, declares its abstractions,
    and manages RabbitMQ messages, passing them to handlers.
    """

    event_handlers: dict[str, EventHandler["Event"]]
    connection: BlockingConnection
    channel: BlockingChannel
    exchange_name: str
    queue_name: str

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        exchange_name: str,
        queue_name: str,
    ) -> None:
        """
        Creates a new event bus.

        :param str host: The RabbitMQ host
        :param str username: The RabbitMQ username
        :param str password: The RabbitMQ password
        :param str exchange_name: The RabbitMQ exchange name containing events across the infrastructure
        :param str host: The RabbitMQ queue name consuming events within the app
        """
        connection_credentials: PlainCredentials = PlainCredentials(
            username=username, password=password
        )
        connection_parameters: ConnectionParameters = ConnectionParameters(
            host=host, port=5672, credentials=connection_credentials
        )
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.connection = BlockingConnection(parameters=connection_parameters)
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=1)
        self.channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=ExchangeType.topic,
            passive=False,
            durable=True,
            auto_delete=False,
        )
        self.channel.queue_declare(queue=queue_name, auto_delete=True)
        self.event_handlers = {}

    def register_handler[E: Event](self, event_handler: EventHandler[E]) -> None:
        """
        Registers a new handler, so when bus starts listening to messages,
        it could be recognized by the bus, and got triggered by events.

        :param EventHandler[E] event_handler: The event handler to register
        """
        event_type: type[Event] = cast(type[Event], event_handler.targ)
        self.event_handlers[event_type.get_event_name()] = cast(
            EventHandler[Event], event_handler
        )

    def start_listening(self) -> None:
        """
        Starts listening for RabbitMQ messages.
        This method is blocking.
        """
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=partial(self.__consume, event_bus=self),
        )
        logger.info("Bourgade is listening for events...")
        self.channel.start_consuming()

    def dispatch(self, event: "Event") -> None:
        """
        Dispatches an event to RabbitMQ exchange.

        :param Event event: The event to dispatch
        """
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=event.get_event_name(),
            body=event.serialize(),
            mandatory=False,
        )

    @staticmethod
    def __consume(
        event_bus: "EventBus",
        event_handlers: dict[str, EventHandler["Event"]],
        channel: BlockingChannel,
        deliver: Basic.Deliver,
        _: BasicProperties,
        message: bytes,
    ) -> None:
        """
        Consumes a RabbitMQ message,
        finds a handler for an event this message represends,
        and triggers the handler to handle the event.
        Used in RabbitMQ `basic_consume` method, and never outside.

        :param EventBus event_bus: The event bus of the handler
        :param dict[str, EventHandler["Event"]] event_handlers: The dictionary of handlers (`routing_key`: `handler`)
        :param BlockingChannel channel: The RabbitMQ channel
        :param Basic.Deliver deliver: The RabbitMQ deliver
        :param BasicProperties _: The RabbitMQ properties, unused here
        :param bytes message: The message body
        """
        delivery_tag: int = cast(int, deliver.delivery_tag)
        routing_key: str = cast(str, deliver.routing_key)

        if routing_key not in event_handlers:
            raise ValueError(f"There is no event handler for '{routing_key}'.")

        event_handler: EventHandler[Event] = event_handlers[routing_key]

        try:
            event_handler.trigger(event_bus=event_bus, deliver=deliver, message=message)
            channel.basic_ack(delivery_tag=delivery_tag)
        except Exception:
            channel.basic_nack(delivery_tag=delivery_tag)


class Event(ABC):
    """
    A base for all events.
    Create a new class, override all abstract methods,
    and see the abstract methods docs.
    """

    event_bus: EventBus
    deliver: Basic.Deliver
    message: bytes

    @abstractmethod
    def hydrate(self) -> None:
        """
        Fills the event with data received from RabbitMQ.
        Depending on the format you chose for the event,
        use `string()`, `json()` methods, and map data from the message
        into the class fields you defined.

        E.g. you have an event `user.created` with user info, for example id, and name.
        If you decided to use JSON as an event format, call `body = self.json()`,
        and pass `self.id = int(body.id)`, `self.name = str(body.name)`.
        So in handlers, you could access fields like `event.id`, or `event.name`.
        """
        ...

    @abstractmethod
    def serialize(self) -> bytes:
        """
        Serializes the field back bytes.
        This method is the opposide of `hydrate`.

        :returns bytes: The serialized event as RabbitMQ message bytes
        """
        ...

    @staticmethod
    @abstractmethod
    def get_event_name() -> str:
        """
        Returns an event name.
        The event name is used as routing keys in RabbitMQ.
        If your class is named `UserCreatedEvent`, return "user.created" from here.

        :returns str: The event name
        """
        ...

    def string(self) -> str:
        """
        Decodes RabbitMQ message as a UTF-8 string.

        :returns str: A message as a string
        """
        return self.message.decode(encoding="utf-8")

    def json(self) -> dict[str, Any]:
        """
        Decodes RabbitMQ message as a JSON payload.

        :returns dict[str, Any]: A message as a JSON payload
        """
        return json.loads(self.string())
