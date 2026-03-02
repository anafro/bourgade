import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import partial
from time import time, sleep
from typing import Any, cast

from pika import BlockingConnection, ConnectionParameters, PlainCredentials
from pika.adapters.blocking_connection import BlockingChannel
from pika.exchange_type import ExchangeType
from pika.spec import Basic, BasicProperties
from pika.exceptions import AMQPConnectionError
from reification import Reified

from bourgade.utils.dicts import optional_entry

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

    def trigger(self, event_bus: "EventBus", message: bytes) -> None:
        """
        Builds, and hydrates a new event object from RabbitMQ deliver,
        and triggers `handle` within this handler.

        :param EventBus event_bus: The event bus containing this handler
        :param Basic.Deliver deliver: The RabbitMQ deliver object considered to be an event for the handler
        :param bytes message: The RabbitMQ message bytes for event hydration
        """
        ThisEvent: type[E] = self.get_event_type()
        happened_at: int = int(time() * 1000)
        event = ThisEvent(event_bus=event_bus, happened_at=happened_at)
        event.hydrate(message=message)
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
        *,
        connection_delay: int = 0,
        connection_retries: int = 10,
        connection_retry_interval: int = 3,
    ) -> None:
        """
        Creates a new event bus.

        :param str host: The RabbitMQ host
        :param str username: The RabbitMQ username
        :param str password: The RabbitMQ password
        :param str exchange_name: The RabbitMQ exchange name containing events across the infrastructure
        :param str host: The RabbitMQ queue name consuming events within the app
        """
        sleep(connection_delay)
        while (connection_retries := connection_retries - 1) > 0:
            try:
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
            except AMQPConnectionError:
                sleep(connection_retry_interval)

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
        self.dispatch_raw(tag=event.get_event_name(), message_bytes=event.serialize())

    def dispatch_raw(self, tag: str, message_bytes: bytes) -> None:
        """
        Dispatches a message with a tag, and bytes.
        Use it to avoid using event abstractions for more complex logic.

        :param str tag: The tag string for the message
        :param bytes message_bytes: The message content
        """
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=tag,
            body=message_bytes,
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
            event_handler.trigger(event_bus=event_bus, message=message)
            channel.basic_ack(delivery_tag=delivery_tag)
        except Exception:
            channel.basic_nack(delivery_tag=delivery_tag)


class Event(ABC):
    """
    Events in Bourgade are Json-based.
    So to use Bourgade events, you must implement get/set contents of the object.
    """

    event_bus: EventBus
    happened_at: int
    sid: str | None

    def __init__(self, event_bus: EventBus, happened_at: int) -> None:
        self.event_bus = event_bus
        self.happened_at = happened_at

    @abstractmethod
    def get_content_as_dict(self) -> dict[str, Any]: ...

    @abstractmethod
    def set_content_from_dict(self, content: dict[str, Any]) -> None: ...

    def hydrate(self, message: bytes) -> None:
        payload: dict[str, Any] = json.loads(message)
        header: dict[str, Any] = payload["header"]
        content: dict[str, Any] = payload["content"]
        self.sid = header["sid"]
        self.set_content_from_dict(content=content)

    def serialize(self) -> bytes:
        return json.dumps(
            {
                "header": {
                    "happenedAt": self.happened_at,
                    **optional_entry("sid", self.sid),
                },
                "content": self.get_content_as_dict(),
            }
        ).encode("utf-8")

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

    @classmethod
    def create(cls, event_bus: EventBus, content: dict[str, Any]) -> "Event":
        happened_at: int = int(time() * 1000)
        event = cls(event_bus=event_bus, happened_at=happened_at)
        event.set_content_from_dict(content=content)
        return event
