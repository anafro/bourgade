import asyncio
import json
import logging
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from time import time
from typing import Any, Protocol, cast

from aio_pika import ExchangeType, Message, connect_robust
from aio_pika.abc import (
    AbstractChannel,
    AbstractConnection,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractMessage,
    AbstractQueue,
    AbstractRobustConnection,
)
from reification import Reified

from bourgade.utils.dicts import optional_entry

logger: logging.Logger = logging.getLogger(__name__)


class AllCatchEventHandler(Protocol):
    def __call__(self, event_name: str, message_bytes: bytes) -> None: ...


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

    async def trigger(self, event_bus: "EventBus", message: bytes) -> None:
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
        await self.handle(event=event)

    @abstractmethod
    async def handle(self, event: E) -> None:
        """
        Handles the event when triggered.
        Override this method to define handling logic.

        :param E event: The event to handle
        """
        ...


@dataclass
class EventBus:
    """
    Connects to RabbitMQ, declares its abstractions,
    and manages RabbitMQ messages, passing them to handlers.
    """

    all_catch_event_handler: AllCatchEventHandler | None
    event_handlers: dict[str, EventHandler["Event"]]
    connection: AbstractConnection
    channel: AbstractChannel
    exchange: AbstractExchange
    queue: AbstractQueue

    @staticmethod
    async def create(
        host: str,
        username: str,
        password: str,
        exchange_name: str,
        queue_name: str,
        *,
        connection_delay: int = 0,
        connection_retries: int = 10,
        connection_retry_interval: int = 3,
    ) -> "EventBus":
        """
        Creates a new event bus.

        :param str host: The RabbitMQ host
        :param str username: The RabbitMQ username
        :param str password: The RabbitMQ password
        :param str exchange_name: The RabbitMQ exchange name containing events across the infrastructure
        :param str host: The RabbitMQ queue name consuming events within the app
        """
        await asyncio.sleep(connection_delay)
        while (connection_retries := connection_retries - 1) > 0:
            try:
                connection: AbstractRobustConnection = await connect_robust(
                    host=host, login=username, password=password
                )
                channel: AbstractChannel = await connection.channel()
                _ = await channel.set_qos(prefetch_count=1)
                exchange: AbstractExchange = await channel.declare_exchange(
                    name=exchange_name,
                    type=ExchangeType.TOPIC,
                    passive=False,
                    durable=True,
                    auto_delete=False,
                )
                queue: AbstractQueue = await channel.declare_queue(
                    name=queue_name, auto_delete=True
                )

                return EventBus(
                    event_handlers={},
                    all_catch_event_handler=None,
                    connection=connection,
                    channel=channel,
                    exchange=exchange,
                    queue=queue,
                )
            except Exception:
                await asyncio.sleep(connection_retry_interval)

        raise ValueError(
            "Bourgade connection to RMQ failed after several retries."
        ) from sys.last_exc

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

    def set_all_catch_handler(
        self, all_catch_event_handler: AllCatchEventHandler
    ) -> None:
        self.all_catch_event_handler = all_catch_event_handler

    async def start_listening(self) -> None:
        """
        Starts listening for RabbitMQ messages.
        This method is blocking.
        """
        _ = await self.queue.bind(exchange=self.exchange, routing_key="*")
        logger.info("Bourgade is listening for events...")
        async with self.queue.iterator() as queue_iterator:
            async for amqp_message in queue_iterator:
                async with amqp_message.process():
                    await self.__consume(amqp_message=amqp_message)

    async def dispatch(self, event: "Event") -> None:
        """
        Dispatches an event to RabbitMQ exchange.

        :param Event event: The event to dispatch
        """
        await self.dispatch_raw(
            tag=event.get_event_name(), message_bytes=event.serialize()
        )

    async def dispatch_raw(
        self,
        tag: str,
        message_bytes: bytes,
        content_type: str = "application/json",
        content_encoding: str = "utf-8",
    ) -> None:
        """
        Dispatches a message with a tag, and bytes.
        Use it to avoid using event abstractions for more complex logic.

        :param str tag: The tag string for the message
        :param bytes message_bytes: The message content
        """

        logger.info("[SEND] %s", tag)
        amqp_message: AbstractMessage = Message(
            body=message_bytes,
            content_type=content_type,
            content_encoding=content_encoding,
        )
        _ = await self.exchange.publish(
            message=amqp_message,
            routing_key=tag,
            mandatory=False,
        )

    async def __consume(
        self,
        amqp_message: AbstractIncomingMessage,
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
        routing_key: str | None = amqp_message.routing_key
        message: bytes = amqp_message.body

        if routing_key is None:
            raise ValueError("Consumed message does not have a routing key.")

        logger.info("[RECV] %s", routing_key)

        try:
            if routing_key in self.event_handlers:
                event_handler: EventHandler[Event] = self.event_handlers[routing_key]
                await event_handler.trigger(event_bus=self, message=message)
            elif self.all_catch_event_handler is not None:
                self.all_catch_event_handler(
                    event_name=routing_key, message_bytes=message
                )
            else:
                raise ValueError(f"There is no event handler for '{routing_key}'.")

            await amqp_message.ack()
        except Exception as exception:
            await amqp_message.nack()
            logger.error(
                msg="Event is NACK because of an exception.", exc_info=exception
            )


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
