from flask import Flask
from flask_socketio import SocketIO, join_room
from os import environ
import sys
from json import loads, dumps
import pika

# Initialization of Flask

app = Flask(__name__)
app.config["SECRET_KEY"] = environ.get("FLASK_SECRET_KEY")
rabbitmq_host = environ.get("RABBITMQ_HOST")
socketio = SocketIO(app, cors_allowed_origins="*")


def connection_handler(host: str) -> pika.adapters.blocking_connection.BlockingChannel:
    """
    Connection handler to the RabbitMQ service
    :param host: RabbitMQ host
    :return: a channel to communicate (writing/reading) to the queue
    """
    print(f"Connecting to ACMESky-Backend [host = {host}]...")
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        channel = connection.channel()
        print("...CONNECTED!")

        return channel
    except pika.exceptions.AMQPConnectionError:
        print("...ERROR!")
        print("ACMESky-Middleware: unable to connect to the ACMESky message queue.")
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(0)


def queue_selection(
        channel: pika.adapters.blocking_connection.BlockingChannel,
        context,
        queue_name: str,
):
    """
    Using the flask context, define a message handler for the message received by the RabbitMQ queue, when a message is
    received, it is converted to JSON and send through the WebSocket to the Client
    :param channel: the RabbitMQ channel from which receive the messages
    :param context: the flask socket-io context to use (necessary to send the message to the right client)
    :param queue_name: the queue name from which read the RabbitMQ messages
    """
    with context:
        def message_handler(ch, method, properties, body: bytes):
            with context:
                json = loads(body)
                socketio.send(dumps(json), json=True, room=queue_name)

        channel.queue_declare(queue=queue_name, durable=True)

        channel.basic_consume(
            queue=queue_name, on_message_callback=message_handler, auto_ack=True
        )

        channel.start_consuming()


# Flask

@socketio.on('join')
def on_join(room):
    """
    Method called when a new client request to connect through WebSocket
    :param room: room used to communicate with the client
    """
    join_room(room)
    queue_selection(
        channel=connection_handler(rabbitmq_host),
        context=app.app_context(),
        queue_name=room
    )


if __name__ == "__main__":
    host = environ.get("MIDDLEWARE_HOST", "0.0.0.0")
    port = environ.get("MIDDLEWARE_PORT", "8080")
    socketio.run(app, host=host, port=port)
