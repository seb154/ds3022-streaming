"""Bluesky posts -> Kafka topic "bluesky_posts".

  python producer.py        # PUSH: new and deleted posts from the Bluesky Jetstream WebSocket

Each post is sent as one Kafka record with the account id (did) as the key. If the connection 
drops, it waits 5 second and reconnects.

Adapted from the class script quakes_producer.py, probe.py, and Session 1 worksheet. 
"""
import asyncio
import json
import logging

from quixstreams import Application
from tornado.httpclient import HTTPRequest
from tornado.websocket import websocket_connect

BROKER = "127.0.0.1:19092"
TOPIC = "bluesky_posts"
WSS_URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
USER_AGENT = "DS3022-Project3-producer/1.0 (UVA student project)"

def send(producer, message):
    """One Kafka record per message. Key = the account id (did)"""
    key = message["data"].get("did") #returns None instead of crashing if missing
    if key is None:
        logging.warning("Meassage without a did: sent without a key")
    producer.produce(topic=TOPIC, key=key, value=json.dumps(message))


async def live(producer):
    """PUSH over a WebSocket, with a reconnect loop. """
    sent = 0 
    while True:            #outer loop: reconnect forever 
        try:
            request = HTTPRequest(WSS_URL, headers={"User-Agent": USER_AGENT})
            ws = await websocket_connect(request, ping_interval=15)
            logging.info("Connected to Bluesky Jetstream")
            while True:            #inner loop: read messages 
                text = await ws.read_message()
                if text is None:
                    logging.warning("The server closed the connection")
                    break
                try:
                    send(producer, {"action": "live", "data": json.loads(text)})
                    sent += 1
                    if sent % 1000 == 0:      #one log line per 1000 messages 
                        logging.info("%d messages sent to %s", sent, TOPIC)
                except (ValueError, KeyError, TypeError, AttributeError) as e:
                    logging.warning("skipped a message could not read: %r", e)
        except Exception as e:
            logging.warning("Connection failed: %r", e)
        logging.info("Reconnecting in 5 s")
        await asyncio.sleep(5)

def main():
    app = Application(broker_address=BROKER, loglevel="WARNING")
    with app.get_producer() as producer:
        asyncio.run(live(producer))


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    try:
        main()
    except KeyboardInterrupt:
        pass
