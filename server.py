import asyncio
import json
import logging
import signal

import google.protobuf.message
import uvloop
import websockets

import events_pb2

USE_PROTOCOL_BUFFER = False


logging.basicConfig(level=logging.DEBUG)

CONNECTED = set()

async def process_protobuf_event(message: str):
    try:
        event = events_pb2.LiveAPIEvent()
        event.ParseFromString(message)
        # print(event)
    except google.protobuf.message.DecodeError:
        logging.error(f"unable to decode message: {message}")

    game_message_class = event.gameMessage.TypeName().split(".")[-1]
    game_message = getattr(events_pb2, game_message_class)()
    event.gameMessage.Unpack(game_message)
    print(game_message)
        
async def process_json_event(message: str):
    try:
        event = json.loads(message)
        print(event)
    except json.JSONDecodeError:
        logging.error(f"unable to decode message: {message}")

async def handler(websocket: websockets.WebSocketServerProtocol):
    CONNECTED.add(websocket)
    try:
        websockets.broadcast(CONNECTED, f"{websocket.remote_address} connected")
        if USE_PROTOCOL_BUFFER:
            async for message in websocket:
                await process_protobuf_event(message)
        else:
            async for message in websocket:
                await process_json_event(message)
    finally:
        CONNECTED.remove(websocket)
        websockets.broadcast(CONNECTED, f"{websocket.remote_address} disconnected")

async def main():
    loop = asyncio.get_running_loop()
    stop = loop.create_future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)

    print("Starting server... Press CTRL+C to stop.")
    async with websockets.serve(handler, "localhost", 7777):
        await stop
    
if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main(), debug=True)
