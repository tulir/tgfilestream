import asyncio
import sys
import os

from telethon import TelegramClient, events
from aiohttp import web

try:
    api_id = int(os.environ["TG_API_ID"])
    api_hash = os.environ["TG_API_HASH"]
except (KeyError, ValueError):
    print("Please set the TG_API_ID and TG_API_HASH environment variables correctly")
    sys.exit(1)

session_name = os.environ.get("TG_SESSION_NAME", "tgfilestream")
loop = asyncio.get_event_loop()
client = TelegramClient(session_name, api_id, api_hash, loop=loop)
routes = web.RouteTableDef()


@client.on(events.NewMessage)
async def handle_message(evt: events.NewMessage.Event) -> None:
    pass


@routes.get(r"/{id:\d+}/{name}")
async def handle_request(req: web.Request) -> web.Response:
    msg_id = int(req.match_info["id"])
    file_name = req.match_info["name"]
    return web.Response(status=501)


server = web.Application()
server.add_routes(routes)

client.start()
web.run_app(server)
client.disconnect()
