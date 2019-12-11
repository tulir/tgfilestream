from typing import Tuple, Union, AsyncIterable, cast
import logging
import asyncio
import sys
import os

from telethon import TelegramClient, events
from telethon.tl.custom import Message
from telethon.tl.types import TypeInputPeer, InputPeerChannel, InputPeerChat, InputPeerUser
from aiohttp import web
from yarl import URL

try:
    api_id = int(os.environ["TG_API_ID"])
    api_hash = os.environ["TG_API_HASH"]
except (KeyError, ValueError):
    print("Please set the TG_API_ID and TG_API_HASH environment variables correctly")
    sys.exit(1)

try:
    port = int(os.environ.get("PORT", "8080"))
except ValueError:
    port = -1
if not 1 <= port <= 65535:
    print("Please make sure the PORT environment variable is an integer between 1 and 65535")
    sys.exit(1)

debug = bool(os.environ.get("DEBUG"))
trust_headers = bool(os.environ.get("TRUST_FORWARD_HEADERS"))
host = os.environ.get("HOST", "localhost")
public_url = URL(os.environ.get("PUBLIC_URL", f"http://{host}:{port}"))
session_name = os.environ.get("TG_SESSION_NAME", "tgfilestream")
pack_bits = 32
pack_bit_mask = (1 << pack_bits) - 1

client = TelegramClient(session_name, api_id, api_hash)
routes = web.RouteTableDef()

log = logging.getLogger("tgfilestream")
logging.basicConfig(level=logging.DEBUG if debug else logging.INFO)
logging.getLogger("telethon").setLevel(logging.INFO if debug else logging.ERROR)

group_bit = 0b01
channel_bit = 0b10
chat_id_offset = 2
msg_id_offset = pack_bits + chat_id_offset


def pack_id(evt: events.NewMessage.Event) -> int:
    file_id = 0
    if evt.is_group:
        file_id |= group_bit
    if evt.is_channel:
        file_id |= channel_bit
    file_id |= evt.chat_id << chat_id_offset
    file_id |= evt.id << msg_id_offset
    return file_id


def unpack_id(file_id: int) -> Tuple[TypeInputPeer, int]:
    is_group = file_id & group_bit
    is_channel = file_id & channel_bit
    chat_id = file_id >> chat_id_offset & pack_bit_mask
    msg_id = file_id >> msg_id_offset & pack_bit_mask
    if is_channel:
        peer = InputPeerChannel(channel_id=chat_id, access_hash=0)
    elif is_group:
        peer = InputPeerChat(chat_id=chat_id)
    else:
        peer = InputPeerUser(user_id=chat_id, access_hash=0)
    return peer, msg_id


def get_file_name(message: Union[Message, events.NewMessage.Event]) -> str:
    if message.file.name:
        return message.file.name
    ext = message.file.ext or ""
    return f"{message.date.strftime('%Y-%m-%d_%H:%M:%S')}{ext}"


def get_requester_ip(req: web.Request) -> str:
    if trust_headers:
        try:
            return req.headers["X-Forwarded-For"]
        except KeyError:
            pass
    peername = req.transport.get_extra_info('peername')
    if peername is not None:
        return peername[0]


async def cut_first_chunk(iterable: AsyncIterable[bytes], cut: int) -> AsyncIterable[bytes]:
    first = True
    async for chunk in iterable:
        if first:
            chunk = chunk[cut:]
            first = False
        yield chunk


async def handle_request(req: web.Request, head: bool = False) -> web.Response:
    file_name = req.match_info["name"]
    file_id = int(req.match_info["id"])
    peer, msg_id = unpack_id(file_id)
    if not peer or not msg_id:
        return web.Response(status=404, text="404: Not Found")

    message = cast(Message, await client.get_messages(entity=peer, ids=msg_id))
    if not message or not message.file or get_file_name(message) != file_name:
        return web.Response(status=404, text="404: Not Found")

    offset = req.http_range.start or 0
    tg_offset = offset - offset % (2 ** 19)
    size = message.file.size

    if not head:
        log.info(
            f"Serving file in {message.id} (chat {message.chat_id}) to {get_requester_ip(req)}")
        body = client.iter_download(message.media, file_size=message.file.size, offset=tg_offset)
        body = cut_first_chunk(body, offset - tg_offset)
    else:
        body = None
    return web.Response(status=206 if offset else 200,
                        body=body,
                        headers={
                            "Content-Type": message.file.mime_type,
                            "Content-Range": f"bytes {offset}-{size}/{size}",
                            "Content-Length": str(size - offset),
                            "Content-Disposition": f'attachment; filename="{file_name}"',
                            "Accept-Ranges": "bytes",
                        })


@routes.head(r"/{id:\d+}/{name}")
async def handle_head_request(req: web.Request) -> web.Response:
    return await handle_request(req, head=True)


@routes.get(r"/{id:\d+}/{name}")
async def handle_get_request(req: web.Request) -> web.Response:
    return await handle_request(req, head=False)


@client.on(events.NewMessage)
async def handle_message(evt: events.NewMessage.Event) -> None:
    if not evt.is_private or not evt.file:
        return
    url = public_url / str(pack_id(evt)) / get_file_name(evt)
    await evt.reply(f"Link to download file: [{url}]({url})")
    log.info(f"Replied with link for {evt.id} to {evt.from_id} in {evt.chat_id}")
    log.debug(f"Link to {evt.id} in {evt.chat_id}: {url}")


server = web.Application()
server.add_routes(routes)
runner = web.AppRunner(server)

loop = asyncio.get_event_loop()


async def start() -> None:
    await client.start()

    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()


async def stop() -> None:
    await runner.cleanup()
    await client.disconnect()


try:
    loop.run_until_complete(start())
except Exception:
    log.fatal("Failed to initialize", exc_info=True)
    sys.exit(2)

log.info("Initialization complete")
log.debug(f"Listening at http://{host}:{port}")
log.debug(f"Public URL prefix is {public_url}")

try:
    loop.run_forever()
except KeyboardInterrupt:
    loop.run_until_complete(stop())
except Exception:
    log.fatal("Fatal error in event loop", exc_info=True)
    sys.exit(3)
