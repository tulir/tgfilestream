from typing import Tuple, Union, AsyncIterable, AsyncGenerator, AsyncContextManager, Dict, cast
from contextlib import asynccontextmanager
import logging
import asyncio
import math
import sys
import os

from telethon import TelegramClient, events, utils
from telethon.crypto import AuthKey
from telethon.network import MTProtoSender
from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest
from telethon.tl.functions.upload import GetFileRequest
from telethon.tl.types import (Document, InputFileLocation, InputDocumentFileLocation,
                               InputPhotoFileLocation, InputPeerPhotoFileLocation)
from telethon.tl.custom import Message
from telethon.tl.types import TypeInputPeer, InputPeerChannel, InputPeerChat, InputPeerUser
from aiohttp import web
from yarl import URL

TypeLocation = Union[Document, InputDocumentFileLocation, InputPeerPhotoFileLocation,
                     InputFileLocation, InputPhotoFileLocation]


class ParallelTransferrer:
    log: logging.Logger = logging.getLogger("tgfilestream.transfer")
    client: TelegramClient
    loop: asyncio.AbstractEventLoop
    dc_id: int
    auth_keys: Dict[int, AuthKey]
    connection_sem: Dict[int, asyncio.Semaphore]
    counter: int

    def __init__(self, client: TelegramClient) -> None:
        self.client = client
        self.loop = self.client.loop
        self.counter = 0
        self.auth_keys = {
            self.client.session.dc_id: self.client.session.auth_key,
        }
        self.connection_sem = {
            1: asyncio.Semaphore(value=20),
            2: asyncio.Semaphore(value=20),
            3: asyncio.Semaphore(value=20),
            4: asyncio.Semaphore(value=20),
            5: asyncio.Semaphore(value=20),
        }

    @asynccontextmanager
    async def _with_sender(self, dc_id: int) -> AsyncContextManager[MTProtoSender]:
        index = self.counter
        self.counter += 1
        async with self.connection_sem[dc_id]:
            dc = await self.client._get_dc(dc_id)
            sender = MTProtoSender(self.auth_keys.get(dc_id), self.loop, loggers=self.client._log)
            self.log.debug(f"Connecting MTProtoSender {index}")
            await sender.connect(self.client._connection(dc.ip_address, dc.port, dc.id,
                                                         loop=self.loop, loggers=self.client._log,
                                                         proxy=self.client._proxy))
            if dc_id not in self.auth_keys:
                self.log.debug(f"Exporting auth to DC {dc_id}")
                auth = await self.client(ExportAuthorizationRequest(dc_id))
                req = self.client._init_with(ImportAuthorizationRequest(
                    id=auth.id, bytes=auth.bytes
                ))
                await sender.send(req)
                self.auth_keys[dc_id] = sender.auth_key
            try:
                yield sender
            except (GeneratorExit, StopAsyncIteration, asyncio.CancelledError):
                pass
            self.log.debug(f"Disconnecting MTProtoSender {index}")
            await sender.disconnect()

    def can_download(self, file: TypeLocation) -> bool:
        dc_id, _ = utils.get_input_location(file)
        return not self.connection_sem[dc_id].locked()

    async def download(self, file: TypeLocation, file_size: int, offset: int, limit: int) -> AsyncGenerator[bytes, None]:
        dc_id, location = utils.get_input_location(file)
        part_size = 512 * 1024
        first_part_cut = offset % part_size
        first_part = math.floor(offset / part_size)
        last_part_cut = part_size - (limit % part_size)
        last_part = math.ceil(limit / part_size)
        part_count = math.ceil(file_size / part_size)
        self.log.debug(f"Starting parallel download: chunks {first_part}-{last_part}"
                       f" of {part_count} {location!s}")
        request = GetFileRequest(location, offset=first_part * part_size, limit=part_size)

        part = first_part
        async with self._with_sender(dc_id) as sender:
            while part <= last_part:
                result = await sender.send(request)
                request.offset += part_size
                if part == first_part:
                    yield result.bytes[first_part_cut:]
                elif part == last_part:
                    yield result.bytes[:last_part_cut]
                else:
                    yield result.bytes
                self.log.debug(f"Part {part}/{last_part} (total {part_count}) downloaded")
                part += 1
        self.log.debug("Parallel download finished")


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
transfer = ParallelTransferrer(client)
routes = web.RouteTableDef()

log = logging.getLogger("tgfilestream")
logging.basicConfig(level=logging.DEBUG if debug else logging.INFO)
logging.getLogger("telethon").setLevel(logging.ERROR)

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


async def handle_request(req: web.Request, head: bool = False) -> web.Response:
    file_name = req.match_info["name"]
    file_id = int(req.match_info["id"])
    peer, msg_id = unpack_id(file_id)
    if not peer or not msg_id:
        return web.Response(status=404, text="404: Not Found")

    message = cast(Message, await client.get_messages(entity=peer, ids=msg_id))
    if not message or not message.file or get_file_name(message) != file_name:
        return web.Response(status=404, text="404: Not Found")

    size = message.file.size
    offset = req.http_range.start or 0
    limit = req.http_range.stop or size

    if not head:
        if not transfer.can_download(message.media):
            # TODO use per-user limits and return HTTP 429 Too Many Requests here
            return web.Response(status=503, headers={"Retry-After": "120"})
        log.info(
            f"Serving file in {message.id} (chat {message.chat_id}) to {get_requester_ip(req)}")
        body = transfer.download(message.media, file_size=size, offset=offset, limit=limit)
    else:
        body = None
    return web.Response(status=206 if offset else 200,
                        body=body,
                        headers={
                            "Content-Type": message.file.mime_type,
                            "Content-Range": f"bytes {offset}-{size}/{size}",
                            "Content-Length": str(limit - offset),
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
