# tgfilestream - A Telegram bot that can stream Telegram files to users over HTTP.
# Copyright (C) 2019 Tulir Asokan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
from typing import Union, AsyncGenerator, AsyncContextManager, Dict, Optional, List
from contextlib import asynccontextmanager
from dataclasses import dataclass
import logging
import asyncio
import math

from telethon import TelegramClient, utils
from telethon.crypto import AuthKey
from telethon.network import MTProtoSender
from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest
from telethon.tl.functions.upload import GetFileRequest
from telethon.tl.types import (Document, InputFileLocation, InputDocumentFileLocation,
                               InputPhotoFileLocation, InputPeerPhotoFileLocation, DcOption)
from telethon.errors import DcIdInvalidError

from .config import connection_limit

TypeLocation = Union[Document, InputDocumentFileLocation, InputPeerPhotoFileLocation,
                     InputFileLocation, InputPhotoFileLocation]

root_log = logging.getLogger(__name__)

if connection_limit > 25:
    root_log.warning("The connection limit should not be set above 25 to avoid"
                     " infinite disconnect/reconnect loops")


@dataclass
class Connection:
    log: logging.Logger
    sender: MTProtoSender
    lock: asyncio.Lock
    users: int = 0


class DCConnectionManager:
    log: logging.Logger
    client: TelegramClient
    loop: asyncio.AbstractEventLoop

    dc_id: int
    dc: Optional[DcOption]
    auth_key: Optional[AuthKey]
    connections: List[Connection]

    _list_lock: asyncio.Lock

    def __init__(self, client: TelegramClient, dc_id: int) -> None:
        self.log = root_log.getChild(f"dc{dc_id}")
        self.client = client
        self.dc_id = dc_id
        self.auth_key = None
        self.connections = []
        self._list_lock = asyncio.Lock()
        self.loop = client.loop
        self.dc = None

    async def _new_connection(self) -> Connection:
        if not self.dc:
            self.dc = await self.client._get_dc(self.dc_id)
        sender = MTProtoSender(self.auth_key, self.loop, loggers=self.client._log)
        index = len(self.connections) + 1
        conn = Connection(sender=sender, log=self.log.getChild(f"conn{index}"), lock=asyncio.Lock())
        self.connections.append(conn)
        async with conn.lock:
            conn.log.info("Connecting...")
            connection_info = self.client._connection(self.dc.ip_address, self.dc.port, self.dc.id,
                                                      loop=self.loop, loggers=self.client._log,
                                                      proxy=self.client._proxy)
            await sender.connect(connection_info)
            if not self.auth_key:
                await self._export_auth_key(conn)
            return conn

    async def _export_auth_key(self, conn: Connection) -> None:
        self.log.info(f"Exporting auth to DC {self.dc.id}"
                      f" (main client is in {self.client.session.dc_id})")
        try:
            auth = await self.client(ExportAuthorizationRequest(self.dc.id))
        except DcIdInvalidError:
            self.log.debug("Got DcIdInvalidError")
            self.auth_key = self.client.session.auth_key
            conn.sender.auth_key = self.auth_key
            return
        req = self.client._init_with(ImportAuthorizationRequest(
            id=auth.id, bytes=auth.bytes
        ))
        await conn.sender.send(req)
        self.auth_key = conn.sender.auth_key

    async def _next_connection(self) -> Connection:
        best_conn: Optional[Connection] = None
        for conn in self.connections:
            if not best_conn or conn.users < best_conn.users:
                best_conn = conn
        if (not best_conn or best_conn.users > 0) and len(self.connections) < connection_limit:
            best_conn = await self._new_connection()
        return best_conn

    @asynccontextmanager
    async def get_connection(self) -> AsyncContextManager[Connection]:
        async with self._list_lock:
            conn: Connection = await asyncio.shield(self._next_connection())
            # The connection is locked so reconnections don't stack
            async with conn.lock:
                conn.users += 1
        try:
            yield conn
        finally:
            conn.users -= 1


class ParallelTransferrer:
    log: logging.Logger = logging.getLogger(__name__)
    client: TelegramClient
    loop: asyncio.AbstractEventLoop

    dc_managers: Dict[int, DCConnectionManager]

    _counter: int

    def __init__(self, client: TelegramClient) -> None:
        self.client = client
        self.loop = self.client.loop
        self._counter = 0
        self.dc_managers = {
            1: DCConnectionManager(client, 1),
            2: DCConnectionManager(client, 2),
            3: DCConnectionManager(client, 3),
            4: DCConnectionManager(client, 4),
            5: DCConnectionManager(client, 5),
        }

    def post_init(self) -> None:
        self.dc_managers[self.client.session.dc_id].auth_key = self.client.session.auth_key

    @property
    def next_index(self) -> int:
        self._counter += 1
        return self._counter

    async def _int_download(self, request: GetFileRequest, first_part: int, last_part: int,
                            part_count: int, part_size: int, dc_id: int, first_part_cut: int,
                            last_part_cut: int) -> AsyncGenerator[bytes, None]:
        log = self.log
        try:
            part = first_part
            dcm = self.dc_managers[dc_id]
            async with dcm.get_connection() as conn:
                log = conn.log
                while part <= last_part:
                    result = await conn.sender.send(request)
                    request.offset += part_size
                    if part == first_part:
                        yield result.bytes[first_part_cut:]
                    elif part == last_part:
                        yield result.bytes[:last_part_cut]
                    else:
                        yield result.bytes
                    log.debug(f"Part {part}/{last_part} (total {part_count}) downloaded")
                    part += 1
                log.debug("Parallel download finished")
        except (GeneratorExit, StopAsyncIteration, asyncio.CancelledError):
            log.debug("Parallel download interrupted")
            raise
        except Exception:
            log.debug("Parallel download errored", exc_info=True)

    def download(self, file: TypeLocation, file_size: int, offset: int, limit: int
                 ) -> AsyncGenerator[bytes, None]:
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

        return self._int_download(request, first_part, last_part, part_count, part_size, dc_id,
                                  first_part_cut, last_part_cut)
