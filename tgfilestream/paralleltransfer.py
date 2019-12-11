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
from typing import Union, AsyncGenerator, AsyncContextManager, Dict
from contextlib import asynccontextmanager
import logging
import asyncio
import math

from telethon import TelegramClient, utils
from telethon.crypto import AuthKey
from telethon.network import MTProtoSender
from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest
from telethon.tl.functions.upload import GetFileRequest
from telethon.tl.types import (Document, InputFileLocation, InputDocumentFileLocation,
                               InputPhotoFileLocation, InputPeerPhotoFileLocation)

TypeLocation = Union[Document, InputDocumentFileLocation, InputPeerPhotoFileLocation,
                     InputFileLocation, InputPhotoFileLocation]


class ParallelTransferrer:
    log: logging.Logger = logging.getLogger(__name__)
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

    async def download(self, file: TypeLocation, file_size: int, offset: int, limit: int
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
