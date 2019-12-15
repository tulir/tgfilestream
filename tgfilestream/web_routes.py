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
from typing import Dict, cast
from collections import defaultdict
import logging

from telethon.tl.custom import Message
from aiohttp import web

from .util import unpack_id, get_file_name, get_requester_ip
from .config import request_limit
from .telegram import client, transfer

log = logging.getLogger(__name__)
routes = web.RouteTableDef()
ongoing_requests: Dict[str, int] = defaultdict(lambda: 0)


@routes.head(r"/{id:\d+}/{name}")
async def handle_head_request(req: web.Request) -> web.Response:
    return await handle_request(req, head=True)


@routes.get(r"/{id:\d+}/{name}")
async def handle_get_request(req: web.Request) -> web.Response:
    return await handle_request(req, head=False)


def allow_request(ip: str) -> None:
    return ongoing_requests[ip] < request_limit


def increment_counter(ip: str) -> None:
    ongoing_requests[ip] += 1


def decrement_counter(ip: str) -> None:
    ongoing_requests[ip] -= 1


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
        ip = get_requester_ip(req)
        if not allow_request(ip):
            return web.Response(status=429)
        log.info(f"Serving file in {message.id} (chat {message.chat_id}) to {ip}")
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
