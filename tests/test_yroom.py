import pytest
from anyio import TASK_STATUS_IGNORED, Event, create_memory_object_stream, create_task_group, sleep
from anyio.abc import TaskStatus
from anyio.lowlevel import checkpoint
from utils import Websocket

from pycrdt import (
    Doc,
    Map,
    YMessageType,
    YSyncMessageType,
    create_sync_message,
    handle_sync_message,
)
from pycrdt.websocket import WebsocketServer, exception_logger
from pycrdt.websocket.yroom import YRoom

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize("websocket_provider_connect", ["fake_websocket"], indirect=True)
@pytest.mark.parametrize("yws_providers", [2], indirect=True)
async def test_yroom(yroom, yws_providers, websocket_provider_connect, room_name):
    async with create_task_group() as tg:
        yws_provider1, yws_provider2 = yws_providers
        # client 1
        async with yws_provider1 as yws_provider1:
            ydoc1, server_ws1 = yws_provider1
            tg.start_soon(yroom.serve, Websocket(server_ws1, room_name))
            ydoc1["map"] = ymap1 = Map()
            ymap1["key"] = "value"
            await sleep(0.1)

        # client 2
        async with yws_provider2 as yws_provider2:
            ydoc2, server_ws2 = yws_provider2
            tg.start_soon(yroom.serve, Websocket(server_ws2, room_name))
            ymap2 = ydoc2.get("map", type=Map)
            await sleep(0.1)

        assert str(ymap2) == '{"key":"value"}'
        tg.cancel_scope.cancel()


@pytest.mark.parametrize("websocket_server_api", ["websocket_server_start_stop"], indirect=True)
@pytest.mark.parametrize("yws_server", [{"exception_handler": exception_logger}], indirect=True)
async def test_yroom_restart(yws_server, yws_provider):
    port, server = yws_server
    yroom = YRoom(exception_handler=exception_logger)

    async def raise_error(task_status: TaskStatus[None] = TASK_STATUS_IGNORED):
        task_status.started()
        raise RuntimeError("foo")

    yroom.ydoc, _ = yws_provider
    await server.start_room(yroom)
    yroom.ydoc["map"] = ymap1 = Map()
    ymap1["key"] = "value"
    task_group_1 = yroom._task_group
    await yroom._task_group.start(raise_error)
    ymap1["key2"] = "value2"
    await sleep(0.1)
    assert yroom._task_group is not task_group_1
    assert yroom._task_group is not None
    assert not yroom._task_group.cancel_scope.cancel_called
    await yroom.stop()


class Peer:
    # a client whose sends can be held, as on a slow network
    path = "room"

    def __init__(self, hold=False):
        self.doc, self.hold, self.held, self.synced, self.early = Doc(), hold, [], False, False
        self.inbox, self._inbox = create_memory_object_stream(16)

    def __aiter__(self):
        return self._inbox

    async def send(self, message):
        if message[0] == YMessageType.SYNC:
            self.early |= message[1] == YSyncMessageType.SYNC_UPDATE and not self.synced
            handle_sync_message(message[1:], self.doc)
        if self.hold:
            self.held.append(held := Event())
            await held.wait()
        self.synced |= message[:2] == bytes((YMessageType.SYNC, YSyncMessageType.SYNC_STEP2))


async def settle():
    # let every task run until it waits on something
    for _ in range(100):
        await checkpoint()


@pytest.mark.parametrize("event", ["insert", "delete", "leave"])
@pytest.mark.parametrize("step", range(4))
async def test_event_while_joining(event, step):
    # whenever b waits on the network while joining, a change to the room or a's leaving
    # must reach b after its SYNC_STEP2, and must not close the room
    a, b = Peer(), Peer(hold=True)
    async with WebsocketServer() as server, create_task_group() as tg:
        await a.inbox.send(create_sync_message(a.doc))
        tg.start_soon(server.serve, a)
        await settle()
        room = server.rooms["room"]
        room.ydoc["map"] = Map({"x": "0"})
        await b.inbox.send(create_sync_message(b.doc))
        tg.start_soon(server.serve, b)
        for i in range(4):
            await settle()
            if i == step:
                if event == "insert":
                    room.ydoc["map"]["y"] = "1"
                elif event == "delete":
                    del room.ydoc["map"]["x"]
                else:
                    await a.inbox.aclose()
                await settle()
            if b.held:
                b.held.pop(0).set()
        b.hold = False
        for held in b.held:
            held.set()
        room.ydoc["map"]["z"] = "2"
        await settle()
        tg.cancel_scope.cancel()
    assert server.rooms.get("room") is room
    assert not b.early
    assert b.doc.get("map", type=Map).to_py() == room.ydoc["map"].to_py()
