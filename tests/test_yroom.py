import pytest
from anyio import (
    TASK_STATUS_IGNORED,
    CancelScope,
    Event,
    create_memory_object_stream,
    create_task_group,
    move_on_after,
    sleep,
    wait_all_tasks_blocked,
)
from anyio.abc import TaskStatus
from utils import Websocket

from pycrdt import (
    Awareness,
    Doc,
    Map,
    YMessageType,
    YSyncMessageType,
    create_awareness_message,
    create_sync_message,
    create_update_message,
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


STEP1, STEP2, UPDATE = (bytes((YMessageType.SYNC, t)) for t in YSyncMessageType)


class Peer:
    # a client of the room: with hold=True, each message sent to it waits for release()
    # before it is delivered, and a set failure is raised by the next delivery instead
    path = "room"

    def __init__(self, hold=False):
        self.doc, self.hold, self.held, self.frames = Doc(), hold, [], []
        self.synced, self.early, self.failure = False, False, None
        self.inbox, self._inbox = create_memory_object_stream(16)
        self.changed, self.done, self.scope = Event(), Event(), CancelScope()

    def __aiter__(self):
        return self._inbox

    async def send(self, message):
        self.frames.append(message)
        self.early |= message[:2] == UPDATE and not self.synced
        if self.hold:
            self.held.append(released := Event())
            self._change()
            await released.wait()
        if self.failure:
            failure, self.failure = self.failure, None
            raise failure
        if message[0] == YMessageType.SYNC:
            handle_sync_message(message[1:], self.doc)
        self.synced |= message[:2] == STEP2
        self._change()

    def _change(self):
        self.changed.set()
        self.changed = Event()

    def release(self):
        self.held.pop(0).set()

    async def until(self, condition):
        # wait for what is sent to this peer to make condition true (the caller asserts it)
        with move_on_after(5):
            while not condition():
                await self.changed.wait()

    def map(self):
        return self.doc.get("map", type=Map).to_py()


async def serve(server, peer):
    with peer.scope:
        await server.serve(peer)
    peer.done.set()


async def join(server, tg):
    # a is synced and gets the room's changes; b has sent an update, which is not a sync
    # request, then its sync request, and its SYNC_STEP2 reply is encoded and held
    a, b = Peer(), Peer(hold=True)
    await a.inbox.send(create_sync_message(a.doc))
    tg.start_soon(serve, server, a)
    await a.until(lambda: a.synced)
    room = server.rooms["room"]
    await room.ydoc_observed.wait()
    room.ydoc["map"] = Map({"x": "0", "y": "0"})
    await a.until(lambda: a.map() == {"x": "0", "y": "0"})
    b.doc["map"] = Map({"b": "0"})
    await b.inbox.send(create_update_message(b.doc.get_update()))
    await b.inbox.send(create_sync_message(b.doc))
    tg.start_soon(serve, server, b)
    await b.until(lambda: b.held)
    b.release()  # the server's SYNC_STEP1
    await b.until(lambda: b.held)
    return room, a, b


@pytest.mark.parametrize("held", ["reply", "catchup"])
@pytest.mark.parametrize("change", ["insert", "delete"])
async def test_change_while_joining(change, held):
    # a change made while b's SYNC_STEP2 is held reaches b right after it, as does one
    # made while that catch-up is held, and no update reaches b before its SYNC_STEP2
    async with WebsocketServer() as server, create_task_group() as tg:
        room, a, b = await join(server, tg)
        if change == "insert":
            room.ydoc["map"]["z"] = "0"
        else:
            del room.ydoc["map"]["x"]
        await a.until(lambda: a.map() == room.ydoc["map"].to_py())
        if held == "catchup":
            b.release()  # b's SYNC_STEP2
            await b.until(lambda: b.held)
            del room.ydoc["map"]["y"]
            await a.until(lambda: a.map() == room.ydoc["map"].to_py())
        b.hold = False
        while b.held:
            b.release()
        await b.until(lambda: b.map() == room.ydoc["map"].to_py())
        assert b.map() == room.ydoc["map"].to_py()
        assert not b.early
        room.ydoc["map"]["w"] = "0"
        await b.until(lambda: "w" in b.map())
        assert "w" in b.map()
        tg.cancel_scope.cancel()


async def test_repeated_sync():
    # while b's SYNC_STEP2 is held, b's next requests wait instead of each holding another
    # snapshot, and they are answered in turn once b has caught up
    async with WebsocketServer() as server, create_task_group() as tg:
        room, a, b = await join(server, tg)
        for _ in range(3):
            await b.inbox.send(create_sync_message(b.doc))
        room.ydoc["map"]["x"] = "1"
        room.ydoc["map"]["x"] = "2"
        await a.until(lambda: a.map() == room.ydoc["map"].to_py())
        await wait_all_tasks_blocked()
        assert len(b.held) == 1
        b.hold = False
        while b.held:
            b.release()
        await b.until(lambda: len(b.frames) == 6)
        assert [frame[:2] for frame in b.frames] == [STEP1, STEP2, UPDATE, STEP2, STEP2, STEP2]
        assert b.map() == room.ydoc["map"].to_py()
        assert not b.early
        tg.cancel_scope.cancel()


@pytest.mark.parametrize("outcome", ["error", "cancel"])
@pytest.mark.parametrize("held", ["reply", "catchup"])
async def test_failure_while_joining(held, outcome):
    # a failed send to a joining b goes to on_message_error with b's request, and b gets
    # no update until it asks again; cancelling b's connection removes it from the room
    errors = []
    async with WebsocketServer() as server, create_task_group() as tg:
        room, a, b = await join(server, tg)
        room.on_message_error = lambda error, message, channel: not errors.append(
            (error, message[:2], channel)
        )
        if held == "catchup":
            room.ydoc["map"]["z"] = "0"
            await a.until(lambda: a.map() == room.ydoc["map"].to_py())
            b.release()  # b's SYNC_STEP2
            await b.until(lambda: b.held)
        if outcome == "cancel":
            b.scope.cancel()
            await b.done.wait()
            assert b not in room.clients and b not in room._joining
        else:
            failure = b.failure = ConnectionError()
            b.release()
            await wait_all_tasks_blocked()
            assert errors == [(failure, STEP1, b)]
            sent = len(b.frames)
            room.ydoc["map"]["w"] = "0"
            await a.until(lambda: "w" in a.map())
            await wait_all_tasks_blocked()
            assert len(b.frames) == sent
            b.hold = False
            await b.inbox.send(create_sync_message(b.doc))
            await b.until(lambda: b.map() == room.ydoc["map"].to_py())
            assert b.map() == room.ydoc["map"].to_py()
        room.ydoc["map"]["v"] = "0"
        await a.until(lambda: "v" in a.map())
        assert "v" in a.map()
        tg.cancel_scope.cancel()


async def test_room_lifetime():
    # b counts as connected while joining, so it gets a's awareness, and the room stays
    # open after a leaves; it closes when b leaves too
    async with WebsocketServer(auto_clean_rooms=True) as server, create_task_group() as tg:
        room, a, b = await join(server, tg)
        awareness = Awareness(a.doc)
        awareness.set_local_state({"name": "a"})
        state = awareness.encode_awareness_update([a.doc.client_id])
        await a.inbox.send(create_awareness_message(state))
        await b.until(lambda: b.frames[-1][0] == YMessageType.AWARENESS)
        assert b.frames[-1][0] == YMessageType.AWARENESS
        b.held.pop().set()  # deliver it, while b's SYNC_STEP2 stays held
        await a.inbox.aclose()
        await a.done.wait()
        assert server.rooms.get("room") is room
        b.hold = False
        while b.held:
            b.release()
        room.ydoc["map"]["w"] = "0"
        await b.until(lambda: b.map() == room.ydoc["map"].to_py())
        assert b.map() == room.ydoc["map"].to_py()
        await b.inbox.aclose()
        await b.done.wait()
        assert "room" not in server.rooms
