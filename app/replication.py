from asyncio import StreamReader, StreamWriter, sleep
from app.resp import RESPReader, RESPWriter
from collections import deque


async def replication_handshake(
    stream_reader: StreamReader, stream_writer: StreamWriter
):
    """ """
    reader, writer = RESPReader(stream_reader), RESPWriter(stream_writer)

    ping = ["PING"]
    await writer.write_array(ping)
    data = await reader.read_simple_string()
    print(data)

    replconf = ["REPLCONF", "listening-port", "6380"]
    await writer.write_array(replconf)
    data = await reader.read_simple_string()
    print(data)

    replconf_capa = ["REPLCONF", "capa", "psync2", "capa", "psync2"]
    await writer.write_array(replconf_capa)
    data = await reader.read_simple_string()
    print(data)

    replconf_capa = ["PSYNC", "?", "-1"]
    await writer.write_array(replconf_capa)
    data = await reader.read_simple_string()
    print(data)

    return


async def propagate_commands(
    replication_buffer: deque[str], replicas: list[tuple[RESPReader, RESPWriter]]
):
    WAIT_TIME = 1  # seconds
    while True:
        if len(replicas) != 0:
            if len(replication_buffer) != 0:
                cmd = replication_buffer.popleft()
                for replica in replicas:
                    _, w = replica
                    await w.write(cmd)
        await sleep(WAIT_TIME)
