from asyncio import IncompleteReadError, StreamReader, StreamWriter, sleep
from collections import deque

from app.expiry import get_expiry_timestamp
from app.resp import RESPReader, RESPWriter

datastore: dict[str, tuple[str, int]] = {}  # key -> (value, expiry_timestamp)


async def replication_handshake(reader: RESPReader, writer: RESPWriter):
    """ """
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

    rdb = await reader.read_rdb()
    print(rdb)

    return


async def propagate_commands(
    replication_buffer: deque[str], replicas: list[tuple[RESPReader, RESPWriter]]
):
    WAIT_TIME = 0.125  # seconds
    while True:
        if len(replicas) != 0:
            if len(replication_buffer) != 0:
                cmd = replication_buffer.popleft()
                for replica in replicas:
                    _, w = replica
                    await w.write(cmd)
        await sleep(WAIT_TIME)


async def replica_tasks(
    stream_reader: StreamReader,
    stream_writer: StreamWriter,
):
    # replication_handshake + command_processing
    reader, writer = RESPReader(stream_reader), RESPWriter(stream_writer)
    await replication_handshake(reader, writer)
    global datastore
    offset = 0  # Count of processed bytes
    while True:
        try:
            msg = await reader.read_message()
            print("Received from master :", msg)
        except (IncompleteReadError, ConnectionResetError) as err:
            print(err)
            await writer.close()
            return
        command = msg[0].upper()
        match command:
            case "SET":
                key, value = msg[1], msg[2]
                datastore[key] = (value, get_expiry_timestamp([]))  # No active expiry
            case "REPLCONF":
                # Master won't send any other REPLCONF message apart from GETACK.
                response = ["REPLCONF", "ACK", str(offset)]
                await writer.write_array(response)
            case _:
                pass
        bytes_to_process = reader.get_byte_offset(msg)
        offset += bytes_to_process
