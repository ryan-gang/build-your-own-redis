from asyncio import StreamReader, StreamWriter

from app.resp import RESPReader, RESPWriter


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
