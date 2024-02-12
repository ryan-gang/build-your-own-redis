from asyncio import StreamReader, StreamWriter

from app.resp import RESPReader, RESPWriter


async def replication_handshake(
    stream_reader: StreamReader, stream_writer: StreamWriter
):
    """ """
    reader, writer = RESPReader(stream_reader), RESPWriter(stream_writer)

    ping = ["PING"]
    await writer.write_array(ping)

    replconf = ["REPLCONF", "listening-port", "6380"]
    await writer.write_array(replconf)

    replconf_capa = ["REPLCONF", "capa", "psync2"]
    await writer.write_array(replconf_capa)
