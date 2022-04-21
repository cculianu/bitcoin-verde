package com.softwareverde.network.socket;

import com.softwareverde.concurrent.threadpool.ThreadPool;

public class JsonSocket extends Socket {
    public JsonSocket(final java.net.Socket socket, final ThreadPool threadPool, final Integer maxPacketByteCount) {
        super(socket, new JsonSocketReadThread(), new JsonSocketWriteThread(BinarySocket.DEFAULT_BUFFER_PAGE_BYTE_COUNT, maxPacketByteCount), threadPool);
    }

    public JsonSocket(final java.net.Socket socket, final ThreadPool threadPool) {
        super(socket, new JsonSocketReadThread(), new JsonSocketWriteThread(), threadPool);
    }

    @Override
    public JsonProtocolMessage popMessage() {
        return (JsonProtocolMessage) super.popMessage();
    }
}
