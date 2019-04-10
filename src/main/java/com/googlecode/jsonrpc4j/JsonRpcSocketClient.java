package com.googlecode.jsonrpc4j;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class JsonRpcSocketClient extends JsonRpcAbstractStreamClient {
    private final InetSocketAddress address;
    private static final long DEFAULT_TIMEOUT = 10000;

    private final Object connectionLock = new Object();

    private Socket connection = null;

    public JsonRpcSocketClient(InetSocketAddress address) {
        this(new ObjectMapper(), address, false, DEFAULT_TIMEOUT);
    }

    public JsonRpcSocketClient(InetSocketAddress address, boolean keepAlive) {
        this(new ObjectMapper(), address, keepAlive, DEFAULT_TIMEOUT);
    }

    public JsonRpcSocketClient(InetSocketAddress address, boolean keepAlive, long timeout) {
        this(new ObjectMapper(), address, keepAlive, timeout);
    }

    protected JsonRpcSocketClient(ObjectMapper objectMapper, InetSocketAddress address, boolean keepAlive, long timeout) {
        super(objectMapper, keepAlive, timeout);

        this.address = address;
    }

    @Override
    protected void ensureStreamsOpen() throws IOException {
        synchronized (connectionLock) {
            if (super.isClosed()) {
                throw new IOException("Client is closed");
            }
            if (connection == null || connection.isClosed()) {
                this.connection = new Socket(address.getHostName(), address.getPort());
                super.outputStream = connection.getOutputStream();
                super.inputStream = connection.getInputStream();
            }
        }
    }

    @Override
    protected boolean isStreamsOpen() {
        synchronized (connectionLock) {
            return (connection != null && !connection.isClosed());
        }
    }

    @Override
    protected void closeStreams() throws IOException {
        synchronized (connectionLock) {
            if (connection == null || connection.isClosed()) return;

            connection.close();
            connection = null;

            super.outputStream = null;
            super.inputStream = null;
        }
    }
}
