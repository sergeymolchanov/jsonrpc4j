package com.googlecode.jsonrpc4j;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * A JSON-RPC client that uses raw tcp socket
 */
public class JsonRpcSocketClient extends JsonRpcAbstractKeepAliveStreamClient {
    private final InetSocketAddress address;
    private static final long DEFAULT_TIMEOUT = 10000;

    private static final byte[] QUERY_DELIMITER = new byte[] { 10, 13 };

    private final Object connectionLock = new Object();

    private Socket connection = null;

    /**
     * Creates the {@link JsonRpcSocketClient} bound to tcp socket
     * @param address {@link InetSocketAddress} of remote host
     */
    public JsonRpcSocketClient(InetSocketAddress address) {
        this(new ObjectMapper(), address, false, DEFAULT_TIMEOUT);
    }

    /**
     * Creates the {@link JsonRpcSocketClient} bound to tcp socket
     * @param address   {@link InetSocketAddress} of remote host
     * @param keepAlive If <code>false</code>, streams are closing after every query.
     *                  If <code>true</code> streams close after IO error.
     */
    public JsonRpcSocketClient(InetSocketAddress address, boolean keepAlive) {
        this(new ObjectMapper(), address, keepAlive, DEFAULT_TIMEOUT);
    }

    /**
     * Creates the {@link JsonRpcSocketClient} bound to tcp socket
     * @param address   {@link InetSocketAddress} of remote host
     * @param keepAlive If <code>false</code>, streams are closing after every query.
     *                  If <code>true</code> streams close after IO error.
     * @param timeout   Request timeout in milliseconds. <code>-1</code> for no timeout.
     */
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
    protected void closeStreams() throws IOException {
        synchronized (connectionLock) {
            if (connection == null || connection.isClosed()) return;

            connection.close();
            connection = null;

            super.outputStream = null;
            super.inputStream = null;
        }
    }

    @Override
    protected byte[] getQueryDelimiter() {
        return QUERY_DELIMITER;
    }
}
