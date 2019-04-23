package com.googlecode.jsonrpc4j;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;

/**
 * A JSON-RPC client that uses OS standard IO
 */
public class JsonRpcStdIOClient extends JsonRpcAbstractKeepAliveStreamClient {
    private static final long DEFAULT_TIMEOUT = 10000;

    private static final byte[] QUERY_DELIMITER = new byte[] { 10, 13 };

    private final Object connectionLock = new Object();

    private final String[] command;

    private Process process = null;
    private InputStream errorStream = null;

    /**
     * Creates the {@link JsonRpcStdIOClient} bound to created process
     * @param command {@link ProcessBuilder} arguments
     */
    public JsonRpcStdIOClient(String... command) {
        this(new ObjectMapper(), false, DEFAULT_TIMEOUT, command);
    }

    /**
     * Creates the {@link JsonRpcStdIOClient} bound to created process
     * @param keepAlive If <code>false</code>, streams are closing after every query.
     *                  If <code>true</code> streams close after IO error.
     * @param command {@link ProcessBuilder} arguments
     */
    public JsonRpcStdIOClient(boolean keepAlive, String... command ) {
        this(new ObjectMapper(), keepAlive, DEFAULT_TIMEOUT, command);
    }

    /**
     * Creates the {@link JsonRpcStdIOClient} bound to created process
     * @param keepAlive If <code>false</code>, streams are closing after every query.
     *                  If <code>true</code> streams close after IO error.
     * @param timeout   Request timeout in milliseconds. <code>-1</code> for no timeout.
     * @param command   {@link ProcessBuilder} arguments
     */
    public JsonRpcStdIOClient(boolean keepAlive, long timeout, String... command) {
        this(new ObjectMapper(), keepAlive, timeout, command);
    }

    /**
     * Creates the {@link JsonRpcStdIOClient} bound to created process
     * @param objectMapper the {@link ObjectMapper}
     * @param keepAlive If <code>false</code>, streams are closing after every query.
     *                  If <code>true</code> streams close after IO error.
     * @param timeout   Request timeout in milliseconds. <code>-1</code> for no timeout.
     * @param command   {@link ProcessBuilder} arguments
     */
    protected JsonRpcStdIOClient(ObjectMapper objectMapper, boolean keepAlive, long timeout, String... command) {
        super(objectMapper, keepAlive, timeout);

        this.command = command;
    }

    @Override
    protected void ensureStreamsOpen() throws IOException {
        synchronized (connectionLock) {
            if (super.isClosed()) {
                throw new IOException("Client is closed");
            }

            if (!processIsAlive()) {
                ProcessBuilder builder = new ProcessBuilder(command);

                process = builder.redirectErrorStream(true).start();

                super.outputStream = process.getOutputStream();
                super.inputStream = process.getInputStream();
                this.errorStream = process.getErrorStream();
            }
        }
    }

    @Override
    protected void closeStreams() throws IOException {
        synchronized (connectionLock) {
            if (!processIsAlive()) return;

            process.destroy();
            process = null;

            super.outputStream = null;
            super.inputStream = null;
        }
    }

    @Override
    protected byte[] getQueryDelimiter() {
        return QUERY_DELIMITER;
    }

    private boolean processIsAlive() {
        if (process == null) return false;

        try {
            process.exitValue();
            return false;
        } catch(IllegalThreadStateException e) {
            return true;
        }
    }
}
