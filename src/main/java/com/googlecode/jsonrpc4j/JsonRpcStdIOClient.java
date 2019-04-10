package com.googlecode.jsonrpc4j;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;

public class JsonRpcStdIOClient extends JsonRpcAbstractStreamClient {
    private static final long DEFAULT_TIMEOUT = 10000;

    private final Object connectionLock = new Object();

    private final String[] command;

    private Process process = null;
    private InputStream errorStream = null;

    public JsonRpcStdIOClient(String... command) {
        this(new ObjectMapper(), false, DEFAULT_TIMEOUT, command);
    }

    public JsonRpcStdIOClient(boolean keepAlive, String... command ) {
        this(new ObjectMapper(), keepAlive, DEFAULT_TIMEOUT, command);
    }

    public JsonRpcStdIOClient(boolean keepAlive, long timeout, String... command) {
        this(new ObjectMapper(), keepAlive, timeout, command);
    }

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

            if (process == null || !process.isAlive()) {
                ProcessBuilder builder = new ProcessBuilder(command);

                process = builder.redirectErrorStream(true).start();

                super.outputStream = process.getOutputStream();
                super.inputStream = process.getInputStream();
                this.errorStream = process.getErrorStream();
            }
        }
    }

    @Override
    protected boolean isStreamsOpen() {
        synchronized (connectionLock) {
            return (process != null && process.isAlive());
        }
    }

    @Override
    protected void closeStreams() throws IOException {
        synchronized (connectionLock) {
            if (process == null || !process.isAlive()) return;

            process.destroy();
            process = null;

            super.outputStream = null;
            super.inputStream = null;
        }
    }
}
