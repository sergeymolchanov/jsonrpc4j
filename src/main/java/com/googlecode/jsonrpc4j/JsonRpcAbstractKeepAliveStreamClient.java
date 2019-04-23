package com.googlecode.jsonrpc4j;

import java.io.*;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Abstract class to create JSON RPC clients with auto-open/close streams and query timeout
 */
public abstract class JsonRpcAbstractKeepAliveStreamClient extends JsonRpcClient implements IJsonRpcClient, Closeable {
    private final ExecutorService executor;

    private final long timeout;
    private final boolean keepAlive;

    private final Object streamLock = new Object();

    /**
     * Stream to write request. Must be initialized in inheritor class.
     */
    protected OutputStream outputStream = null;

    /**
     * Stream to read responce. Must be initialized in inheritor class.
     */
    protected InputStream inputStream = null;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    /**
     * Creates the client
     * @param objectMapper the {@link ObjectMapper}
     * @param keepAlive If <code>false</code>, streams are closing after every query.
     *                  If <code>true</code> streams close after IO error.
     * @param timeout   Request timeout in milliseconds. <code>-1</code> for no timeout.
     */
    protected JsonRpcAbstractKeepAliveStreamClient(ObjectMapper objectMapper, boolean keepAlive, long timeout) {
        super(objectMapper);

        this.timeout = timeout;
        this.keepAlive = keepAlive;

        if (timeout > 0) {
            this.executor = Executors.newSingleThreadExecutor();
        } else {
            this.executor = null;
        }
    }

    /**
     * Inheritor class checks {@link #outputStream} and {@link #inputStream} is open
     * @throws IOException
     */
    protected abstract void ensureStreamsOpen() throws IOException;

    /**
     * Inheritor class closes {@link #outputStream}, {@link #inputStream}, and all other resources
     * @throws IOException
     */
    protected abstract void closeStreams() throws IOException;

    /**
     * Returns query delimiter. Executor writes it to stream after every query to mark query end.
     * @return
     */
    protected abstract byte[] getQueryDelimiter();

    @Override
    /** @inheritdoc */
    public Object invoke(String methodName, Object argument, Type returnType, Map<String, String> extraHeaders) throws Throwable {
        if (isClosed.get()) throw new IOException("Client is closed");

        ReadResponceCallable resp = new ReadResponceCallable(methodName, argument, returnType);

        Object result;
        try {
            if (this.executor == null) {
                result = resp.call();
            } else {
                Future<Object> resultFuture = null;
                try {
                    resultFuture = this.executor.submit(resp);

                    result = resultFuture.get(this.timeout, TimeUnit.MILLISECONDS);
                } finally {
                    if (resultFuture != null && !resultFuture.isDone()) {
                        resultFuture.cancel(true);
                    }
                }
            }
        } catch (Exception e) {
            closeStreamsQuietly();
            throw e;
        }

        if (!keepAlive) {
            closeStreams();
        }

        return result;
    }

    private void closeStreamsQuietly() {
        try {
            closeStreams();
        } catch (Exception e) {
            logger.error("Unable to close streams", e);
        }
    }

    /**
     * Query executor class
     */
    private class ReadResponceCallable implements Callable<Object> {
        private final String methodName;
        private final Object argument;
        private final Type returnType;

        public ReadResponceCallable(String methodName, Object argument, Type returnType) {
            this.methodName = methodName;
            this.argument = argument;
            this.returnType = returnType;
        }

        @Override
        public Object call() throws Exception {
            synchronized (streamLock) {
                ensureStreamsOpen();

                JsonRpcAbstractKeepAliveStreamClient.super.invoke(methodName, argument, outputStream);

                byte[] queryDelimiter = JsonRpcAbstractKeepAliveStreamClient.this.getQueryDelimiter();
                if (queryDelimiter != null) {
                    outputStream.write(queryDelimiter);
                }

                outputStream.flush();

                try {
                    return JsonRpcAbstractKeepAliveStreamClient.super.readResponse(returnType, inputStream);
                } catch (Exception e) {
                    throw e;
                } catch (Throwable t) { // Wrap throwable
                    throw new Exception(t);
                }
            }
        }
    }

    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    /** @inheritdoc */
    public void close() throws IOException {
        isClosed.set(true);
        executor.shutdownNow();

        closeStreams();
    }

    @Override
    /** @inheritdoc */
    public void invoke(String methodName, Object argument) throws Throwable {
        invoke(methodName, argument, (Type)null);
    }

    @Override
    /** @inheritdoc */
    public Object invoke(String methodName, Object argument, Type returnType) throws Throwable {
        return invoke(methodName, argument, returnType, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    /** @inheritdoc */
    public <T> T invoke(String methodName, Object argument, Class<T> clazz) throws Throwable {
        return (T)invoke(methodName, argument, Type.class.cast(clazz));
    }

    @SuppressWarnings("unchecked")
    @Override
    /** @inheritdoc */
    public <T> T invoke(String methodName, Object argument, Class<T> clazz, Map<String, String> extraHeaders) throws Throwable {
        return (T)invoke(methodName, argument, Type.class.cast(clazz), extraHeaders);
    }
}
