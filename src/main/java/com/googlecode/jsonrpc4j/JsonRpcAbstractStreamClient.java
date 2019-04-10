package com.googlecode.jsonrpc4j;

import java.io.*;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.databind.ObjectMapper;

@SuppressWarnings("unused")
public abstract class JsonRpcAbstractStreamClient extends JsonRpcClient implements IJsonRpcClient, Closeable {
    private final ExecutorService executor;

    private static final byte[] QUERY_END = new byte[] { 10, 13 };

    private final long timeout;
    private final boolean keepAlive;

    private final Object streamLock = new Object();
    protected OutputStream outputStream = null;
    protected InputStream inputStream = null;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    protected JsonRpcAbstractStreamClient(ObjectMapper objectMapper, boolean keepAlive, long timeout) {
        super(objectMapper);

        this.timeout = timeout;
        this.keepAlive = keepAlive;

        if (timeout > 0) {
            this.executor = Executors.newSingleThreadExecutor();
        } else {
            this.executor = null;
        }
    }

    protected abstract void ensureStreamsOpen() throws IOException;
    protected abstract boolean isStreamsOpen();
    protected abstract void closeStreams() throws IOException;

    @Override
    public Object invoke(String methodName, Object argument, Type returnType, Map<String, String> extraHeaders) throws Throwable {
        ReadResponceCallable resp = new ReadResponceCallable(methodName, argument, returnType);

        try {
            Object result;
            if (this.executor == null) {
                result = resp.call();
            } else {
                Future<Object> resultFuture = this.executor.submit(resp);

                result = resultFuture.get(this.timeout, TimeUnit.MILLISECONDS);
            }

            if (!keepAlive) {
                closeStreams();
            }

            return result;
        } catch (ExecutionException e) {
            closeStreamsQuietly();
            throw e.getCause();
        } catch (Throwable t) {
            closeStreamsQuietly();
            throw t;
        }
    }

    private void closeStreamsQuietly() {
        try {
            closeStreams();
        } catch (Exception e) {
            logger.error("Unable to close streams", e);
        }
    }

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

                JsonRpcAbstractStreamClient.super.invoke(methodName, argument, outputStream);
                outputStream.write(QUERY_END);
                outputStream.flush();

                try {
                    return JsonRpcAbstractStreamClient.super.readResponse(returnType, inputStream);
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
    public void close() throws IOException {
        isClosed.set(true);
        executor.shutdownNow();

        closeStreams();
    }

    @Override
    public void invoke(String methodName, Object argument) throws Throwable {
        invoke(methodName, argument, (Type)null);
    }

    @Override
    public Object invoke(String methodName, Object argument, Type returnType) throws Throwable {
        return invoke(methodName, argument, returnType, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T invoke(String methodName, Object argument, Class<T> clazz) throws Throwable {
        return (T)invoke(methodName, argument, Type.class.cast(clazz));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T invoke(String methodName, Object argument, Class<T> clazz, Map<String, String> extraHeaders) throws Throwable {
        return (T)invoke(methodName, argument, Type.class.cast(clazz), extraHeaders);
    }
}
