package com.googlecode.jsonrpc4j;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JsonRpcStdIOServer extends JsonRpcBasicServer {
    private static final Logger logger = LoggerFactory.getLogger(JsonRpcStdIOServer.class);

    private final StdIOHandler stdIOHandler = new StdIOHandler();

    private Thread handleThread = null;

    public JsonRpcStdIOServer(ObjectMapper mapper, Object handler) {
        super(mapper, handler);
    }

    public JsonRpcStdIOServer(ObjectMapper mapper, Object handler, Class<?> remoteInterface) {
        super(mapper, handler, remoteInterface);
    }

    public JsonRpcStdIOServer(Object handler, Class<?> remoteInterface) {
        super(handler, remoteInterface);
    }

    public JsonRpcStdIOServer(Object handler) {
        super(handler);
    }

    public synchronized void start() {
        if (handleThread != null) {
            throw new IllegalStateException();
        }

        handleThread = new Thread(stdIOHandler, "JsonRpcStdIOServerHandlerThread");
        handleThread.start();
    }

    public synchronized void stop() {
        if (handleThread == null) {
            throw new IllegalStateException();
        }

        handleThread.interrupt();
        handleThread = null;
    }

    private void handleRequest() throws IOException {
        super.handleRequest(System.in, System.out);
    }

    private class StdIOHandler implements Runnable {
        @Override
        public void run() {
            while (!Thread.interrupted()) {
                try {
                    handleRequest();
                } catch (Exception e) {
                    logger.error("Handle request error", e);
                }
            }
        }
    }
}
