package com.googlecode.jsonrpc4j;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

public class ReadContextTest {

    @Test
    public void splitJson() throws IOException {
        PipedOutputStream outstr = new PipedOutputStream();
        PipedInputStream instr = new PipedInputStream(outstr);

        ReadContext readContext = ReadContext.getReadContext(instr, new ObjectMapper());

        JsonNode node;

       /* outstr.write("{\"jsonrpc\":\"2.0\",\"method\":\"locate\",\"params\":[750,172,376,644.6875,true]}".getBytes());
        node = readContext.nextValue();
        System.out.println(node);*/

       String message = "{\"jsonrpc\":\"2.0\",\"method\":\"locate\",\"params\":[750,172,376,644.6875,true]}{\"id\":15,\"jsonrpc\":\"2.0\",\"method\":\"playUrl\",\"params\":[\"http://localhost:8886/stream-file/114231\"]}";

       System.out.println("msg len = " + message.length());
        outstr.write(message.getBytes());
        node = readContext.nextValue();
        System.out.println(node);

        node = readContext.nextValue();
        System.out.println(node);
    }
}
