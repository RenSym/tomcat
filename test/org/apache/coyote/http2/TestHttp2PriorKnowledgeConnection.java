/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.coyote.http2;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.WebConnection;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import org.apache.catalina.Context;
import org.apache.catalina.startup.Tomcat;
import org.apache.catalina.util.IOTools;
import org.apache.coyote.Adapter;
import org.apache.coyote.Request;
import org.apache.coyote.http11.upgrade.InternalHttpUpgradeHandler;
import org.apache.tomcat.util.net.SocketWrapperBase;

/**
 * Tests for HTTP/2 connections started with prior knowledge, i.e. the client sends the connection preface directly
 * rather than using an HTTP/1.1 upgrade request.
 */
public class TestHttp2PriorKnowledgeConnection extends Http2TestBase {

    private static final int DATA_A_LENGTH = 2000;
    private static final int DATA_B_LENGTH = 3000;
    private static final byte[] SETTINGS_ACK_FRAME = { 0x00, 0x00, 0x00, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00 };

    private RaceHooks raceHooks;


    @Override
    protected Http2Protocol createHttp2Protocol() {
        if (raceHooks == null) {
            return super.createHttp2Protocol();
        }
        return new RaceTestHttp2Protocol(raceHooks);
    }


    @Test
    public void testSimpleRequest() throws Exception {
        enableHttp2();
        configureAndStartWebApplication();
        openClientConnection();
        sendClientPreface();

        // Server settings, acknowledgement of the client settings and the initial ping
        parser.readFrame();
        parser.readFrame();
        parser.readFrame();
        Assert.assertEquals("0-Settings-[3]-[200]\n" + "0-Settings-End\n" + "0-Settings-Ack\n" +
                "0-Ping-[0,0,0,0,0,0,0,1]\n", output.getTrace());
        output.clearTrace();

        sendSimpleGetRequest(1);
        readSimpleGetResponse();
        Assert.assertEquals(getSimpleResponseTrace(1), output.getTrace());
    }


    /*
     * With asynchronous I/O, the first call to upgradeDispatch(OPEN_READ) for a connection started with prior knowledge
     * calls init() which starts a non-blocking read of the client preface. The completion handler for that read starts
     * reading frames. If the first call to upgradeDispatch() also read frames, two threads could read frames at the same
     * time and frames could be processed out of order.
     *
     * The time windows for this race are very short. To make the test deterministic, the threads are paused at the
     * critical points:
     * - the first call to upgradeDispatch() at the end of init(), until another thread is processing a read;
     * - the thread processing DATA frame A, until DATA frame B has been processed by another thread or, if no other
     *   thread reads DATA frame B, for at most two seconds.
     */
    @Test
    public void testFramesAreNotReadConcurrentlyAtConnectionStart() throws Exception {
        Assume.assumeTrue("The client preface is only read asynchronously with asynchronous I/O", useAsyncIO);

        raceHooks = new RaceHooks();
        enableHttp2();
        Context ctxt = getProgrammaticRootContext();
        Tomcat.addServlet(ctxt, "summary", new BodySummaryServlet());
        ctxt.addServletMapping("/summary", "summary");
        getTomcatInstance().start();

        // The client must not write anything the test does not control, such as settings acknowledgements
        openClientConnection(false, false);
        output.setTraceBody(true);

        // The first call to upgradeDispatch() processes the client preface within init()
        sendClientPreface();
        assertReached(raceHooks.firstDispatchWaiting, "The first call to upgradeDispatch() did not complete init()");

        // A single write so it is read by a single read on another thread
        byte[] headersFrameHeader = new byte[9];
        ByteBuffer headersPayload = ByteBuffer.allocate(128);
        buildPostRequest(headersFrameHeader, headersPayload, false, null, -1, "/summary", new byte[9],
                ByteBuffer.allocate(0), null, false, 1);
        os.write(SETTINGS_ACK_FRAME);
        os.write(headersFrameHeader);
        os.write(headersPayload.array(), headersPayload.arrayOffset(), headersPayload.limit());
        os.write(createDataFrame(1, 'A', DATA_A_LENGTH, false));
        os.flush();
        assertReached(raceHooks.dataAPaused, "DATA frame A was not processed by another thread");

        // DATA frame B must not be read before the processing of DATA frame A has completed
        os.write(createDataFrame(1, 'B', DATA_B_LENGTH, false));
        os.flush();
        assertReached(raceHooks.dataFramesProcessed, "DATA frames A and B were not both processed");

        // End of the request body
        os.write(createDataFrame(1, 'x', 0, true));
        os.flush();

        int frameCount = 0;
        while (!output.getTrace().contains("1-EndOfStream") && frameCount++ < 20) {
            parser.readFrame();
        }

        Assert.assertEquals(Arrays.asList(Integer.valueOf(DATA_A_LENGTH), Integer.valueOf(DATA_B_LENGTH)),
                raceHooks.processedDataFrameLengths);
        Assert.assertTrue(output.getTrace(),
                output.getTrace().contains("1-Body-A" + DATA_A_LENGTH + " B" + DATA_B_LENGTH + "\n"));
    }


    /*
     * The client sends the preface but not its settings. The connection has to be closed after a timeout.
     */
    @Test
    public void testIncompleteClientPreface() throws Exception {
        // Short read and keep-alive timeouts so the test completes sooner
        enableHttp2(200, false, 2000, 5000, 2000, 5000, 5000);
        configureAndStartWebApplication();
        openClientConnection(false, false);
        s.setSoTimeout(15000);

        os.write(Http2Parser.CLIENT_PREFACE_START);
        os.flush();

        // A SocketTimeoutException fails the test
        readUntilClosed();
    }


    private void readUntilClosed() throws IOException {
        InputStream is = s.getInputStream();
        byte[] buf = new byte[1024];
        while (is.read(buf) != -1) {
            // Ignore the frames sent by the server
        }
    }


    private static void assertReached(CountDownLatch latch, String message) throws InterruptedException {
        Assert.assertTrue(message, latch.await(10, TimeUnit.SECONDS));
    }


    private static void awaitQuietly(CountDownLatch latch, int seconds) {
        try {
            latch.await(seconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    private static byte[] createDataFrame(int streamId, char content, int length, boolean endOfStream) {
        byte[] frame = new byte[9 + length];
        ByteUtil.setThreeBytes(frame, 0, length);
        frame[3] = FrameType.DATA.getIdByte();
        if (endOfStream) {
            frame[4] = 0x01;
        }
        ByteUtil.set31Bits(frame, 5, streamId);
        Arrays.fill(frame, 9, frame.length, (byte) content);
        return frame;
    }


    /*
     * Synchronisation between the test (client) and the hooks in RaceTestHttp2AsyncUpgradeHandler (server).
     */
    private static class RaceHooks {
        // The first call to upgradeDispatch() has completed init()
        private final CountDownLatch firstDispatchWaiting = new CountDownLatch(1);
        // Another thread has paused before processing DATA frame A
        private final CountDownLatch dataAPaused = new CountDownLatch(1);
        // DATA frame B has been processed while the processing of DATA frame A was paused
        private final CountDownLatch dataBProcessed = new CountDownLatch(1);
        // DATA frames A and B have been processed
        private final CountDownLatch dataFramesProcessed = new CountDownLatch(2);
        // The lengths of the DATA frames in the order they were processed
        private final List<Integer> processedDataFrameLengths = Collections.synchronizedList(new ArrayList<>());

        private volatile Thread firstDispatchThread;
        private volatile Thread pausedThread;
    }


    private static class RaceTestHttp2Protocol extends Http2Protocol {

        private final RaceHooks hooks;

        RaceTestHttp2Protocol(RaceHooks hooks) {
            this.hooks = hooks;
        }

        @Override
        public InternalHttpUpgradeHandler getInternalUpgradeHandler(SocketWrapperBase<?> socketWrapper,
                Adapter adapter, Request coyoteRequest) {
            if (!socketWrapper.hasAsyncIO()) {
                return super.getInternalUpgradeHandler(socketWrapper, adapter, coyoteRequest);
            }
            return new RaceTestHttp2AsyncUpgradeHandler(this, adapter, coyoteRequest, socketWrapper, hooks);
        }
    }


    private static class RaceTestHttp2AsyncUpgradeHandler extends Http2AsyncUpgradeHandler {

        private final RaceHooks hooks;
        private final AtomicBoolean firstInit = new AtomicBoolean(true);

        RaceTestHttp2AsyncUpgradeHandler(Http2Protocol protocol, Adapter adapter, Request coyoteRequest,
                SocketWrapperBase<?> socketWrapper, RaceHooks hooks) {
            super(protocol, adapter, coyoteRequest, socketWrapper);
            this.hooks = hooks;
        }

        @Override
        public void init(WebConnection webConnection) {
            // The first call is made by the first call to upgradeDispatch(). Further calls are NO-OPs.
            boolean firstCall = firstInit.compareAndSet(true, false);
            if (firstCall) {
                hooks.firstDispatchThread = Thread.currentThread();
            }
            super.init(webConnection);
            if (firstCall) {
                // Hold the first call to upgradeDispatch() here until another thread is processing a read
                hooks.firstDispatchWaiting.countDown();
                awaitQuietly(hooks.dataAPaused, 5);
            }
        }

        @Override
        public ByteBuffer startRequestBodyFrame(int streamId, int dataLength, boolean endOfStream)
                throws Http2Exception {
            if (dataLength == DATA_A_LENGTH && Thread.currentThread() != hooks.firstDispatchThread) {
                // The read semaphore has been released but the frames that were read have not yet been processed
                hooks.pausedThread = Thread.currentThread();
                hooks.dataAPaused.countDown();
                awaitQuietly(hooks.dataBProcessed, 2);
                hooks.pausedThread = null;
            }
            return super.startRequestBodyFrame(streamId, dataLength, endOfStream);
        }

        @Override
        public void endRequestBodyFrame(int streamId, int dataLength) throws Http2Exception, IOException {
            super.endRequestBodyFrame(streamId, dataLength);
            if (dataLength > 0) {
                hooks.processedDataFrameLengths.add(Integer.valueOf(dataLength));
                hooks.dataFramesProcessed.countDown();
            }
            Thread pausedThread = hooks.pausedThread;
            if (pausedThread != null && pausedThread != Thread.currentThread()) {
                hooks.dataBProcessed.countDown();
            }
        }
    }


    /*
     * Responds with the request body as runs of equal bytes, e.g. "A2000 B3000".
     */
    private static class BodySummaryServlet extends HttpServlet {

        private static final long serialVersionUID = 1L;

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            IOTools.flow(req.getInputStream(), baos);
            byte[] body = baos.toByteArray();

            StringBuilder summary = new StringBuilder();
            int start = 0;
            for (int i = 1; i <= body.length; i++) {
                if (i == body.length || body[i] != body[start]) {
                    if (summary.length() > 0) {
                        summary.append(' ');
                    }
                    summary.append((char) body[start]).append(i - start);
                    start = i;
                }
            }

            resp.setContentType("text/plain");
            resp.setCharacterEncoding("ISO-8859-1");
            resp.getWriter().print(summary);
        }
    }
}
