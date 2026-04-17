/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Time: Created - TASK-WS3.01
package kafka.server.http.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link WsPublisherConfirmTracker}.
 *
 * <p>These verify:</p>
 * <ul>
 *   <li>enable/isEnabled flag behaviour</li>
 *   <li>recordPending only stores when enabled</li>
 *   <li>confirmSuccess writes a {@code published} frame and clears pending</li>
 *   <li>confirmFailure writes a {@code publish-failed} frame with errorCode/errorMessage</li>
 *   <li>confirming an unknown publishId is a no-op</li>
 *   <li>pendingCount tracks in-flight correctly</li>
 *   <li>publishId without confirms enabled is silently ignored (no frame emitted)</li>
 * </ul>
 *
 * // Time: Created - TASK-WS3.01
 */
class WsPublisherConfirmTrackerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ChannelHandlerContext mockChannelCtx;
    private WsConnectionContext connCtx;
    private final List<String> writtenFrames = new ArrayList<>();

    @BeforeEach
    void setUp() {
        mockChannelCtx = mock(ChannelHandlerContext.class);
        Channel mockChannel = mock(Channel.class);
        when(mockChannelCtx.channel()).thenReturn(mockChannel);
        when(mockChannel.isActive()).thenReturn(true);

        ChannelFuture future = mock(ChannelFuture.class);
        when(mockChannelCtx.writeAndFlush(any())).thenAnswer(inv -> {
            Object arg = inv.getArgument(0);
            if (arg instanceof TextWebSocketFrame) {
                TextWebSocketFrame f = (TextWebSocketFrame) arg;
                writtenFrames.add(f.text());
                f.release();
            }
            return future;
        });

        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");
        InetSocketAddress remote = new InetSocketAddress("127.0.0.1", 12345);
        connCtx = new WsConnectionContext(
            "ws-1-test0001", principal, "/", mockChannelCtx, remote);
    }

    // ------------------------------------------------------------------
    //  Constructor validation
    // ------------------------------------------------------------------

    @Test
    void constructor_nullContext_throws() {
        assertThrows(NullPointerException.class, () -> new WsPublisherConfirmTracker(null));
    }

    // ------------------------------------------------------------------
    //  enable / isEnabled
    // ------------------------------------------------------------------

    @Test
    void enable_setsFlag() {
        WsPublisherConfirmTracker tracker = new WsPublisherConfirmTracker(connCtx);
        assertFalse(tracker.isEnabled(), "default disabled");
        tracker.enable();
        assertTrue(tracker.isEnabled());
    }

    @Test
    void enable_isIdempotent() {
        WsPublisherConfirmTracker tracker = new WsPublisherConfirmTracker(connCtx);
        tracker.enable();
        tracker.enable();
        tracker.enable();
        assertTrue(tracker.isEnabled());
    }

    // ------------------------------------------------------------------
    //  recordPending
    // ------------------------------------------------------------------

    @Test
    void recordPending_whenDisabled_noOp() {
        WsPublisherConfirmTracker tracker = new WsPublisherConfirmTracker(connCtx);
        tracker.recordPending(1L);
        tracker.recordPending(2L);
        assertEquals(0, tracker.pendingCount(),
            "recordPending must not store anything when confirms are disabled");
    }

    @Test
    void recordPending_whenEnabled_incrementsPending() {
        WsPublisherConfirmTracker tracker = new WsPublisherConfirmTracker(connCtx);
        tracker.enable();
        tracker.recordPending(1L);
        tracker.recordPending(2L);
        tracker.recordPending(3L);
        assertEquals(3, tracker.pendingCount());
    }

    // ------------------------------------------------------------------
    //  confirmSuccess
    // ------------------------------------------------------------------

    @Test
    void confirmSuccess_writesPublishedFrame() throws Exception {
        WsPublisherConfirmTracker tracker = new WsPublisherConfirmTracker(connCtx);
        tracker.enable();
        tracker.recordPending(42L);

        tracker.confirmSuccess(42L);

        assertEquals(1, writtenFrames.size(), "a published frame should be written");
        JsonNode frame = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("published", frame.get("type").asText());
        assertEquals(42L, frame.get("publishId").asLong());
    }

    @Test
    void confirmSuccess_removesPending() {
        WsPublisherConfirmTracker tracker = new WsPublisherConfirmTracker(connCtx);
        tracker.enable();
        tracker.recordPending(1L);
        tracker.recordPending(2L);
        assertEquals(2, tracker.pendingCount());

        tracker.confirmSuccess(1L);

        assertEquals(1, tracker.pendingCount(),
            "confirmSuccess must remove the pending id");
    }

    @Test
    void confirmSuccess_unknownPublishId_noOp() {
        WsPublisherConfirmTracker tracker = new WsPublisherConfirmTracker(connCtx);
        tracker.enable();

        // Never recorded 7L — confirm should be silent.
        tracker.confirmSuccess(7L);

        assertEquals(0, writtenFrames.size(),
            "no frame must be written when publishId was never pending");
    }

    @Test
    void confirmSuccess_whenDisabled_noFrame() {
        WsPublisherConfirmTracker tracker = new WsPublisherConfirmTracker(connCtx);
        // confirms not enabled -> recordPending is no-op, so confirmSuccess must no-op.
        tracker.recordPending(1L);
        tracker.confirmSuccess(1L);

        assertEquals(0, writtenFrames.size(),
            "confirmSuccess without confirms enabled must write no frame");
    }

    // ------------------------------------------------------------------
    //  confirmFailure
    // ------------------------------------------------------------------

    @Test
    void confirmFailure_writesPublishFailedFrame() throws Exception {
        WsPublisherConfirmTracker tracker = new WsPublisherConfirmTracker(connCtx);
        tracker.enable();
        tracker.recordPending(7L);

        tracker.confirmFailure(7L, "NOT_ENOUGH_REPLICAS", "ISR below minimum");

        assertEquals(1, writtenFrames.size());
        JsonNode frame = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("publish-failed", frame.get("type").asText());
        assertEquals(7L, frame.get("publishId").asLong());
        assertEquals("NOT_ENOUGH_REPLICAS", frame.get("errorCode").asText());
        assertEquals("ISR below minimum", frame.get("errorMessage").asText());
    }

    @Test
    void confirmFailure_removesPending() {
        WsPublisherConfirmTracker tracker = new WsPublisherConfirmTracker(connCtx);
        tracker.enable();
        tracker.recordPending(1L);

        tracker.confirmFailure(1L, "E", "m");

        assertEquals(0, tracker.pendingCount());
    }

    @Test
    void confirmFailure_unknownPublishId_noOp() {
        WsPublisherConfirmTracker tracker = new WsPublisherConfirmTracker(connCtx);
        tracker.enable();

        tracker.confirmFailure(999L, "E", "m");

        assertEquals(0, writtenFrames.size());
    }

    @Test
    void confirmFailure_whenDisabled_noFrame() {
        WsPublisherConfirmTracker tracker = new WsPublisherConfirmTracker(connCtx);
        tracker.recordPending(1L);
        tracker.confirmFailure(1L, "E", "m");

        assertEquals(0, writtenFrames.size(),
            "confirmFailure without confirms enabled must write no frame");
    }

    @Test
    void confirmFailure_handlesNullErrorMessage() throws Exception {
        WsPublisherConfirmTracker tracker = new WsPublisherConfirmTracker(connCtx);
        tracker.enable();
        tracker.recordPending(1L);

        tracker.confirmFailure(1L, "E", null);

        assertEquals(1, writtenFrames.size());
        JsonNode frame = MAPPER.readTree(writtenFrames.get(0));
        // A null errorMessage should serialise to empty string or be omitted - just
        // check the frame is valid JSON with type + publishId + errorCode.
        assertEquals("publish-failed", frame.get("type").asText());
        assertEquals(1L, frame.get("publishId").asLong());
        assertEquals("E", frame.get("errorCode").asText());
    }

    @Test
    void confirmFailure_escapesErrorMessage() throws Exception {
        // If the server ever injects a message with a quote or backslash, the
        // JSON must still parse cleanly — this guards against naive string
        // concat in the frame builder.
        WsPublisherConfirmTracker tracker = new WsPublisherConfirmTracker(connCtx);
        tracker.enable();
        tracker.recordPending(1L);

        tracker.confirmFailure(1L, "E", "a\"b\\c");

        assertEquals(1, writtenFrames.size());
        JsonNode frame = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("a\"b\\c", frame.get("errorMessage").asText());
    }

    // ------------------------------------------------------------------
    //  pendingCount
    // ------------------------------------------------------------------

    @Test
    void pendingCount_tracksInFlight() {
        WsPublisherConfirmTracker tracker = new WsPublisherConfirmTracker(connCtx);
        tracker.enable();
        assertEquals(0, tracker.pendingCount());

        tracker.recordPending(1L);
        tracker.recordPending(2L);
        tracker.recordPending(3L);
        assertEquals(3, tracker.pendingCount());

        tracker.confirmSuccess(1L);
        assertEquals(2, tracker.pendingCount());

        tracker.confirmFailure(2L, "E", "m");
        assertEquals(1, tracker.pendingCount());

        tracker.confirmSuccess(3L);
        assertEquals(0, tracker.pendingCount());
    }

    // ------------------------------------------------------------------
    //  publishId without confirms enabled -> silent
    // ------------------------------------------------------------------

    @Test
    void publishIdWithoutConfirmsEnabled_silentlyIgnored() {
        WsPublisherConfirmTracker tracker = new WsPublisherConfirmTracker(connCtx);
        // Never enabled.
        tracker.recordPending(1L);
        tracker.confirmSuccess(1L);
        tracker.recordPending(2L);
        tracker.confirmFailure(2L, "E", "m");

        assertEquals(0, writtenFrames.size(),
            "no frames should ever be written when confirms are not enabled");
        assertEquals(0, tracker.pendingCount());
    }
}
