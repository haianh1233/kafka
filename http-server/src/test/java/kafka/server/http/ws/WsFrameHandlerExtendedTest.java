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
// Time: Created - TASK-WS-T.02
package kafka.server.http.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Supplementary unit tests for {@link WsFrameHandler} covering cross-cutting
 * scenarios that complement {@link WsFrameHandlerTest}'s per-type dispatch and
 * per-error-path coverage.
 *
 * <p>Focus areas:
 * <ul>
 *   <li>Multi-frame sequences where handler state and connection context state
 *       must be preserved across invocations.</li>
 *   <li>Interleaving of valid and malformed frames — bad frames must not poison
 *       the handler's ability to dispatch subsequent valid frames.</li>
 *   <li>Two independent connections driven through two handler instances must
 *       not cross-talk through the shared {@code ObjectMapper}.</li>
 *   <li>Concurrent dispatch from multiple threads on separate handlers — verifies
 *       the handler has no accidental static mutable state.</li>
 *   <li>Subscription cleanup on {@code channelInactive} after several frames have
 *       registered subscriptions via a handler override.</li>
 *   <li>Non-textual {@code id} fields (numeric / array) silently degrade to no
 *       correlation id rather than crashing dispatch.</li>
 *   <li>Correlation id echo survives a successful dispatch that throws an
 *       {@link IllegalArgumentException} inside the handler body.</li>
 * </ul>
 *
 * // Time: Created - TASK-WS-T.02
 */
class WsFrameHandlerExtendedTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ------------------------------------------------------------------
    //  Per-handler fixture — each test builds its own so state is isolated.
    // ------------------------------------------------------------------

    /**
     * Lightweight fixture: a mocked {@link ChannelHandlerContext} plus a
     * real {@link WsConnectionContext}, capturing every written frame's text.
     */
    private static final class Fixture {
        final ChannelHandlerContext mockCtx;
        final WsConnectionContext connCtx;
        final WsConfigs wsConfigs;
        final List<String> writtenFrames = new ArrayList<>();

        Fixture(String sessionId) {
            this.mockCtx = mock(ChannelHandlerContext.class);
            Channel ch = mock(Channel.class);
            when(mockCtx.channel()).thenReturn(ch);
            when(ch.isActive()).thenReturn(true);
            ChannelFuture future = mock(ChannelFuture.class);
            when(mockCtx.writeAndFlush(any())).thenAnswer(inv -> {
                Object arg = inv.getArgument(0);
                if (arg instanceof TextWebSocketFrame) {
                    TextWebSocketFrame f = (TextWebSocketFrame) arg;
                    writtenFrames.add(f.text());
                    f.release();
                }
                return future;
            });
            KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");
            InetSocketAddress remote = new InetSocketAddress("127.0.0.1", 40000);
            this.connCtx = new WsConnectionContext(sessionId, principal, "/", mockCtx, remote);
            this.wsConfigs = WsConfigs.withDefaults();
        }
    }

    /** Deliver a raw JSON text frame to the handler using {@code channelRead}. */
    private static void deliver(WsFrameHandler handler, ChannelHandlerContext ctx, String text) {
        TextWebSocketFrame f = new TextWebSocketFrame(text);
        try {
            handler.channelRead(ctx, f);
        } catch (Exception e) {
            throw new AssertionError("channelRead threw", e);
        }
    }

    // ------------------------------------------------------------------
    //  1. Multi-frame sequences — state preserved across invocations
    // ------------------------------------------------------------------

    @Test
    void sequentialFrames_eachDispatchedIndependently() {
        Fixture fx = new Fixture("ws-seq-1");
        List<String> invokedTypes = new ArrayList<>();

        WsFrameHandler handler = new WsFrameHandler(fx.connCtx, fx.wsConfigs) {
            @Override void handlePublish(WsConnectionContext ctx, JsonNode msg) {
                invokedTypes.add("publish:" + msg.get("id").asText());
            }
            @Override void handleAck(WsConnectionContext ctx, JsonNode msg) {
                invokedTypes.add("ack:" + msg.get("id").asText());
            }
            @Override void handleSubscribe(WsConnectionContext ctx, JsonNode msg) {
                invokedTypes.add("subscribe:" + msg.get("id").asText());
            }
        };

        deliver(handler, fx.mockCtx, "{\"type\":\"publish\",\"id\":\"p1\"}");
        deliver(handler, fx.mockCtx, "{\"type\":\"subscribe\",\"id\":\"s1\"}");
        deliver(handler, fx.mockCtx, "{\"type\":\"ack\",\"id\":\"a1\"}");
        deliver(handler, fx.mockCtx, "{\"type\":\"publish\",\"id\":\"p2\"}");

        assertEquals(List.of("publish:p1", "subscribe:s1", "ack:a1", "publish:p2"), invokedTypes,
                "every frame in a sequence must be dispatched, in order");
        assertTrue(fx.writtenFrames.isEmpty(),
                "no error frames when handlers succeed; got: " + fx.writtenFrames);
    }

    @Test
    void invalidFrameDoesNotPoisonSubsequentDispatch() {
        // A malformed frame mid-stream must not prevent the next valid frame from
        // reaching its handler — the handler is stateless between frames and the
        // parser's failure path should exit cleanly after emitting the error frame.
        Fixture fx = new Fixture("ws-seq-2");
        AtomicInteger publishCalls = new AtomicInteger();
        WsFrameHandler handler = new WsFrameHandler(fx.connCtx, fx.wsConfigs) {
            @Override void handlePublish(WsConnectionContext ctx, JsonNode msg) {
                publishCalls.incrementAndGet();
            }
        };

        // valid → malformed → valid → empty → valid
        deliver(handler, fx.mockCtx, "{\"type\":\"publish\",\"id\":\"p1\"}");
        deliver(handler, fx.mockCtx, "not json at all");
        deliver(handler, fx.mockCtx, "{\"type\":\"publish\",\"id\":\"p2\"}");
        deliver(handler, fx.mockCtx, "");
        deliver(handler, fx.mockCtx, "{\"type\":\"publish\",\"id\":\"p3\"}");

        assertEquals(3, publishCalls.get(), "three valid publishes should have dispatched");
        assertEquals(2, fx.writtenFrames.size(),
                "exactly two error frames expected (malformed + empty); got: " + fx.writtenFrames);
    }

    @Test
    void enableConfirmsPersistsAcrossLaterFrames() {
        // Once enable-confirms flips the ctx flag, subsequent publish frames should
        // observe the flag as true. Any number of dispatches between the two must
        // not reset the flag.
        Fixture fx = new Fixture("ws-seq-3");
        AtomicReference<Boolean> publishSawEnabled = new AtomicReference<>();
        WsFrameHandler handler = new WsFrameHandler(fx.connCtx, fx.wsConfigs) {
            @Override void handlePublish(WsConnectionContext ctx, JsonNode msg) {
                publishSawEnabled.compareAndSet(null, ctx.isPublishConfirmsEnabled());
            }
        };

        deliver(handler, fx.mockCtx, "{\"type\":\"enable-confirms\"}");
        // Intervening malformed frame must not unset the flag.
        deliver(handler, fx.mockCtx, "not json");
        deliver(handler, fx.mockCtx, "{\"type\":\"publish\",\"id\":\"p1\"}");

        assertTrue(fx.connCtx.isPublishConfirmsEnabled(),
                "enable-confirms flag must persist");
        assertEquals(Boolean.TRUE, publishSawEnabled.get(),
                "publish handler should observe the enabled flag after enable-confirms");
    }

    // ------------------------------------------------------------------
    //  2. Isolation — two handlers on two connections don't cross-talk
    // ------------------------------------------------------------------

    @Test
    void twoConnections_independentStateAndFrames() {
        Fixture fxA = new Fixture("ws-A");
        Fixture fxB = new Fixture("ws-B");

        List<String> hitA = new ArrayList<>();
        List<String> hitB = new ArrayList<>();

        WsFrameHandler handlerA = new WsFrameHandler(fxA.connCtx, fxA.wsConfigs) {
            @Override void handlePublish(WsConnectionContext ctx, JsonNode msg) {
                hitA.add("A:" + msg.get("id").asText());
            }
        };
        WsFrameHandler handlerB = new WsFrameHandler(fxB.connCtx, fxB.wsConfigs) {
            @Override void handlePublish(WsConnectionContext ctx, JsonNode msg) {
                hitB.add("B:" + msg.get("id").asText());
            }
        };

        // enable-confirms only on A
        deliver(handlerA, fxA.mockCtx, "{\"type\":\"enable-confirms\"}");
        deliver(handlerA, fxA.mockCtx, "{\"type\":\"publish\",\"id\":\"pa\"}");
        deliver(handlerB, fxB.mockCtx, "{\"type\":\"publish\",\"id\":\"pb\"}");

        assertTrue(fxA.connCtx.isPublishConfirmsEnabled(),
                "connection A confirms must be enabled");
        assertTrue(!fxB.connCtx.isPublishConfirmsEnabled(),
                "connection B confirms must NOT leak from A");
        assertEquals(List.of("A:pa"), hitA);
        assertEquals(List.of("B:pb"), hitB);

        // Frame capture stays on its own mock channel.
        assertEquals(1, fxA.writtenFrames.size(), "A sees its own confirms-enabled frame only");
        JsonNode confirmsEnabled = readJson(fxA.writtenFrames.get(0));
        assertEquals("confirms-enabled", confirmsEnabled.get("type").asText());
        assertTrue(fxB.writtenFrames.isEmpty(),
                "B should have written no frames; got: " + fxB.writtenFrames);
    }

    // ------------------------------------------------------------------
    //  3. Concurrent dispatch across independent handlers
    // ------------------------------------------------------------------

    @Test
    void concurrentDispatch_acrossConnections_noCrossTalk() throws Exception {
        // Each Netty channel runs on its own event loop in production, so the
        // handler is not expected to be thread-safe within a single connection.
        // But two handler instances on two connections running on two threads
        // must not interfere via any accidental static state.
        int connections = 4;
        int framesPerConnection = 50;
        List<Fixture> fixtures = new ArrayList<>();
        List<WsFrameHandler> handlers = new ArrayList<>();
        List<List<String>> perConnectionHits = new ArrayList<>();
        for (int i = 0; i < connections; i++) {
            Fixture fx = new Fixture("ws-conc-" + i);
            fixtures.add(fx);
            List<String> hits = new CopyOnWriteArrayList<>();
            perConnectionHits.add(hits);
            handlers.add(new WsFrameHandler(fx.connCtx, fx.wsConfigs) {
                @Override void handlePublish(WsConnectionContext ctx, JsonNode msg) {
                    hits.add(ctx.sessionId() + ":" + msg.get("id").asText());
                }
            });
        }

        ExecutorService exec = Executors.newFixedThreadPool(connections);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<java.util.concurrent.Future<?>> tasks = new ArrayList<>();
            for (int i = 0; i < connections; i++) {
                final int connIdx = i;
                tasks.add(exec.submit(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    WsFrameHandler h = handlers.get(connIdx);
                    ChannelHandlerContext c = fixtures.get(connIdx).mockCtx;
                    for (int j = 0; j < framesPerConnection; j++) {
                        deliver(h, c, "{\"type\":\"publish\",\"id\":\"" + j + "\"}");
                    }
                }));
            }
            start.countDown();
            for (java.util.concurrent.Future<?> t : tasks) {
                t.get(10, TimeUnit.SECONDS);
            }
        } finally {
            exec.shutdownNow();
            assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS),
                    "executor must shut down");
        }

        for (int i = 0; i < connections; i++) {
            assertEquals(framesPerConnection, perConnectionHits.get(i).size(),
                    "connection " + i + " must have received all frames");
            for (String s : perConnectionHits.get(i)) {
                assertTrue(s.startsWith("ws-conc-" + i + ":"),
                        "connection " + i + " saw a message from another connection: " + s);
            }
        }
    }

    // ------------------------------------------------------------------
    //  4. Subscription cleanup on channelInactive after population
    // ------------------------------------------------------------------

    @Test
    void channelInactive_clearsSubscriptionsPopulatedByHandler() {
        // A handler override populates subscriptions when a subscribe frame
        // arrives. After several subscribe frames, channelInactive must clear
        // the map so a stale reference can't outlive the connection.
        Fixture fx = new Fixture("ws-sub-clear");
        WsFrameHandler handler = new WsFrameHandler(fx.connCtx, fx.wsConfigs) {
            @Override void handleSubscribe(WsConnectionContext ctx, JsonNode msg) {
                String id = msg.get("id").asText();
                ctx.subscriptions().put(id, "placeholder-state");
            }
        };

        deliver(handler, fx.mockCtx, "{\"type\":\"subscribe\",\"id\":\"s1\"}");
        deliver(handler, fx.mockCtx, "{\"type\":\"subscribe\",\"id\":\"s2\"}");
        deliver(handler, fx.mockCtx, "{\"type\":\"subscribe\",\"id\":\"s3\"}");

        assertEquals(3, fx.connCtx.subscriptions().size(),
                "handler override should have added three subs");

        handler.channelInactive(fx.mockCtx);

        assertTrue(fx.connCtx.subscriptions().isEmpty(),
                "channelInactive must clear subscriptions even when populated mid-session");
    }

    // ------------------------------------------------------------------
    //  5. Non-textual correlation id degrades gracefully
    // ------------------------------------------------------------------

    @Test
    void numericCorrelationId_degradesToNull_andDispatchStillHappens() {
        // {"id":42} is not a textual correlation id per our contract; the frame
        // must still dispatch to the right handler, and error frames emitted
        // after dispatch must omit the id rather than embedding the numeric.
        Fixture fx = new Fixture("ws-id-num");
        AtomicReference<JsonNode> received = new AtomicReference<>();
        WsFrameHandler handler = new WsFrameHandler(fx.connCtx, fx.wsConfigs) {
            @Override void handlePublish(WsConnectionContext ctx, JsonNode msg) {
                received.set(msg);
                throw new IllegalStateException("simulated");
            }
        };

        deliver(handler, fx.mockCtx, "{\"type\":\"publish\",\"id\":42}");

        assertNotNull(received.get(), "handler should still run");
        assertEquals(1, fx.writtenFrames.size(), "INTERNAL_ERROR frame should be emitted");
        JsonNode err = readJson(fx.writtenFrames.get(0));
        assertEquals("error", err.get("type").asText());
        assertEquals("INTERNAL_ERROR", err.get("errorCode").asText());
        assertNull(err.get("id"),
                "numeric id is not textual so it must be dropped from the error frame");
    }

    @Test
    void arrayCorrelationId_degradesToNull_onUnknownType() {
        // An array-typed id must not crash extraction; the UNKNOWN_MESSAGE_TYPE
        // frame should simply omit the id entirely.
        Fixture fx = new Fixture("ws-id-arr");
        WsFrameHandler handler = new WsFrameHandler(fx.connCtx, fx.wsConfigs);

        deliver(handler, fx.mockCtx, "{\"type\":\"nope\",\"id\":[1,2,3]}");

        assertEquals(1, fx.writtenFrames.size());
        JsonNode err = readJson(fx.writtenFrames.get(0));
        assertEquals("UNKNOWN_MESSAGE_TYPE", err.get("errorCode").asText());
        assertNull(err.get("id"),
                "array-typed id should be treated as missing");
    }

    // ------------------------------------------------------------------
    //  6. Exception in handler preserves correlation id in error frame
    // ------------------------------------------------------------------

    @Test
    void illegalArgumentException_inHandler_emitsInternalErrorWithCorrelationId() {
        // Existing tests cover UnsupportedOperationException and IllegalStateException;
        // verify the catch-RuntimeException arm also handles IAE and that the
        // original correlation id is preserved on the outbound error frame.
        Fixture fx = new Fixture("ws-iae");
        WsFrameHandler handler = new WsFrameHandler(fx.connCtx, fx.wsConfigs) {
            @Override void handleSubscribe(WsConnectionContext ctx, JsonNode msg) {
                throw new IllegalArgumentException("bad queue name");
            }
        };

        deliver(handler, fx.mockCtx, "{\"type\":\"subscribe\",\"id\":\"req-99\"}");

        assertEquals(1, fx.writtenFrames.size());
        JsonNode err = readJson(fx.writtenFrames.get(0));
        assertEquals("INTERNAL_ERROR", err.get("errorCode").asText());
        assertEquals("req-99", err.get("id").asText(),
                "correlation id must survive the RuntimeException catch");
        assertTrue(err.get("errorMessage").asText().contains("subscribe"),
                "error message should mention the failing type");
    }

    // ------------------------------------------------------------------
    //  7. Mixed dispatch sequence — all 15 types across one handler instance
    // ------------------------------------------------------------------

    @Test
    void fullDispatchSequence_allTypesReachTheirHandlers() {
        // One handler, one connection, drives every one of the 15 message types
        // in one pass. Verifies the dispatch switch handles all cases in the
        // same session without leaking between cases.
        Fixture fx = new Fixture("ws-full");
        List<String> hit = Collections.synchronizedList(new ArrayList<>());

        WsFrameHandler handler = new WsFrameHandler(fx.connCtx, fx.wsConfigs) {
            @Override void handleDeclareExchange(WsConnectionContext c, JsonNode m) {
                hit.add("declare-exchange");
            }
            @Override void handleDeleteExchange(WsConnectionContext c, JsonNode m) {
                hit.add("delete-exchange");
            }
            @Override void handleDeclareQueue(WsConnectionContext c, JsonNode m) {
                hit.add("declare-queue");
            }
            @Override void handleDeleteQueue(WsConnectionContext c, JsonNode m) {
                hit.add("delete-queue");
            }
            @Override void handleBind(WsConnectionContext c, JsonNode m) {
                hit.add("bind");
            }
            @Override void handleUnbind(WsConnectionContext c, JsonNode m) {
                hit.add("unbind");
            }
            @Override void handlePublish(WsConnectionContext c, JsonNode m) {
                hit.add("publish");
            }
            @Override void handleSubscribe(WsConnectionContext c, JsonNode m) {
                hit.add("subscribe");
            }
            @Override void handleUnsubscribe(WsConnectionContext c, JsonNode m) {
                hit.add("unsubscribe");
            }
            @Override void handleGet(WsConnectionContext c, JsonNode m) {
                hit.add("get");
            }
            @Override void handlePurgeQueue(WsConnectionContext c, JsonNode m) {
                hit.add("purge-queue");
            }
            @Override void handleAck(WsConnectionContext c, JsonNode m) {
                hit.add("ack");
            }
            @Override void handleNack(WsConnectionContext c, JsonNode m) {
                hit.add("nack");
            }
            @Override void handleCredits(WsConnectionContext c, JsonNode m) {
                hit.add("credits");
            }
            @Override void handleEnableConfirms(WsConnectionContext c, JsonNode m) {
                hit.add("enable-confirms");
            }
        };

        List<String> types = List.of(
                "declare-exchange", "delete-exchange", "declare-queue", "delete-queue",
                "bind", "unbind", "publish", "subscribe", "unsubscribe", "get",
                "purge-queue", "ack", "nack", "credits", "enable-confirms");
        for (String t : types) {
            deliver(handler, fx.mockCtx, "{\"type\":\"" + t + "\"}");
        }

        assertEquals(types, hit,
                "every type must dispatch in submitted order");
        assertTrue(fx.writtenFrames.isEmpty(),
                "no error frames expected; got: " + fx.writtenFrames);
    }

    // ------------------------------------------------------------------
    //  8. Extremely long valid JSON payload (but under default frame limit)
    //     exercises the parser on a non-trivial body — verifies handler
    //     doesn't truncate or silently drop oversized-but-legal frames.
    // ------------------------------------------------------------------

    @Test
    void largeButValidJsonPayload_dispatchesCleanly() {
        Fixture fx = new Fixture("ws-large");
        AtomicReference<String> bodySeen = new AtomicReference<>();
        WsFrameHandler handler = new WsFrameHandler(fx.connCtx, fx.wsConfigs) {
            @Override void handlePublish(WsConnectionContext ctx, JsonNode msg) {
                bodySeen.set(msg.get("body").asText());
            }
        };

        // 4 KiB body — well under the default 1 MiB frame limit but large
        // enough to exercise Jackson's streaming parser.
        StringBuilder body = new StringBuilder(4096);
        for (int i = 0; i < 4096; i++) {
            body.append('x');
        }
        String expected = body.toString();
        String frame = "{\"type\":\"publish\",\"id\":\"big\",\"body\":\"" + expected + "\"}";

        deliver(handler, fx.mockCtx, frame);

        assertEquals(expected, bodySeen.get(),
                "full JSON body must reach the handler intact");
        assertTrue(fx.writtenFrames.isEmpty(),
                "no error frame expected for a valid large frame; got: " + fx.writtenFrames);
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    private static JsonNode readJson(String text) {
        try {
            return MAPPER.readTree(text);
        } catch (Exception e) {
            throw new AssertionError("captured frame is not valid JSON: " + text, e);
        }
    }
}
