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
// Time: Created - TASK-WS3.05
package kafka.server.http.ws;

import kafka.server.http.HttpRequestTranslator;
import kafka.server.http.routing.ExchangeManager;
import kafka.server.http.routing.RoutingEngine;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link WsAuthorizationHelper} — the ACL check helper for WS
 * operations introduced in TASK-WS3.05.
 *
 * <p>Each operation surface from the design doc §19.2 ACL table is exercised
 * under authorized + denied scenarios. The {@link FakeAuthorizer} is an
 * intentionally tiny in-memory implementation: a seeded map of {@code (principal,
 * resourceType, resourceName, operation)} → decision, used purely so the helper
 * can be unit-tested without a real broker.
 *
 * // Time: Created - TASK-WS3.05
 */
class WsAclAuthorizationTest {

    private static final KafkaPrincipal ALICE =
        new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");
    private static final KafkaPrincipal MALLORY =
        new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "mallory");
    private static final String CLUSTER_NAME = "kafka-cluster";

    private FakeAuthorizer authorizer;
    private WsAuthorizationHelper helper;

    @BeforeEach
    void setUp() {
        authorizer = new FakeAuthorizer();
        helper = new WsAuthorizationHelper(authorizer);
    }

    // ------------------------------------------------------------------
    //  Constructor validation
    // ------------------------------------------------------------------

    @Test
    void constructor_nullAuthorizer_throws() {
        assertThrows(NullPointerException.class, () -> new WsAuthorizationHelper(null));
    }

    // ------------------------------------------------------------------
    //  publish → TOPIC:WRITE on each target queue
    // ------------------------------------------------------------------

    @Test
    void publish_authorized_allQueues() {
        authorizer.allow(ALICE, ResourceType.TOPIC, "ws.a", AclOperation.WRITE);
        authorizer.allow(ALICE, ResourceType.TOPIC, "ws.b", AclOperation.WRITE);

        Set<String> denied = helper.filterUnauthorizedQueues(
            ALICE, Set.of("ws.a", "ws.b"), AclOperation.WRITE);

        assertTrue(denied.isEmpty(), "no queues denied when all WRITE ACLs are granted");
    }

    @Test
    void publish_unauthorized_oneQueue_rejectsAll() {
        authorizer.allow(ALICE, ResourceType.TOPIC, "ws.a", AclOperation.WRITE);
        // ws.b intentionally NOT granted — the helper surfaces it as denied.

        Set<String> denied = helper.filterUnauthorizedQueues(
            ALICE, Set.of("ws.a", "ws.b"), AclOperation.WRITE);

        assertEquals(Set.of("ws.b"), denied,
            "the single unauthorized queue is returned; caller must reject the entire publish");
    }

    @Test
    void publish_allUnauthorized_returnsAllTopics() {
        Set<String> denied = helper.filterUnauthorizedQueues(
            MALLORY, Set.of("ws.a", "ws.b", "ws.c"), AclOperation.WRITE);

        assertEquals(Set.of("ws.a", "ws.b", "ws.c"), denied);
    }

    @Test
    void filterUnauthorizedQueues_emptyInput_returnsEmpty() {
        Set<String> denied = helper.filterUnauthorizedQueues(
            ALICE, Set.of(), AclOperation.WRITE);
        assertTrue(denied.isEmpty());
    }

    // ------------------------------------------------------------------
    //  subscribe → TOPIC:READ
    // ------------------------------------------------------------------

    @Test
    void subscribe_authorized() {
        authorizer.allow(ALICE, ResourceType.TOPIC, "ws.orders", AclOperation.READ);
        assertTrue(helper.authorize(ALICE, ResourceType.TOPIC, "ws.orders", AclOperation.READ));
    }

    @Test
    void subscribe_unauthorized_returnsAccessRefused() {
        // No ACL granted — helper returns false and the caller is responsible for
        // emitting an ACCESS_REFUSED error frame.
        assertFalse(helper.authorize(MALLORY, ResourceType.TOPIC, "ws.orders", AclOperation.READ));
        assertEquals("ACCESS_REFUSED", WsAuthorizationHelper.ACCESS_REFUSED,
            "helper exposes the error code constant used in WS error frames");
    }

    // ------------------------------------------------------------------
    //  declare/delete-exchange → CLUSTER:ALTER
    // ------------------------------------------------------------------

    @Test
    void declareExchange_requiresClusterAlter() {
        authorizer.allow(ALICE, ResourceType.CLUSTER, CLUSTER_NAME, AclOperation.ALTER);

        assertTrue(helper.authorize(ALICE, ResourceType.CLUSTER, CLUSTER_NAME, AclOperation.ALTER),
            "ALTER on CLUSTER is the requirement for declare-exchange");
        assertFalse(helper.authorize(MALLORY, ResourceType.CLUSTER, CLUSTER_NAME, AclOperation.ALTER),
            "other users must be denied without explicit ACL");
    }

    @Test
    void deleteExchange_requiresClusterAlter() {
        authorizer.allow(ALICE, ResourceType.CLUSTER, CLUSTER_NAME, AclOperation.ALTER);
        // Same resource/operation as declare — symmetric check.
        assertTrue(helper.authorize(ALICE, ResourceType.CLUSTER, CLUSTER_NAME, AclOperation.ALTER));
    }

    // ------------------------------------------------------------------
    //  declare/delete-queue → TOPIC:CREATE / TOPIC:DELETE
    // ------------------------------------------------------------------

    @Test
    void declareQueue_requiresTopicCreate() {
        authorizer.allow(ALICE, ResourceType.TOPIC, "ws.newq", AclOperation.CREATE);
        assertTrue(helper.authorize(ALICE, ResourceType.TOPIC, "ws.newq", AclOperation.CREATE));
        assertFalse(helper.authorize(MALLORY, ResourceType.TOPIC, "ws.newq", AclOperation.CREATE));
    }

    @Test
    void deleteQueue_requiresTopicDelete() {
        authorizer.allow(ALICE, ResourceType.TOPIC, "ws.oldq", AclOperation.DELETE);
        assertTrue(helper.authorize(ALICE, ResourceType.TOPIC, "ws.oldq", AclOperation.DELETE));
        assertFalse(helper.authorize(MALLORY, ResourceType.TOPIC, "ws.oldq", AclOperation.DELETE));
    }

    // ------------------------------------------------------------------
    //  bind/unbind → TOPIC:ALTER
    // ------------------------------------------------------------------

    @Test
    void bind_requiresTopicAlter() {
        authorizer.allow(ALICE, ResourceType.TOPIC, "ws.orders", AclOperation.ALTER);
        assertTrue(helper.authorize(ALICE, ResourceType.TOPIC, "ws.orders", AclOperation.ALTER));
        assertFalse(helper.authorize(MALLORY, ResourceType.TOPIC, "ws.orders", AclOperation.ALTER));
    }

    @Test
    void unbind_requiresTopicAlter() {
        authorizer.allow(ALICE, ResourceType.TOPIC, "ws.orders", AclOperation.ALTER);
        assertTrue(helper.authorize(ALICE, ResourceType.TOPIC, "ws.orders", AclOperation.ALTER));
    }

    // ------------------------------------------------------------------
    //  userId field validation
    // ------------------------------------------------------------------

    @Test
    void userId_matchesPrincipal_allowed() {
        assertTrue(helper.validateUserId(ALICE, "alice"));
    }

    @Test
    void userId_mismatch_rejected() {
        assertFalse(helper.validateUserId(ALICE, "bob"),
            "userId mismatch must be rejected — caller emits ACCESS_REFUSED");
    }

    @Test
    void userId_null_allowed() {
        assertTrue(helper.validateUserId(ALICE, null));
    }

    @Test
    void userId_empty_allowed() {
        assertTrue(helper.validateUserId(ALICE, ""));
    }

    // ------------------------------------------------------------------
    //  AuthorizableRequestContext carries the principal
    // ------------------------------------------------------------------

    @Test
    void authorize_passesPrincipalThroughRequestContext() {
        authorizer.allow(ALICE, ResourceType.TOPIC, "ws.foo", AclOperation.WRITE);
        helper.authorize(ALICE, ResourceType.TOPIC, "ws.foo", AclOperation.WRITE);

        assertFalse(authorizer.receivedContexts.isEmpty());
        AuthorizableRequestContext ctx = authorizer.receivedContexts.get(0);
        assertEquals(ALICE, ctx.principal(), "authorizer sees the ws connection's principal");
        assertNotNull(ctx.listenerName(), "listenerName must be non-null");
        assertNotNull(ctx.clientAddress(), "clientAddress must be non-null");
    }

    @Test
    void authorize_buildsLiteralResourcePattern() {
        authorizer.allow(ALICE, ResourceType.TOPIC, "ws.foo", AclOperation.WRITE);
        helper.authorize(ALICE, ResourceType.TOPIC, "ws.foo", AclOperation.WRITE);

        assertEquals(1, authorizer.receivedActions.size());
        Action action = authorizer.receivedActions.get(0);
        ResourcePattern pat = action.resourcePattern();
        assertEquals(ResourceType.TOPIC, pat.resourceType());
        assertEquals("ws.foo", pat.name());
        assertEquals(AclOperation.WRITE, action.operation());
    }

    @Test
    void authorize_nullInputs_throw() {
        assertThrows(NullPointerException.class,
            () -> helper.authorize(null, ResourceType.TOPIC, "ws.x", AclOperation.WRITE));
        assertThrows(NullPointerException.class,
            () -> helper.authorize(ALICE, null, "ws.x", AclOperation.WRITE));
        assertThrows(NullPointerException.class,
            () -> helper.authorize(ALICE, ResourceType.TOPIC, null, AclOperation.WRITE));
        assertThrows(NullPointerException.class,
            () -> helper.authorize(ALICE, ResourceType.TOPIC, "ws.x", null));
    }

    // ------------------------------------------------------------------
    //  Integration with WsPublishHandler — multi-queue + userId end-to-end
    // ------------------------------------------------------------------

    @Test
    void publishHandler_userIdMismatch_emitsAccessRefusedFrame() {
        PublishHarness h = new PublishHarness(authorizer);
        ObjectNode frame = h.buildPublishFrame("events", "k", "body", 1L, false);
        ((ObjectNode) frame.get("message")).put("userId", "bob"); // principal is alice

        h.handler.handlePublish(frame, h.connCtx);

        assertEquals(1, h.writtenFrames.size(), "expected one error frame");
        String f = h.writtenFrames.get(0);
        assertTrue(f.contains("\"type\":\"error\""), "frame=" + f);
        assertTrue(f.contains("\"errorCode\":\"ACCESS_REFUSED\""), "frame=" + f);
        assertTrue(h.captured.isEmpty(), "no produce should be enqueued on denial");
    }

    @Test
    void publishHandler_userIdMatches_publishProceeds() {
        authorizer.allow(ALICE, ResourceType.TOPIC, "ws.orders", AclOperation.WRITE);
        PublishHarness h = new PublishHarness(authorizer);
        h.mockRoute("events", "k", Set.of("orders"));

        ObjectNode frame = h.buildPublishFrame("events", "k", "body", 1L, false);
        ((ObjectNode) frame.get("message")).put("userId", "alice"); // matches principal

        h.handler.handlePublish(frame, h.connCtx);

        assertEquals(1, h.captured.size(), "produce should be enqueued when userId matches");
        assertTrue(h.writtenFrames.isEmpty(), "no error frame when userId matches");
    }

    @Test
    void publishHandler_multiQueueAllAuthorized_fansOutToAll() {
        authorizer.allow(ALICE, ResourceType.TOPIC, "ws.a", AclOperation.WRITE);
        authorizer.allow(ALICE, ResourceType.TOPIC, "ws.b", AclOperation.WRITE);
        PublishHarness h = new PublishHarness(authorizer);
        h.mockRoute("fan", "k", new LinkedHashSet<>(List.of("a", "b")));

        h.handler.handlePublish(h.buildPublishFrame("fan", "k", "body", 2L, false), h.connCtx);

        assertEquals(2, h.captured.size(), "one produce per authorized queue");
    }

    @Test
    void publishHandler_multiQueueOneDenied_rejectsEntirePublish() {
        // WRITE granted only on ws.a; ws.b is unauthorized → ENTIRE publish is rejected
        // (no partial fanout, per design §19.2).
        authorizer.allow(ALICE, ResourceType.TOPIC, "ws.a", AclOperation.WRITE);
        PublishHarness h = new PublishHarness(authorizer);
        h.mockRoute("fan", "k", new LinkedHashSet<>(List.of("a", "b")));

        h.handler.handlePublish(h.buildPublishFrame("fan", "k", "body", 3L, false), h.connCtx);

        assertTrue(h.captured.isEmpty(), "no queue should receive the message — publish must be atomic");
        assertEquals(1, h.writtenFrames.size(), "expected one error frame");
        String f = h.writtenFrames.get(0);
        assertTrue(f.contains("\"errorCode\":\"ACCESS_REFUSED\""), "frame=" + f);
        assertTrue(f.contains("ws.b"), "unauthorized topic should be named in the error: frame=" + f);
    }

    // ------------------------------------------------------------------
    //  Integration with WsFrameHandler — exchange/queue admin ACL hooks
    // ------------------------------------------------------------------

    @Test
    void frameHandler_requireExchangeAdmin_deniesWithoutClusterAlter() {
        FrameHandlerHarness h = new FrameHandlerHarness(new WsAuthorizationHelper(authorizer));

        boolean allowed = h.handler.requireExchangeAdmin("req-1", "declare-exchange");

        assertFalse(allowed);
        assertEquals(1, h.writtenFrames.size());
        String f = h.writtenFrames.get(0);
        assertTrue(f.contains("\"errorCode\":\"ACCESS_REFUSED\""), "frame=" + f);
        assertTrue(f.contains("\"id\":\"req-1\""), "correlation id echoed: frame=" + f);
    }

    @Test
    void frameHandler_requireExchangeAdmin_allowsWithClusterAlter() {
        authorizer.allow(ALICE, ResourceType.CLUSTER, CLUSTER_NAME, AclOperation.ALTER);
        FrameHandlerHarness h = new FrameHandlerHarness(new WsAuthorizationHelper(authorizer));

        boolean allowed = h.handler.requireExchangeAdmin("req-1", "declare-exchange");

        assertTrue(allowed);
        assertTrue(h.writtenFrames.isEmpty(), "no error frame when authorized");
    }

    @Test
    void frameHandler_requireQueueAccess_declareQueueRequiresCreate() {
        authorizer.allow(ALICE, ResourceType.TOPIC, "ws.newq", AclOperation.CREATE);
        FrameHandlerHarness h = new FrameHandlerHarness(new WsAuthorizationHelper(authorizer));

        // CREATE granted → allowed
        assertTrue(h.handler.requireQueueAccess("req-1", "declare-queue", "newq", AclOperation.CREATE));
        // DELETE not granted → denied
        assertFalse(h.handler.requireQueueAccess("req-2", "delete-queue", "newq", AclOperation.DELETE));
    }

    @Test
    void frameHandler_requireQueueAccess_bindRequiresTopicAlter() {
        authorizer.allow(ALICE, ResourceType.TOPIC, "ws.orders", AclOperation.ALTER);
        FrameHandlerHarness h = new FrameHandlerHarness(new WsAuthorizationHelper(authorizer));

        assertTrue(h.handler.requireQueueAccess("req-1", "bind", "orders", AclOperation.ALTER));
        assertFalse(h.handler.requireQueueAccess("req-2", "bind", "other", AclOperation.ALTER));
    }

    @Test
    void frameHandler_nullAuthorizationHelper_permitsAll() {
        FrameHandlerHarness h = new FrameHandlerHarness(null);

        assertTrue(h.handler.requireExchangeAdmin("req-1", "declare-exchange"));
        assertTrue(h.handler.requireQueueAccess("req-2", "declare-queue", "x", AclOperation.CREATE));
        assertTrue(h.writtenFrames.isEmpty(),
            "with no authorizer wired, the handler is permissive and emits no frames");
    }

    // ------------------------------------------------------------------
    //  Integration with WsSubscriptionManager — subscribe ACL check
    // ------------------------------------------------------------------

    @Test
    void subscriptionManager_checkSubscribeAuthorized_allowsReadGrant() {
        authorizer.allow(ALICE, ResourceType.TOPIC, "ws.orders", AclOperation.READ);
        assertTrue(WsSubscriptionManager.checkSubscribeAuthorized(helper, ALICE, "ws.orders"));
    }

    @Test
    void subscriptionManager_checkSubscribeAuthorized_deniesWithoutReadGrant() {
        assertFalse(WsSubscriptionManager.checkSubscribeAuthorized(helper, ALICE, "ws.orders"));
    }

    @Test
    void subscriptionManager_checkSubscribeAuthorized_nullHelperPermits() {
        assertTrue(WsSubscriptionManager.checkSubscribeAuthorized(null, ALICE, "ws.orders"));
    }

    // ------------------------------------------------------------------
    //  Test harnesses
    // ------------------------------------------------------------------

    /** Wires a {@link WsPublishHandler} with the ACL helper for end-to-end tests. */
    private static final class PublishHarness {
        private static final ObjectMapper MAPPER = HttpRequestTranslator.MAPPER;

        final WsPublishHandler handler;
        final WsConnectionContext connCtx;
        final ExchangeManager exchangeManager;
        final RoutingEngine routingEngine;
        final List<String> writtenFrames = new ArrayList<>();
        final List<String> captured = new ArrayList<>();

        PublishHarness(Authorizer authorizer) {
            exchangeManager = mock(ExchangeManager.class);
            routingEngine = mock(RoutingEngine.class);
            WsMessageSerializer serializer = new WsMessageSerializer();

            WsPublishHandler.ProduceRequestSink sink = (topic, s, pid, ce, c) -> captured.add(topic);

            ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
            Channel mockChannel = mock(Channel.class);
            when(mockCtx.channel()).thenReturn(mockChannel);
            when(mockChannel.isActive()).thenReturn(true);
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
            InetSocketAddress remote = new InetSocketAddress("127.0.0.1", 12345);
            connCtx = new WsConnectionContext("ws-acl-test", ALICE, "/", mockCtx, remote);

            handler = new WsPublishHandler(
                exchangeManager, routingEngine, serializer,
                q -> "ws." + q, sink, null, null, new WsAuthorizationHelper(authorizer));
        }

        void mockRoute(String exchangeName, String routingKey, Set<String> matched) {
            when(exchangeManager.getExchange("/", exchangeName)).thenReturn(
                new ExchangeMetadata(exchangeName, "/", "direct", true, false, false, Map.of()));
            when(routingEngine.route(eq(exchangeName), eq(routingKey), any())).thenReturn(matched);
        }

        ObjectNode buildPublishFrame(String exchange, String routingKey, String body,
                                     long publishId, boolean mandatory) {
            ObjectNode frame = MAPPER.createObjectNode();
            frame.put("type", "publish");
            frame.put("exchange", exchange);
            frame.put("routingKey", routingKey);
            frame.put("mandatory", mandatory);
            frame.put("publishId", publishId);
            ObjectNode message = frame.putObject("message");
            message.put("body", body);
            return frame;
        }
    }

    /** Wires a {@link WsFrameHandler} so the {@code requireXxx} helpers can be invoked. */
    private static final class FrameHandlerHarness {
        final WsFrameHandler handler;
        final List<String> writtenFrames = new ArrayList<>();

        FrameHandlerHarness(WsAuthorizationHelper authHelper) {
            ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
            Channel mockChannel = mock(Channel.class);
            when(mockCtx.channel()).thenReturn(mockChannel);
            when(mockChannel.isActive()).thenReturn(true);
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
            InetSocketAddress remote = new InetSocketAddress("127.0.0.1", 12345);
            WsConnectionContext ctx = new WsConnectionContext("ws-acl-fh", ALICE, "/", mockCtx, remote);
            handler = new WsFrameHandler(ctx, WsConfigs.withDefaults(), null, authHelper);
        }
    }

    // ------------------------------------------------------------------
    //  Fake Authorizer — in-memory allow-list with recording
    // ------------------------------------------------------------------

    /**
     * Tiny in-memory {@link Authorizer} that records every request and returns
     * {@link AuthorizationResult#ALLOWED} only when an exact
     * {@code (principal, resourceType, resourceName, operation)} tuple has been
     * seeded via {@link #allow}. Everything else is {@link AuthorizationResult#DENIED}.
     */
    private static final class FakeAuthorizer implements Authorizer {

        private final java.util.Set<String> allowSet = new java.util.HashSet<>();
        final List<AuthorizableRequestContext> receivedContexts = new ArrayList<>();
        final List<Action> receivedActions = new ArrayList<>();

        void allow(KafkaPrincipal p, ResourceType rt, String name, AclOperation op) {
            allowSet.add(key(p, rt, name, op));
        }

        private static String key(KafkaPrincipal p, ResourceType rt, String name, AclOperation op) {
            return p.toString() + "|" + rt.name() + "|" + name + "|" + op.name();
        }

        @Override
        public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
            receivedContexts.add(requestContext);
            receivedActions.addAll(actions);
            List<AuthorizationResult> out = new ArrayList<>(actions.size());
            for (Action a : actions) {
                boolean allowed = allowSet.contains(key(
                    requestContext.principal(),
                    a.resourcePattern().resourceType(),
                    a.resourcePattern().name(),
                    a.operation()));
                out.add(allowed ? AuthorizationResult.ALLOWED : AuthorizationResult.DENIED);
            }
            return out;
        }

        // -- Unused Authorizer interface plumbing (no-op stubs) --

        @Override public void configure(Map<String, ?> configs) { }
        @Override public void close() { }
        @Override public Map<org.apache.kafka.common.Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo) {
            return Collections.emptyMap();
        }
        @Override public List<? extends CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext requestContext, List<AclBinding> aclBindings) {
            return Collections.emptyList();
        }
        @Override public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext requestContext, List<AclBindingFilter> aclBindingFilters) {
            return Collections.emptyList();
        }
        @Override public Iterable<AclBinding> acls(AclBindingFilter filter) {
            return Collections.emptyList();
        }
        @Override public AuthorizationResult authorizeByResourceType(AuthorizableRequestContext requestContext, AclOperation op, ResourceType rt) {
            return AuthorizationResult.DENIED;
        }
    }
}
