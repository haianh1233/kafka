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
// Time: Created - TASK-WS1.03
package kafka.server.http.ws;

import io.netty.util.AttributeKey;

/**
 * Netty {@link AttributeKey} registry for the WebSocket layer.  Centralised so
 * producers (the upgrade handler) and consumers (frame handler, ack handler,
 * etc. — introduced in later tasks) reference the same key.
 *
 * <p>Kept package-private for now; promote to {@code public} when another
 * package needs to read the attached state.
 *
 * // Time: Created - TASK-WS1.03
 */
final class WsAttributes {

    /**
     * Per-connection state produced by {@link WsUpgradeOrHttpHandler} immediately
     * after a successful WebSocket handshake.  Subsequent handlers on the same
     * channel use this attribute to recover the {@link WsConnectionContext}.
     */
    static final AttributeKey<WsConnectionContext> CONNECTION_CONTEXT =
            AttributeKey.valueOf(WsConnectionContext.class, "connectionContext");

    private WsAttributes() {
        // no instances
    }
}
