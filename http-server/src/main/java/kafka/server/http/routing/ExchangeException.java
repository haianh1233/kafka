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

// Time: Created - TASK-WS1.06

package kafka.server.http.routing;

import java.util.Objects;

/**
 * Typed exception for exchange lifecycle operations. The {@link ErrorCode} allows
 * the caller (a protocol handler such as the WebSocket control handler) to map
 * the failure to the appropriate wire-level error code without relying on
 * exception-message string matching.
 *
 * // Time: Created - TASK-WS1.06
 */
public class ExchangeException extends Exception {

    private static final long serialVersionUID = 1L;

    public enum ErrorCode {
        /** Passive declare / lookup failed because the exchange does not exist. */
        EXCHANGE_NOT_FOUND,
        /** Re-declare with a different type than the one stored in the cache. */
        EXCHANGE_TYPE_MISMATCH,
        /** Attempt to delete one of the 5 built-in pre-declared exchanges. */
        EXCHANGE_PROTECTED,
        /** Delete with ifUnused=true when the exchange still has bindings. */
        EXCHANGE_IN_USE,
        /** Declare would exceed {@code ws.max.exchanges.per.vhost}. */
        EXCHANGE_LIMIT_EXCEEDED
    }

    private final ErrorCode errorCode;

    public ExchangeException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = Objects.requireNonNull(errorCode, "errorCode");
    }

    public ErrorCode errorCode() {
        return errorCode;
    }
}
