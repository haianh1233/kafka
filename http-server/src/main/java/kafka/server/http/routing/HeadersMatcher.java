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

// Time: Created - TASK-WS2.03

package kafka.server.http.routing;

import java.util.Map;

/**
 * Headers exchange matcher — matches message headers against binding criteria.
 *
 * <p>The binding arguments contain an {@code x-match} key that determines the
 * matching mode:
 * <ul>
 *   <li>{@code "all"} (default): all criteria must match</li>
 *   <li>{@code "any"}: at least one criterion must match</li>
 * </ul>
 *
 * <p>Empty criteria (no keys other than {@code x-match}) matches everything.
 * Null binding arguments matches everything; null message headers are treated
 * as an empty map.
 *
 * <p>Stateless utility class. All methods are static and thread-safe.
 */
public final class HeadersMatcher {

    private HeadersMatcher() {
        // utility class — no instances
    }

    /**
     * Tests whether message headers match binding criteria.
     *
     * @param bindingArgs binding arguments including {@code x-match} and header criteria
     * @param msgHeaders  message headers to check
     * @return true if the message matches the binding
     */
    public static boolean matches(Map<String, String> bindingArgs, Map<String, String> msgHeaders) {
        if (bindingArgs == null || bindingArgs.isEmpty()) {
            return true;
        }

        String xMatch = bindingArgs.getOrDefault("x-match", "all");
        boolean matchAll = !"any".equals(xMatch);

        Map<String, String> headers = (msgHeaders != null) ? msgHeaders : Map.of();

        int criteriaCount = 0;
        int matchCount = 0;

        for (Map.Entry<String, String> entry : bindingArgs.entrySet()) {
            if ("x-match".equals(entry.getKey())) {
                continue; // skip the x-match meta-key
            }
            criteriaCount++;
            String msgValue = headers.get(entry.getKey());
            if (msgValue != null && msgValue.equals(entry.getValue())) {
                matchCount++;
            }
        }

        if (criteriaCount == 0) {
            return true; // no criteria → matches everything
        }

        return matchAll ? matchCount == criteriaCount : matchCount > 0;
    }
}
