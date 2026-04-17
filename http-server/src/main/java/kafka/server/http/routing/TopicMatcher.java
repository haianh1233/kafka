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

// Time: Created - TASK-WS2.01

package kafka.server.http.routing;

/**
 * AMQP topic exchange pattern matcher.
 *
 * <p>Implements dot-separated word matching with two wildcards:
 * <ul>
 *   <li>{@code *} — matches exactly one word</li>
 *   <li>{@code #} — matches zero or more words; may appear anywhere in the
 *       pattern (including mid-pattern, e.g. {@code foo.#.bar})</li>
 * </ul>
 *
 * <p>Words are separated by the literal {@code .} character. Empty strings
 * are treated as a single empty word (so {@code *} matches {@code ""}).
 *
 * <p>Fast path: patterns containing no wildcards use direct {@link String#equals}.
 *
 * <p>This class is stateless and thread-safe. All methods are static.
 *
 * // Time: Created - TASK-WS2.01
 */
public final class TopicMatcher {

    private TopicMatcher() {
        // utility class
    }

    /**
     * Tests whether a binding pattern matches a published routing key.
     *
     * @param pattern    the binding routing-key pattern (may contain {@code *}
     *                   and {@code #} wildcards, dot-separated); must not be
     *                   {@code null}
     * @param routingKey the published routing key (no wildcards, dot-separated);
     *                   must not be {@code null}
     * @return {@code true} iff the routing key matches the pattern
     */
    public static boolean matches(String pattern, String routingKey) {
        // Fast path: patterns without wildcards degenerate to exact equality.
        if (pattern.indexOf('*') < 0 && pattern.indexOf('#') < 0) {
            return pattern.equals(routingKey);
        }

        String[] patternWords = splitByDot(pattern);
        String[] routingWords = splitByDot(routingKey);

        return matchWords(patternWords, 0, routingWords, 0);
    }

    /**
     * Recursive word-by-word match supporting {@code #} in any position.
     *
     * <p>When a {@code #} is encountered mid-pattern, the matcher tries to
     * consume 0, 1, 2, ... remaining routing words and recurses on the rest
     * of the pattern — this is what lets patterns like {@code foo.#.bar}
     * match {@code foo.bar}, {@code foo.x.bar}, {@code foo.x.y.z.bar}, etc.
     *
     * @param pattern the pattern words
     * @param pi      current index into {@code pattern}
     * @param routing the routing key words
     * @param ri      current index into {@code routing}
     * @return {@code true} iff the pattern suffix starting at {@code pi}
     *         matches the routing-key suffix starting at {@code ri}
     */
    private static boolean matchWords(String[] pattern, int pi, String[] routing, int ri) {
        while (pi < pattern.length) {
            String pw = pattern[pi];

            if ("#".equals(pw)) {
                // # at the end swallows everything remaining (including zero words).
                if (pi == pattern.length - 1) {
                    return true;
                }
                // Mid-pattern #: try consuming 0, 1, 2, ... remaining routing words
                // and recurse on the rest of the pattern.
                for (int skip = ri; skip <= routing.length; skip++) {
                    if (matchWords(pattern, pi + 1, routing, skip)) {
                        return true;
                    }
                }
                return false;
            }

            // Past this point we need a routing word to consume.
            if (ri >= routing.length) {
                return false;
            }

            if ("*".equals(pw)) {
                // * matches exactly one word — advance both indices.
                pi++;
                ri++;
                continue;
            }

            // Literal word — must match exactly (case-sensitive).
            if (!pw.equals(routing[ri])) {
                return false;
            }
            pi++;
            ri++;
        }

        // Pattern exhausted — routing must also be fully consumed.
        return ri == routing.length;
    }

    /**
     * Splits a string by the {@code .} delimiter without regex or
     * {@link String#split(String)}. Kept on the hot path per design doc §8.5.
     *
     * <p>Empty strings yield a single empty word: {@code splitByDot("")} returns
     * {@code [""]}. Leading, trailing, and consecutive dots produce empty words
     * at the corresponding positions (e.g. {@code "a..b"} → {@code ["a", "", "b"]}).
     *
     * @param s the string to split; must not be {@code null}
     * @return array of dot-separated words (length == number of dots + 1)
     */
    static String[] splitByDot(String s) {
        int count = 1;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == '.') {
                count++;
            }
        }

        String[] parts = new String[count];
        int partIndex = 0;
        int start = 0;

        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == '.') {
                parts[partIndex++] = s.substring(start, i);
                start = i + 1;
            }
        }
        parts[partIndex] = s.substring(start);

        return parts;
    }
}
