package io.github.alex59.metainfo.routing;

import java.util.Map;
import java.util.Objects;

/** Source record and payload. The position is retained when the event is routed. */
public record StreamEvent<K, V>(
        String topic,
        int partition,
        long offset,
        K key,
        V value,
        Map<String, String> headers
) {
    public StreamEvent {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(headers, "headers");
        if (topic.isBlank() || partition < 0 || offset < 0) {
            throw new IllegalArgumentException("invalid source position");
        }
        headers = Map.copyOf(headers);
    }
}
