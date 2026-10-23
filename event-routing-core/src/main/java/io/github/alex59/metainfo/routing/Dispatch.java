package io.github.alex59.metainfo.routing;

import java.util.Objects;

/** A destination plus the original source record for acknowledgment tracking. */
public record Dispatch<K, V>(String routeName, String destinationTopic, StreamEvent<K, V> source) {
    public Dispatch {
        Objects.requireNonNull(routeName);
        Objects.requireNonNull(destinationTopic);
        Objects.requireNonNull(source);
    }
}
