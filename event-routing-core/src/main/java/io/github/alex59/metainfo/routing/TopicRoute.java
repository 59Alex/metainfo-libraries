package io.github.alex59.metainfo.routing;

import java.util.Objects;

/** A named mapping from one source topic to one destination topic. */
public record TopicRoute(String name, String sourceTopic, String destinationTopic, boolean enabled) {
    public TopicRoute {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(sourceTopic, "sourceTopic");
        Objects.requireNonNull(destinationTopic, "destinationTopic");
        if (name.isBlank() || sourceTopic.isBlank() || destinationTopic.isBlank()) {
            throw new IllegalArgumentException("route fields must be nonblank");
        }
    }
}
