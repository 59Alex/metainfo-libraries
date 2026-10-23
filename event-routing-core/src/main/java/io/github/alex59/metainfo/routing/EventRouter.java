package io.github.alex59.metainfo.routing;

import java.util.List;
import java.util.Objects;

/** Pure routing; publishing, acknowledgment and retries belong to an adapter. */
public final class EventRouter {
    private final List<TopicRoute> routes;

    public EventRouter(List<TopicRoute> routes) {
        this.routes = List.copyOf(Objects.requireNonNull(routes));
    }

    public <K, V> List<Dispatch<K, V>> route(StreamEvent<K, V> event) {
        Objects.requireNonNull(event);
        return routes.stream()
                .filter(route -> route.enabled() && route.sourceTopic().equals(event.topic()))
                .<Dispatch<K, V>>map(route -> new Dispatch<>(route.name(), route.destinationTopic(), event))
                .toList();
    }
}
