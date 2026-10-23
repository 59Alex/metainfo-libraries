package io.github.alex59.metainfo.jolt.ports;

import org.reactivestreams.Publisher;

public interface GenericHttpPort {
    Publisher<String> callHttp(Object input, String httpMethod, String uri);

    Publisher<String> streamHttp(Object input, String httpMethod, String uri);


}
