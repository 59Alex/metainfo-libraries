package io.github.alex59.metainfo.audit;

import org.junit.jupiter.api.Test;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AuditAspectTest {
    @Test
    void recordsSynchronousSuccessAndFailure() {
        List<AuditRecord> records = new ArrayList<>();
        Service service = proxied(records);

        assertEquals("ok", service.succeed());
        assertThrows(IllegalStateException.class, service::fail);

        assertEquals(2, records.size());
        assertTrue(records.get(0).success());
        assertEquals("read", records.get(0).action());
        assertFalse(records.get(1).success());
        assertEquals("IllegalStateException", records.get(1).errorType());
    }

    @Test
    void recordsReactiveCompletionAfterSubscription() {
        List<AuditRecord> records = new ArrayList<>();
        Service service = proxied(records);

        Mono<String> result = service.reactive();
        assertTrue(records.isEmpty());
        assertEquals("done", result.block());
        assertEquals(1, records.size());
        assertTrue(records.get(0).success());
    }

    private Service proxied(List<AuditRecord> records) {
        AspectJProxyFactory factory = new AspectJProxyFactory(new Service());
        factory.setProxyTargetClass(true);
        factory.addAspect(new AuditAspect(records::add));
        return factory.getProxy();
    }

    static class Service {
        @Audited("read")
        public String succeed() {
            return "ok";
        }

        @Audited("write")
        public String fail() {
            throw new IllegalStateException("not persisted");
        }

        @Audited("async")
        public Mono<String> reactive() {
            return Mono.just("done");
        }
    }
}
