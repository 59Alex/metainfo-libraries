# Metainfo Libraries

Three Java components extracted and adapted from a larger event-driven platform. This repository keeps the reusable code together while removing deployment settings and application-specific models.

| Module | Purpose | Boundary |
| --- | --- | --- |
| [`jolt-pipeline`](jolt-pipeline/) | Compose reactive processing steps around Jolt transformations, including branching, retry, context and HTTP ports. | Requires an application to supply its own HTTP port and execution settings. |
| [`audit-aspect`](audit-aspect/) | Record the outcome of annotated synchronous and Reactor methods through a caller-provided sink. | Spring AOP proxy methods only; no persistence or user identity is assumed. |
| [`event-routing-core`](event-routing-core/) | Turn a source event and enabled topic routes into dispatch plans while retaining source position. | Pure routing; Kafka publishing, offset acknowledgment, retries and deduplication are separate concerns. |

## Build

Use JDK 21 to build Java 17-compatible classes with Gradle 9.1:

```bash
./gradlew test
```

The modules use Maven Central dependencies. Nothing is published to Maven or npm from this repository yet.

## Modules

### Jolt pipeline

`JoltPipeHandler` reads a list of step specifications and runs them as a Reactor `Flux`. Step implementations live in `io.github.alex59.metainfo.jolt.steps`. `GenericHttpPort` is an interface: the host application controls network calls and credentials. HTTP retry settings are `metainfo.jolt.http.retry-max-attempts` and `metainfo.jolt.http.retry-delay-sec`.

The current tests cover placeholder replacement. Before adopting it in production, add integration tests for each step type, cancellation, retry boundaries and HTTP failures.

### Audit aspect

Create an `AuditAspect` with an `AuditSink` and register it as a Spring AOP aspect. Mark a proxied method with `@Audited("action-name")`. It emits an `AuditRecord` after a synchronous call or when a returned `Mono`/`Flux` completes or fails. The record contains the action, completion time, result, exception type and duration; it does not capture arguments or payloads.

Cancellation does not currently emit a record. Calls bypassing a Spring proxy are not intercepted. If writing audit records must be atomic with a business transaction, the application must provide a transactional sink and define its failure policy.

### Event routing core

`EventRouter` accepts enabled `TopicRoute` definitions and returns a `Dispatch` for every matching destination. Each dispatch holds the original `StreamEvent` including topic, partition and offset. This helps an adapter retain source position until delivery is confirmed; the module itself does not claim at-least-once delivery or idempotent business effects.

## Status

The repository is a code showcase and starting point for independently usable libraries. The modules compile and their current tests pass, but package releases and full integration tests are still pending.

[Further extraction candidates](docs/EXTRACTION-ROADMAP.md) cover version diffing, metadata queries, S3 streaming and background orchestration.
