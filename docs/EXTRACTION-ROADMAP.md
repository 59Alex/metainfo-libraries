# Further library candidates

The three modules in this repository cover the most separable code found in the archived project set. The following ideas require design work before source can become an independent package.

| Candidate | Reusable part | Required work | Priority |
| --- | --- | --- | --- |
| Model version diff | Compare two versions of a typed object graph and produce structured differences. | Replace application-specific model types; define stable equality and ordering; avoid collecting unbounded streams during sort. | High |
| Metadata query builder | Build joins, filters, aggregates and projections from a declared model graph with jOOQ. | Extract a small schema model and query AST; validate identifiers and allowed operations; test generated SQL across supported dialects. | Medium |
| Streaming S3 ingest | Accept a continuous incoming byte stream and upload multipart data to S3. | Bound memory and concurrency, handle cancellation and abort, cover retry/complete failures. Continue this work in the separate [`s3-file-streams`](https://github.com/59Alex/s3-file-streams) project rather than duplicating it here. | High |
| Background job orchestration | Compose scheduled tasks, state transitions and retries. | Remove application-specific state and database entities; define persistence and recovery interfaces. | Medium |
| Architecture document parser | Turn structured YAML into a validated graph of nodes and relations. | Replace fixed domain vocabulary with a schema; add validation and error locations. | Low |

These are candidates, not implemented modules. Any promoted module should compile independently, include failure-path tests, avoid internal credentials and configuration, and state its actual guarantees in its README.
