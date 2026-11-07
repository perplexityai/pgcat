# SSL-negotiated CancelRequest handling

Status: local implementation proposed for review. This change restores protocol parity between direct cancellation connections and cancellation connections that first request SSL; it does not select or enforce a transport policy.

## Protocol requirement and failure

A PostgreSQL client opens a separate connection to cancel an active query and sends the PID and cancellation key supplied during its original session. PostgreSQL permits an initial SSLRequest on that cancellation connection, and the server closes it without a response after processing the request ([PostgreSQL message flow, cancellation and SSL encryption](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-CANCELING-REQUESTS)).

PgCat already parses CancelRequest and dispatches direct plaintext requests through its existing cancellation handler. However, the startup branches after both an SSLRequest refusal and a successful TLS handshake currently accept only StartupMessage; asyncpg's normal prefer mode and a verified TLS connection therefore produce a local cancellation while the backend query continues.

## Decision and alternatives

- **Reuse existing cancellation dispatch, selected:** Accept the already-parsed CancelQuery variant after an SSLRequest refusal and after TLS negotiation, then call the same existing `Client::cancel` constructor and `handle` path as direct cancellation. This is the smallest change and preserves the current PID/key lookup and upstream forwarding.
- **Disable SSL for cancellation, rejected:** This would require driver-specific changes, loses encrypted cancellation, and does not repair PgCat's PostgreSQL protocol support. Requiring clients to select plaintext transport is not an acceptable compatibility fix.
- **Reject all plaintext startup, rejected as unrelated:** That changes listener policy and would break clients that use encrypted main sessions with native plaintext cancellation connections, including pgx. This patch preserves direct CancelRequest handling.
- **Unify the entire startup state machine, deferred:** A broad refactor could remove dispatch duplication but would touch authentication, TLS failure handling, and shutdown paths unrelated to the defect. Two explicit dispatch arms are easier to audit.
- **Redesign cancellation routing and backend leases, deferred:** Cross-instance cancellation routing and cancellation/query-completion races are separate problems discussed in [upstream issue 297](https://github.com/postgresml/pgcat/issues/297). This patch does not promise to solve those problems.

## Security and compatibility boundaries

- **Existing key checks remain mandatory:** Cancellation still requires a matching `(process_id, secret_key)` in the existing client-to-server mapping; an unknown key must silently close the cancellation connection without canceling another client's query.
- **No new logging of credentials:** Do not log cancellation keys, authentication material, or raw cancellation packet bodies. Tests may record message type and actual query state but must not emit keys.
- **No new application authentication:** CancelRequest does not run normal startup authentication, send an acknowledgment, or become an authenticated query session. It remains a separate protocol request.
- **No deployment or TLS-policy changes:** This patch does not enable TLS, change certificate issuance or trust, require client certificates, change upstream encryption, or disable existing plaintext protocol behavior.

## Regression coverage

The integration tests must observe a uniquely marked long-running query on the actual PostgreSQL backend before canceling it and must prove it stops, rather than only asserting a client-side cancellation exception. Existing Ruby protocol coverage uses direct plaintext cancellation, and the existing asyncpg suite does not exercise cancellation after SSL negotiation.

Python coverage exercises both prefer negotiation against a non-TLS listener and a CA/hostname-verified TLS listener. Native Go pgx cancellation coverage preserves plaintext cancellation alongside an encrypted main session, checks that a mismatched key sent over TLS cannot cancel that session, and independently observes server-side query state; pgx 5.7.2 retains the existing Go 1.21 test-toolchain requirement.

The tests start and stop only their own PgCat process and clean up only their own marked queries. They use fixture certificates and local test PostgreSQL, require no production configuration, and introduce no production-only test hook.
