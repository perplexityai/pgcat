# SQL observer: extended-protocol design (Stage B)

This note records the lasting design and rationale of the extended-protocol SQL observer (Stage B; the Stage C relation labels followed). [CONFIG.md](../CONFIG.md#sql_observer_enabled) (`sql_observer_enabled`) is the reference for the setting, metrics, label values and gap-reason names; this note explains why the observer behaves as it does.

A later change (option A0) cut the per-client state: names are kept as keyed hashes, the classifier scratch is one per worker thread, and the undo journal is gone, so a refused or failed batch now stands as staged. Option D, on top of A0, classifies named statements on the shared cached `Parse` when the prepared-statement cache is on (see [the cache-on section](#prepared-statement-cache-on-option-d)). The sections below describe the observer after these changes; the Stage B and Stage C sizes are kept as history.

## What is counted

The observer is default off. When enabled it reports a monotonic lower bound of known SQL attempts, plus explicit gap events for what it cannot count. The one known exception is a portal whose rebind pgcat refused, whose next Execute can be counted again (see [Refused and failed batches stand](#refused-and-failed-batches-stand)).

- An attempt is a statement pgcat admits, as the client wrote it, before plugins rewrite or refuse it. It is not a backend success and not a transaction. Counting is not undone when the server errors or ignores the statement.
- Parse, Bind and Describe count nothing. An extended attempt is admitted when an Execute is staged into the batch buffer.
- Frames pgcat itself discards after a protocol error (`extended_protocol_error`, `discard_until_sync`) are never staged, so they are neither counted nor gaps. Consequence: a statement refused at Parse contributes 0 because its Execute is discarded, while a known simple Q refused by a later plugin still counts, since Q admission precedes plugins. This asymmetry is deliberate and follows the frame rule.
- Identity commands and pgcat's own internal SQL are never attempts.
- There is no transaction denominator, no full-coverage claim, and no per-Execute outcome ledger. Counts carry the client's `PGCAT_IDENTITY` state at admission as the `identity` label (`present`, `absent`, `expired`, `refused`; see CONFIG.md). It is a label only: it never changes authorization or plugins, and it does not claim the backend accepted the identity.

## Metadata is immutable per Bind generation

Every successfully staged Bind creates a new generation (a fresh portal entry, not yet executed), even when names and SQL are unchanged. A portal stores a snapshot of its statement's metadata taken at Bind, using the client-supplied name before pgcat's cache renames it. Consequences:

- Later Parse replacement or Close of the statement cannot retarget an existing portal.
- Only the first Execute of a generation counts, using the original (pre-plugin) parse: an empty statement is 0, a nonempty prepared statement is 1, regardless of rewritten length. Further Executes and PortalSuspended continuations are 0 with no gap.
- The `identity` label is read at Execute admission, not at Parse or Bind.
- The statement name is never resolved at Execute time. Observer keys are client names (kept as keyed hashes, see below), not pgcat's renamed server-side names. With the cache on, a named statement's metadata is found at Bind through the client's own map of prepared statements, by its client name (see below).
- A statement the parser did not parse is retained as an `Unparsed` marker. Parse/Bind/Describe add no count or gap; an admitted Execute produces an `unparsed_statement` gap. Admission does not establish that PostgreSQL ran the SQL.

## Fixed state budget

Observer state per client is bounded and heap-allocated only for clients of enabled pools. With the cache off both tables are allocated at their full size up front; with the cache on they start empty and grow to the same caps (see below). Nothing per client grows beyond these sizes.

| Component | Cap | Layout guard |
| --- | ---: | ---: |
| Statement slots | 128 | `Option<StatementEntry>` <= 64 B |
| Portal slots | 128 | `Option<PortalEntry>` <= 64 B |
| Control block (`ClientObserver`) | 1 | <= 128 B |

The per-client ceiling is 16,512 bytes (128 × 64 + 128 × 64 + 128), enforced by compile-time size assertions on the real types and checked in tests. Under A0 the layout is 16,424 bytes (a 40-byte control block); option D's control block is 48 bytes (a `cached` flag), so the cache-off layout is 16,432 bytes. This is a layout bound, not an allocator RSS or stack-depth promise.

Statement and portal names are kept as a 128-bit keyed hash (two SipHash outputs under two keys derived from one random seed: two consecutive std `RandomState`s, made once per process), not as bytes. Neither the keys nor the hashes are ever exposed. The unnamed statement and portal have a reserved key no other name maps to. Names longer than 128 bytes are still refused, so which names are retained is the same as with inline names. A collision can only make the observer take one name for another, which mislabels telemetry and never reaches what pgcat forwards.

The Stage C relation classifier scratch (<= 16,384 B) is one per worker thread, allocated at the thread's first classification, not per client. Classification is synchronous and resets every field it reads, so nothing passes between clients.

History: Stage B as first merged had 128 statements, 128 portals and a 512-record undo journal with names inline, with guards of 160/176/192 bytes and a 142,336-byte ceiling. Stage C widened the entries to carry relation snapshots (192/208/224 bytes) and added a per-client 16 KiB scratch, for a 183,296-byte ceiling ("at most 179 KiB").

The Stage C relation extension also keeps relation counters per (pool, user), shared by that pool's clients and outside the per-client ceiling. They hold up to 1,000 first-seen names, allocated 64 at a time and never evicted while the pool exists (see CONFIG.md).

The `client` label splits the attempt and gap counters by the client's `application_name`, folded to the pool's configured `sql_observer_clients` (at most 32 names) plus `other`; the list is empty by default, so every client is `other`. It adds no per-client state: the observer reads the name pgcat already tracks for the client at each count, and the pool's counters become a fixed 33-row table (2,376 bytes per pool and user). A configured list, not first-seen names, bounds it, because `application_name` is client-chosen and a first-seen table would let one client fill it; a name outside the list is `other`, never a new series. Names keep their row across reloads (rows are assigned append-only along the pool's lineage), so the counters stay monotonic. It is never derived from `PGCAT_IDENTITY` or its token. The `pgcat::sql_observer::unidentified` log is how to identify what lands in `other`: a line per statement counted without a `present` identity, with the name (cut to 64 bytes, so the line stays bounded) and the peer address, under a fixed budget of 10 lines per minute per pool and user (a few dozen bytes per pool and user, behind a `try_lock` so a busy budget is skipped, never waited for). Neither changes what pgcat forwards or refuses.

Overflow degrades telemetry, never SQL. If a retained name or slot cannot fit, the observer clears both tables, sets the invalidation bit, and quarantines metadata tracking until the dispatches already sent are answered and the batch being staged is over. An oversized Execute lookup is different: no retained name can match, so it reports a missing-portal gap without clearing the whole state. The observer never returns a client or protocol error, and never touches SQL, buffers, caches, plugins, identity, routing or replies. Counts already made stay. Later Executes needing lost metadata become gaps; fresh Parse and Bind rebuild known state after quarantine ends.

## Refused and failed batches stand

pgcat can reject a batch locally (checkout or bind hook, own Sync batch, Flush path) before anything reaches the backend, and the backend can fail a batch it was sent. The observer keeps no journal and takes nothing back in either case: what a batch staged stands once staged, as if it had been accepted.

- Counts already admitted stay. A staged first Execute that is refused keeps its admitted count, and its portal generation stays marked executed, so retrying the Execute of that generation counts nothing. A fresh Bind is a new generation and counts again. An admitted attempt is not a success.
- A portal whose rebind pgcat refused gets a fresh generation, so a later Execute continuing the server's old, already-counted portal is counted again. For example, in a transaction: `[Parse s, Bind p, Execute p (row limit), Sync]` counts 1; `[Bind p, Sync]` refused by pgcat counts nothing; a later `Execute p` counts a second attempt for the same server portal. The refusal fails the transaction (`fail_transaction`), so that Execute fails at the server: the extra count is a failed attempt, not a second run. Outside a transaction the idle ReadyForQuery drops the portals first.
- A name a refused or failed batch defined is kept, so its Executes count even if the server never created it.
- A name a refused or failed batch redefined keeps the new definition until it is defined again. With the prepared-statement cache off, a refused `[Close s1, Parse s1=B]` never reaches the server, which keeps `s1=A`; the observer counts `s1`'s Executes as B while A runs, until `s1` is redefined. A failed batch does the same: in `[Bind x (fails), Close s1, Parse s1=B, Sync]` the server skips to Sync and keeps `s1=A`, while the observer, which ignores the error, has staged B. This is a disagreement between the observer and the server, telemetry only. With the cache on, pgcat's refusal rolls back the client's map of prepared statements (`StatementPlans`, `reset_buffered_state`), so the map keeps `s1=A` and runs A. The observer reads `s1` through the map, and the label it keeps for a statement whose shared classification isn't the client's (the unnamed statement, one a plugin rewrote, one the client classified otherwise) is kept in the map entry, so it is rolled back with it: the observer counts A, as the map runs (`cache_tests::a_refused_redefinition_is_counted_as_what_the_map_runs`, `cache_tests::a_refused_redefinition_puts_the_label_back`). A batch the server fails was sent, so its changes to the map stand, and the observer counts what the map then runs. What pgcat answers itself (`local_statements`, `local_portals`) isn't rolled back by pgcat, and the observer's table for it stands as well, so the two agree.
- A name a refused or failed batch closed is forgotten, so its later Executes are gaps until it is defined again. With the cache on, a statement in the map is forgotten only when the map drops it: when its Close is buffered, the unnamed statement's included, or when pgcat couldn't prepare it. The drop is journaled with the batch's other changes to the map, so a batch pgcat refuses puts the statement back with its label, and the observer counts it as the map runs it; a batch the server fails was sent, so its Closes stand (`cache_tests::the_unnamed_statement_is_counted_while_the_map_runs_it`).
- Admission-time clears (below) are never undone either.

The journal this replaces mirrored local rejection with an undo log of up to 512 records and attributed backend errors to dispatch segments.

## Prepared-statement cache on (option D)

With the prepared-statement cache on (`ClientObserver::cached`, chosen when the client's pool has the cache in transaction mode), pgcat already keeps each client's map of prepared statements (`Client::prepared_statements`, unbounded) and a pool-wide cache of `Parse`s. Option D uses them instead of a per-client statement table for named statements:

- Each `Parse` carries a `SharedSnap` (a `OnceLock<Snap>`), which makes every `Parse` 48 bytes larger (136 B), with the observer off too, and for every live `Parse`, including evicted ones still held by clients' maps, not only per cache entry. It stays ungated, by design: gating it on `sql_observer_enabled` would make `Parse`'s layout depend on the config, to save about 48 KB per 1,000 live `Parse`s. It is set once, by the first client that stages the statement unrewritten and parsed (`Snap::Known`), so it is the classification of the cached SQL text under the pool's parser and observer config, never of a client or tenant. An unparsed first offer doesn't set it. A clone of a `Parse` starts with it empty.
- Where the shared classification isn't the client's (the unnamed statement, a statement a plugin rewrote, or one the client classified otherwise or didn't parse), the observer's own label is kept in the client's map entry (`Cached::label`), with a `rewritten` flag, so it is rolled back and dropped with the statement it describes. Each hash bucket of the map grows by 16 bytes for this (`Cached`, 16 to 32 bytes), with the observer off too. The label is boxed, so with the observer on, each labelled statement takes one 44-byte allocation, made with its Parse. `Client` loses the 8-byte set of rewritten names that this replaces.
- A Bind of a statement in the map reads its label if it has one, and otherwise the `SharedSnap` of the `Parse` the map holds for that name. A rewritten statement and the unnamed statement without a label (parsed before the observer was on) are gaps.
- So the 128-statement bound doesn't apply to statements in the map: they are counted for as long as the map makes them run. The statement table only keeps statements pgcat answers itself. Portals are per client, as with the cache off. Both tables start empty and double from 4 slots up to the same 128 entries, so 129 distinct live portals (or statements pgcat answers) still overflow.
- Lifecycle clears that drop both namespaces (DEALLOCATE, DISCARD, an unparsed simple query, an Execute of an unparsed or invalidated statement), and the simple Query that destroys the server's unnamed statement, don't drop statements in the map with the cache on, because they stay in the map and pgcat still binds them under their cached names (a simple Query destroys only the server's unnamed statement, not pgcat's renamed one). An overflow doesn't lose a label either.
- Nothing about what runs changes: the prepared-statement rename, the cache contents and eviction, and server-side behavior are the same with the observer on or off.

With the cache off, the observer is A0's, with the bounds above. Option D's layout, as `cache_tests::layout` prints it: 48 bytes for a fresh cache-on client, still 48 bytes after the unnamed statement (its label is in the map entry), 304 bytes with 4 portals, and at most the cache-off layout (16,432 bytes).

## Irreversible invalidation

Some clears are conservative losses of knowledge, not backend effects, and nothing undoes them:

- Admitting Commit, any Rollback (including savepoint and CHAIN forms), or SQL Close clears all portal observations. Deallocate and Discard clear both namespaces. SQL with unknown AST, when admitted, clears both namespaces because its effect is unknown. Named RELEASE SAVEPOINT is not an invalidator.
- On such a clear, live entries are removed and nothing restores them, so a later refusal of the batch cannot resurrect a destroyed portal, and a retry becomes a gap instead of stale attribution.
- Recovery is forward only: after a portal-only clear a fresh Bind from retained statement metadata restores observation; after a both-namespace clear a fresh Parse and Bind are needed.

DEALLOCATE is deliberately conservative: PostgreSQL may or may not destroy an already-bound portal, but observer attribution is gone either way.

## Backend errors, idle and Flush ownership

The observer cannot know which statements in a batch ran, so it does not guess.

- A backend ErrorResponse forgets nothing: the names a failed dispatch Parsed, Bound or Closed keep what the observer staged for them (see above). Admitted counts do not decrement.
- The observer counts the dispatches it sent whose ReadyForQuery hasn't come back, because Flush can return without RFQ and COPY-IN can return before it. Each RFQ answers one; quarantine ends only when none are left and no batch is being staged.
- Only an actual idle RFQ (`I`) clears all portal observations and resets the invalidation bit; `T` and `E` do not. The bit is therefore not a memory of past reasons, and a lookup after idle reports ordinary reasons.
- Server detach also drops portal observations (setting the bit if there were any); terminal client or transport failure forgets all observer state.

## Gap reasons and the idle caveat

Stage B keeps the Q reasons and adds `unparsed_statement`, `unknown_portal` and `metadata_invalidated` (Stage A's placeholder `extended_unobserved` is gone). A missing portal reports `metadata_invalidated` if the bit is set, else `unknown_portal` (for example a SQL cursor). A Bind to a missing statement freezes its reason at Bind time; only a later admitted Execute emits that gap and clears both namespaces, because the SQL's effects are unknown.

Accepted caveat: `unparsed_statement` is not purely parser-coverage evidence. Missing statement metadata after an idle boundary can also surface as `unparsed_statement`, since the reset bit no longer remembers the earlier invalidation. Consumers should read these gaps as "statement metadata unavailable".

## Limitations

- Lower bound only; not complete. Stage C relation labels are syntactic bare table names; they count references per table, not per-table coverage.
- Zero reported gaps doesn't prove exact per-table counts for batches pgcat refused or the server failed. What such a batch staged stands without a gap ([Refused and failed batches stand](#refused-and-failed-batches-stand)):
  - Cache off, refused rebind: a portal whose rebind pgcat refused is counted again at its next Execute, though the server's portal was already counted.
  - Cache off, skipped redefinition: after a redefinition that pgcat refused or that a failed batch skipped, PostgreSQL keeps running the old statement A while the observer attributes its Executes to B.
  - Cache on, refused batch: the observer's labels are kept in the client's map of prepared statements and rolled back with it. A refused redefinition is counted as A, the statement pgcat runs, whether it is named, unnamed or rewritten by a plugin.
  - Cache on, failed batch: the batch was sent, so its changes stand in the map, and the observer counts what pgcat then runs.
  - Locally answered state: statements and portals pgcat answers itself aren't rolled back after a refused batch, by pgcat or by the observer, so their counts follow what pgcat answers.
  - Reading SQL-observer coverage: readings are attributed attempts; exact per-table counts can't be read from them where clients' batches are refused or fail.
- Re-enabling or reloading never reconstructs metadata from caches; Executes without retained metadata are gaps, while the shared pool counters keep monotonic reload behavior. A Q admitted as unparsed is never reclassified after a reload.
- No change to auth, parser activation, routing, rejection or wire behavior. Enablement and public exposure need separate approval.
