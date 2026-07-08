# Client-side statistics

The Go SDK statistics implementation follows the Oracle NoSQL Java SDK stats
contract where practical. The emitted payload uses Java-style profile behavior,
field names, request names, latency units, and the `Client stats|` log prefix.

## Files involved

* `nosqldb/stats.go`: public stats types, runtime control, aggregation,
  snapshot generation, periodic emission, handler delivery, and Java-compatible
  JSON payloads.
* `nosqldb/config.go`: stats configuration fields and validation.
* `nosqldb/client.go`: request lifecycle instrumentation, query hooks, and
  client-owned stats emitter lifecycle.
* `nosqldb/request.go`: request metadata and prepared-statement accessors used
  by query-level stats.
* `nosqldb/stats_test.go`: unit tests for config, runtime control, request
  aggregation, query aggregation, periodic emission, and Java-compatible JSON
  shape.
* `examples/stats/stats_example.go`: runnable example that logs stats to
  terminal output.

## Data flow

1. A user enables stats in `nosqldb.Config` before calling `NewClient`.
2. `NewClient` creates a `StatsControl` and starts its periodic emitter.
3. Requests run through `Client.doExecute`, where success/error, request size,
   response size, latency, retries, auth retries, throttle retries, rate-limit
   delay, and request metadata are recorded.
4. Query requests also call `observeQuery` for logical query count. In `ALL`,
   HTTP query request stats are attached to the same logical query entry.
5. On each interval, and once during `Client.Close`, `StatsControl` snapshots
   the current interval, resets the interval counters, calls the optional
   `StatsHandler`, and logs JSON if `StatsEnableLog` is enabled.

`rateLimitDelayMs` includes both waits introduced by the SDK's local rate
limiters and delay reported by the server or proxy in the
`X-Nosql-RL-Delay-Ms` response header.

## Profile behavior

* `NONE`: no request collection and no periodic stats payload.
* `REGULAR`: aggregate request stats only.
* `MORE`: aggregate request stats plus `95th` and `99th` latency fields.
* `ALL`: `MORE` behavior plus query-level `queries` entries.

Empty non-`NONE` intervals are intentionally emitted with only `clientId`,
`startTime`, `endTime`, and `requests: []`, matching Java lifecycle behavior.

Changing the profile at runtime emits the current partial interval using the
old profile before the new profile takes effect. `Start` cannot restart stats
after the owning client is closed, and `SetProfile` returns an `IllegalState`
error after close.

## Query data and cardinality

`ALL` stores raw SQL text as the key for each query entry and emits that text,
plus the client-side driver plan when available, through the handler and stats
logger. SQL literals can therefore expose sensitive values. Statements that
embed changing literal values also create distinct map entries and can increase
memory use and log volume throughout an interval.

Use `ALL` only when the handler and logs are appropriately protected. Prefer
bind variables or otherwise normalized statements, and use `MORE` when query
details are not required. Stats logging is enabled by default for every
non-`NONE` profile; set `StatsEnableLog` to false when snapshots should only be
sent to a handler.

## Percentile memory and CPU

`StatsPercentileExact`, the default, retains one `int64` latency sample for each
successful request for the duration of the interval. Snapshot generation sorts
those samples once to calculate both percentiles, so its storage is O(requests
per interval) and its percentile work is O(N log N).

`StatsPercentileHDR` uses a fixed-size histogram. It has bounded memory and is
recommended for high-throughput clients or long stats intervals, at the cost of
approximate percentile values.

## Handler lifecycle

Handlers can run on the emitter goroutine, during profile changes, or during
the final close flush. They must tolerate concurrent calls and should return
quickly. A handler may call `Client.Close`; if any callback is active, the final
callback is deferred until all active callbacks return and may run after
`Client.Close` returns. The final interval is still logged synchronously when
stats logging is enabled.

## Verification

Run the unit tests:

```sh
go test ./nosqldb -run 'TestStats|TestQueryStats|TestDoExecute'
go test -race ./nosqldb -run 'TestStats|TestQueryStats|TestDoExecute'
```

Run the example against Cloud Simulator:

```sh
go run ./examples/stats -profile=MORE -interval=5 localhost:8080
go run ./examples/stats -profile=ALL -interval=5 localhost:8080
```

Exact latency values are not deterministic. Tests should validate field
presence, numeric values, request counts, profile behavior, and Java-compatible
JSON shape rather than fixed latency numbers.
