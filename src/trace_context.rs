//! Statement spans that join the caller's trace.
//!
//! A client that traces its statements can put its W3C trace context in a
//! leading sqlcommenter comment: `/*traceparent='00-<trace>-<span>-01'*/ SELECT …`.
//! pgcat reads only `traceparent` from that comment (never `tracestate`) and
//! records one span per simple Query and per Execute whose caller sampled the
//! trace, keeping `trace_sample_ratio` of those traces. The decision is
//! `TraceIdRatioBased`'s, made on the trace id, so every pgcat keeps or drops
//! a given trace alike. A statement without a traceparent, or whose caller
//! didn't sample it, isn't traced, except in the known gaps `CONFIG.md`
//! lists: pgcat starts no traces of its own.
//!
//! At ratio 0, the default, no exporter is built and each client message costs
//! one atomic load. Spans go to an OTLP/HTTP endpoint through a bounded batch
//! queue that drops spans when it's full; exporting never delays a statement,
//! and a failed export only loses spans. Requests carry only pgcat's own
//! `Host` and `Content-Type`: `OTEL_EXPORTER_OTLP_*HEADERS` aren't forwarded.
//!
//! Nothing here logs or exports the comment, the statement, or anything else
//! the client sent besides the trace and span ids: span attributes are pool,
//! user, role and shard from the config, the protocol, and the SQLSTATE.
//!
//! A span starts when pgcat reads the Query or Execute and ends when the
//! server's ReadyForQuery for its batch arrives, so the Executes of one
//! pipelined batch all end at its Sync. A batch pgcat drops without sending it
//! (a plugin refused it, or no server could be checked out) is forgotten with
//! its statements. One pgcat answers itself (an identity command) gets no
//! ReadyForQuery from a server: its spans are discarded when the client's next
//! batch starts, as are those of a client that disconnects before its reply.
//! A lone Sync doesn't start a batch (the server ignores it during COPY), so
//! if pgcat sends one to the server, its reply ends them instead: a known gap.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::header::{HeaderMap, HeaderValue, CONTENT_TYPE, HOST};
use hyper::http::uri::Authority;
use hyper_util::rt::TokioIo;
use log::{error, info, warn};
use opentelemetry::trace::{
    Link, Span as _, SpanContext, SpanId, SpanKind, Status, TraceContextExt, TraceFlags, TraceId,
    TraceState, Tracer, TracerProvider,
};
use opentelemetry::{Context, InstrumentationScope, Key, KeyValue};
use opentelemetry_http::{HttpClient, HttpError, Request, Response};
use opentelemetry_otlp::{Protocol, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::trace::{
    BatchSpanProcessor, Sampler, SamplingDecision, SamplingResult, SdkTracer, SdkTracerProvider,
    ShouldSample,
};
use opentelemetry_sdk::Resource;
use tokio::runtime::Handle;

use crate::config::{Address, General, Role};
use crate::errors::Error;

const SPAN_NAME: &str = "pgcat.statement";

/// How far into a statement pgcat looks for the end of its leading comment.
/// sqlcommenter comments are short; a longer one isn't read at all.
const MAX_COMMENT: usize = 2048;

/// Prepared statement and portal names longer than this aren't tracked.
const MAX_NAME: usize = 256;
/// Per-client bounds: past them a client's traced names are forgotten
/// (their statements then go untraced), never grown.
const MAX_STATEMENTS: usize = 256;
const MAX_PORTALS: usize = 64;
const MAX_PENDING: usize = 64;

/// Bounds each export request, connection included.
const EXPORT_TIMEOUT: Duration = Duration::from_secs(5);
/// Bounds the final flush at shutdown.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
/// The collector's reply is read and dropped; an oversized one fails the export.
const MAX_RESPONSE_BODY: usize = 64 * 1024;

/// The sample ratio's bits; 0 while statement tracing is off.
static RATIO: AtomicU64 = AtomicU64::new(0);
static EXPORTER: ArcSwapOption<Exporter> = ArcSwapOption::const_empty();
/// Serializes `configure` (start and reloads) with `shutdown`.
static CONFIGURE: parking_lot::Mutex<()> = parking_lot::const_mutex(());

struct Exporter {
    provider: SdkTracerProvider,
    tracer: SdkTracer,
    endpoint: Option<String>,
}

/// Whether statement tracing is on: one relaxed load, for every client message.
#[inline]
pub fn enabled() -> bool {
    RATIO.load(Ordering::Relaxed) != 0
}

fn ratio() -> f64 {
    f64::from_bits(RATIO.load(Ordering::Relaxed))
}

/// Checks the tracing settings, for config validation. Logs why a setting
/// is refused, never the endpoint itself: a URL can carry credentials.
pub fn validate(general: &General) -> Result<(), Error> {
    let ratio = general.trace_sample_ratio;
    if !(0.0..=1.0).contains(&ratio) {
        error!("trace_sample_ratio must be between 0 and 1, got: {}", ratio);
        return Err(Error::BadConfig);
    }
    if let Some(endpoint) = &general.trace_otlp_endpoint {
        if let Err(reason) = check_endpoint(endpoint) {
            error!(
                "trace_otlp_endpoint must be an http:// URL with a host, no credentials and no query: {}",
                reason
            );
            return Err(Error::BadConfig);
        }
    }
    Ok(())
}

fn check_endpoint(endpoint: &str) -> Result<(), &'static str> {
    // OTLP/HTTP needs no query or fragment, and a query can carry a key that
    // would be logged with the endpoint. The URI parser drops a fragment, so
    // this checks the text.
    if endpoint.contains(['?', '#']) {
        return Err("it has a query or fragment");
    }
    let uri: hyper::Uri = endpoint.parse().map_err(|_| "it isn't a URL")?;
    if uri.scheme_str() != Some("http") {
        return Err("its scheme isn't http");
    }
    let authority = uri.authority().ok_or("it has no host")?;
    if authority.as_str().contains('@') {
        return Err("it has credentials");
    }
    if authority.host().is_empty() {
        return Err("it has no host");
    }
    port(authority).ok_or("its port isn't valid")?;
    Ok(())
}

/// The endpoint's port: 80 when none is written, `None` when the written one
/// isn't valid. The URI parser accepts a port it can't read, such as one past
/// 65535 or one that isn't a number, and then reports none.
fn port(authority: &Authority) -> Option<u16> {
    let written = authority
        .as_str()
        .rsplit(']')
        .next()
        .is_some_and(|rest| rest.contains(':'));
    match authority.port_u16() {
        None if !written => Some(80),
        port => port,
    }
}

/// Applies the tracing settings, at start and at each reload: builds the
/// exporter the first time the ratio is above 0. The endpoint is read then
/// only; setting the ratio back to 0 stops new spans and keeps the exporter.
/// Must run on pgcat's runtime, which sends the exports.
pub fn configure(general: &General) {
    let _configuring = CONFIGURE.lock();
    let mut ratio = general.trace_sample_ratio;
    if !(ratio > 0.0 && ratio <= 1.0) {
        ratio = 0.0;
    }
    if ratio > 0.0 {
        let current = EXPORTER.load();
        match current.as_deref() {
            Some(exporter) => {
                if exporter.endpoint != general.trace_otlp_endpoint {
                    warn!("trace_otlp_endpoint changes take effect at restart");
                }
            }
            None => {
                match build(general.trace_otlp_endpoint.as_deref()) {
                    Ok(exporter) => {
                        match &exporter.endpoint {
                        Some(endpoint) => info!("Exporting statement spans to {}", endpoint),
                        None => info!("Exporting statement spans to the OTLP endpoint from the environment"),
                    }
                        EXPORTER.store(Some(Arc::new(exporter)));
                    }
                    Err(reason) => {
                        error!("Statement tracing stays off: {}", reason);
                        ratio = 0.0;
                    }
                }
            }
        }
    }
    let bits = if ratio > 0.0 { ratio.to_bits() } else { 0 };
    if RATIO.swap(bits, Ordering::Relaxed) != bits {
        info!("Statement trace sample ratio: {}", ratio);
    }
}

/// Stops tracing and flushes the queued spans, within `SHUTDOWN_TIMEOUT`.
pub async fn shutdown() {
    let exporter = {
        let _configuring = CONFIGURE.lock();
        RATIO.store(0, Ordering::Relaxed);
        EXPORTER.swap(None)
    };
    let Some(exporter) = exporter else {
        return;
    };
    // The flush waits on the batch processor's thread, which exports through
    // this runtime: wait on a blocking thread, not on a worker.
    match tokio::task::spawn_blocking(move || {
        exporter.provider.shutdown_with_timeout(SHUTDOWN_TIMEOUT)
    })
    .await
    {
        Ok(Ok(())) => info!("Statement spans flushed"),
        Ok(Err(err)) => warn!("Statement spans not flushed: {}", err),
        Err(err) => warn!("Statement spans not flushed: {}", err),
    }
}

/// Fails with a fixed reason: the SDK's errors can quote an endpoint from the
/// environment, which isn't validated and isn't logged.
fn build(endpoint: Option<&str>) -> Result<Exporter, &'static str> {
    let runtime = Handle::try_current().map_err(|_| "no Tokio runtime")?;
    let client = RuntimeHttpClient {
        runtime,
        failing: AtomicBool::new(false),
    };
    let mut builder = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_timeout(EXPORT_TIMEOUT)
        .with_http_client(client);
    // The SDK reads an endpoint from the environment only when none is given,
    // so resolving it here means 0.32's fallbacks never apply.
    builder = match endpoint {
        Some(endpoint) => builder.with_endpoint(endpoint),
        None => builder.with_endpoint(endpoint_from_env(|name| std::env::var_os(name))?),
    };
    let exporter = builder
        .build()
        .map_err(|_| "the OTLP exporter can't be built from the endpoint settings")?;
    // The processor's queue is bounded (OTEL_BSP_MAX_QUEUE_SIZE, 2048 by
    // default) and drops spans when full, so a slow collector costs spans,
    // not memory or statement latency.
    let provider = SdkTracerProvider::builder()
        .with_span_processor(BatchSpanProcessor::builder(exporter).build())
        .with_sampler(StatementSampler)
        .with_resource(resource())
        .build();
    let tracer = provider.tracer_with_scope(
        InstrumentationScope::builder("pgcat")
            .with_version(env!("CARGO_PKG_VERSION"))
            .build(),
    );
    Ok(Exporter {
        provider,
        tracer,
        endpoint: endpoint.map(String::from),
    })
}

const TRACES_ENDPOINT_VAR: &str = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT";
const ENDPOINT_VAR: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const DEFAULT_ENDPOINT: &str = "http://localhost:4318";
const TRACES_PATH: &str = "/v1/traces";

/// The OTLP/HTTP traces URL the environment gives, resolved as
/// opentelemetry-otlp 0.33 does (the OTLP exporter spec's order): the traces
/// variable as is, else the generic one with `/v1/traces` appended, else
/// localhost. An empty variable counts as unset. 0.32 instead skips a value
/// that isn't Unicode or a valid URI and falls back to the next, and turns an
/// empty generic one into the relative `/v1/traces`; resolving here keeps a
/// typo from exporting somewhere else. Errors don't quote the value: it isn't
/// validated beyond parsing and may carry a key.
fn endpoint_from_env(
    var: impl Fn(&str) -> Option<std::ffi::OsString>,
) -> Result<String, &'static str> {
    let read = |name: &str| match var(name).map(std::ffi::OsString::into_string) {
        None => Ok(None),
        Some(Ok(value)) if value.is_empty() => Ok(None),
        Some(Ok(value)) => Ok(Some(value)),
        Some(Err(_)) => Err("the OTLP endpoint from the environment isn't valid Unicode"),
    };
    let endpoint = match read(TRACES_ENDPOINT_VAR)? {
        Some(traces) => traces,
        None => with_traces_path(&read(ENDPOINT_VAR)?.unwrap_or_else(|| DEFAULT_ENDPOINT.into())),
    };
    // "collector:4318" parses as an authority-only URI: require a scheme and a
    // host.
    match endpoint.parse::<hyper::Uri>() {
        Ok(uri) if uri.scheme().is_some() && uri.authority().is_some() => Ok(endpoint),
        _ => Err("the OTLP endpoint from the environment isn't a valid URI"),
    }
}

/// `/v1/traces` appended to a base endpoint, with one slash between them.
fn with_traces_path(base: &str) -> String {
    match base.strip_suffix('/') {
        Some(base) => format!("{}{}", base, TRACES_PATH),
        None => format!("{}{}", base, TRACES_PATH),
    }
}

/// The SDK's resource (OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES), named
/// pgcat unless the environment names it.
fn resource() -> Resource {
    let detected = Resource::builder().build();
    let named = detected
        .get(&Key::from_static_str("service.name"))
        .is_some_and(|name| !name.as_str().starts_with("unknown_service"));
    if named {
        detected
    } else {
        Resource::builder().with_service_name("pgcat").build()
    }
}

/// Keeps `ratio` of the traces whose caller sampled them, by trace id, as
/// `TraceIdRatioBased(ratio)` does; drops everything else. Unlike
/// `ParentBased(TraceIdRatioBased)`, a sampled parent doesn't keep every
/// statement: the ratio bounds the span volume pgcat adds to traced requests.
fn decide(
    ratio: f64,
    parent_context: Option<&Context>,
    trace_id: TraceId,
    name: &str,
    span_kind: &SpanKind,
    attributes: &[KeyValue],
    links: &[Link],
) -> SamplingResult {
    let parent = parent_context
        .filter(|cx| cx.has_active_span())
        .map(|cx| cx.span().span_context().clone());
    match &parent {
        Some(parent) if parent.is_valid() && parent.is_sampled() && ratio > 0.0 => {
            Sampler::TraceIdRatioBased(ratio).should_sample(
                parent_context,
                trace_id,
                name,
                span_kind,
                attributes,
                links,
            )
        }
        _ => SamplingResult {
            decision: SamplingDecision::Drop,
            attributes: Vec::new(),
            trace_state: parent
                .map(|parent| parent.trace_state().clone())
                .unwrap_or_default(),
        },
    }
}

/// The provider's sampler, reading the current ratio.
#[derive(Clone, Debug)]
struct StatementSampler;

impl ShouldSample for StatementSampler {
    fn should_sample(
        &self,
        parent_context: Option<&Context>,
        trace_id: TraceId,
        name: &str,
        span_kind: &SpanKind,
        attributes: &[KeyValue],
        links: &[Link],
    ) -> SamplingResult {
        decide(
            ratio(),
            parent_context,
            trace_id,
            name,
            span_kind,
            attributes,
            links,
        )
    }
}

/// Whether the sampler will keep a span under this parent: checked when a
/// statement arrives, so that pgcat tracks only statements it will export.
fn keeps(ratio: f64, parent: &Parent) -> bool {
    if !parent.sampled || ratio <= 0.0 {
        return false;
    }
    let cx = Context::new().with_remote_span_context(parent.span_context());
    decide(
        ratio,
        Some(&cx),
        parent.trace_id,
        SPAN_NAME,
        &SpanKind::Server,
        &[],
        &[],
    )
    .decision
        == SamplingDecision::RecordAndSample
}

/// The caller's span, from a traceparent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Parent {
    trace_id: TraceId,
    span_id: SpanId,
    sampled: bool,
}

impl Parent {
    fn span_context(&self) -> SpanContext {
        let flags = if self.sampled {
            TraceFlags::SAMPLED
        } else {
            TraceFlags::default()
        };
        SpanContext::new(self.trace_id, self.span_id, flags, true, TraceState::NONE)
    }
}

/// The traceparent of a statement's leading sqlcommenter comment: the
/// statement must start with the comment, every `key='value'` pair in it must
/// be well formed, and exactly one must be a valid version-00 traceparent.
/// Anything else, including a comment PostgreSQL would read differently than
/// this does (nested, escaped, or cut by a NUL), yields none.
pub fn traceparent(statement: &[u8]) -> Option<Parent> {
    let after = statement.strip_prefix(b"/*")?;
    let window = &after[..after.len().min(MAX_COMMENT)];
    let end = window.windows(2).position(|pair| pair == b"*/")?;
    let comment = &window[..end];
    if comment.windows(2).any(|pair| pair == b"/*")
        || comment.contains(&0)
        || comment.contains(&b'\\')
    {
        return None;
    }

    let mut found = None;
    let mut rest = comment;
    loop {
        let equals = rest.iter().position(|&byte| byte == b'=')?;
        let key = &rest[..equals];
        if key.is_empty() || !key.iter().all(|&byte| is_key_byte(byte)) {
            return None;
        }
        let quoted = rest[equals + 1..].strip_prefix(b"'")?;
        let close = quoted.iter().position(|&byte| byte == b'\'')?;
        if key == b"traceparent" {
            if found.is_some() {
                return None;
            }
            found = Some(parse_traceparent(&quoted[..close])?);
        }
        rest = &quoted[close + 1..];
        match rest.split_first() {
            None => return found,
            Some((b',', more)) => rest = more,
            Some(_) => return None,
        }
    }
}

/// sqlcommenter keys are URL-encoded.
fn is_key_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-' | b'~' | b'%')
}

/// A version-00 traceparent: `00-<32 hex>-<16 hex>-<2 hex>`, lowercase, with
/// non-zero ids. Flags other than sampled are ignored, as the spec says.
/// Other versions are refused rather than read as 00: a caller that sends one
/// gets untraced statements, not misread ones.
fn parse_traceparent(value: &[u8]) -> Option<Parent> {
    if value.len() != 55 || !value.starts_with(b"00-") || value[35] != b'-' || value[52] != b'-' {
        return None;
    }
    let trace_id: [u8; 16] = lower_hex(&value[3..35])?;
    let span_id: [u8; 8] = lower_hex(&value[36..52])?;
    let [flags]: [u8; 1] = lower_hex(&value[53..55])?;
    if trace_id == [0; 16] || span_id == [0; 8] {
        return None;
    }
    Some(Parent {
        trace_id: TraceId::from_bytes(trace_id),
        span_id: SpanId::from_bytes(span_id),
        sampled: flags & TraceFlags::SAMPLED.to_u8() != 0,
    })
}

fn lower_hex<const N: usize>(digits: &[u8]) -> Option<[u8; N]> {
    fn nibble(digit: u8) -> Option<u8> {
        match digit {
            b'0'..=b'9' => Some(digit - b'0'),
            b'a'..=b'f' => Some(digit - b'a' + 10),
            _ => None,
        }
    }
    if digits.len() != 2 * N {
        return None;
    }
    let mut bytes = [0; N];
    for (byte, pair) in bytes.iter_mut().zip(digits.chunks_exact(2)) {
        *byte = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Some(bytes)
}

/// A client's statements that are being traced: allocated at its first
/// sampled statement, dropped when tracing is turned off.
#[derive(Debug, Default)]
pub struct ClientTraces {
    /// Prepared statements whose Parse carried a kept traceparent.
    statements: HashMap<Box<[u8]>, Parent>,
    /// Portals bound to those statements.
    portals: HashMap<Box<[u8]>, Parent>,
    /// The statements and portals traced since the server's last
    /// ReadyForQuery. A batch the server rejects may not have created them (a
    /// Parse of a name in use fails, and the old statement stays), nor one
    /// pgcat drops, so they're forgotten then; past the caps, all of them are.
    batch_statements: Vec<Box<[u8]>>,
    batch_portals: Vec<Box<[u8]>>,
    batch_overflowed: bool,
    /// The spans of the batch in flight, ended by the server's ReadyForQuery.
    pending: Vec<Pending>,
    /// The client ended the pending spans' batch (Query, Sync or Flush). A
    /// batch that was answered without a server reply leaves its spans here:
    /// the next batch discards them.
    sealed: bool,
    outcome: Outcome,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Pending {
    parent: Parent,
    start: SystemTime,
    protocol: &'static str,
}

/// How the pending batch went: its first error.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Outcome {
    failed: bool,
    sqlstate: Option<[u8; 5]>,
    /// The error pgcat saw itself, when the server sent none.
    error_type: Option<&'static str>,
}

/// Records what a client message starts, for a client in a pool. Call for
/// every message pgcat reads from the client, before handling it.
#[inline]
pub fn client_message(traces: &mut Option<Box<ClientTraces>>, message: &[u8]) {
    if !enabled() {
        if traces.is_some() {
            *traces = None;
        }
        return;
    }
    let ratio = ratio();
    if traces.is_none() {
        // Only a sampled Query or Parse gives a client something to track.
        let starts = match message.first() {
            Some(b'Q') => message.get(5..).and_then(traceparent),
            Some(b'P') => parse_fields(message).and_then(|(_, text)| traceparent(text)),
            _ => None,
        };
        if !starts.is_some_and(|parent| keeps(ratio, &parent)) {
            return;
        }
        *traces = Some(Box::default());
    }
    if let Some(traces) = traces.as_deref_mut() {
        traces.client_message(message, ratio, SystemTime::now());
    }
}

/// Records a reply from the server: ReadyForQuery ends the pending spans.
#[inline]
pub fn server_replied(
    traces: &mut Option<Box<ClientTraces>>,
    reply: &[u8],
    error: bool,
    ready_for_query: Option<u8>,
    address: &Address,
) {
    if let Some(traces) = traces.as_deref_mut() {
        if let Some(outcome) = traces.server_replied(reply, error, ready_for_query.is_some()) {
            traces.export(outcome, address);
        }
    }
}

/// Ends the pending spans as failed: the server didn't answer.
pub fn server_failed(
    traces: &mut Option<Box<ClientTraces>>,
    address: &Address,
    error_type: &'static str,
) {
    if let Some(traces) = traces.as_deref_mut() {
        if let Some(outcome) = traces.server_failed(error_type) {
            traces.export(outcome, address);
        }
    }
}

/// pgcat dropped the client's buffered batch without sending it: what it
/// parsed and bound doesn't exist, and no reply will end its spans.
pub fn batch_dropped(traces: &mut Option<Box<ClientTraces>>) {
    if let Some(traces) = traces.as_deref_mut() {
        traces.batch_dropped();
    }
}

/// A simple Query runs SQL `DEALLOCATE` or `DISCARD`, which share protocol
/// statements' names: a later SQL `PREPARE` can reuse one.
pub fn statements_discarded(traces: &mut Option<Box<ClientTraces>>) {
    if let Some(traces) = traces.as_deref_mut() {
        traces.statements_discarded();
    }
}

impl ClientTraces {
    fn client_message(&mut self, message: &[u8], ratio: f64, now: SystemTime) {
        // pgcat finishes a batch before it reads the next one, so a message
        // that starts a batch while the last one is sealed means that batch
        // got no ReadyForQuery from a server: pgcat answered it. COPY data
        // (d, c, f) belongs to the sealed batch and doesn't end it.
        if self.sealed
            && matches!(
                message.first(),
                Some(b'Q' | b'P' | b'B' | b'E' | b'D' | b'C')
            )
        {
            self.discard();
        }
        match message.first() {
            Some(b'Q') => {
                self.sealed = true;
                if let Some(parent) = message
                    .get(5..)
                    .and_then(traceparent)
                    .filter(|parent| keeps(ratio, parent))
                {
                    self.pending.push(Pending {
                        parent,
                        start: now,
                        protocol: "simple",
                    });
                }
            }
            Some(b'P') => {
                let Some((name, text)) = parse_fields(message) else {
                    return;
                };
                match traceparent(text).filter(|parent| keeps(ratio, parent)) {
                    Some(parent) => {
                        insert_bounded(&mut self.statements, name, parent, MAX_STATEMENTS);
                        self.batch_overflowed |=
                            !remember(&mut self.batch_statements, name, MAX_STATEMENTS);
                    }
                    None => {
                        self.statements.remove(name);
                    }
                }
            }
            Some(b'B') => {
                let Some((portal, statement)) = bind_fields(message) else {
                    return;
                };
                // A statement name too long to track can't be a traced one.
                match statement.and_then(|statement| self.statements.get(statement).copied()) {
                    Some(parent) => {
                        insert_bounded(&mut self.portals, portal, parent, MAX_PORTALS);
                        self.batch_overflowed |=
                            !remember(&mut self.batch_portals, portal, MAX_PORTALS);
                    }
                    None => {
                        self.portals.remove(portal);
                    }
                }
            }
            Some(b'E') => {
                let Some(portal) = message.get(5..).and_then(name) else {
                    return;
                };
                if let Some(parent) = self.portals.get(portal).copied() {
                    if self.pending.len() < MAX_PENDING {
                        self.pending.push(Pending {
                            parent,
                            start: now,
                            protocol: "extended",
                        });
                    }
                }
            }
            Some(b'S' | b'H') => {
                if !self.pending.is_empty() {
                    self.sealed = true;
                }
            }
            Some(b'C') => {
                let Some((&kind, rest)) = message.get(5..).and_then(<[u8]>::split_first) else {
                    return;
                };
                let Some(closed) = name(rest) else {
                    return;
                };
                match kind {
                    b'S' => {
                        self.statements.remove(closed);
                    }
                    b'P' => {
                        self.portals.remove(closed);
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    /// The batch's outcome when this reply ends it.
    fn server_replied(
        &mut self,
        reply: &[u8],
        error: bool,
        ready_for_query: bool,
    ) -> Option<Outcome> {
        if error || ready_for_query {
            self.end_batch(error);
        }
        if self.pending.is_empty() {
            return None;
        }
        if error && !self.outcome.failed {
            self.outcome.failed = true;
            self.outcome.sqlstate = sqlstate(reply);
        }
        ready_for_query.then(|| std::mem::take(&mut self.outcome))
    }

    fn server_failed(&mut self, error_type: &'static str) -> Option<Outcome> {
        if self.pending.is_empty() {
            return None;
        }
        let mut outcome = std::mem::take(&mut self.outcome);
        if !outcome.failed {
            outcome = Outcome {
                failed: true,
                sqlstate: None,
                error_type: Some(error_type),
            };
        }
        Some(outcome)
    }

    fn batch_dropped(&mut self) {
        self.end_batch(true);
        self.discard();
    }

    fn statements_discarded(&mut self) {
        self.statements.clear();
        self.portals.clear();
    }

    /// Stops tracking what the batch traced, first forgetting it when the
    /// batch may not have created it; past the caps, everything is.
    fn end_batch(&mut self, forget: bool) {
        if forget && self.batch_overflowed {
            self.statements.clear();
            self.portals.clear();
        } else if forget {
            for name in &self.batch_statements {
                self.statements.remove(name);
            }
            for name in &self.batch_portals {
                self.portals.remove(name);
            }
        }
        self.batch_statements.clear();
        self.batch_portals.clear();
        self.batch_overflowed = false;
    }

    fn discard(&mut self) {
        self.pending.clear();
        self.sealed = false;
        self.outcome = Outcome::default();
    }

    fn export(&mut self, outcome: Outcome, address: &Address) {
        let end = SystemTime::now();
        let exporter = EXPORTER.load();
        if let Some(exporter) = exporter.as_deref() {
            for pending in &self.pending {
                export(exporter, pending, &outcome, address, end);
            }
        }
        self.discard();
    }
}

fn export(
    exporter: &Exporter,
    pending: &Pending,
    outcome: &Outcome,
    address: &Address,
    end: SystemTime,
) {
    let mut attributes = Vec::with_capacity(8);
    attributes.push(KeyValue::new("db.system.name", "postgresql"));
    attributes.push(KeyValue::new("pgcat.pool.name", address.pool_name.clone()));
    attributes.push(KeyValue::new("pgcat.pool.user", address.username.clone()));
    attributes.push(KeyValue::new("pgcat.server.role", role_name(address.role)));
    attributes.push(KeyValue::new("pgcat.shard", address.shard as i64));
    attributes.push(KeyValue::new("pgcat.protocol", pending.protocol));
    if let Some(code) = outcome.sqlstate {
        let code = String::from_utf8_lossy(&code).into_owned();
        attributes.push(KeyValue::new("db.response.status_code", code.clone()));
        attributes.push(KeyValue::new("error.type", code));
    } else if outcome.failed {
        attributes.push(KeyValue::new(
            "error.type",
            outcome.error_type.unwrap_or("_OTHER"),
        ));
    }
    let parent = Context::new().with_remote_span_context(pending.parent.span_context());
    let mut span = exporter
        .tracer
        .span_builder(SPAN_NAME)
        .with_kind(SpanKind::Server)
        .with_start_time(pending.start)
        .with_attributes(attributes)
        .start_with_context(&exporter.tracer, &parent);
    if outcome.failed {
        span.set_status(Status::error(""));
    }
    span.end_with_timestamp(end);
}

fn role_name(role: Role) -> &'static str {
    match role {
        Role::Primary => "primary",
        Role::Replica => "replica",
        Role::Mirror => "mirror",
    }
}

/// A NUL-terminated name at the start of `bytes`, if it's short enough to track.
fn name(bytes: &[u8]) -> Option<&[u8]> {
    let window = &bytes[..bytes.len().min(MAX_NAME + 1)];
    let end = window.iter().position(|&byte| byte == 0)?;
    Some(&window[..end])
}

/// A Parse's statement name and query text.
fn parse_fields(message: &[u8]) -> Option<(&[u8], &[u8])> {
    let body = message.get(5..)?;
    let statement = name(body)?;
    Some((statement, &body[statement.len() + 1..]))
}

/// A Bind's portal name and, if it's short enough to track, statement name.
fn bind_fields(message: &[u8]) -> Option<(&[u8], Option<&[u8]>)> {
    let body = message.get(5..)?;
    let portal = name(body)?;
    Some((portal, name(&body[portal.len() + 1..])))
}

/// Adds a name to a batch's names; false when they're full.
fn remember(names: &mut Vec<Box<[u8]>>, name: &[u8], cap: usize) -> bool {
    if names.iter().any(|known| **known == *name) {
        return true;
    }
    if names.len() >= cap {
        return false;
    }
    names.push(name.into());
    true
}

fn insert_bounded(map: &mut HashMap<Box<[u8]>, Parent>, key: &[u8], parent: Parent, cap: usize) {
    if let Some(existing) = map.get_mut(key) {
        *existing = parent;
        return;
    }
    if map.len() >= cap {
        map.clear();
    }
    map.insert(key.into(), parent);
}

/// The SQLSTATE of the first ErrorResponse among complete backend messages.
fn sqlstate(reply: &[u8]) -> Option<[u8; 5]> {
    let mut rest = reply;
    while rest.len() >= 5 {
        let length = u32::from_be_bytes([rest[1], rest[2], rest[3], rest[4]]) as usize;
        if length < 4 || rest.len() < 1 + length {
            return None;
        }
        if rest[0] == b'E' {
            return error_code(&rest[5..1 + length]);
        }
        rest = &rest[1 + length..];
    }
    None
}

fn error_code(mut fields: &[u8]) -> Option<[u8; 5]> {
    while let Some((&kind, more)) = fields.split_first() {
        if kind == 0 {
            return None;
        }
        let end = more.iter().position(|&byte| byte == 0)?;
        if kind == b'C' {
            let code: [u8; 5] = more[..end].try_into().ok()?;
            return code
                .iter()
                .all(|byte| byte.is_ascii_digit() || byte.is_ascii_uppercase())
                .then_some(code);
        }
        fields = &more[end + 1..];
    }
    None
}

/// Sends OTLP requests on pgcat's runtime, with the hyper client pgcat
/// already has. The batch processor calls it from its own thread.
#[derive(Debug)]
struct RuntimeHttpClient {
    runtime: Handle,
    /// Whether the last export failed: failures are logged when they start
    /// and when they stop, not per batch.
    failing: AtomicBool,
}

#[async_trait]
impl HttpClient for RuntimeHttpClient {
    async fn send_bytes(&self, request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        let result = match self.runtime.spawn(send(request)).await {
            Ok(result) => result,
            Err(err) => Err(err.into()),
        };
        let failed = !matches!(&result, Ok(response) if response.status().is_success());
        if failed != self.failing.swap(failed, Ordering::Relaxed) {
            match &result {
                Ok(response) if failed => {
                    warn!("Statement span export failed: HTTP {}", response.status())
                }
                Err(err) => warn!("Statement span export failed: {}", err),
                _ => info!("Statement span export recovered"),
            }
        }
        result
    }
}

async fn send(request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
    match tokio::time::timeout(EXPORT_TIMEOUT, send_once(request)).await {
        Ok(result) => result,
        Err(_) => Err("timed out".into()),
    }
}

async fn send_once(request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
    let (mut parts, body) = request.into_parts();
    if parts.uri.scheme_str() != Some("http") {
        return Err("the OTLP endpoint isn't an http:// URL".into());
    }
    let authority = parts
        .uri
        .authority()
        .cloned()
        .ok_or("the OTLP endpoint has no host")?;
    if authority.as_str().contains('@') {
        return Err("the OTLP endpoint has credentials".into());
    }
    if parts.uri.query().is_some() {
        return Err("the OTLP endpoint has a query".into());
    }
    let host = authority
        .host()
        .trim_start_matches('[')
        .trim_end_matches(']');
    let port = port(&authority).ok_or("the OTLP endpoint's port isn't valid")?;
    let stream = tokio::net::TcpStream::connect((host, port)).await?;
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
    // The connection task ends with the request: one connection per export,
    // a few per second at most.
    let _connection = AbortOnDrop(tokio::spawn(connection));
    parts.uri = parts
        .uri
        .path_and_query()
        .map_or("/", |path| path.as_str())
        .parse()?;
    // pgcat's own headers, none of the SDK's: it adds OTEL_EXPORTER_OTLP_HEADERS
    // and OTEL_EXPORTER_OTLP_TRACES_HEADERS from the environment, over the
    // ones it sets, and those usually carry credentials. This build has no
    // compression, so the body is plain protobuf.
    let mut headers = HeaderMap::with_capacity(2);
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/x-protobuf"),
    );
    headers.insert(HOST, HeaderValue::from_str(authority.as_str())?);
    parts.headers = headers;
    let response = sender
        .send_request(hyper::Request::from_parts(parts, Full::new(body)))
        .await?;
    let (parts, body) = response.into_parts();
    let body = Limited::new(body, MAX_RESPONSE_BODY)
        .collect()
        .await?
        .to_bytes();
    Ok(Response::from_parts(parts, body))
}

struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::propagation::TextMapPropagator;
    use opentelemetry_sdk::propagation::TraceContextPropagator;

    const TRACE: &str = "0af7651916cd43dd8448eb211c80319c";
    const SPAN: &str = "b7ad6b7169203331";

    fn resolve(traces: Option<&str>, generic: Option<&str>) -> Result<String, &'static str> {
        resolve_os(traces.map(Into::into), generic.map(Into::into))
    }

    fn resolve_os(
        traces: Option<std::ffi::OsString>,
        generic: Option<std::ffi::OsString>,
    ) -> Result<String, &'static str> {
        endpoint_from_env(move |name| match name {
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" => traces.clone(),
            "OTEL_EXPORTER_OTLP_ENDPOINT" => generic.clone(),
            _ => None,
        })
    }

    #[test]
    fn the_environment_endpoint_resolves_as_in_otlp_0_33() {
        let ok = |url: &str| Ok(url.to_string());
        // Neither set, or set empty: the default.
        assert_eq!(resolve(None, None), ok("http://localhost:4318/v1/traces"));
        assert_eq!(
            resolve(Some(""), Some("")),
            ok("http://localhost:4318/v1/traces")
        );
        assert_eq!(
            resolve(None, Some("")),
            ok("http://localhost:4318/v1/traces")
        );
        // The traces variable is used as is, its path included.
        assert_eq!(
            resolve(
                Some("http://collector:4318/custom"),
                Some("http://other:4318")
            ),
            ok("http://collector:4318/custom")
        );
        assert_eq!(
            resolve(Some("http://collector:4318"), None),
            ok("http://collector:4318")
        );
        // The generic one gets the signal path, one slash between.
        assert_eq!(
            resolve(None, Some("http://collector:4318")),
            ok("http://collector:4318/v1/traces")
        );
        assert_eq!(
            resolve(Some(""), Some("http://collector:4318/")),
            ok("http://collector:4318/v1/traces")
        );
        assert_eq!(
            resolve(None, Some("http://collector:4318/otlp")),
            ok("http://collector:4318/otlp/v1/traces")
        );
    }

    #[test]
    fn a_bad_environment_endpoint_is_refused_not_skipped() {
        let bad = "http://collector:4318/v1/traces?key=xq7 zv9";
        let good = "http://collector:4318";
        for (traces, generic) in [
            (Some(bad), None),
            (Some(bad), Some(good)),
            (None, Some(bad)),
            (Some(""), Some(bad)),
            (Some("collector:4318"), None),
            (None, Some("collector:4318")),
            (Some("/v1/traces"), None),
        ] {
            let reason = resolve(traces, generic).unwrap_err();
            assert!(
                !reason.contains("xq7") && !reason.contains("zv9"),
                "{}",
                reason
            );
        }
        // A valid traces variable wins, so a bad generic one isn't read.
        assert_eq!(resolve(Some(good), Some(bad)), Ok(good.to_string()));
    }

    #[cfg(unix)]
    #[test]
    fn a_non_unicode_environment_endpoint_is_refused_not_skipped() {
        use std::os::unix::ffi::OsStringExt;
        let invalid = || Some(std::ffi::OsString::from_vec(vec![b'h', 0xff]));
        let good = Some("http://collector:4318".into());
        assert!(resolve_os(invalid(), good.clone()).is_err());
        assert!(resolve_os(None, invalid()).is_err());
        assert!(resolve_os(Some("".into()), invalid()).is_err());
        assert_eq!(
            resolve_os(good.clone(), invalid()),
            Ok("http://collector:4318".to_string())
        );
    }

    fn statement(comment: &str) -> Vec<u8> {
        format!("{} SELECT 1", comment).into_bytes()
    }

    fn sampled() -> String {
        format!("/*traceparent='00-{}-{}-01'*/", TRACE, SPAN)
    }

    #[test]
    fn reads_the_traceparent_of_a_sqlcommenter_comment() {
        let parent = traceparent(&statement(&sampled())).unwrap();
        assert_eq!(parent.trace_id, TraceId::from_hex(TRACE).unwrap());
        assert_eq!(parent.span_id, SpanId::from_hex(SPAN).unwrap());
        assert!(parent.sampled);

        // As clients write it: sorted keys, URL-encoded values.
        let full = format!(
            "/*application='api',db_driver='psycopg%3A3.2',route='%2Fv1%2Fitems',traceparent='00-{}-{}-01'*/",
            TRACE, SPAN
        );
        assert_eq!(traceparent(&statement(&full)), Some(parent));
        // A comment followed directly by SQL.
        assert_eq!(
            traceparent(format!("{}SELECT 1", sampled()).as_bytes()),
            Some(parent)
        );
    }

    #[test]
    fn reads_only_the_sampled_flag() {
        for (flags, sampled) in [
            ("00", false),
            ("01", true),
            ("02", false),
            ("03", true),
            ("ff", true),
        ] {
            let comment = format!("/*traceparent='00-{}-{}-{}'*/", TRACE, SPAN, flags);
            assert_eq!(
                traceparent(&statement(&comment)).unwrap().sampled,
                sampled,
                "flags {}",
                flags
            );
        }
    }

    #[test]
    fn refuses_anything_but_a_strict_leading_comment() {
        let tp = |value: &str| format!("traceparent='{}'", value);
        let good = format!("00-{}-{}-01", TRACE, SPAN);
        let refused = [
            // No comment, or not at the start.
            "SELECT 1".to_string(),
            format!(" /*{}*/", tp(&good)),
            format!("SELECT 1 /*{}*/", tp(&good)),
            format!("-- {}", tp(&good)),
            // Unterminated, or longer than pgcat reads.
            format!("/*{}", tp(&good)),
            format!("/*a='{}',{}*/", "x".repeat(MAX_COMMENT), tp(&good)),
            // PostgreSQL nests comments; NUL ends the string; no escapes.
            format!("/*/*{}*/*/", tp(&good)),
            format!("/*{},a='/*'*/", tp(&good)),
            format!("/*{},a='\0'*/", tp(&good)),
            format!("/*{},a='\\''*/", tp(&good)),
            // Malformed pairs anywhere in the comment.
            format!("/* {}*/", tp(&good)),
            format!("/*{} */", tp(&good)),
            format!("/*{},*/", tp(&good)),
            format!("/*{},a=b*/", tp(&good)),
            format!("/*{},='b'*/", tp(&good)),
            format!("/*{},a b='c'*/", tp(&good)),
            format!("/*traceparent={}*/", good),
            "/**/".to_string(),
            // No traceparent, or two.
            "/*traceparentx='x'*/".to_string(),
            format!("/*{},{}*/", tp(&good), tp(&good)),
            // Only version 00, lowercase, with valid ids.
            format!("/*{}*/", tp(&format!("01-{}-{}-01", TRACE, SPAN))),
            format!("/*{}*/", tp(&format!("ff-{}-{}-01", TRACE, SPAN))),
            format!("/*{}*/", tp(&format!("00-{}-{}-01-00", TRACE, SPAN))),
            format!("/*{}*/", tp(&good.to_uppercase())),
            format!("/*{}*/", tp(&format!("00-{}-{}-01", "0".repeat(32), SPAN))),
            format!("/*{}*/", tp(&format!("00-{}-{}-01", TRACE, "0".repeat(16)))),
            format!("/*{}*/", tp(&format!("00-{}-{}-0", TRACE, SPAN))),
            format!("/*{}*/", tp(&format!("00-{}0-{}-01", TRACE, &SPAN[1..]))),
            format!("/*{}*/", tp(&format!("00-{}-{}-0g", TRACE, SPAN))),
            format!("/*{}*/", tp(&good.replace('-', "%2D"))),
        ];
        for text in refused {
            assert_eq!(traceparent(&statement(&text)), None, "{:?}", text);
        }
    }

    /// pgcat accepts only traceparents the OpenTelemetry propagator reads
    /// alike: same ids, same sampled flag.
    #[test]
    fn agrees_with_the_opentelemetry_propagator() {
        let propagator = TraceContextPropagator::new();
        for value in [
            format!("00-{}-{}-01", TRACE, SPAN),
            format!("00-{}-{}-00", TRACE, SPAN),
            format!("00-{}-{}-09", TRACE, SPAN),
            format!("00-{}-{}-01", "f".repeat(32), "f".repeat(16)),
            format!("00-{}-{}-01", "0".repeat(31) + "1", "0".repeat(15) + "1"),
        ] {
            let ours = traceparent(&statement(&format!("/*traceparent='{}'*/", value))).unwrap();
            let carrier = HashMap::from([("traceparent".to_string(), value.clone())]);
            let theirs = propagator.extract(&carrier);
            let theirs = theirs.span().span_context().clone();
            assert!(theirs.is_valid() && theirs.is_remote(), "{}", value);
            assert_eq!(
                ours.span_context().trace_id(),
                theirs.trace_id(),
                "{}",
                value
            );
            assert_eq!(ours.span_context().span_id(), theirs.span_id(), "{}", value);
            assert_eq!(
                ours.span_context().is_sampled(),
                theirs.is_sampled(),
                "{}",
                value
            );
        }
    }

    fn parent(trace_id: u128, sampled: bool) -> Parent {
        Parent {
            trace_id: TraceId::from(trace_id),
            span_id: SpanId::from(7u64),
            sampled,
        }
    }

    fn decision(ratio: f64, parent: Option<Parent>, trace_id: TraceId) -> SamplingDecision {
        let cx =
            parent.map(|parent| Context::new().with_remote_span_context(parent.span_context()));
        decide(
            ratio,
            cx.as_ref(),
            trace_id,
            SPAN_NAME,
            &SpanKind::Server,
            &[],
            &[],
        )
        .decision
    }

    #[test]
    fn samples_only_under_a_sampled_parent() {
        let trace_id = TraceId::from(1u128);
        assert_eq!(decision(1.0, None, trace_id), SamplingDecision::Drop);
        assert_eq!(
            decision(1.0, Some(parent(1, false)), trace_id),
            SamplingDecision::Drop
        );
        assert_eq!(
            decision(0.0, Some(parent(1, true)), trace_id),
            SamplingDecision::Drop
        );
        assert_eq!(
            decision(1.0, Some(parent(1, true)), trace_id),
            SamplingDecision::RecordAndSample
        );
        assert!(keeps(1.0, &parent(1, true)));
        assert!(!keeps(1.0, &parent(1, false)));
        assert!(!keeps(0.0, &parent(1, true)));
    }

    /// Under a sampled parent the decision is TraceIdRatioBased's, so pgcat
    /// keeps the traces any other ratio-sampling OpenTelemetry SDK would.
    #[test]
    fn keeps_the_ratio_by_trace_id() {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            // splitmix64: deterministic, well-spread ids.
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        };
        for ratio in [0.01, 0.25, 0.5] {
            let reference = Sampler::TraceIdRatioBased(ratio);
            let trials = 100_000;
            let mut kept = 0;
            for _ in 0..trials {
                let id = ((next() as u128) << 64) | next() as u128;
                let parent = parent(id, true);
                let ours = decision(ratio, Some(parent), parent.trace_id);
                let cx = Context::new().with_remote_span_context(parent.span_context());
                let theirs = reference
                    .should_sample(
                        Some(&cx),
                        parent.trace_id,
                        SPAN_NAME,
                        &SpanKind::Server,
                        &[],
                        &[],
                    )
                    .decision;
                assert_eq!(ours, theirs);
                assert_eq!(
                    keeps(ratio, &parent),
                    ours == SamplingDecision::RecordAndSample
                );
                kept += (ours == SamplingDecision::RecordAndSample) as usize;
            }
            let fraction = kept as f64 / trials as f64;
            assert!(
                (fraction - ratio).abs() < ratio * 0.1 + 0.002,
                "{} kept {}",
                ratio,
                fraction
            );
        }
    }

    fn frame(code: u8, body: &[u8]) -> Vec<u8> {
        let mut message = vec![code];
        message.extend_from_slice(&(body.len() as u32 + 4).to_be_bytes());
        message.extend_from_slice(body);
        message
    }

    fn query(text: &str) -> Vec<u8> {
        frame(b'Q', &[text.as_bytes(), b"\0"].concat())
    }

    fn parse(name: &str, text: &str) -> Vec<u8> {
        frame(
            b'P',
            &[name.as_bytes(), b"\0", text.as_bytes(), b"\0\0\0"].concat(),
        )
    }

    fn bind(portal: &str, statement: &str) -> Vec<u8> {
        frame(
            b'B',
            &[
                portal.as_bytes(),
                b"\0",
                statement.as_bytes(),
                b"\0\0\0\0\0\0\0",
            ]
            .concat(),
        )
    }

    fn execute(portal: &str) -> Vec<u8> {
        frame(b'E', &[portal.as_bytes(), b"\0\0\0\0\0"].concat())
    }

    fn close(kind: u8, name: &str) -> Vec<u8> {
        frame(b'C', &[&[kind], name.as_bytes(), b"\0"].concat())
    }

    fn sampled_sql(trace_id: u128) -> String {
        format!(
            "/*traceparent='00-{:032x}-{}-01'*/ SELECT 1",
            trace_id, SPAN
        )
    }

    fn run(traces: &mut ClientTraces, messages: &[Vec<u8>]) {
        for message in messages {
            traces.client_message(message, 1.0, SystemTime::UNIX_EPOCH);
        }
    }

    fn traced(traces: &ClientTraces) -> Vec<(u128, &'static str)> {
        traces
            .pending
            .iter()
            .map(|pending| {
                (
                    u128::from_be_bytes(pending.parent.trace_id.to_bytes()),
                    pending.protocol,
                )
            })
            .collect()
    }

    /// The server's ReadyForQuery, ending the batch as `server_replied` does.
    fn ready(traces: &mut ClientTraces) -> Option<Outcome> {
        let outcome = traces.server_replied(&frame(b'Z', b"I"), false, true);
        if outcome.is_some() {
            traces.discard();
        }
        outcome
    }

    #[test]
    fn traces_simple_queries_until_ready_for_query() {
        let mut traces = ClientTraces::default();
        run(&mut traces, &[query(&sampled_sql(1))]);
        assert_eq!(traced(&traces), [(1, "simple")]);
        assert_eq!(
            traces.server_replied(&frame(b'C', b"SELECT 1\0"), false, false),
            None
        );
        assert_eq!(ready(&mut traces), Some(Outcome::default()));

        // Unsampled and untraced queries add nothing.
        run(&mut traces, &[query("SELECT 1")]);
        run(
            &mut traces,
            &[query(&sampled_sql(2).replace("-01'", "-00'"))],
        );
        assert_eq!(traced(&traces), []);
        assert_eq!(ready(&mut traces), None);
    }

    #[test]
    fn traces_executes_of_traced_statements() {
        let mut traces = ClientTraces::default();
        run(
            &mut traces,
            &[
                parse("s1", &sampled_sql(1)),
                bind("", "s1"),
                execute(""),
                frame(b'S', b""),
            ],
        );
        assert_eq!(traced(&traces), [(1, "extended")]);
        assert!(ready(&mut traces).is_some());

        // A prepared statement keeps its parent for later executions.
        run(
            &mut traces,
            &[
                bind("p", "s1"),
                execute("p"),
                execute("p"),
                frame(b'S', b""),
            ],
        );
        assert_eq!(traced(&traces), [(1, "extended"), (1, "extended")]);
        assert!(ready(&mut traces).is_some());

        // Re-parsing a name without a traceparent, or closing it, forgets it.
        run(
            &mut traces,
            &[parse("s1", "SELECT 1"), bind("", "s1"), execute("")],
        );
        assert_eq!(traced(&traces), []);
        run(
            &mut traces,
            &[
                parse("s2", &sampled_sql(2)),
                close(b'S', "s2"),
                bind("", "s2"),
                execute(""),
            ],
        );
        assert_eq!(traced(&traces), []);
        run(
            &mut traces,
            &[
                parse("s3", &sampled_sql(3)),
                bind("q", "s3"),
                close(b'P', "q"),
                execute("q"),
            ],
        );
        assert_eq!(traced(&traces), []);

        // Binding a portal to a statement whose name is too long to track
        // unbinds it from the traced statement it had.
        run(
            &mut traces,
            &[
                parse("s4", &sampled_sql(4)),
                bind("", "s4"),
                execute(""),
                frame(b'S', b""),
            ],
        );
        assert_eq!(traced(&traces), [(4, "extended")]);
        assert!(ready(&mut traces).is_some());
        let long = "n".repeat(MAX_NAME + 1);
        run(
            &mut traces,
            &[parse(&long, "SELECT 1"), bind("", &long), execute("")],
        );
        assert_eq!(traced(&traces), []);

        // The server keeps a statement whose name a rejected Parse reused,
        // so it isn't traced as the rejected one.
        run(&mut traces, &[parse("s5", "SELECT 1"), frame(b'S', b"")]);
        assert_eq!(ready(&mut traces), None);
        run(
            &mut traces,
            &[parse("s5", &sampled_sql(5)), frame(b'S', b"")],
        );
        assert_eq!(
            traces.server_replied(&error_response("42P05"), true, false),
            None
        );
        assert_eq!(ready(&mut traces), None);
        run(
            &mut traces,
            &[bind("", "s5"), execute(""), frame(b'S', b"")],
        );
        assert_eq!(traced(&traces), []);
        assert_eq!(ready(&mut traces), None);

        // Nor as one in a batch pgcat dropped without sending it.
        run(&mut traces, &[parse("s6", "SELECT 1"), frame(b'S', b"")]);
        assert_eq!(ready(&mut traces), None);
        run(
            &mut traces,
            &[parse("s6", &sampled_sql(6)), frame(b'S', b"")],
        );
        traces.batch_dropped();
        run(
            &mut traces,
            &[bind("", "s6"), execute(""), frame(b'S', b"")],
        );
        assert_eq!(traced(&traces), []);
        assert_eq!(ready(&mut traces), None);
    }

    #[test]
    fn sql_deallocate_forgets_traced_statements() {
        let mut traces = ClientTraces::default();
        run(
            &mut traces,
            &[
                parse("s1", &sampled_sql(1)),
                bind("p1", "s1"),
                frame(b'S', b""),
            ],
        );
        assert_eq!(ready(&mut traces), None);
        traces.statements_discarded();
        run(
            &mut traces,
            &[bind("", "s1"), execute(""), execute("p1"), frame(b'S', b"")],
        );
        assert_eq!(traced(&traces), []);
    }

    #[test]
    fn discards_batches_no_server_answered() {
        let mut traces = ClientTraces::default();
        // pgcat answered this one itself: no ReadyForQuery from a server.
        run(&mut traces, &[query(&sampled_sql(1))]);
        run(
            &mut traces,
            &[
                parse("", &sampled_sql(2)),
                bind("", ""),
                execute(""),
                frame(b'S', b""),
            ],
        );
        assert_eq!(traced(&traces), [(2, "extended")]);
        run(&mut traces, &[bind("", ""), execute("")]);
        assert_eq!(traced(&traces), [(2, "extended")]);
        // Within a batch (no Sync yet), Executes accumulate.
        run(&mut traces, &[execute("")]);
        assert_eq!(traced(&traces), [(2, "extended"), (2, "extended")]);
        run(&mut traces, &[frame(b'S', b""), query(&sampled_sql(3))]);
        assert_eq!(traced(&traces), [(3, "simple")]);
        // Any batch that reaches a server ends a batch pgcat answered, even
        // one without an Execute, such as a driver's prepare step.
        run(&mut traces, &[parse("", "SELECT 1"), frame(b'H', b"")]);
        assert_eq!(traced(&traces), []);
        // COPY data belongs to the batch it follows.
        run(
            &mut traces,
            &[
                query(&sampled_sql(4)),
                frame(b'd', b"1\n"),
                frame(b'c', b""),
            ],
        );
        assert_eq!(traced(&traces), [(4, "simple")]);
    }

    #[test]
    fn stays_bounded() {
        let mut traces = ClientTraces::default();
        for i in 0..MAX_STATEMENTS * 3 {
            run(&mut traces, &[parse(&format!("s{}", i), &sampled_sql(1))]);
            assert!(traces.statements.len() <= MAX_STATEMENTS);
        }
        run(&mut traces, &[parse("s", &sampled_sql(1))]);
        for i in 0..MAX_PORTALS * 3 {
            run(&mut traces, &[bind(&format!("p{}", i), "s")]);
            assert!(traces.portals.len() <= MAX_PORTALS);
        }
        run(&mut traces, &[bind("", "s")]);
        for _ in 0..MAX_PENDING * 3 {
            run(&mut traces, &[execute("")]);
        }
        assert_eq!(traces.pending.len(), MAX_PENDING);
        let long = "n".repeat(MAX_NAME + 1);
        run(&mut traces, &[parse(&long, &sampled_sql(1))]);
        assert!(!traces.statements.contains_key(long.as_bytes()));
    }

    fn error_response(code: &str) -> Vec<u8> {
        frame(
            b'E',
            &[
                b"SERROR\0VERROR\0C",
                code.as_bytes(),
                b"\0Mdivision by zero\0\0",
            ]
            .concat(),
        )
    }

    #[test]
    fn records_the_batch_error() {
        let mut traces = ClientTraces::default();
        run(&mut traces, &[query(&sampled_sql(1))]);
        let reply = [frame(b'T', b"\0\0"), error_response("22012")].concat();
        assert_eq!(traces.server_replied(&reply, true, false), None);
        // A later error doesn't replace the first.
        assert_eq!(
            traces.server_replied(&error_response("25P02"), true, false),
            None
        );
        assert_eq!(
            ready(&mut traces),
            Some(Outcome {
                failed: true,
                sqlstate: Some(*b"22012"),
                error_type: None,
            })
        );

        run(&mut traces, &[query(&sampled_sql(1))]);
        assert_eq!(
            traces.server_failed("statement_timeout"),
            Some(Outcome {
                failed: true,
                sqlstate: None,
                error_type: Some("statement_timeout"),
            })
        );
    }

    #[test]
    fn reads_sqlstate_only_from_well_formed_errors() {
        assert_eq!(sqlstate(&error_response("22012")), Some(*b"22012"));
        assert_eq!(
            sqlstate(&[frame(b'N', b"C01000\0\0"), error_response("42P01")].concat()),
            Some(*b"42P01")
        );
        assert_eq!(sqlstate(&error_response("2201")), None);
        assert_eq!(sqlstate(&error_response("22o12")), None);
        assert_eq!(sqlstate(&frame(b'E', b"SERROR\0Mno code\0\0")), None);
        let truncated = error_response("22012");
        assert_eq!(sqlstate(&truncated[..truncated.len() - 12]), None);
        assert_eq!(sqlstate(&frame(b'Z', b"I")), None);
    }

    fn general(ratio: f64, endpoint: Option<&str>) -> General {
        General {
            trace_sample_ratio: ratio,
            trace_otlp_endpoint: endpoint.map(String::from),
            ..General::default()
        }
    }

    #[test]
    fn validates_the_settings() {
        assert!(validate(&general(0.0, None)).is_ok());
        assert!(validate(&general(0.01, Some("http://127.0.0.1:4318/v1/traces"))).is_ok());
        assert!(validate(&general(1.0, Some("http://collector:4318/v1/traces"))).is_ok());
        assert!(validate(&general(1.0, Some("http://[::1]:4318/v1/traces"))).is_ok());
        for ratio in [-0.1, 1.5, f64::NAN, f64::INFINITY] {
            assert!(validate(&general(ratio, None)).is_err(), "{}", ratio);
        }
        for endpoint in [
            "https://collector:4318/v1/traces",
            "collector:4318",
            "/v1/traces",
            "http://user:secret@collector:4318/v1/traces",
            "http://collector:4318/v1/traces?api_key=secret",
            "http://collector:4318?api_key=secret",
            "http://collector:4318/v1/traces#secret",
            "http://collector:431800/v1/traces",
            "http://collector:abc/v1/traces",
            "not a url",
        ] {
            assert!(
                validate(&general(0.5, Some(endpoint))).is_err(),
                "{}",
                endpoint
            );
        }
    }
}
