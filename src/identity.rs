//! The caller identity a client attaches to its connection with
//! `SET PGCAT_IDENTITY TO '<token>'`.
//!
//! The identity belongs to the client connection. pgcat answers the command
//! itself and never sends it to the server, and keeps the token until the
//! client sets another one, runs `RESET PGCAT_IDENTITY`, `DISCARD ALL` or
//! `RESET ALL`, or disconnects. Transactions don't scope it: COMMIT and ROLLBACK
//! leave it as it is.
//!
//! A plugin can verify the token when it's set and name the tenant it belongs
//! to. pgcat then keeps that tenant in a session parameter of whichever server
//! connection the client is using (the pool's `identity_tenant_parameter`), and
//! refuses client statements that would set that parameter themselves.
//!
//! The token is never logged: its `Debug` and `Display` only show a short fingerprint.

use std::any::Any;
use std::sync::Arc;
use std::time::SystemTime;

use hmac::{Hmac, Mac};
use once_cell::sync::Lazy;
use regex::Regex;
use sha2::Sha256;

use crate::codec::encode_hex;
use crate::errors::Error;
use crate::server::ServerParameters;

/// `SET [SESSION] PGCAT_IDENTITY { TO | = } '<token>'`, with `''` escaping a quote.
static SET_IDENTITY: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?is)^\s*SET\s+(?:SESSION\s+)?PGCAT_IDENTITY\s*(?:TO|=)\s*'((?:[^']|'')*)'\s*;?\s*$",
    )
    .unwrap()
});

static RESET_IDENTITY: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?is)^\s*RESET\s+PGCAT_IDENTITY\s*;?\s*$").unwrap());

static SHOW_IDENTITY: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?is)^\s*SHOW\s+PGCAT_IDENTITY\s*;?\s*$").unwrap());

/// Anything that looks like an identity command, anywhere in the query.
static MENTIONS_IDENTITY: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?is)\b(?:SET|RESET|SHOW)\s+(?:(?:SESSION|LOCAL)\s+)?PGCAT_IDENTITY\b").unwrap()
});

/// A setting named with Unicode escapes, `SET U&"..."`, after `normalize`
/// (which unquotes it): it can spell `pgcat_identity` any way.
static UNICODE_SETTING: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?is)\b(?:SET|RESET|SHOW)\s+(?:(?:SESSION|LOCAL)\s+)?U&").unwrap());

/// Statements that reset the session, and the identity with it, anywhere in the query.
static CLEARS_IDENTITY: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?is)(?:^|;)\s*(?:DISCARD|RESET)\s+ALL\b").unwrap());

/// `set_config(` and where its first argument starts.
static SET_CONFIG: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?is)\bset_config\s*\(\s*").unwrap());

/// A DO block or a function or procedure definition: SQL in a string, which
/// has its own quoting and escapes.
static BODY: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?is)(?:^|;)\s*(?:DO|CREATE\s+(?:OR\s+REPLACE\s+)?(?:FUNCTION|PROCEDURE))\b")
        .unwrap()
});

/// Parameters that change how Postgres splits a query into strings and
/// comments. `lex` reads queries as Postgres does only with
/// `standard_conforming_strings` on and the UTF8 client encoding: in SJIS, for
/// one, the second byte of a character can be a backslash.
const LEXER_PARAMETERS: [&str; 2] = ["standard_conforming_strings", "client_encoding"];

/// `SET client_encoding` to UTF8, on its own: drivers send it, and it keeps
/// what `lex` relies on.
static SETS_UTF8: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?is)^\s*SET\s+(?:SESSION\s+)?(?:client_encoding\s*(?:TO|=)|NAMES)\s*'?(?:UTF-?8|UNICODE)'?\s*;?\s*$",
    )
    .unwrap()
});

/// Tenant values: identifiers such as org or project ids.
static TENANT: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[A-Za-z0-9._:@-]{1,256}$").unwrap());

/// Names pgcat accepts for the tenant parameter: a custom parameter, `prefix.name`.
static TENANT_PARAMETER: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$").unwrap());

/// Key of the fingerprints, random per process, so a fingerprint seen in a log
/// can't be checked against guessed tokens.
static FINGERPRINT_KEY: Lazy<[u8; 32]> = Lazy::new(rand::random);

/// An opaque caller identity token.
#[derive(Clone, PartialEq, Eq)]
pub struct IdentityToken(String);

impl IdentityToken {
    pub fn new(token: String) -> IdentityToken {
        IdentityToken(token)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// First 12 hex characters of the token's HMAC-SHA256 under a per-process key,
    /// safe to show and log. The same token gets the same fingerprint until pgcat restarts.
    pub fn fingerprint(&self) -> String {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&*FINGERPRINT_KEY).expect("any key length works");
        mac.update(self.0.as_bytes());
        encode_hex(&mac.finalize().into_bytes()[..6])
    }
}

impl std::fmt::Debug for IdentityToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IdentityToken({})", self.fingerprint())
    }
}

impl std::fmt::Display for IdentityToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "identity {}", self.fingerprint())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum IdentityCommand {
    Set(IdentityToken),
    Reset,
    Show,
}

impl IdentityCommand {
    /// Parse a query that might be an identity command.
    ///
    /// Returns an error for queries that mention an identity command but aren't
    /// exactly one, e.g. `SET PGCAT_IDENTITY TO 'x'; SELECT 1`: those can't be
    /// forwarded to the server without leaking the token.
    pub fn parse(query: &str) -> Result<Option<IdentityCommand>, String> {
        // Cheap check first, this runs for every query.
        if !mentions_identity(query.as_bytes()) {
            return Ok(None);
        }

        // Comments and quoted identifiers don't hide a command from us:
        // `SET /* x */ "pgcat_identity" TO '...'` is one too.
        let (query, unicode_escapes) = lex(query, false);
        let query = &query;

        if let Some(captures) = SET_IDENTITY.captures(query) {
            let token = captures[1].replace("''", "'");
            return Ok(Some(IdentityCommand::Set(IdentityToken::new(token))));
        }

        if RESET_IDENTITY.is_match(query) {
            return Ok(Some(IdentityCommand::Reset));
        }

        if SHOW_IDENTITY.is_match(query) {
            return Ok(Some(IdentityCommand::Show));
        }

        // It doesn't set the identity, and would send the token to the server,
        // which logs the statement when it fails, as it does: pgcat_identity
        // isn't a server setting.
        if sets_identity_config(query, unicode_escapes) {
            return Err("PGCAT_IDENTITY can't be set with set_config(): \
                 send SET PGCAT_IDENTITY TO '<token>' on its own"
                .into());
        }

        // Ignoring string literals, so values that mention the command are fine.
        let query = &normalize(query, true);
        if MENTIONS_IDENTITY.is_match(query) {
            return Err("PGCAT_IDENTITY commands must be sent on their own, as \
                 SET PGCAT_IDENTITY TO '<token>', RESET PGCAT_IDENTITY or SHOW PGCAT_IDENTITY"
                .into());
        }

        if UNICODE_SETTING.is_match(query) {
            return Err("settings can't be named with Unicode escapes (U&\"...\"): \
                 they could spell PGCAT_IDENTITY"
                .into());
        }

        Ok(None)
    }

    /// CommandComplete tag for this command.
    pub fn tag(&self) -> &'static str {
        match self {
            IdentityCommand::Set(_) => "SET",
            IdentityCommand::Reset => "RESET",
            IdentityCommand::Show => "SHOW",
        }
    }
}

/// Whether `query`, as `lex` returns it, calls `set_config()` with
/// `pgcat_identity` (in any case, padded with whitespace or not), or might: with
/// a name that isn't a plain string literal, in a query that mentions
/// `pgcat_identity` or has Unicode escapes. String literals are searched too,
/// so a call in a `DO` block counts, and so does a value that reads like one.
/// `lex` reads literals as Postgres does with `standard_conforming_strings` on:
/// on a pool without `identity_tenant_parameter` a client can turn it off, and a
/// name spelled with backslash escapes is then skipped.
fn sets_identity_config(query: &str, unicode_escapes: bool) -> bool {
    SET_CONFIG
        .find_iter(query)
        .any(|call| match string_literal(&query[call.end()..]) {
            Some((name, rest)) if rest.trim_start().starts_with(',') => {
                name.trim().eq_ignore_ascii_case("pgcat_identity")
            }
            _ => unicode_escapes || contains_ignore_case(query.as_bytes(), b"pgcat_identity"),
        })
}

/// `DISCARD ALL` and `RESET ALL` reset the session, including the identity,
/// also when they're one of several statements in the query.
pub fn clears_identity(query: &str) -> bool {
    may_reset(query.as_bytes()) && CLEARS_IDENTITY.is_match(&normalize(query, true))
}

/// Quick test on a query, without copying it: it may be an identity command or
/// reset the session. Most queries are neither.
pub fn might_concern_identity(query: &[u8]) -> bool {
    mentions_identity(query) || may_reset(query)
}

/// Mentions `pgcat_identity`, or might: Unicode escapes can spell it.
fn mentions_identity(query: &[u8]) -> bool {
    contains_ignore_case(query, b"pgcat_identity") || contains_ignore_case(query, b"u&")
}

/// Mentions DISCARD or RESET.
fn may_reset(query: &[u8]) -> bool {
    contains_ignore_case(query, b"reset") || contains_ignore_case(query, b"discard")
}

fn contains_ignore_case(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

/// `word` is in `haystack` as a whole word, as Postgres would read a keyword.
fn contains_word_ignore_case(haystack: &[u8], word: &[u8]) -> bool {
    haystack
        .windows(word.len())
        .enumerate()
        .any(|(at, window)| {
            window.eq_ignore_ascii_case(word)
                && starts_token(haystack, at)
                && !haystack
                    .get(at + word.len())
                    .is_some_and(|&byte| identifier_byte(byte))
        })
}

/// Whether `name` can be the tenant parameter: a custom parameter
/// (`prefix.name`) in lowercase identifier characters, so pgcat can use it in
/// SET and RESET as is.
pub fn valid_tenant_parameter(name: &str) -> bool {
    TENANT_PARAMETER.is_match(name)
}

/// Whether pgcat accepts this tenant from a plugin. Tenants are identifiers,
/// e.g. org or project ids: anything else is refused rather than escaped.
pub fn valid_tenant(tenant: &str) -> bool {
    TENANT.is_match(tenant)
}

/// Refuses the client statements that would set the tenant parameter, or that
/// pgcat couldn't check for that. Built once per pool.
#[derive(Clone, Debug)]
pub struct TenantGuard {
    parameter: String,

    /// `SET` or `RESET` of the parameter or of a `LEXER_PARAMETERS` one, and
    /// `SET NAMES`, which sets `client_encoding`.
    sets: Regex,
}

impl TenantGuard {
    pub fn new(parameter: &str) -> Result<TenantGuard, Error> {
        // `prefix . name` is the same parameter.
        let name = parameter
            .split('.')
            .map(regex::escape)
            .collect::<Vec<_>>()
            .join(r"\s*\.\s*");

        // `NAMES` followed by `=` is a column: `UPDATE t SET names = ...`.
        let sets = Regex::new(&format!(
            r"(?is)\b(?:SET|RESET)\s+(?:(?:SESSION|LOCAL)\s+)?(?:{}|{})\b|\bSET\s+(?:(?:SESSION|LOCAL)\s+)?NAMES\b\s*[^=\s]",
            name,
            LEXER_PARAMETERS.join("|"),
        ))
        .map_err(|_| Error::BadConfig)?;

        Ok(TenantGuard {
            parameter: parameter.to_string(),
            sets,
        })
    }

    /// The tenant parameter, e.g. `app.tenant`.
    pub fn parameter(&self) -> &str {
        &self.parameter
    }

    /// Quick test on a query, without copying it: it may set the tenant
    /// parameter. That takes naming both parts of it, `set_config`, `EXECUTE`,
    /// or a Unicode escape, which can spell any name.
    pub fn might_set_tenant(&self, query: &[u8]) -> bool {
        self.parameter
            .split('.')
            .all(|part| contains_ignore_case(query, part.as_bytes()))
            || contains_ignore_case(query, b"set_config")
            || contains_ignore_case(query, b"execute")
            || contains_ignore_case(query, b"u&")
    }

    /// Quick test on a query, without copying it: `refusal` may find one.
    pub fn might_refuse(&self, query: &[u8]) -> bool {
        self.might_set_tenant(query)
            || LEXER_PARAMETERS
                .iter()
                .any(|name| contains_ignore_case(query, name.as_bytes()))
            || [&b"names"[..], b"do", b"function", b"procedure"]
                .iter()
                .any(|word| contains_word_ignore_case(query, word))
    }

    /// Why the query is refused, if it is: it sets or resets the tenant
    /// parameter or a `LEXER_PARAMETERS` one, or might. That's `SET`,
    /// `SET SESSION`, `SET LOCAL` and `RESET` of it, also inside `ALTER ... SET`
    /// or a function's `SET` clause, and `set_config()` calls that name it or
    /// whose name isn't a plain string literal. `SET client_encoding` to UTF8 on
    /// its own is fine.
    ///
    /// String literals are searched too, so a value that mentions such a
    /// statement is refused as well. pgcat doesn't try to read what it can't
    /// read as Postgres would: Unicode escapes, which can spell any name, and
    /// the bodies of `DO` blocks and function or procedure definitions, which
    /// are SQL with their own quoting and escapes in a string. Functions that
    /// already exist and set the parameter can't be checked.
    pub fn refusal(&self, query: &str) -> Option<&'static str> {
        if !self.might_refuse(query.as_bytes()) {
            return None;
        }

        let (query, unicode_escapes) = lex(query, false);

        if unicode_escapes {
            return Some("Unicode escapes (U&'...', U&\"...\") can't be used: they could name it");
        }

        if BODY.is_match(&query) {
            return Some(
                "DO blocks and function or procedure definitions can't be used: \
                 pgcat can't check their bodies",
            );
        }

        let sets = (self.sets.is_match(&query) && !SETS_UTF8.is_match(&query))
            || SET_CONFIG.find_iter(&query).any(|call| {
                match string_literal(&query[call.end()..]) {
                    // Only `set_config('<name>', ...)` can be told apart.
                    Some((name, rest)) if rest.trim_start().starts_with(',') => {
                        let name = name.trim();
                        name.eq_ignore_ascii_case(&self.parameter)
                            || LEXER_PARAMETERS
                                .iter()
                                .any(|parameter| name.eq_ignore_ascii_case(parameter))
                    }
                    _ => true,
                }
            });

        sets.then_some(
            "clients can't set it, standard_conforming_strings, \
             or client_encoding other than to UTF8",
        )
    }
}

/// Why a `TenantGuard` can't check the queries of a client with these session
/// parameters, if it can't: see `LEXER_PARAMETERS`.
pub fn unreadable_session(parameters: &ServerParameters) -> Option<String> {
    let value = |name| {
        parameters
            .get(name)
            .unwrap_or_default()
            .trim_matches('\'')
            .to_ascii_lowercase()
    };

    // Postgres ignores case and punctuation in encoding names: `utf-8` is UTF8.
    let encoding: String = value("client_encoding")
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect();

    let readable =
        value("standard_conforming_strings") == "on" && matches!(&*encoding, "utf8" | "unicode");

    (!readable).then(|| {
        "pools with identity_tenant_parameter need standard_conforming_strings on and \
         client_encoding UTF8"
            .into()
    })
}

/// A plain `'...'` literal at the start of `text`, and what follows it.
fn string_literal(text: &str) -> Option<(String, &str)> {
    let mut chars = text.char_indices();
    if !matches!(chars.next(), Some((_, '\''))) {
        return None;
    }

    let mut value = String::new();
    let mut quote = false;
    for (index, c) in chars {
        match (quote, c) {
            (true, '\'') => {
                value.push('\'');
                quote = false;
            }
            (true, _) => return Some((value, &text[index..])),
            (false, '\'') => quote = true,
            (false, c) => value.push(c),
        }
    }

    quote.then_some((value, ""))
}

/// What a plugin keeps from an identity token it verified: any type that
/// compares with `==`. After a reload changes what verifies identities, a
/// client keeps its identity only if its token, verified again, gives the
/// same tenant and equal claims.
pub trait IdentityClaims: Any + Send + Sync {
    /// The claims, to downcast to their type.
    fn as_any(&self) -> &dyn Any;

    /// Whether `other` are the same claims: of the same type, and equal.
    fn same_claims(&self, other: &dyn IdentityClaims) -> bool;
}

impl<T: Any + Send + Sync + PartialEq> IdentityClaims for T {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn same_claims(&self, other: &dyn IdentityClaims) -> bool {
        other.as_any().downcast_ref::<T>() == Some(self)
    }
}

/// What a plugin verified about an identity token.
#[derive(Clone, Default)]
pub struct VerifiedIdentity {
    /// The tenant the token names. pgcat sets the tenant parameter to it.
    pub tenant: Option<String>,

    /// When the token stops being valid. pgcat forgets the identity then.
    pub expires_at: Option<SystemTime>,

    /// What the plugin wants to keep from the token, opaque to pgcat but
    /// for `==` (see [`IdentityClaims`]).
    pub claims: Option<Arc<dyn IdentityClaims>>,
}

impl VerifiedIdentity {
    /// Whether `other` verified the same identity: the same tenant and equal
    /// claims. When it expires isn't compared (see [`Identity::recheck`]).
    fn same_identity(&self, other: &VerifiedIdentity) -> bool {
        self.tenant == other.tenant
            && match (&self.claims, &other.claims) {
                (None, None) => true,
                (Some(claims), Some(other)) => claims.same_claims(other.as_ref()),
                _ => false,
            }
    }
}

/// What becomes of a client's identity after a reload changed what verifies
/// identities on its pool, once its token is verified again.
#[derive(Debug, PartialEq)]
pub enum Recheck {
    /// The same tenant and claims: the client keeps the identity, as the
    /// verifier verified it now, its expiry included.
    Keep,
    /// The verifier timed out or got no hook slot (`PH001`, `PH002`): the
    /// statement fails with this error, and the identity is verified again
    /// at the next one.
    Unavailable(String),
    /// Refused, another tenant or other claims, or nothing verifies
    /// identities any more: the client must connect again.
    End,
}

impl std::fmt::Debug for VerifiedIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedIdentity")
            .field("tenant", &self.tenant.is_some())
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// The identity attached to a client connection: the token as the client sent
/// it, and what a plugin verified about it, if any plugin does.
#[derive(Clone)]
pub struct Identity {
    token: IdentityToken,
    verified: VerifiedIdentity,
}

impl Identity {
    pub fn new(token: IdentityToken, verified: VerifiedIdentity) -> Identity {
        Identity { token, verified }
    }

    pub fn token(&self) -> &IdentityToken {
        &self.token
    }

    /// The tenant the verified token names.
    pub fn tenant(&self) -> Option<&str> {
        self.verified.tenant.as_deref()
    }

    /// What the verifying plugin kept from the token.
    pub fn claims<T: 'static>(&self) -> Option<&T> {
        self.verified.claims.as_deref()?.as_any().downcast_ref()
    }

    /// What becomes of the identity, given its token `verified` again after
    /// a reload: `None` if nothing verifies identities now. A kept identity
    /// takes the new verification, which names the same tenant and claims, so
    /// it expires when the verifier says now, as a new `SET` of the token
    /// would.
    pub fn recheck(&mut self, verified: Option<Result<VerifiedIdentity, String>>) -> Recheck {
        match verified {
            Some(Ok(verified)) if self.verified.same_identity(&verified) => {
                self.verified = verified;
                Recheck::Keep
            }
            Some(Err(error))
                if matches!(
                    crate::messages::sqlstate_of(&error).0,
                    Some(crate::plugins::HOOK_TIMED_OUT | crate::plugins::HOOK_BUSY)
                ) =>
            {
                Recheck::Unavailable(error)
            }
            _ => Recheck::End,
        }
    }

    /// The token has expired.
    pub fn expired(&self, now: SystemTime) -> bool {
        self.verified
            .expires_at
            .map_or(false, |expires_at| now >= expires_at)
    }
}

impl std::fmt::Debug for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Identity")
            .field("token", &self.token)
            .field("verified", &self.verified)
            .finish()
    }
}

/// The query with comments replaced by spaces and quoted identifiers unquoted.
/// With `strip_literals`, string literals are emptied too.
fn normalize(query: &str, strip_literals: bool) -> String {
    lex(query, strip_literals).0
}

/// `normalize`, and whether a string or identifier in it has Unicode escapes:
/// `U&` right before its quote, starting a token.
///
/// It walks bytes: every byte that starts a comment or literal is ASCII, and
/// never part of a multibyte UTF-8 character.
fn lex(query: &str, strip_literals: bool) -> (String, bool) {
    let bytes = query.as_bytes();
    let mut out = String::with_capacity(query.len());
    let mut unicode_escapes = false;
    let mut i = 0;

    let prefixed = |quote: usize, prefix: &[u8]| {
        quote >= prefix.len()
            && bytes[quote - prefix.len()..quote].eq_ignore_ascii_case(prefix)
            && starts_token(bytes, quote - prefix.len())
    };

    while i < bytes.len() {
        let start = i;

        match bytes[i] {
            // -- comment
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i = line_comment_end(bytes, i);
                out.push(' ');
            }

            // /* comment */
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i = block_comment_end(bytes, i);
                out.push(' ');
            }

            // 'string', '' escapes a quote; E'string' also has backslash escapes
            b'\'' => {
                unicode_escapes |= prefixed(i, b"u&");
                let escapes = prefixed(i, b"e");
                i += 1;
                while i < bytes.len() {
                    let escaped = escapes && bytes[i] == b'\\';
                    let doubled = bytes[i] == b'\'' && bytes.get(i + 1) == Some(&b'\'');
                    if escaped || doubled {
                        i += 2;
                    } else if bytes[i] == b'\'' {
                        i += 1;
                        match string_continues(bytes, i) {
                            Some(quote) if escapes => i = quote + 1,
                            _ => break,
                        }
                    } else {
                        i += 1;
                    }
                }
                i = i.min(bytes.len());
                match strip_literals {
                    true => out.push_str("''"),
                    false => out.push_str(&query[start..i]),
                }
            }

            // "identifier", "" escapes a quote
            b'"' => {
                unicode_escapes |= prefixed(i, b"u&");
                loop {
                    i += 1;
                    let end = bytes[i..]
                        .iter()
                        .position(|&byte| byte == b'"')
                        .map_or(bytes.len(), |end| i + end);
                    out.push_str(&query[i..end]);
                    i = end + 1;
                    if bytes.get(i) != Some(&b'"') {
                        break;
                    }
                    out.push('"');
                }
            }

            // $tag$ string $tag$
            b'$' => match dollar_quote_end(bytes, i) {
                Some(end) => {
                    i = end;
                    match strip_literals {
                        true => out.push_str("''"),
                        false => out.push_str(&query[start..i]),
                    }
                }
                None => {
                    i += 1;
                    out.push('$');
                }
            },

            // Anything else, up to the next byte that may start a comment or literal.
            _ => {
                i += 1;
                while i < bytes.len() && !matches!(bytes[i], b'-' | b'/' | b'\'' | b'"' | b'$') {
                    i += 1;
                }
                out.push_str(&query[start..i]);
            }
        }
    }

    (out, unicode_escapes)
}

/// Postgres reads letters, digits, `_`, `$` and every non-ASCII byte as part
/// of an identifier or keyword.
fn identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$' || !byte.is_ascii()
}

/// A token starts at `at`. `E'`, `U&'` and `$$` open a literal only there:
/// `name'\'` is a type name and a plain string, `a$$` an identifier.
fn starts_token(bytes: &[u8], at: usize) -> bool {
    at == 0 || !identifier_byte(bytes[at - 1])
}

/// Where the `--` comment at `start` ends: at a newline, `\n` or `\r`.
fn line_comment_end(bytes: &[u8], start: usize) -> usize {
    bytes[start..]
        .iter()
        .position(|&byte| byte == b'\n' || byte == b'\r')
        .map_or(bytes.len(), |end| start + end)
}

/// Where the `/* */` comment at `start` ends. They nest in Postgres.
fn block_comment_end(bytes: &[u8], start: usize) -> usize {
    let mut depth = 0;
    let mut i = start;
    while i < bytes.len() {
        match (bytes[i], bytes.get(i + 1)) {
            (b'/', Some(b'*')) => {
                depth += 1;
                i += 2;
            }
            (b'*', Some(b'/')) => {
                depth -= 1;
                i += 2;
                if depth == 0 {
                    return i;
                }
            }
            _ => i += 1,
        }
    }
    bytes.len()
}

/// The quote that goes on with the string closed just before `start`, if one
/// does. Postgres joins `E'a'` and a `'...'` after it, across whitespace with a
/// newline and `--` comments, into one string that keeps backslash escapes.
/// Without a newline, two strings in a row are a syntax error, so reading them
/// as one is right whenever the query runs.
fn string_continues(bytes: &[u8], start: usize) -> Option<usize> {
    let mut i = start;
    loop {
        match *bytes.get(i)? {
            b' ' | b'\t' | b'\n' | b'\r' | b'\x0b' | b'\x0c' => i += 1,
            b'-' if bytes.get(i + 1) == Some(&b'-') => i = line_comment_end(bytes, i),
            b'/' if bytes.get(i + 1) == Some(&b'*') => i = block_comment_end(bytes, i),
            b'\'' => return Some(i),
            _ => return None,
        }
    }
}

/// Where the `$tag$...$tag$` string at `start` ends, if one starts there.
fn dollar_quote_end(bytes: &[u8], start: usize) -> Option<usize> {
    if !starts_token(bytes, start) {
        return None;
    }

    let tag_len = bytes[start + 1..]
        .iter()
        .position(|&byte| byte == b'$' || !identifier_byte(byte))?
        + 2;
    let tag = &bytes[start..start + tag_len];

    // A tag can't start with a digit: `$1` is a parameter.
    if tag[tag_len - 1] != b'$' || tag[1].is_ascii_digit() {
        return None;
    }

    let body = start + tag_len;
    let end = bytes[body..]
        .windows(tag_len)
        .position(|window| window == tag)
        .map_or(bytes.len(), |end| body + end + tag_len);
    Some(end)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(token: &str) -> Option<IdentityCommand> {
        Some(IdentityCommand::Set(IdentityToken::new(token.into())))
    }

    #[test]
    fn test_parse_identity_commands() {
        assert_eq!(
            IdentityCommand::parse("SET PGCAT_IDENTITY TO 'abc'").unwrap(),
            set("abc")
        );
        assert_eq!(
            IdentityCommand::parse("  set session pgcat_identity = 'a''b c';  ").unwrap(),
            set("a'b c")
        );
        assert_eq!(
            IdentityCommand::parse("SET PGCAT_IDENTITY TO ''").unwrap(),
            set("")
        );
        assert_eq!(
            IdentityCommand::parse("reset PGCAT_IDENTITY;").unwrap(),
            Some(IdentityCommand::Reset)
        );
        assert_eq!(
            IdentityCommand::parse("SHOW pgcat_identity").unwrap(),
            Some(IdentityCommand::Show)
        );

        // Not identity commands at all.
        assert_eq!(IdentityCommand::parse("SELECT 1").unwrap(), None);
        assert_eq!(
            IdentityCommand::parse("SELECT pgcat_identity FROM t").unwrap(),
            None
        );

        // Comments and quoted identifiers.
        assert_eq!(
            IdentityCommand::parse("SET/**/PGCAT_IDENTITY TO 'abc'").unwrap(),
            set("abc")
        );
        assert_eq!(
            IdentityCommand::parse("SET\n-- comment\nPGCAT_IDENTITY TO 'a--b /* c */'").unwrap(),
            set("a--b /* c */")
        );
        assert_eq!(
            IdentityCommand::parse("SET \"pgcat_identity\" TO 'abc'").unwrap(),
            set("abc")
        );
        assert_eq!(
            IdentityCommand::parse("SET /* a /* nested */ comment */ \"PGCAT_IDENTITY\" = 'x'")
                .unwrap(),
            set("x")
        );

        // Values that mention the command are fine.
        assert_eq!(
            IdentityCommand::parse("INSERT INTO docs VALUES ('SET pgcat_identity first')").unwrap(),
            None
        );
        assert_eq!(
            IdentityCommand::parse("SELECT $$RESET PGCAT_IDENTITY$$").unwrap(),
            None
        );

        // Identity commands mixed with other SQL, or in a form we don't support,
        // are refused so the token never reaches the server.
        assert!(IdentityCommand::parse("SET/**/PGCAT_IDENTITY TO 'abc'; SELECT 1").is_err());
        assert!(IdentityCommand::parse("SELECT 1; SET \"pgcat_identity\" TO 'abc'").is_err());
        assert!(IdentityCommand::parse("SET PGCAT_IDENTITY TO 'abc'; SELECT 1").is_err());
        assert!(IdentityCommand::parse("SELECT 1; SET PGCAT_IDENTITY TO 'abc'").is_err());
        assert!(IdentityCommand::parse("SET LOCAL PGCAT_IDENTITY TO 'abc'").is_err());
        assert!(IdentityCommand::parse("SET PGCAT_IDENTITY TO abc").is_err());
        // Nor hidden from pgcat in what it would read as a comment.
        assert!(
            IdentityCommand::parse("SELECT name'\\', ' -- '; SET PGCAT_IDENTITY TO 'abc'").is_err()
        );
        assert!(IdentityCommand::parse("SELECT 1 -- a\rSET PGCAT_IDENTITY TO 'abc'").is_err());
        // Nor spelled with Unicode escapes, which pgcat doesn't decode.
        assert!(IdentityCommand::parse("SET U&\"pgcat_identity\" TO 'abc'").is_err());
        assert!(IdentityCommand::parse("set session u&\"pgcat\\005fidentity\" = 'abc'").is_err());
        assert!(IdentityCommand::parse("SELECT 1; RESET U&\"x\"").is_err());
        assert_eq!(IdentityCommand::parse("SELECT U&'x', colu&1"), Ok(None));
    }

    #[test]
    fn test_set_config_of_the_identity_is_refused() {
        for query in [
            "SELECT set_config('pgcat_identity', 'abc', false)",
            "select SET_CONFIG( 'PGCAT_IDENTITY' , 'abc', true);",
            "SELECT set_config(' pgcat_identity\t', 'abc', false)",
            "SELECT \"set_config\"('Pgcat_Identity', 'abc', false)",
            "SELECT * FROM set_config('pgcat_identity', 'abc', false)",
            "SELECT pg_catalog.set_config('pgcat_identity', 'abc', false)",
            "SELECT pg_catalog . \"set_config\" /* c */ (\n'pgcat_identity', 'abc', false)",
            "SELECT 1; SELECT set_config('pgcat_identity', 'abc', false)",
            "SELECT set_config('app.x', 'y', false), set_config('pgcat_identity', 'abc', false)",
            // A name that isn't a plain literal, in a query naming the identity.
            "SELECT set_config(E'pgcat_identity', 'abc', false)",
            "SELECT set_config($$pgcat_identity$$, 'abc', false)",
            "SELECT set_config('pgcat_identity'::text, 'abc', false)",
            "SELECT set_config(setting_name => 'pgcat_identity', new_value => 'abc', is_local => false)",
            "SELECT set_config(name, 'abc', false) FROM t WHERE name = 'pgcat_identity'",
            "SELECT set_config(U&'pgcat\\005fidentity', 'abc', false)",
            // Calls in a DO block, dollar-quoted or not.
            "DO $$ BEGIN PERFORM set_config('pgcat_identity', 'abc', false); END $$",
            "DO 'BEGIN PERFORM set_config(''pgcat_identity'', ''abc'', false); END'",
        ] {
            assert_eq!(
                IdentityCommand::parse(query),
                Err("PGCAT_IDENTITY can't be set with set_config(): \
                     send SET PGCAT_IDENTITY TO '<token>' on its own"
                    .into()),
                "{}",
                query
            );
        }

        for query in [
            // Other settings, also with values that mention the identity.
            "SELECT set_config('app.note', 'pgcat_identity', false)",
            "SELECT set_config('pgcat.identity', 'abc', false)",
            "SELECT set_config('pgcat_identity_x', 'abc', false), 'pgcat_identity'",
            // Reading it, or naming it in a comment.
            "SELECT current_setting('pgcat_identity', true)",
            "SELECT set_config('app.x', 'y', false) -- pgcat_identity",
            "SELECT set_config($1, $2, false) /* pgcat_identity */",
        ] {
            assert_eq!(IdentityCommand::parse(query), Ok(None), "{}", query);
        }
    }

    #[test]
    fn test_might_concern_identity() {
        assert!(might_concern_identity(b"SET PGCAT_IDENTITY TO 'x'"));
        assert!(might_concern_identity(b"  discard all"));
        assert!(might_concern_identity(b"/* c */ RESET ALL"));
        assert!(might_concern_identity(
            b"SET U&\"pgcat\\005fidentity\" TO 'x'"
        ));
        assert!(!might_concern_identity(b"SELECT 1"));
        assert!(!might_concern_identity(b"INSERT INTO t VALUES (1)"));
    }

    #[test]
    fn test_clears_identity() {
        assert!(clears_identity("DISCARD ALL"));
        assert!(clears_identity(" reset all ; "));
        assert!(clears_identity("DISCARD /* everything */ ALL"));
        assert!(clears_identity("SELECT 1; RESET ALL"));
        assert!(clears_identity("-- reset\nDISCARD ALL"));
        assert!(!clears_identity("RESET search_path"));
        assert!(!clears_identity("DISCARD PLANS"));
        assert!(!clears_identity("SELECT 'x; reset all'"));
        assert!(!clears_identity("SELECT reset_all FROM t"));
    }

    #[test]
    fn test_tenant_values() {
        assert!(valid_tenant_parameter("app.tenant"));
        assert!(!valid_tenant_parameter("tenant"));
        assert!(!valid_tenant_parameter("app.Tenant"));
        assert!(!valid_tenant_parameter("app.tenant; DROP"));

        assert!(valid_tenant("00000000-0000-4000-8000-000000000001"));
        assert!(valid_tenant("project:example-42"));
        assert!(!valid_tenant(""));
        assert!(!valid_tenant("a'b"));
        assert!(!valid_tenant("a b"));
        assert!(!valid_tenant(&"a".repeat(257)));
    }

    #[test]
    fn test_tenant_guard_refuses() {
        let guard = TenantGuard::new("app.tenant").unwrap();
        let sets = |query: &str| guard.refusal(query).is_some();

        for query in [
            "SET app.tenant TO 'b'",
            "set app.tenant = 'b'",
            "SET SESSION app.tenant TO 'b'",
            "SET LOCAL app.tenant TO 'b'",
            "SET \"app\".\"tenant\" TO 'b'",
            "SET \"app.tenant\" TO 'b'",
            "SET app . tenant TO 'b'",
            "SET /* hi */ app.tenant TO 'b'",
            "SELECT 1; SET app.tenant TO 'b'",
            "RESET app.tenant",
            "ALTER ROLE app SET app.tenant = 'b'",
            "ALTER ROLE app IN DATABASE d RESET app.tenant",
            "CREATE FUNCTION f() RETURNS int LANGUAGE sql SET app.tenant = 'b' AS 'SELECT 1'",
            "DO $$ BEGIN SET app.tenant = 'b'; END $$",
            "SELECT set_config('app.tenant', 'b', false)",
            "SELECT pg_catalog.set_config('APP.TENANT', 'b', false)",
            "SELECT set_config( 'app.tenant' , 'b', true)",
            "SELECT \"set_config\"('app.tenant', 'b', false)",
            "DO $$ BEGIN PERFORM set_config('app.tenant', 'b', false); END $$",
            // Names we can't read are refused.
            "SELECT set_config('app.' || 'tenant', 'b', false)",
            "SELECT set_config(name, 'b', false) FROM settings",
            "SELECT set_config(E'app.tenant', 'b', false)",
            "SELECT set_config($1, $2, false)",
            // Unicode escapes can spell the name, or set_config, any way.
            "SET U&\"app.tenant\" TO 'b'",
            "set u&\"app\\002etenant\" to 'b'",
            "SET U&\"\\0061\\0070p\\002e\\0074enant\" TO 'b'",
            "SET U&\"app!002etenant\" UESCAPE '!' TO 'b'",
            "SET app.U&\"\\0074enant\" TO 'b'",
            "SELECT U&\"\\0073et_config\"('app.x', 'b', false)",
            "SELECT set_config(U&'app\\002etenant', 'b', false)",
            "SELECT 1; SET U&\"\\0061pp\" TO 'b'",
            // Dynamic SQL could build any of the above.
            "DO $$ BEGIN EXECUTE 'SELECT set_' || 'config(''x'', ''y'', false)'; END $$",
            "do language plpgsql $f$ begin execute format('SET %s TO 1', 'x'); end $f$",
            "CREATE OR REPLACE FUNCTION f(n text) RETURNS void LANGUAGE plpgsql AS $$ BEGIN EXECUTE n; END $$",
            // Bodies have their own quoting and escapes, so pgcat doesn't read them.
            "DO $$BEGIN SET \"app.tenant\" = 'b'; END$$; SELECT * FROM secrets",
            "CREATE FUNCTION pg_temp.f() RETURNS void LANGUAGE sql AS $$ SET \"app\".\"tenant\" = 'b'; SELECT 1 $$",
            "DO E'BEGIN SET app.\\x74enant = \\'b\\'; END'",
            "DO $$ BEGIN RAISE NOTICE 'hi'; END $$",
            "/* x */ do language plpgsql 'BEGIN NULL; END'",
            "CREATE OR REPLACE PROCEDURE p() LANGUAGE sql AS 'SELECT 1'",
            // Hidden behind what Postgres reads as a string, and pgcat mustn't
            // read as a comment.
            "SELECT name'\\', ' -- '; SET app.tenant TO 'b'",
            "SELECT \u{2192}e'\\', ' -- '; SET app.tenant TO 'b'",
            "SELECT 1 AS a$$, '$$ -- '; SET app.tenant TO 'b'",
            "SELECT 1 -- a\rSET app.tenant TO 'b'",
            "SELECT E'a'\n'\\' -- '; SET app.tenant TO 'b'",
            "SELECT E'a' -- c\n'\\' -- '; SET app.tenant TO 'b'",
            "SELECT E'\\' -- '; SET app.tenant TO 'b'",
            "SELECT (e'\\' -- '); SET app.tenant TO 'b'",
            // Changing how Postgres reads strings.
            "SET standard_conforming_strings = off",
            "SET SESSION standard_conforming_strings TO off",
            "ALTER ROLE app SET standard_conforming_strings = off",
            "SELECT set_config('standard_conforming_strings', 'off', false)",
            "SET client_encoding TO 'SJIS'",
            "RESET client_encoding",
            "SET NAMES 'SJIS'",
            "SET NAMES 'UTF8'; SET app.tenant TO 'b'",
            "SELECT set_config('client_encoding', 'UTF8', false)",
        ] {
            assert!(sets(query), "{}", query);
        }

        for query in [
            "SELECT current_setting('app.tenant', true)",
            "SELECT * FROM t WHERE tenant = current_setting('app.tenant')",
            "SET app.tenant_hint TO 'b'",
            "SET search_path TO public",
            "SELECT set_config('search_path', 'public', false)",
            "SELECT tenant_id FROM accounts",
            "CREATE POLICY p ON t USING (org = current_setting('app.tenant', true))",
            "EXECUTE my_statement(1)",
            "SELECT 'execute' FROM t",
            // `u&` that isn't a Unicode escape.
            "SELECT 'U&\"app.tenant\"' FROM t",
            "SELECT 1 -- U&\"app.tenant\"",
            "SELECT u&1, colu&'x' FROM t",
            "SELECT \u{2192}u&'x' FROM t",
            // Strings that only look like the above.
            "SELECT name'\\', current_setting('app.tenant')",
            "SELECT 1 AS a$$, '$$' FROM tenant_app",
            "SELECT 'a'\n'\\', current_setting('app.tenant')",
            // Drivers set UTF8, and NAMES can be a column.
            "SET client_encoding TO 'UTF8'",
            "set client_encoding = utf8",
            "SET NAMES 'utf-8'",
            "UPDATE people SET names = 'b'",
            "UPDATE t SET namesake = 1",
            "SHOW standard_conforming_strings",
            "INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING",
            "CREATE TRIGGER t AFTER INSERT ON a EXECUTE FUNCTION f()",
        ] {
            assert!(!sets(query), "{}", query);
        }
    }

    #[test]
    fn test_tenant_guard_refuses_reading_strings_differently() {
        // What a client would send first so that Postgres reads an ordinary
        // SELECT, e.g. `SELECT 'a\'';UPDATE ... --'`, as two statements.
        let guard = TenantGuard::new("app.tenant").unwrap();
        for query in [
            "SELECT set_config($$standard_conforming_strings$$, 'off', false)",
            "SELECT set_config($1, 'off', false)",
            "SELECT set_config('standard_conforming' || '_strings', 'off', true)",
            "SELECT set_config(E'standard_conforming_strings', 'off', true)",
            "SELECT set_config(U&'standard_conforming_strings', 'off', false)",
            "SELECT set_config(name, 'off', false) FROM pg_settings WHERE name LIKE 'standard_c%'",
            "ALTER ROLE CURRENT_USER SET standard_conforming_strings = off",
            "ALTER DATABASE app SET standard_conforming_strings TO off",
            "SET client_encoding = 'SJIS'",
            "SELECT set_config('client_encoding', 'SJIS', false)",
            // A body is read with its own quoting, whatever it holds.
            "CREATE FUNCTION f() RETURNS void LANGUAGE sql AS $$ UPDATE secrets SET secret = 'plain' WHERE id = 1 $$",
        ] {
            assert!(guard.refusal(query).is_some(), "{}", query);
        }
    }

    #[test]
    fn test_might_set_tenant() {
        let guard = TenantGuard::new("app.tenant").unwrap();
        assert!(guard.might_set_tenant(b"SET \"APP\" . tenant TO 'b'"));
        assert!(guard.might_set_tenant(b"SELECT set_config($1, $2, false)"));
        assert!(!guard.might_set_tenant(b"SELECT tenant_id FROM accounts"));
        assert!(!guard.might_set_tenant(b"SET client_encoding TO 'UTF8'"));

        // Keywords count as whole words only.
        assert!(guard.might_refuse(b"DO E'BEGIN SET app.\\x74enant = 1; END'"));
        assert!(guard.might_refuse(b"SET NAMES 'SJIS'"));
        assert!(!guard.might_refuse(b"SELECT usernames, hostnames, done FROM t"));
    }

    #[test]
    fn test_unreadable_session() {
        let session = |conforming: &str, encoding: &str| {
            let mut parameters = ServerParameters::new();
            parameters.set_param(
                "standard_conforming_strings".into(),
                conforming.into(),
                true,
            );
            parameters.set_param("client_encoding".into(), encoding.into(), true);
            unreadable_session(&parameters)
        };

        assert!(session("on", "UTF8").is_none());
        assert!(session("on", "utf-8").is_none());
        assert!(session("'on'", "'unicode'").is_none());
        assert!(session("off", "UTF8").is_some());
        assert!(session("on", "SJIS").is_some());
        assert!(session("on", "LATIN1").is_some());
    }

    #[test]
    fn test_token_is_redacted() {
        let token = IdentityToken::new("super-secret-token".into());
        let fingerprint = token.fingerprint();

        assert_eq!(fingerprint.len(), 12);
        assert_eq!(
            IdentityToken::new("super-secret-token".into()).fingerprint(),
            fingerprint
        );
        assert_ne!(
            IdentityToken::new("other-token".into()).fingerprint(),
            fingerprint
        );
        assert!(!format!("{:?}", token).contains("secret"));
        assert!(!format!("{}", token).contains("secret"));
        assert!(!format!("{:?}", IdentityCommand::Set(token.clone())).contains("secret"));
        assert!(format!("{:?}", token).contains(&fingerprint));
    }

    fn verified(tenant: &str, claims: Option<Arc<dyn IdentityClaims>>) -> VerifiedIdentity {
        VerifiedIdentity {
            tenant: Some(tenant.into()),
            expires_at: None,
            claims,
        }
    }

    #[test]
    fn test_recheck_keeps_only_the_same_identity() {
        #[derive(PartialEq)]
        struct Claims(&'static str);
        #[derive(PartialEq)]
        struct Other(&'static str);

        let token = || IdentityToken::new("token".into());
        let mut first = verified("acme", Some(Arc::new(Claims("a"))));
        first.expires_at = Some(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(100));
        let mut identity = Identity::new(token(), first);
        assert_eq!(
            identity.claims::<Claims>().map(|claims| claims.0),
            Some("a")
        );
        assert!(identity.claims::<Other>().is_none());

        // The same tenant and claims, whenever the token expires now.
        let same = verified("acme", Some(Arc::new(Claims("a"))));
        assert_eq!(identity.recheck(Some(Ok(same))), Recheck::Keep);

        // Another tenant, other claims, claims of another type or none.
        for other in [
            verified("globex", Some(Arc::new(Claims("a")))),
            verified("acme", Some(Arc::new(Claims("b")))),
            verified("acme", Some(Arc::new(Other("a")))),
            verified("acme", None),
            VerifiedIdentity::default(),
        ] {
            assert_eq!(identity.recheck(Some(Ok(other))), Recheck::End);
        }

        // Refused, with or without a SQLSTATE, or nothing verifies identities.
        assert_eq!(identity.recheck(Some(Err("refused".into()))), Recheck::End);
        let refused = crate::messages::with_sqlstate(crate::plugins::IDENTITY_REFUSED, "refused");
        assert_eq!(identity.recheck(Some(Err(refused))), Recheck::End);
        assert_eq!(identity.recheck(None), Recheck::End);

        // The verifier timed out, or got no hook slot.
        for code in [crate::plugins::HOOK_TIMED_OUT, crate::plugins::HOOK_BUSY] {
            let error = crate::messages::with_sqlstate(code, "a plugin hook took too long");
            assert_eq!(
                identity.recheck(Some(Err(error.clone()))),
                Recheck::Unavailable(error)
            );
        }

        // An identity without a tenant or claims, kept by a verifier that
        // names none, ends when nothing verifies identities.
        let mut bare = Identity::new(token(), VerifiedIdentity::default());
        assert_eq!(
            bare.recheck(Some(Ok(VerifiedIdentity::default()))),
            Recheck::Keep
        );
        assert_eq!(bare.recheck(None), Recheck::End);
    }
    #[test]
    fn test_a_kept_identity_expires_when_verified_again() {
        let at = |secs| SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(secs);
        let expiring = |expires_at| VerifiedIdentity {
            expires_at,
            ..verified("acme", None)
        };
        let kept = |first, now| {
            let mut identity = Identity::new(IdentityToken::new("token".into()), expiring(first));
            assert_eq!(identity.recheck(Some(Ok(expiring(now)))), Recheck::Keep);
            identity
        };

        // An earlier expiry now: it expires then.
        let identity = kept(Some(at(100)), Some(at(50)));
        assert!(!identity.expired(at(49)));
        assert!(identity.expired(at(50)));

        // None before, one now: it expires then.
        let identity = kept(None, Some(at(50)));
        assert!(!identity.expired(at(49)));
        assert!(identity.expired(at(50)));

        // One already past: expired at once.
        let identity = kept(Some(at(u32::MAX as u64)), Some(at(1)));
        assert!(identity.expired(SystemTime::now()));

        // A later one: usable past the old expiry, until the new one.
        let identity = kept(Some(at(100)), Some(at(150)));
        assert!(!identity.expired(at(100)));
        assert!(!identity.expired(at(149)));
        assert!(identity.expired(at(150)));

        // None now: it doesn't expire.
        assert!(!kept(Some(at(100)), None).expired(at(u32::MAX as u64)));
    }
}
