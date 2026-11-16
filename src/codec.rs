//! Conversion between the text and binary wire formats of a few Postgres types.
//!
//! Only the types pgcat itself produces or has to look at are supported;
//! anything else is an error, never a guess.

use crate::errors::Error;

pub const BOOL_OID: i32 = 16;
pub const BYTEA_OID: i32 = 17;
pub const NAME_OID: i32 = 19;
pub const INT8_OID: i32 = 20;
pub const INT2_OID: i32 = 21;
pub const INT4_OID: i32 = 23;
pub const TEXT_OID: i32 = 25;
pub const OID_OID: i32 = 26;
pub const JSON_OID: i32 = 114;
pub const UNKNOWN_OID: i32 = 705;
pub const BPCHAR_OID: i32 = 1042;
pub const VARCHAR_OID: i32 = 1043;
pub const UUID_OID: i32 = 2950;
pub const JSONB_OID: i32 = 3802;

/// Version byte of the jsonb binary format.
const JSONB_VERSION: u8 = 1;

fn unsupported(type_oid: i32) -> Error {
    Error::PluginError(format!(
        "binary format of type {} is not supported by pgcat",
        type_oid
    ))
}

fn invalid(type_oid: i32) -> Error {
    Error::PluginError(format!("invalid value for type {}", type_oid))
}

/// The binary form of a value given in text form.
pub fn text_to_binary(type_oid: i32, text: &[u8]) -> Result<Vec<u8>, Error> {
    let as_str = || std::str::from_utf8(text).map_err(|_| invalid(type_oid));

    Ok(match type_oid {
        TEXT_OID | VARCHAR_OID | BPCHAR_OID | NAME_OID | UNKNOWN_OID | JSON_OID => text.to_vec(),

        JSONB_OID => {
            let mut binary = Vec::with_capacity(text.len() + 1);
            binary.push(JSONB_VERSION);
            binary.extend_from_slice(text);
            binary
        }

        BOOL_OID => match as_str()? {
            "t" | "true" | "on" | "yes" | "1" => vec![1],
            "f" | "false" | "off" | "no" | "0" => vec![0],
            _ => return Err(invalid(type_oid)),
        },

        INT2_OID => as_str()?
            .trim()
            .parse::<i16>()
            .map_err(|_| invalid(type_oid))?
            .to_be_bytes()
            .to_vec(),

        INT4_OID => as_str()?
            .trim()
            .parse::<i32>()
            .map_err(|_| invalid(type_oid))?
            .to_be_bytes()
            .to_vec(),

        INT8_OID => as_str()?
            .trim()
            .parse::<i64>()
            .map_err(|_| invalid(type_oid))?
            .to_be_bytes()
            .to_vec(),

        OID_OID => as_str()?
            .trim()
            .parse::<u32>()
            .map_err(|_| invalid(type_oid))?
            .to_be_bytes()
            .to_vec(),

        BYTEA_OID => {
            let hex = as_str()?
                .strip_prefix("\\x")
                .ok_or_else(|| invalid(type_oid))?;
            decode_hex(hex).ok_or_else(|| invalid(type_oid))?
        }

        UUID_OID => {
            // Postgres also accepts braces, upper case and hyphens between any
            // groups of four digits.
            let text = as_str()?.trim();
            let text = match (text.strip_prefix('{'), text.ends_with('}')) {
                (Some(inner), true) => &inner[..inner.len() - 1],
                (None, false) => text,
                _ => return Err(invalid(type_oid)),
            };
            let hex: String = text.chars().filter(|c| *c != '-').collect();
            let bytes = decode_hex(&hex).ok_or_else(|| invalid(type_oid))?;
            if bytes.len() != 16 {
                return Err(invalid(type_oid));
            }
            bytes
        }

        _ => return Err(unsupported(type_oid)),
    })
}

/// The text form of a value given in binary form.
pub fn binary_to_text(type_oid: i32, binary: &[u8]) -> Result<Vec<u8>, Error> {
    let fixed = |len: usize| {
        if binary.len() == len {
            Ok(binary)
        } else {
            Err(invalid(type_oid))
        }
    };

    Ok(match type_oid {
        TEXT_OID | VARCHAR_OID | BPCHAR_OID | NAME_OID | UNKNOWN_OID | JSON_OID => binary.to_vec(),

        JSONB_OID => match binary.split_first() {
            Some((&JSONB_VERSION, text)) => text.to_vec(),
            _ => return Err(invalid(type_oid)),
        },

        BOOL_OID => match fixed(1)?[0] {
            0 => b"f".to_vec(),
            _ => b"t".to_vec(),
        },

        INT2_OID => i16::from_be_bytes(fixed(2)?.try_into().unwrap())
            .to_string()
            .into_bytes(),

        INT4_OID => i32::from_be_bytes(fixed(4)?.try_into().unwrap())
            .to_string()
            .into_bytes(),

        INT8_OID => i64::from_be_bytes(fixed(8)?.try_into().unwrap())
            .to_string()
            .into_bytes(),

        OID_OID => u32::from_be_bytes(fixed(4)?.try_into().unwrap())
            .to_string()
            .into_bytes(),

        BYTEA_OID => format!("\\x{}", encode_hex(binary)).into_bytes(),

        UUID_OID => {
            let hex = encode_hex(fixed(16)?);
            format!(
                "{}-{}-{}-{}-{}",
                &hex[0..8],
                &hex[8..12],
                &hex[12..16],
                &hex[16..20],
                &hex[20..32]
            )
            .into_bytes()
        }

        _ => return Err(unsupported(type_oid)),
    })
}

/// Convert a value between formats (0 text, 1 binary).
pub fn convert(type_oid: i32, value: &[u8], from: i16, to: i16) -> Result<Vec<u8>, Error> {
    match (from, to) {
        (0, 1) => text_to_binary(type_oid, value),
        (1, 0) => binary_to_text(type_oid, value),
        _ => Ok(value.to_vec()),
    }
}

pub fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;

    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut hex, byte| {
            let _ = write!(hex, "{:02x}", byte);
            hex
        })
}

pub fn decode_hex(hex: &str) -> Option<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return None;
    }

    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect()
}

/// A bytea value from its text form: hex (`\\x0a1b`, whitespace between the
/// pairs allowed) or the escape format (`\\\\` and `\\ooo` octal escapes).
pub fn decode_bytea_text(text: &str) -> Option<Vec<u8>> {
    if let Some(hex) = text.strip_prefix("\\x") {
        let digits: String = hex.chars().filter(|c| !c.is_ascii_whitespace()).collect();
        return decode_hex(&digits);
    }

    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' if bytes.get(i + 1) == Some(&b'\\') => {
                out.push(b'\\');
                i += 2;
            }
            b'\\' => {
                let octal = std::str::from_utf8(bytes.get(i + 1..i + 4)?).ok()?;
                if !octal.bytes().all(|b| (b'0'..=b'7').contains(&b)) {
                    return None;
                }
                out.push(u8::from_str_radix(octal, 8).ok()?);
                i += 4;
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_bytea_text() {
        assert_eq!(decode_bytea_text("\\x0a1B"), Some(vec![0x0a, 0x1b]));
        assert_eq!(decode_bytea_text("\\x0a 1b"), Some(vec![0x0a, 0x1b]));
        assert_eq!(decode_bytea_text("\\x"), Some(vec![]));
        assert_eq!(decode_bytea_text("\\x0"), None);
        assert_eq!(
            decode_bytea_text("ab\\\\c\\001"),
            Some(b"ab\\c\x01".to_vec())
        );
        assert_eq!(decode_bytea_text("a\\9"), None);
    }

    fn round_trip(type_oid: i32, text: &str, binary: &[u8]) {
        assert_eq!(text_to_binary(type_oid, text.as_bytes()).unwrap(), binary);
        assert_eq!(binary_to_text(type_oid, binary).unwrap(), text.as_bytes());
    }

    #[test]
    fn test_round_trips() {
        round_trip(TEXT_OID, "héllo", "héllo".as_bytes());
        round_trip(JSONB_OID, r#"{"a": 1}"#, b"\x01{\"a\": 1}");
        round_trip(BOOL_OID, "t", &[1]);
        round_trip(BOOL_OID, "f", &[0]);
        round_trip(INT2_OID, "-2", &[0xff, 0xfe]);
        round_trip(INT4_OID, "258", &[0, 0, 1, 2]);
        round_trip(INT8_OID, "-1", &[0xff; 8]);
        round_trip(OID_OID, "4294967295", &[0xff; 4]);
        round_trip(BYTEA_OID, "\\x00ff10", &[0, 0xff, 0x10]);
        round_trip(
            UUID_OID,
            "00000000-0000-0000-0000-0000000000ff",
            &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff],
        );
    }

    #[test]
    fn test_uuid_spellings() {
        let canonical = b"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11".to_vec();
        for spelling in [
            "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
            "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}",
            "a0eebc999c0b4ef8bb6d6bb9bd380a11",
            "a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11",
            "{a0eebc99-9c0b4ef8-bb6d6bb9-bd380a11}",
        ] {
            let binary = text_to_binary(UUID_OID, spelling.as_bytes()).unwrap();
            assert_eq!(
                binary_to_text(UUID_OID, &binary).unwrap(),
                canonical,
                "{}",
                spelling
            );
        }
        assert!(text_to_binary(UUID_OID, b"{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11").is_err());
    }

    #[test]
    fn test_invalid_values() {
        assert!(text_to_binary(INT4_OID, b"abc").is_err());
        assert!(text_to_binary(BYTEA_OID, b"00ff").is_err());
        assert!(text_to_binary(UUID_OID, b"1234").is_err());
        assert!(binary_to_text(INT4_OID, &[1, 2]).is_err());
        assert!(binary_to_text(JSONB_OID, b"{}").is_err());

        // Unknown types are never guessed.
        assert!(text_to_binary(1700, b"1.5").is_err());
        assert!(binary_to_text(1700, &[0]).is_err());

        // Same format: nothing to convert, even for unknown types.
        assert_eq!(convert(1700, b"1.5", 0, 0).unwrap(), b"1.5");
    }
}
