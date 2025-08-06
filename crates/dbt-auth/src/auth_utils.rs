pub const PEM_ENCRYPTED_START: &str = "-----BEGIN ENCRYPTED PRIVATE KEY-----";
pub const PEM_ENCRYPTED_END: &str = "-----END ENCRYPTED PRIVATE KEY-----";
pub const PEM_UNENCRYPTED_START: &str = "-----BEGIN PRIVATE KEY-----";
pub const PEM_UNENCRYPTED_END: &str = "-----END PRIVATE KEY-----";

/// Ensures a private key string has proper PEM format headers and footers.
/// If the key is already in PEM format, returns it unchanged.
/// Otherwise, if the key is a base64-encoded DER, decodes it and wraps it in proper PEM headers/footers.
/// If decoding fails, wraps the original key.
///
/// This doesn't validate/attempt to correct the `key`'s content, it simply wraps it when necessary
pub fn ensure_private_key_header_footer(key: &str, is_encrypted_key: bool) -> String {
    use base64::{Engine as _, engine::general_purpose};

    if (is_encrypted_key && key.contains(PEM_ENCRYPTED_START) && key.contains(PEM_ENCRYPTED_END))
        || (!is_encrypted_key
            && key.contains(PEM_UNENCRYPTED_START)
            && key.contains(PEM_UNENCRYPTED_END))
    {
        return key.to_string();
    }

    // base64 encoded string cannot have '-'
    let key = if !key.contains("-") {
        let decoded = general_purpose::STANDARD.decode(key.trim());
        match decoded {
            Ok(bytes) => match String::from_utf8(bytes) {
                Ok(s) => s,
                Err(_) => key.to_string(),
            },
            Err(_) => key.to_string(),
        }
    } else {
        key.to_string()
    };

    if is_encrypted_key {
        format!("{PEM_ENCRYPTED_START}\n{key}\n{PEM_ENCRYPTED_END}")
    } else {
        format!("{PEM_UNENCRYPTED_START}\n{key}\n{PEM_UNENCRYPTED_END}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_already_formatted_encrypted_key() {
        let key = format!(
            "{}\n{}\n{}",
            PEM_ENCRYPTED_START, "base64content", PEM_ENCRYPTED_END
        );
        assert_eq!(ensure_private_key_header_footer(&key, true), key);
    }

    #[test]
    fn test_already_formatted_unencrypted_key() {
        let key = format!(
            "{}\n{}\n{}",
            PEM_UNENCRYPTED_START, "base64content", PEM_UNENCRYPTED_END
        );
        assert_eq!(ensure_private_key_header_footer(&key, false), key);
    }

    #[test]
    fn test_format_encrypted_key() {
        let key = "base64content";
        let expected = format!(
            "{}\n{}\n{}",
            PEM_ENCRYPTED_START, "base64content", PEM_ENCRYPTED_END
        );
        assert_eq!(ensure_private_key_header_footer(key, true), expected);
    }

    #[test]
    fn test_format_unencrypted_key() {
        let key = "base64content";
        let expected = format!("{PEM_UNENCRYPTED_START}\n{key}\n{PEM_UNENCRYPTED_END}");
        assert_eq!(ensure_private_key_header_footer(key, false), expected);
    }

    #[test]
    fn test_base64_decode_unencrypted_key() {
        let base64_key = "aGVsbG8=";
        let expected_content = "hello";
        let expected =
            format!("{PEM_UNENCRYPTED_START}\n{expected_content}\n{PEM_UNENCRYPTED_END}");

        let result = ensure_private_key_header_footer(base64_key, false);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_invalid_base64_decoded_unencrypted_key() {
        // `ensure_private_key_header_footer` doesn't error but just returns the wrapped input as it is
        let invalid_base64_key = "invalid!@#$%base64";
        let expected =
            format!("{PEM_UNENCRYPTED_START}\n{invalid_base64_key}\n{PEM_UNENCRYPTED_END}");
        let result = ensure_private_key_header_footer(invalid_base64_key, false);
        assert_eq!(result, expected);
    }
}
