//! Test utilities for working with generated protobuf metadata.
//!
//! This module is compiled in this crate's tests and in dependents that enable
//! the `test-utils` feature. Keep helpers broadly useful and focused on testing.

use std::collections::HashMap;

/// Return fully-qualified names (e.g. `v1.public.events.fusion.log.LogMessage`)
/// for all top-level messages across all packages.
///
/// Notes:
/// - Uses the pre-generated `dbtlabs_proto_public.bin` emitted by `xtask protogen`.
/// - Only considers top-level messages (nested message types are ignored).
pub fn all_message_full_names() -> Vec<String> {
    // FileDescriptorSet generated in `src/gen/dbtlabs_proto_public.bin` alongside rust code.
    // This is a stable artifact that doesn't affect production code paths.
    let bytes = include_bytes!("gen/dbtlabs_proto_public.bin");
    let fds = match prost_types::FileDescriptorSet::decode(bytes.as_ref()) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let mut out = Vec::new();
    for file in fds.file.iter() {
        let package = file.package.as_deref().unwrap_or("");
        // Only enumerate top-level messages in the file (ignore nested types).
        for m in file.message_type.iter() {
            if let Some(name) = m.name.as_deref() {
                out.push(format!("{package}.{name}"));
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Return a map of fully-qualified message names to their oneof field names.
///
/// For each message that has oneof fields, this returns a mapping from the
/// message's fully-qualified name to a vector of oneof field names within that message.
///
/// Notes:
/// - Uses the pre-generated `dbtlabs_proto_public.bin` emitted by `xtask protogen`.
/// - Only considers top-level messages (nested message types are ignored).
/// - Messages without oneof fields are excluded from the result.
/// - Filters out synthetic oneofs (proto3 optional fields) which have names starting with `_`.
///
/// # Example
///
/// ```
/// use proto_rust::test_utils::message_oneofs;
///
/// let oneofs = message_oneofs();
/// if let Some(fields) = oneofs.get("v1.public.events.fusion.node.NodeEvaluated") {
///     assert!(fields.contains(&"node_outcome_detail".to_string()));
/// }
/// ```
pub fn message_oneofs() -> HashMap<String, Vec<String>> {
    let bytes = include_bytes!("gen/dbtlabs_proto_public.bin");
    let fds = match prost_types::FileDescriptorSet::decode(bytes.as_ref()) {
        Ok(v) => v,
        Err(_) => return HashMap::new(),
    };

    let mut result = HashMap::new();

    for file in fds.file.iter() {
        let package = file.package.as_deref().unwrap_or("");

        // Only consider top-level messages
        for message in file.message_type.iter() {
            if let Some(message_name) = message.name.as_deref() {
                let full_name = format!("{package}.{message_name}");

                // Check if this message has any oneof declarations
                if !message.oneof_decl.is_empty() {
                    let mut oneof_names = Vec::new();
                    for oneof in message.oneof_decl.iter() {
                        if let Some(oneof_name) = oneof.name.as_deref() {
                            // Filter out synthetic oneofs for proto3 optional fields
                            // These have names starting with '_' (e.g., "_field_name")
                            if !oneof_name.starts_with('_') {
                                oneof_names.push(oneof_name.to_string());
                            }
                        }
                    }

                    // Only include messages with real (non-synthetic) oneofs
                    if !oneof_names.is_empty() {
                        result.insert(full_name, oneof_names);
                    }
                }
            }
        }
    }

    result
}

// Prost types needed for decoding the descriptor set
use prost::Message as _;
use prost_types as _;
