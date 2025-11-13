//! Registry for telemetry attribute types.

use proto_rust::{StaticName, v1::public::events::fusion::log::UserLogMessage};
use std::{collections::HashMap, sync::LazyLock};

use super::traits::AnyTelemetryEvent;
use crate::{
    attributes::traits::ArrowSerializableTelemetryEvent,
    schemas::{
        ArtifactWritten, CallTrace, CompiledCodeInline, Invocation, ListItemOutput, LogMessage,
        NodeEvaluated, OnboardingScreenShown, PhaseExecuted, Process, QueryExecuted,
        ShowDataOutput, Unknown,
    },
    serialize::arrow::ArrowAttributes,
};

/// Helper function that converts trait deserializer method to one compatible with the registry.
fn arrow_deserialize_for_type<T>(
    attrs: &ArrowAttributes,
) -> Result<Box<dyn AnyTelemetryEvent>, String>
where
    T: AnyTelemetryEvent + ArrowSerializableTelemetryEvent,
{
    T::from_arrow_record(attrs).map(|t| Box::new(t) as Box<dyn AnyTelemetryEvent>)
}

/// Helper function to create a faker for a given type.
#[cfg(any(test, feature = "test-utils"))]
fn faker_for_type<T>(seed: &str) -> Box<dyn AnyTelemetryEvent>
where
    T: AnyTelemetryEvent + fake::Dummy<fake::Faker>,
{
    use fake::rand::SeedableRng;
    use fake::rand::rngs::StdRng;
    use fake::{Fake, Faker};
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    // Generate pseudo-random but deterministic values for testing
    fn hash_seed(seed: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        hasher.finish()
    }

    let hashed_seed = hash_seed(seed);
    let mut rng = StdRng::seed_from_u64(hashed_seed);

    let atrrs: T = Faker.fake_with_rng(&mut rng);

    Box::new(atrrs) as Box<dyn AnyTelemetryEvent>
}

pub type ArrowDeserializerFn = fn(&ArrowAttributes) -> Result<Box<dyn AnyTelemetryEvent>, String>;
#[cfg(any(test, feature = "test-utils"))]
pub type FakerFn = fn(&str) -> Box<dyn AnyTelemetryEvent>;

/// Registry for telemetry attribute type deserializers.
///
/// This registry maps event type identifiers to deserializer functions,
/// enabling runtime dispatch during deserialization.
#[derive(Clone, Default)]
pub struct TelemetryEventTypeRegistry {
    arrow_deserializers: HashMap<&'static str, ArrowDeserializerFn>,

    #[cfg(any(test, feature = "test-utils"))]
    fakers: HashMap<&'static str, FakerFn>,
}

static PUBLIC_TELEMETRY_EVENT_REGISTRY: LazyLock<TelemetryEventTypeRegistry> =
    LazyLock::new(|| {
        let mut registry = TelemetryEventTypeRegistry::new();

        // Register span event types
        registry.register(
            CallTrace::FULL_NAME,
            arrow_deserialize_for_type::<CallTrace>,
            #[cfg(any(test, feature = "test-utils"))]
            faker_for_type::<CallTrace>,
        );
        registry.register(
            Invocation::FULL_NAME,
            arrow_deserialize_for_type::<Invocation>,
            #[cfg(any(test, feature = "test-utils"))]
            faker_for_type::<Invocation>,
        );
        registry.register(
            Process::FULL_NAME,
            arrow_deserialize_for_type::<Process>,
            #[cfg(any(test, feature = "test-utils"))]
            faker_for_type::<Process>,
        );
        registry.register(
            PhaseExecuted::FULL_NAME,
            arrow_deserialize_for_type::<PhaseExecuted>,
            #[cfg(any(test, feature = "test-utils"))]
            faker_for_type::<PhaseExecuted>,
        );
        registry.register(
            OnboardingScreenShown::FULL_NAME,
            arrow_deserialize_for_type::<OnboardingScreenShown>,
            #[cfg(any(test, feature = "test-utils"))]
            faker_for_type::<OnboardingScreenShown>,
        );
        registry.register(
            NodeEvaluated::FULL_NAME,
            arrow_deserialize_for_type::<NodeEvaluated>,
            #[cfg(any(test, feature = "test-utils"))]
            faker_for_type::<NodeEvaluated>,
        );
        registry.register(
            ArtifactWritten::FULL_NAME,
            arrow_deserialize_for_type::<ArtifactWritten>,
            #[cfg(any(test, feature = "test-utils"))]
            faker_for_type::<ArtifactWritten>,
        );
        registry.register(
            Unknown::FULL_NAME,
            arrow_deserialize_for_type::<Unknown>,
            #[cfg(any(test, feature = "test-utils"))]
            faker_for_type::<Unknown>,
        );
        registry.register(
            QueryExecuted::FULL_NAME,
            arrow_deserialize_for_type::<QueryExecuted>,
            #[cfg(any(test, feature = "test-utils"))]
            faker_for_type::<QueryExecuted>,
        );

        // Register log attributes
        registry.register(
            LogMessage::FULL_NAME,
            arrow_deserialize_for_type::<LogMessage>,
            #[cfg(any(test, feature = "test-utils"))]
            faker_for_type::<LogMessage>,
        );
        registry.register(
            UserLogMessage::FULL_NAME,
            arrow_deserialize_for_type::<UserLogMessage>,
            #[cfg(any(test, feature = "test-utils"))]
            faker_for_type::<UserLogMessage>,
        );
        registry.register(
            CompiledCodeInline::FULL_NAME,
            arrow_deserialize_for_type::<CompiledCodeInline>,
            #[cfg(any(test, feature = "test-utils"))]
            faker_for_type::<CompiledCodeInline>,
        );
        registry.register(
            ListItemOutput::FULL_NAME,
            arrow_deserialize_for_type::<ListItemOutput>,
            #[cfg(any(test, feature = "test-utils"))]
            faker_for_type::<ListItemOutput>,
        );
        registry.register(
            ShowDataOutput::FULL_NAME,
            arrow_deserialize_for_type::<ShowDataOutput>,
            #[cfg(any(test, feature = "test-utils"))]
            faker_for_type::<ShowDataOutput>,
        );

        registry
    });

impl TelemetryEventTypeRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Default::default()
    }

    /// Register with explicit event type string.
    pub fn register(
        &mut self,
        event_type: &'static str,
        arrow_deserializer: fn(&ArrowAttributes) -> Result<Box<dyn AnyTelemetryEvent>, String>,
        #[cfg(any(test, feature = "test-utils"))] faker: fn(&str) -> Box<dyn AnyTelemetryEvent>,
    ) {
        self.arrow_deserializers
            .insert(event_type, arrow_deserializer);

        #[cfg(any(test, feature = "test-utils"))]
        self.fakers.insert(event_type, faker);
    }

    /// Get an arrow deserializer for the given event type.
    pub fn get_arrow_deserializer(&self, event_type: &str) -> Option<ArrowDeserializerFn> {
        self.arrow_deserializers.get(event_type).copied()
    }

    /// Get a faker function for the given event type.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn get_faker(&self, event_type: &str) -> Option<FakerFn> {
        self.fakers.get(event_type).copied()
    }

    /// Create a registry with all built-in attribute types from this crate.
    pub fn public() -> &'static Self {
        &PUBLIC_TELEMETRY_EVENT_REGISTRY
    }

    /// Merge another registry into this one.
    ///
    /// This is useful for combining the public registry with custom attribute types.
    pub fn merge(&mut self, other: TelemetryEventTypeRegistry) {
        self.arrow_deserializers.extend(other.arrow_deserializers);
        #[cfg(any(test, feature = "test-utils"))]
        self.fakers.extend(other.fakers);
    }

    pub fn iter(&self) -> impl Iterator<Item = &'static str> {
        self.arrow_deserializers.keys().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    // Smoke test: ensure the registry contains all message types we expose
    // under v1.public.events.fusion subpackages that dbt-telemetry supports.
    #[test]
    fn registry_covers_fusion_subpackage_messages() {
        // Enumerate all top-level messages and filter to supported fusion subpackages.
        let all = proto_rust::test_utils::all_message_full_names();

        // We intentionally maintain a opt out list of known non-top-level
        // messages that are not first-class events in the registry. This
        // way we ensure that when new ones are added, they will be
        // automatically picked up by this test.
        let expected: HashSet<String> = all
            .into_iter()
            // As of today this will unfortunately pull in vortex messages
            // that are not properly scoped in a subpackage
            .filter(|n| n.starts_with("v1.public.events.fusion."))
            .filter(|n| {
                ![
                    // Filter legacy vortex events
                    "v1.public.events.fusion.AdapterInfo",
                    "v1.public.events.fusion.AdapterInfoV2",
                    "v1.public.events.fusion.Invocation",
                    "v1.public.events.fusion.InvocationEnv",
                    "v1.public.events.fusion.PackageInstall",
                    "v1.public.events.fusion.ResourceCounts",
                    "v1.public.events.fusion.RunModel",
                    "v1.public.events.fusion.Onboarding",
                    "v1.public.events.fusion.OnboardingScreen",
                    "v1.public.events.fusion.OnboardingAction",
                    // Ignore helper/embedded types that are not first-class events in the registry.
                    // dev
                    "v1.public.events.fusion.dev.DebugValue",
                    // invocation helpers
                    "v1.public.events.fusion.invocation.InvocationEvalArgs",
                    "v1.public.events.fusion.invocation.InvocationMetrics",
                    // node
                    "v1.public.events.fusion.node.NodeCacheDetail",
                    "v1.public.events.fusion.node.NodeSkipUpstreamDetail",
                    "v1.public.events.fusion.node.SourceFreshnessDetail",
                    "v1.public.events.fusion.node.TestEvaluationDetail",
                    // Not exported to parquet, so doesn't participate in registry
                    "v1.public.events.fusion.update.PackageUpdate",
                ]
                .contains(&n.as_str())
            })
            .collect();

        // Build the actual set from the registry keys.
        let registry = TelemetryEventTypeRegistry::public();
        let actual: HashSet<String> = registry.iter().map(str::to_string).collect();

        // Compute missing ones and report the full list for easier debugging.
        let mut missing: Vec<String> = expected.difference(&actual).cloned().collect();
        missing.sort();
        let mut unexpected: Vec<String> = actual.difference(&expected).cloned().collect();
        unexpected.sort();

        assert!(
            missing.is_empty(),
            "Missing deserializers for: {}",
            missing.join(", ")
        );
        assert!(
            unexpected.is_empty(),
            "Unexpected deserializers for: {}",
            unexpected.join(", ")
        );
    }
}
