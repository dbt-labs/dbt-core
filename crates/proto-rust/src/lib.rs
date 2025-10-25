#![allow(unused_qualifications)]

// Various manual trait impls and utilities for working with the generated proto code.
pub mod impls;
pub mod macros;
pub mod serde_timestamp_micros;
mod static_full_name;

pub use static_full_name::StaticName;

#[allow(
    clippy::cognitive_complexity,
    clippy::large_enum_variant,
    clippy::doc_lazy_continuation,
    clippy::module_inception
)]
pub mod v1 {
    pub mod events {
        pub mod vortex {
            include!("gen/v1.events.vortex.rs");
        }
    }
    pub mod public {
        pub mod events {
            pub mod fusion {
                pub mod compat {
                    include!("gen/v1.public.events.fusion.compat.rs");
                }
                pub mod dev {
                    include!("gen/v1.public.events.fusion.dev.rs");
                    include!("gen/v1.public.events.fusion.dev.serde.rs");
                }
                pub mod onboarding {
                    include!("gen/v1.public.events.fusion.onboarding.rs");
                    include!("gen/v1.public.events.fusion.onboarding.serde.rs");
                }
                pub mod phase {
                    include!("gen/v1.public.events.fusion.phase.rs");
                    include!("gen/v1.public.events.fusion.phase.serde.rs");
                }
                pub mod invocation {
                    include!("gen/v1.public.events.fusion.invocation.rs");
                    include!("gen/v1.public.events.fusion.invocation.serde.rs");
                }
                pub mod log {
                    include!("gen/v1.public.events.fusion.log.rs");
                    include!("gen/v1.public.events.fusion.log.serde.rs");
                }
                pub mod artifact {
                    include!("gen/v1.public.events.fusion.artifact.rs");
                    include!("gen/v1.public.events.fusion.artifact.serde.rs");
                }
                pub mod node {
                    include!("gen/v1.public.events.fusion.node.rs");
                    include!("gen/v1.public.events.fusion.node.serde.rs");
                }
                pub mod process {
                    include!("gen/v1.public.events.fusion.process.rs");
                    include!("gen/v1.public.events.fusion.process.serde.rs");
                }
                pub mod query {
                    include!("gen/v1.public.events.fusion.query.rs");
                    include!("gen/v1.public.events.fusion.query.serde.rs");
                }
                pub mod update {
                    include!("gen/v1.public.events.fusion.update.rs");
                    include!("gen/v1.public.events.fusion.update.serde.rs");
                }
                include!("gen/v1.public.events.fusion.rs");
            }
        }
        pub mod fields {
            pub mod adapter_types {
                include!("gen/v1.public.fields.adapter_types.rs");
            }
            pub mod core_types {
                include!("gen/v1.public.fields.core_types.rs");
            }
        }
    }
}

// Test-only utilities for enumerating proto message types.
// Available in this crate's tests or when dependents opt-in via feature.
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
