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
                include!("gen/v1.public.events.fusion.rs");
            }
        }
        pub mod fields {
            pub mod adapter_types {
                include!("gen/v1.public.fields.adapter_types.rs");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use prost::Name;

    // We only enable reflection for the public protobuf events.
    static PUBLIC_DESCRIPTOR_POOL: &[u8] = include_bytes!("gen/dbtlabs_proto.bin");

    #[test]
    /// Test for presence of adapter events
    fn test_adapter_events() {
        let pool = prost_reflect::DescriptorPool::decode(PUBLIC_DESCRIPTOR_POOL).unwrap();

        let file_descriptor = pool.get_message_by_name(
            crate::v1::public::fields::adapter_types::SqlQueryStatus::type_url()
                .split("/")
                .last()
                .unwrap(),
        );

        println!("file_descriptor = {file_descriptor:?}");

        if let Some(file_descriptor) = file_descriptor {
            let extensions = file_descriptor.extensions();
            for extension in extensions {
                println!("extension = {extension:?}");
            }
        }
    }
}
