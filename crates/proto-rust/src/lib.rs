include!("gen/mod.rs");

pub static FULL_FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("gen/dbtlabs_proto.bin");

#[cfg(test)]
mod tests {

    use prost::Name;

    #[test]
    #[cfg(feature = "v1-public-fields-adapter_types")]
    /// Test for presence of adapter events
    fn test_adapter_events() {
        let pool = prost_reflect::DescriptorPool::decode(crate::FULL_FILE_DESCRIPTOR_SET).unwrap();

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
