impl serde::Serialize for VortexIcebergNamespace {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "VORTEX_ICEBERG_NAMESPACE_UNSPECIFIED",
            Self::CostMonitoring => "VORTEX_ICEBERG_NAMESPACE_COST_MONITORING",
            Self::Fusion => "VORTEX_ICEBERG_NAMESPACE_FUSION",
            Self::Telemetry => "VORTEX_ICEBERG_NAMESPACE_TELEMETRY",
            Self::Dlq => "VORTEX_ICEBERG_NAMESPACE_DLQ",
            Self::Mantle => "VORTEX_ICEBERG_NAMESPACE_MANTLE",
            Self::Codex => "VORTEX_ICEBERG_NAMESPACE_CODEX",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for VortexIcebergNamespace {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "VORTEX_ICEBERG_NAMESPACE_UNSPECIFIED",
            "VORTEX_ICEBERG_NAMESPACE_COST_MONITORING",
            "VORTEX_ICEBERG_NAMESPACE_FUSION",
            "VORTEX_ICEBERG_NAMESPACE_TELEMETRY",
            "VORTEX_ICEBERG_NAMESPACE_DLQ",
            "VORTEX_ICEBERG_NAMESPACE_MANTLE",
            "VORTEX_ICEBERG_NAMESPACE_CODEX",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = VortexIcebergNamespace;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "VORTEX_ICEBERG_NAMESPACE_UNSPECIFIED" => Ok(VortexIcebergNamespace::Unspecified),
                    "VORTEX_ICEBERG_NAMESPACE_COST_MONITORING" => Ok(VortexIcebergNamespace::CostMonitoring),
                    "VORTEX_ICEBERG_NAMESPACE_FUSION" => Ok(VortexIcebergNamespace::Fusion),
                    "VORTEX_ICEBERG_NAMESPACE_TELEMETRY" => Ok(VortexIcebergNamespace::Telemetry),
                    "VORTEX_ICEBERG_NAMESPACE_DLQ" => Ok(VortexIcebergNamespace::Dlq),
                    "VORTEX_ICEBERG_NAMESPACE_MANTLE" => Ok(VortexIcebergNamespace::Mantle),
                    "VORTEX_ICEBERG_NAMESPACE_CODEX" => Ok(VortexIcebergNamespace::Codex),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
