// @generated
impl serde::Serialize for VortexClientIp {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.ip.is_empty() {
            len += 1;
        }
        if self.proxy {
            len += 1;
        }
        if self.geo.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.events.vortex.VortexClientIp", len)?;
        if !self.ip.is_empty() {
            struct_ser.serialize_field("ip", &self.ip)?;
        }
        if self.proxy {
            struct_ser.serialize_field("proxy", &self.proxy)?;
        }
        if let Some(v) = self.geo.as_ref() {
            struct_ser.serialize_field("geo", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for VortexClientIp {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ip",
            "proxy",
            "geo",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Ip,
            Proxy,
            Geo,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "ip" => Ok(GeneratedField::Ip),
                            "proxy" => Ok(GeneratedField::Proxy),
                            "geo" => Ok(GeneratedField::Geo),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = VortexClientIp;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.events.vortex.VortexClientIp")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<VortexClientIp, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut ip__ = None;
                let mut proxy__ = None;
                let mut geo__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Ip => {
                            if ip__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ip"));
                            }
                            ip__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Proxy => {
                            if proxy__.is_some() {
                                return Err(serde::de::Error::duplicate_field("proxy"));
                            }
                            proxy__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Geo => {
                            if geo__.is_some() {
                                return Err(serde::de::Error::duplicate_field("geo"));
                            }
                            geo__ = map_.next_value()?;
                        }
                    }
                }
                Ok(VortexClientIp {
                    ip: ip__.unwrap_or_default(),
                    proxy: proxy__.unwrap_or_default(),
                    geo: geo__,
                })
            }
        }
        deserializer.deserialize_struct("v1.events.vortex.VortexClientIp", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for VortexClientPlatform {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.raw.is_empty() {
            len += 1;
        }
        if !self.service.is_empty() {
            len += 1;
        }
        if !self.service_version.is_empty() {
            len += 1;
        }
        if !self.client.is_empty() {
            len += 1;
        }
        if !self.client_version.is_empty() {
            len += 1;
        }
        if !self.dbt_proto_library.is_empty() {
            len += 1;
        }
        if !self.dbt_proto_library_version.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.events.vortex.VortexClientPlatform", len)?;
        if !self.raw.is_empty() {
            struct_ser.serialize_field("raw", &self.raw)?;
        }
        if !self.service.is_empty() {
            struct_ser.serialize_field("service", &self.service)?;
        }
        if !self.service_version.is_empty() {
            struct_ser.serialize_field("serviceVersion", &self.service_version)?;
        }
        if !self.client.is_empty() {
            struct_ser.serialize_field("client", &self.client)?;
        }
        if !self.client_version.is_empty() {
            struct_ser.serialize_field("clientVersion", &self.client_version)?;
        }
        if !self.dbt_proto_library.is_empty() {
            struct_ser.serialize_field("dbtProtoLibrary", &self.dbt_proto_library)?;
        }
        if !self.dbt_proto_library_version.is_empty() {
            struct_ser.serialize_field("dbtProtoLibraryVersion", &self.dbt_proto_library_version)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for VortexClientPlatform {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "raw",
            "service",
            "service_version",
            "serviceVersion",
            "client",
            "client_version",
            "clientVersion",
            "dbt_proto_library",
            "dbtProtoLibrary",
            "dbt_proto_library_version",
            "dbtProtoLibraryVersion",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Raw,
            Service,
            ServiceVersion,
            Client,
            ClientVersion,
            DbtProtoLibrary,
            DbtProtoLibraryVersion,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "raw" => Ok(GeneratedField::Raw),
                            "service" => Ok(GeneratedField::Service),
                            "serviceVersion" | "service_version" => Ok(GeneratedField::ServiceVersion),
                            "client" => Ok(GeneratedField::Client),
                            "clientVersion" | "client_version" => Ok(GeneratedField::ClientVersion),
                            "dbtProtoLibrary" | "dbt_proto_library" => Ok(GeneratedField::DbtProtoLibrary),
                            "dbtProtoLibraryVersion" | "dbt_proto_library_version" => Ok(GeneratedField::DbtProtoLibraryVersion),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = VortexClientPlatform;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.events.vortex.VortexClientPlatform")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<VortexClientPlatform, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut raw__ = None;
                let mut service__ = None;
                let mut service_version__ = None;
                let mut client__ = None;
                let mut client_version__ = None;
                let mut dbt_proto_library__ = None;
                let mut dbt_proto_library_version__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Raw => {
                            if raw__.is_some() {
                                return Err(serde::de::Error::duplicate_field("raw"));
                            }
                            raw__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Service => {
                            if service__.is_some() {
                                return Err(serde::de::Error::duplicate_field("service"));
                            }
                            service__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ServiceVersion => {
                            if service_version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("serviceVersion"));
                            }
                            service_version__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Client => {
                            if client__.is_some() {
                                return Err(serde::de::Error::duplicate_field("client"));
                            }
                            client__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ClientVersion => {
                            if client_version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("clientVersion"));
                            }
                            client_version__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DbtProtoLibrary => {
                            if dbt_proto_library__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dbtProtoLibrary"));
                            }
                            dbt_proto_library__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DbtProtoLibraryVersion => {
                            if dbt_proto_library_version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dbtProtoLibraryVersion"));
                            }
                            dbt_proto_library_version__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(VortexClientPlatform {
                    raw: raw__.unwrap_or_default(),
                    service: service__.unwrap_or_default(),
                    service_version: service_version__.unwrap_or_default(),
                    client: client__.unwrap_or_default(),
                    client_version: client_version__.unwrap_or_default(),
                    dbt_proto_library: dbt_proto_library__.unwrap_or_default(),
                    dbt_proto_library_version: dbt_proto_library_version__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.events.vortex.VortexClientPlatform", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for VortexDeadLetterMessage {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.type_url.is_empty() {
            len += 1;
        }
        if !self.value.is_empty() {
            len += 1;
        }
        if !self.value_bytes.is_empty() {
            len += 1;
        }
        if !self.reason.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.events.vortex.VortexDeadLetterMessage", len)?;
        if !self.type_url.is_empty() {
            struct_ser.serialize_field("typeUrl", &self.type_url)?;
        }
        if !self.value.is_empty() {
            struct_ser.serialize_field("value", &self.value)?;
        }
        if !self.value_bytes.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("valueBytes", pbjson::private::base64::encode(&self.value_bytes).as_str())?;
        }
        if !self.reason.is_empty() {
            struct_ser.serialize_field("reason", &self.reason)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for VortexDeadLetterMessage {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "type_url",
            "typeUrl",
            "value",
            "value_bytes",
            "valueBytes",
            "reason",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TypeUrl,
            Value,
            ValueBytes,
            Reason,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "typeUrl" | "type_url" => Ok(GeneratedField::TypeUrl),
                            "value" => Ok(GeneratedField::Value),
                            "valueBytes" | "value_bytes" => Ok(GeneratedField::ValueBytes),
                            "reason" => Ok(GeneratedField::Reason),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = VortexDeadLetterMessage;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.events.vortex.VortexDeadLetterMessage")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<VortexDeadLetterMessage, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut type_url__ = None;
                let mut value__ = None;
                let mut value_bytes__ = None;
                let mut reason__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::TypeUrl => {
                            if type_url__.is_some() {
                                return Err(serde::de::Error::duplicate_field("typeUrl"));
                            }
                            type_url__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ValueBytes => {
                            if value_bytes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("valueBytes"));
                            }
                            value_bytes__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Reason => {
                            if reason__.is_some() {
                                return Err(serde::de::Error::duplicate_field("reason"));
                            }
                            reason__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(VortexDeadLetterMessage {
                    type_url: type_url__.unwrap_or_default(),
                    value: value__.unwrap_or_default(),
                    value_bytes: value_bytes__.unwrap_or_default(),
                    reason: reason__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.events.vortex.VortexDeadLetterMessage", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for VortexGeolocation {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.country.is_empty() {
            len += 1;
        }
        if !self.city.is_empty() {
            len += 1;
        }
        if self.latitude != 0. {
            len += 1;
        }
        if self.longitude != 0. {
            len += 1;
        }
        if !self.timezone.is_empty() {
            len += 1;
        }
        if !self.continent.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.events.vortex.VortexGeolocation", len)?;
        if !self.country.is_empty() {
            struct_ser.serialize_field("country", &self.country)?;
        }
        if !self.city.is_empty() {
            struct_ser.serialize_field("city", &self.city)?;
        }
        if self.latitude != 0. {
            struct_ser.serialize_field("latitude", &self.latitude)?;
        }
        if self.longitude != 0. {
            struct_ser.serialize_field("longitude", &self.longitude)?;
        }
        if !self.timezone.is_empty() {
            struct_ser.serialize_field("timezone", &self.timezone)?;
        }
        if !self.continent.is_empty() {
            struct_ser.serialize_field("continent", &self.continent)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for VortexGeolocation {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "country",
            "city",
            "latitude",
            "longitude",
            "timezone",
            "continent",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Country,
            City,
            Latitude,
            Longitude,
            Timezone,
            Continent,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "country" => Ok(GeneratedField::Country),
                            "city" => Ok(GeneratedField::City),
                            "latitude" => Ok(GeneratedField::Latitude),
                            "longitude" => Ok(GeneratedField::Longitude),
                            "timezone" => Ok(GeneratedField::Timezone),
                            "continent" => Ok(GeneratedField::Continent),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = VortexGeolocation;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.events.vortex.VortexGeolocation")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<VortexGeolocation, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut country__ = None;
                let mut city__ = None;
                let mut latitude__ = None;
                let mut longitude__ = None;
                let mut timezone__ = None;
                let mut continent__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Country => {
                            if country__.is_some() {
                                return Err(serde::de::Error::duplicate_field("country"));
                            }
                            country__ = Some(map_.next_value()?);
                        }
                        GeneratedField::City => {
                            if city__.is_some() {
                                return Err(serde::de::Error::duplicate_field("city"));
                            }
                            city__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Latitude => {
                            if latitude__.is_some() {
                                return Err(serde::de::Error::duplicate_field("latitude"));
                            }
                            latitude__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Longitude => {
                            if longitude__.is_some() {
                                return Err(serde::de::Error::duplicate_field("longitude"));
                            }
                            longitude__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Timezone => {
                            if timezone__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timezone"));
                            }
                            timezone__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Continent => {
                            if continent__.is_some() {
                                return Err(serde::de::Error::duplicate_field("continent"));
                            }
                            continent__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(VortexGeolocation {
                    country: country__.unwrap_or_default(),
                    city: city__.unwrap_or_default(),
                    latitude: latitude__.unwrap_or_default(),
                    longitude: longitude__.unwrap_or_default(),
                    timezone: timezone__.unwrap_or_default(),
                    continent: continent__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.events.vortex.VortexGeolocation", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for VortexMessage {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.any.is_some() {
            len += 1;
        }
        if self.vortex_event_created_at.is_some() {
            len += 1;
        }
        if self.vortex_client_sent_at.is_some() {
            len += 1;
        }
        if self.vortex_backend_received_at.is_some() {
            len += 1;
        }
        if self.vortex_backend_processed_at.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.events.vortex.VortexMessage", len)?;
        if let Some(v) = self.any.as_ref() {
            struct_ser.serialize_field("any", v)?;
        }
        if let Some(v) = self.vortex_event_created_at.as_ref() {
            struct_ser.serialize_field("vortexEventCreatedAt", v)?;
        }
        if let Some(v) = self.vortex_client_sent_at.as_ref() {
            struct_ser.serialize_field("vortexClientSentAt", v)?;
        }
        if let Some(v) = self.vortex_backend_received_at.as_ref() {
            struct_ser.serialize_field("vortexBackendReceivedAt", v)?;
        }
        if let Some(v) = self.vortex_backend_processed_at.as_ref() {
            struct_ser.serialize_field("vortexBackendProcessedAt", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for VortexMessage {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "any",
            "vortex_event_created_at",
            "vortexEventCreatedAt",
            "vortex_client_sent_at",
            "vortexClientSentAt",
            "vortex_backend_received_at",
            "vortexBackendReceivedAt",
            "vortex_backend_processed_at",
            "vortexBackendProcessedAt",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Any,
            VortexEventCreatedAt,
            VortexClientSentAt,
            VortexBackendReceivedAt,
            VortexBackendProcessedAt,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "any" => Ok(GeneratedField::Any),
                            "vortexEventCreatedAt" | "vortex_event_created_at" => Ok(GeneratedField::VortexEventCreatedAt),
                            "vortexClientSentAt" | "vortex_client_sent_at" => Ok(GeneratedField::VortexClientSentAt),
                            "vortexBackendReceivedAt" | "vortex_backend_received_at" => Ok(GeneratedField::VortexBackendReceivedAt),
                            "vortexBackendProcessedAt" | "vortex_backend_processed_at" => Ok(GeneratedField::VortexBackendProcessedAt),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = VortexMessage;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.events.vortex.VortexMessage")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<VortexMessage, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut any__ = None;
                let mut vortex_event_created_at__ = None;
                let mut vortex_client_sent_at__ = None;
                let mut vortex_backend_received_at__ = None;
                let mut vortex_backend_processed_at__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Any => {
                            if any__.is_some() {
                                return Err(serde::de::Error::duplicate_field("any"));
                            }
                            any__ = map_.next_value()?;
                        }
                        GeneratedField::VortexEventCreatedAt => {
                            if vortex_event_created_at__.is_some() {
                                return Err(serde::de::Error::duplicate_field("vortexEventCreatedAt"));
                            }
                            vortex_event_created_at__ = map_.next_value()?;
                        }
                        GeneratedField::VortexClientSentAt => {
                            if vortex_client_sent_at__.is_some() {
                                return Err(serde::de::Error::duplicate_field("vortexClientSentAt"));
                            }
                            vortex_client_sent_at__ = map_.next_value()?;
                        }
                        GeneratedField::VortexBackendReceivedAt => {
                            if vortex_backend_received_at__.is_some() {
                                return Err(serde::de::Error::duplicate_field("vortexBackendReceivedAt"));
                            }
                            vortex_backend_received_at__ = map_.next_value()?;
                        }
                        GeneratedField::VortexBackendProcessedAt => {
                            if vortex_backend_processed_at__.is_some() {
                                return Err(serde::de::Error::duplicate_field("vortexBackendProcessedAt"));
                            }
                            vortex_backend_processed_at__ = map_.next_value()?;
                        }
                    }
                }
                Ok(VortexMessage {
                    any: any__,
                    vortex_event_created_at: vortex_event_created_at__,
                    vortex_client_sent_at: vortex_client_sent_at__,
                    vortex_backend_received_at: vortex_backend_received_at__,
                    vortex_backend_processed_at: vortex_backend_processed_at__,
                })
            }
        }
        deserializer.deserialize_struct("v1.events.vortex.VortexMessage", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for VortexMessageBatch {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.request_id.is_empty() {
            len += 1;
        }
        if !self.payload.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.events.vortex.VortexMessageBatch", len)?;
        if !self.request_id.is_empty() {
            struct_ser.serialize_field("requestId", &self.request_id)?;
        }
        if !self.payload.is_empty() {
            struct_ser.serialize_field("payload", &self.payload)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for VortexMessageBatch {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "request_id",
            "requestId",
            "payload",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            RequestId,
            Payload,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "requestId" | "request_id" => Ok(GeneratedField::RequestId),
                            "payload" => Ok(GeneratedField::Payload),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = VortexMessageBatch;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.events.vortex.VortexMessageBatch")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<VortexMessageBatch, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut request_id__ = None;
                let mut payload__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::RequestId => {
                            if request_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("requestId"));
                            }
                            request_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Payload => {
                            if payload__.is_some() {
                                return Err(serde::de::Error::duplicate_field("payload"));
                            }
                            payload__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(VortexMessageBatch {
                    request_id: request_id__.unwrap_or_default(),
                    payload: payload__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.events.vortex.VortexMessageBatch", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for VortexMessageEnrichment {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.client_ip.is_some() {
            len += 1;
        }
        if self.client_platform.is_some() {
            len += 1;
        }
        if self.user_agent.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.events.vortex.VortexMessageEnrichment", len)?;
        if let Some(v) = self.client_ip.as_ref() {
            struct_ser.serialize_field("clientIp", v)?;
        }
        if let Some(v) = self.client_platform.as_ref() {
            struct_ser.serialize_field("clientPlatform", v)?;
        }
        if let Some(v) = self.user_agent.as_ref() {
            struct_ser.serialize_field("userAgent", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for VortexMessageEnrichment {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "client_ip",
            "clientIp",
            "client_platform",
            "clientPlatform",
            "user_agent",
            "userAgent",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ClientIp,
            ClientPlatform,
            UserAgent,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "clientIp" | "client_ip" => Ok(GeneratedField::ClientIp),
                            "clientPlatform" | "client_platform" => Ok(GeneratedField::ClientPlatform),
                            "userAgent" | "user_agent" => Ok(GeneratedField::UserAgent),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = VortexMessageEnrichment;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.events.vortex.VortexMessageEnrichment")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<VortexMessageEnrichment, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut client_ip__ = None;
                let mut client_platform__ = None;
                let mut user_agent__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ClientIp => {
                            if client_ip__.is_some() {
                                return Err(serde::de::Error::duplicate_field("clientIp"));
                            }
                            client_ip__ = map_.next_value()?;
                        }
                        GeneratedField::ClientPlatform => {
                            if client_platform__.is_some() {
                                return Err(serde::de::Error::duplicate_field("clientPlatform"));
                            }
                            client_platform__ = map_.next_value()?;
                        }
                        GeneratedField::UserAgent => {
                            if user_agent__.is_some() {
                                return Err(serde::de::Error::duplicate_field("userAgent"));
                            }
                            user_agent__ = map_.next_value()?;
                        }
                    }
                }
                Ok(VortexMessageEnrichment {
                    client_ip: client_ip__,
                    client_platform: client_platform__,
                    user_agent: user_agent__,
                })
            }
        }
        deserializer.deserialize_struct("v1.events.vortex.VortexMessageEnrichment", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for VortexUserAgent {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.raw.is_empty() {
            len += 1;
        }
        if !self.browser.is_empty() {
            len += 1;
        }
        if !self.browser_version.is_empty() {
            len += 1;
        }
        if !self.os.is_empty() {
            len += 1;
        }
        if !self.os_version.is_empty() {
            len += 1;
        }
        if !self.device.is_empty() {
            len += 1;
        }
        if !self.device_type.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.events.vortex.VortexUserAgent", len)?;
        if !self.raw.is_empty() {
            struct_ser.serialize_field("raw", &self.raw)?;
        }
        if !self.browser.is_empty() {
            struct_ser.serialize_field("browser", &self.browser)?;
        }
        if !self.browser_version.is_empty() {
            struct_ser.serialize_field("browserVersion", &self.browser_version)?;
        }
        if !self.os.is_empty() {
            struct_ser.serialize_field("os", &self.os)?;
        }
        if !self.os_version.is_empty() {
            struct_ser.serialize_field("osVersion", &self.os_version)?;
        }
        if !self.device.is_empty() {
            struct_ser.serialize_field("device", &self.device)?;
        }
        if !self.device_type.is_empty() {
            struct_ser.serialize_field("deviceType", &self.device_type)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for VortexUserAgent {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "raw",
            "browser",
            "browser_version",
            "browserVersion",
            "os",
            "os_version",
            "osVersion",
            "device",
            "device_type",
            "deviceType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Raw,
            Browser,
            BrowserVersion,
            Os,
            OsVersion,
            Device,
            DeviceType,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl serde::de::Visitor<'_> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "raw" => Ok(GeneratedField::Raw),
                            "browser" => Ok(GeneratedField::Browser),
                            "browserVersion" | "browser_version" => Ok(GeneratedField::BrowserVersion),
                            "os" => Ok(GeneratedField::Os),
                            "osVersion" | "os_version" => Ok(GeneratedField::OsVersion),
                            "device" => Ok(GeneratedField::Device),
                            "deviceType" | "device_type" => Ok(GeneratedField::DeviceType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = VortexUserAgent;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.events.vortex.VortexUserAgent")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<VortexUserAgent, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut raw__ = None;
                let mut browser__ = None;
                let mut browser_version__ = None;
                let mut os__ = None;
                let mut os_version__ = None;
                let mut device__ = None;
                let mut device_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Raw => {
                            if raw__.is_some() {
                                return Err(serde::de::Error::duplicate_field("raw"));
                            }
                            raw__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Browser => {
                            if browser__.is_some() {
                                return Err(serde::de::Error::duplicate_field("browser"));
                            }
                            browser__ = Some(map_.next_value()?);
                        }
                        GeneratedField::BrowserVersion => {
                            if browser_version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("browserVersion"));
                            }
                            browser_version__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Os => {
                            if os__.is_some() {
                                return Err(serde::de::Error::duplicate_field("os"));
                            }
                            os__ = Some(map_.next_value()?);
                        }
                        GeneratedField::OsVersion => {
                            if os_version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("osVersion"));
                            }
                            os_version__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Device => {
                            if device__.is_some() {
                                return Err(serde::de::Error::duplicate_field("device"));
                            }
                            device__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DeviceType => {
                            if device_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("deviceType"));
                            }
                            device_type__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(VortexUserAgent {
                    raw: raw__.unwrap_or_default(),
                    browser: browser__.unwrap_or_default(),
                    browser_version: browser_version__.unwrap_or_default(),
                    os: os__.unwrap_or_default(),
                    os_version: os_version__.unwrap_or_default(),
                    device: device__.unwrap_or_default(),
                    device_type: device_type__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.events.vortex.VortexUserAgent", FIELDS, GeneratedVisitor)
    }
}
