impl serde::Serialize for AdapterInfo {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.enrichment.is_some() {
            len += 1;
        }
        if !self.event_id.is_empty() {
            len += 1;
        }
        if !self.invocation_id.is_empty() {
            len += 1;
        }
        if !self.adapter_type.is_empty() {
            len += 1;
        }
        if !self.adapter_unique_id.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.events.fusion.AdapterInfo", len)?;
        if let Some(v) = self.enrichment.as_ref() {
            struct_ser.serialize_field("enrichment", v)?;
        }
        if !self.event_id.is_empty() {
            struct_ser.serialize_field("eventId", &self.event_id)?;
        }
        if !self.invocation_id.is_empty() {
            struct_ser.serialize_field("invocationId", &self.invocation_id)?;
        }
        if !self.adapter_type.is_empty() {
            struct_ser.serialize_field("adapterType", &self.adapter_type)?;
        }
        if !self.adapter_unique_id.is_empty() {
            struct_ser.serialize_field("adapterUniqueId", &self.adapter_unique_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdapterInfo {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "enrichment",
            "event_id",
            "eventId",
            "invocation_id",
            "invocationId",
            "adapter_type",
            "adapterType",
            "adapter_unique_id",
            "adapterUniqueId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Enrichment,
            EventId,
            InvocationId,
            AdapterType,
            AdapterUniqueId,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
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
                            "enrichment" => Ok(GeneratedField::Enrichment),
                            "eventId" | "event_id" => Ok(GeneratedField::EventId),
                            "invocationId" | "invocation_id" => Ok(GeneratedField::InvocationId),
                            "adapterType" | "adapter_type" => Ok(GeneratedField::AdapterType),
                            "adapterUniqueId" | "adapter_unique_id" => Ok(GeneratedField::AdapterUniqueId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdapterInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.events.fusion.AdapterInfo")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdapterInfo, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut enrichment__ = None;
                let mut event_id__ = None;
                let mut invocation_id__ = None;
                let mut adapter_type__ = None;
                let mut adapter_unique_id__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Enrichment => {
                            if enrichment__.is_some() {
                                return Err(serde::de::Error::duplicate_field("enrichment"));
                            }
                            enrichment__ = map_.next_value()?;
                        }
                        GeneratedField::EventId => {
                            if event_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("eventId"));
                            }
                            event_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::InvocationId => {
                            if invocation_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("invocationId"));
                            }
                            invocation_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::AdapterType => {
                            if adapter_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("adapterType"));
                            }
                            adapter_type__ = Some(map_.next_value()?);
                        }
                        GeneratedField::AdapterUniqueId => {
                            if adapter_unique_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("adapterUniqueId"));
                            }
                            adapter_unique_id__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(AdapterInfo {
                    enrichment: enrichment__,
                    event_id: event_id__.unwrap_or_default(),
                    invocation_id: invocation_id__.unwrap_or_default(),
                    adapter_type: adapter_type__.unwrap_or_default(),
                    adapter_unique_id: adapter_unique_id__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.events.fusion.AdapterInfo", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdapterInfoV2 {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.enrichment.is_some() {
            len += 1;
        }
        if !self.event_id.is_empty() {
            len += 1;
        }
        if !self.run_model_id.is_empty() {
            len += 1;
        }
        if !self.adapter_name.is_empty() {
            len += 1;
        }
        if !self.base_adapter_version.is_empty() {
            len += 1;
        }
        if !self.adapter_version.is_empty() {
            len += 1;
        }
        if !self.model_adapter_details.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.events.fusion.AdapterInfoV2", len)?;
        if let Some(v) = self.enrichment.as_ref() {
            struct_ser.serialize_field("enrichment", v)?;
        }
        if !self.event_id.is_empty() {
            struct_ser.serialize_field("eventId", &self.event_id)?;
        }
        if !self.run_model_id.is_empty() {
            struct_ser.serialize_field("runModelId", &self.run_model_id)?;
        }
        if !self.adapter_name.is_empty() {
            struct_ser.serialize_field("adapterName", &self.adapter_name)?;
        }
        if !self.base_adapter_version.is_empty() {
            struct_ser.serialize_field("baseAdapterVersion", &self.base_adapter_version)?;
        }
        if !self.adapter_version.is_empty() {
            struct_ser.serialize_field("adapterVersion", &self.adapter_version)?;
        }
        if !self.model_adapter_details.is_empty() {
            struct_ser.serialize_field("modelAdapterDetails", &self.model_adapter_details)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdapterInfoV2 {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "enrichment",
            "event_id",
            "eventId",
            "run_model_id",
            "runModelId",
            "adapter_name",
            "adapterName",
            "base_adapter_version",
            "baseAdapterVersion",
            "adapter_version",
            "adapterVersion",
            "model_adapter_details",
            "modelAdapterDetails",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Enrichment,
            EventId,
            RunModelId,
            AdapterName,
            BaseAdapterVersion,
            AdapterVersion,
            ModelAdapterDetails,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
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
                            "enrichment" => Ok(GeneratedField::Enrichment),
                            "eventId" | "event_id" => Ok(GeneratedField::EventId),
                            "runModelId" | "run_model_id" => Ok(GeneratedField::RunModelId),
                            "adapterName" | "adapter_name" => Ok(GeneratedField::AdapterName),
                            "baseAdapterVersion" | "base_adapter_version" => Ok(GeneratedField::BaseAdapterVersion),
                            "adapterVersion" | "adapter_version" => Ok(GeneratedField::AdapterVersion),
                            "modelAdapterDetails" | "model_adapter_details" => Ok(GeneratedField::ModelAdapterDetails),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdapterInfoV2;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.events.fusion.AdapterInfoV2")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdapterInfoV2, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut enrichment__ = None;
                let mut event_id__ = None;
                let mut run_model_id__ = None;
                let mut adapter_name__ = None;
                let mut base_adapter_version__ = None;
                let mut adapter_version__ = None;
                let mut model_adapter_details__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Enrichment => {
                            if enrichment__.is_some() {
                                return Err(serde::de::Error::duplicate_field("enrichment"));
                            }
                            enrichment__ = map_.next_value()?;
                        }
                        GeneratedField::EventId => {
                            if event_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("eventId"));
                            }
                            event_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::RunModelId => {
                            if run_model_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("runModelId"));
                            }
                            run_model_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::AdapterName => {
                            if adapter_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("adapterName"));
                            }
                            adapter_name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::BaseAdapterVersion => {
                            if base_adapter_version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("baseAdapterVersion"));
                            }
                            base_adapter_version__ = Some(map_.next_value()?);
                        }
                        GeneratedField::AdapterVersion => {
                            if adapter_version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("adapterVersion"));
                            }
                            adapter_version__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ModelAdapterDetails => {
                            if model_adapter_details__.is_some() {
                                return Err(serde::de::Error::duplicate_field("modelAdapterDetails"));
                            }
                            model_adapter_details__ = Some(
                                map_.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                    }
                }
                Ok(AdapterInfoV2 {
                    enrichment: enrichment__,
                    event_id: event_id__.unwrap_or_default(),
                    run_model_id: run_model_id__.unwrap_or_default(),
                    adapter_name: adapter_name__.unwrap_or_default(),
                    base_adapter_version: base_adapter_version__.unwrap_or_default(),
                    adapter_version: adapter_version__.unwrap_or_default(),
                    model_adapter_details: model_adapter_details__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.events.fusion.AdapterInfoV2", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Invocation {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.enrichment.is_some() {
            len += 1;
        }
        if !self.event_id.is_empty() {
            len += 1;
        }
        if !self.invocation_id.is_empty() {
            len += 1;
        }
        if !self.project_id.is_empty() {
            len += 1;
        }
        if !self.user_id.is_empty() {
            len += 1;
        }
        if !self.command.is_empty() {
            len += 1;
        }
        if !self.progress.is_empty() {
            len += 1;
        }
        if !self.version.is_empty() {
            len += 1;
        }
        if !self.result_type.is_empty() {
            len += 1;
        }
        if !self.git_commit_sha.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.events.fusion.Invocation", len)?;
        if let Some(v) = self.enrichment.as_ref() {
            struct_ser.serialize_field("enrichment", v)?;
        }
        if !self.event_id.is_empty() {
            struct_ser.serialize_field("eventId", &self.event_id)?;
        }
        if !self.invocation_id.is_empty() {
            struct_ser.serialize_field("invocationId", &self.invocation_id)?;
        }
        if !self.project_id.is_empty() {
            struct_ser.serialize_field("projectId", &self.project_id)?;
        }
        if !self.user_id.is_empty() {
            struct_ser.serialize_field("userId", &self.user_id)?;
        }
        if !self.command.is_empty() {
            struct_ser.serialize_field("command", &self.command)?;
        }
        if !self.progress.is_empty() {
            struct_ser.serialize_field("progress", &self.progress)?;
        }
        if !self.version.is_empty() {
            struct_ser.serialize_field("version", &self.version)?;
        }
        if !self.result_type.is_empty() {
            struct_ser.serialize_field("resultType", &self.result_type)?;
        }
        if !self.git_commit_sha.is_empty() {
            struct_ser.serialize_field("gitCommitSha", &self.git_commit_sha)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Invocation {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "enrichment",
            "event_id",
            "eventId",
            "invocation_id",
            "invocationId",
            "project_id",
            "projectId",
            "user_id",
            "userId",
            "command",
            "progress",
            "version",
            "result_type",
            "resultType",
            "git_commit_sha",
            "gitCommitSha",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Enrichment,
            EventId,
            InvocationId,
            ProjectId,
            UserId,
            Command,
            Progress,
            Version,
            ResultType,
            GitCommitSha,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
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
                            "enrichment" => Ok(GeneratedField::Enrichment),
                            "eventId" | "event_id" => Ok(GeneratedField::EventId),
                            "invocationId" | "invocation_id" => Ok(GeneratedField::InvocationId),
                            "projectId" | "project_id" => Ok(GeneratedField::ProjectId),
                            "userId" | "user_id" => Ok(GeneratedField::UserId),
                            "command" => Ok(GeneratedField::Command),
                            "progress" => Ok(GeneratedField::Progress),
                            "version" => Ok(GeneratedField::Version),
                            "resultType" | "result_type" => Ok(GeneratedField::ResultType),
                            "gitCommitSha" | "git_commit_sha" => Ok(GeneratedField::GitCommitSha),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Invocation;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.events.fusion.Invocation")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Invocation, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut enrichment__ = None;
                let mut event_id__ = None;
                let mut invocation_id__ = None;
                let mut project_id__ = None;
                let mut user_id__ = None;
                let mut command__ = None;
                let mut progress__ = None;
                let mut version__ = None;
                let mut result_type__ = None;
                let mut git_commit_sha__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Enrichment => {
                            if enrichment__.is_some() {
                                return Err(serde::de::Error::duplicate_field("enrichment"));
                            }
                            enrichment__ = map_.next_value()?;
                        }
                        GeneratedField::EventId => {
                            if event_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("eventId"));
                            }
                            event_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::InvocationId => {
                            if invocation_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("invocationId"));
                            }
                            invocation_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ProjectId => {
                            if project_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("projectId"));
                            }
                            project_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::UserId => {
                            if user_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("userId"));
                            }
                            user_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Command => {
                            if command__.is_some() {
                                return Err(serde::de::Error::duplicate_field("command"));
                            }
                            command__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Progress => {
                            if progress__.is_some() {
                                return Err(serde::de::Error::duplicate_field("progress"));
                            }
                            progress__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Version => {
                            if version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("version"));
                            }
                            version__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ResultType => {
                            if result_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resultType"));
                            }
                            result_type__ = Some(map_.next_value()?);
                        }
                        GeneratedField::GitCommitSha => {
                            if git_commit_sha__.is_some() {
                                return Err(serde::de::Error::duplicate_field("gitCommitSha"));
                            }
                            git_commit_sha__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Invocation {
                    enrichment: enrichment__,
                    event_id: event_id__.unwrap_or_default(),
                    invocation_id: invocation_id__.unwrap_or_default(),
                    project_id: project_id__.unwrap_or_default(),
                    user_id: user_id__.unwrap_or_default(),
                    command: command__.unwrap_or_default(),
                    progress: progress__.unwrap_or_default(),
                    version: version__.unwrap_or_default(),
                    result_type: result_type__.unwrap_or_default(),
                    git_commit_sha: git_commit_sha__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.events.fusion.Invocation", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for InvocationEnv {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.enrichment.is_some() {
            len += 1;
        }
        if !self.event_id.is_empty() {
            len += 1;
        }
        if !self.invocation_id.is_empty() {
            len += 1;
        }
        if !self.environment.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.events.fusion.InvocationEnv", len)?;
        if let Some(v) = self.enrichment.as_ref() {
            struct_ser.serialize_field("enrichment", v)?;
        }
        if !self.event_id.is_empty() {
            struct_ser.serialize_field("eventId", &self.event_id)?;
        }
        if !self.invocation_id.is_empty() {
            struct_ser.serialize_field("invocationId", &self.invocation_id)?;
        }
        if !self.environment.is_empty() {
            struct_ser.serialize_field("environment", &self.environment)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for InvocationEnv {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "enrichment",
            "event_id",
            "eventId",
            "invocation_id",
            "invocationId",
            "environment",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Enrichment,
            EventId,
            InvocationId,
            Environment,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
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
                            "enrichment" => Ok(GeneratedField::Enrichment),
                            "eventId" | "event_id" => Ok(GeneratedField::EventId),
                            "invocationId" | "invocation_id" => Ok(GeneratedField::InvocationId),
                            "environment" => Ok(GeneratedField::Environment),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = InvocationEnv;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.events.fusion.InvocationEnv")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<InvocationEnv, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut enrichment__ = None;
                let mut event_id__ = None;
                let mut invocation_id__ = None;
                let mut environment__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Enrichment => {
                            if enrichment__.is_some() {
                                return Err(serde::de::Error::duplicate_field("enrichment"));
                            }
                            enrichment__ = map_.next_value()?;
                        }
                        GeneratedField::EventId => {
                            if event_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("eventId"));
                            }
                            event_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::InvocationId => {
                            if invocation_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("invocationId"));
                            }
                            invocation_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Environment => {
                            if environment__.is_some() {
                                return Err(serde::de::Error::duplicate_field("environment"));
                            }
                            environment__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(InvocationEnv {
                    enrichment: enrichment__,
                    event_id: event_id__.unwrap_or_default(),
                    invocation_id: invocation_id__.unwrap_or_default(),
                    environment: environment__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.events.fusion.InvocationEnv", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Onboarding {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.enrichment.is_some() {
            len += 1;
        }
        if !self.event_id.is_empty() {
            len += 1;
        }
        if !self.invocation_id.is_empty() {
            len += 1;
        }
        if self.screen != 0 {
            len += 1;
        }
        if self.action != 0 {
            len += 1;
        }
        if self.success {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.events.fusion.Onboarding", len)?;
        if let Some(v) = self.enrichment.as_ref() {
            struct_ser.serialize_field("enrichment", v)?;
        }
        if !self.event_id.is_empty() {
            struct_ser.serialize_field("eventId", &self.event_id)?;
        }
        if !self.invocation_id.is_empty() {
            struct_ser.serialize_field("invocationId", &self.invocation_id)?;
        }
        if self.screen != 0 {
            let v = OnboardingScreen::try_from(self.screen)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.screen)))?;
            struct_ser.serialize_field("screen", &v)?;
        }
        if self.action != 0 {
            let v = OnboardingAction::try_from(self.action)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.action)))?;
            struct_ser.serialize_field("action", &v)?;
        }
        if self.success {
            struct_ser.serialize_field("success", &self.success)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Onboarding {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "enrichment",
            "event_id",
            "eventId",
            "invocation_id",
            "invocationId",
            "screen",
            "action",
            "success",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Enrichment,
            EventId,
            InvocationId,
            Screen,
            Action,
            Success,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
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
                            "enrichment" => Ok(GeneratedField::Enrichment),
                            "eventId" | "event_id" => Ok(GeneratedField::EventId),
                            "invocationId" | "invocation_id" => Ok(GeneratedField::InvocationId),
                            "screen" => Ok(GeneratedField::Screen),
                            "action" => Ok(GeneratedField::Action),
                            "success" => Ok(GeneratedField::Success),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Onboarding;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.events.fusion.Onboarding")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Onboarding, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut enrichment__ = None;
                let mut event_id__ = None;
                let mut invocation_id__ = None;
                let mut screen__ = None;
                let mut action__ = None;
                let mut success__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Enrichment => {
                            if enrichment__.is_some() {
                                return Err(serde::de::Error::duplicate_field("enrichment"));
                            }
                            enrichment__ = map_.next_value()?;
                        }
                        GeneratedField::EventId => {
                            if event_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("eventId"));
                            }
                            event_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::InvocationId => {
                            if invocation_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("invocationId"));
                            }
                            invocation_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Screen => {
                            if screen__.is_some() {
                                return Err(serde::de::Error::duplicate_field("screen"));
                            }
                            screen__ = Some(map_.next_value::<OnboardingScreen>()? as i32);
                        }
                        GeneratedField::Action => {
                            if action__.is_some() {
                                return Err(serde::de::Error::duplicate_field("action"));
                            }
                            action__ = Some(map_.next_value::<OnboardingAction>()? as i32);
                        }
                        GeneratedField::Success => {
                            if success__.is_some() {
                                return Err(serde::de::Error::duplicate_field("success"));
                            }
                            success__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Onboarding {
                    enrichment: enrichment__,
                    event_id: event_id__.unwrap_or_default(),
                    invocation_id: invocation_id__.unwrap_or_default(),
                    screen: screen__.unwrap_or_default(),
                    action: action__.unwrap_or_default(),
                    success: success__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.events.fusion.Onboarding", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for OnboardingAction {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::ActionUnspecified => "ACTION_UNSPECIFIED",
            Self::Initiated => "INITIATED",
            Self::ScreenShown => "SCREEN_SHOWN",
            Self::RunStarted => "RUN_STARTED",
            Self::RunFinished => "RUN_FINISHED",
            Self::StepCompleted => "STEP_COMPLETED",
            Self::StepFailed => "STEP_FAILED",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for OnboardingAction {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ACTION_UNSPECIFIED",
            "INITIATED",
            "SCREEN_SHOWN",
            "RUN_STARTED",
            "RUN_FINISHED",
            "STEP_COMPLETED",
            "STEP_FAILED",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = OnboardingAction;

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
                    "ACTION_UNSPECIFIED" => Ok(OnboardingAction::ActionUnspecified),
                    "INITIATED" => Ok(OnboardingAction::Initiated),
                    "SCREEN_SHOWN" => Ok(OnboardingAction::ScreenShown),
                    "RUN_STARTED" => Ok(OnboardingAction::RunStarted),
                    "RUN_FINISHED" => Ok(OnboardingAction::RunFinished),
                    "STEP_COMPLETED" => Ok(OnboardingAction::StepCompleted),
                    "STEP_FAILED" => Ok(OnboardingAction::StepFailed),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for OnboardingScreen {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::ScreenUnspecified => "SCREEN_UNSPECIFIED",
            Self::Welcome => "WELCOME",
            Self::ProfileCheck => "PROFILE_CHECK",
            Self::ProfileFound => "PROFILE_FOUND",
            Self::ProfileSetup => "PROFILE_SETUP",
            Self::LinkAccount => "LINK_ACCOUNT",
            Self::DbtParse => "DBT_PARSE",
            Self::ParseErrorAutofix => "PARSE_ERROR_AUTOFIX",
            Self::DbtParseRetry => "DBT_PARSE_RETRY",
            Self::ParseErrorFail => "PARSE_ERROR_FAIL",
            Self::CompileNoSa => "COMPILE_NO_SA",
            Self::CompileNoSaFail => "COMPILE_NO_SA_FAIL",
            Self::Compile => "COMPILE",
            Self::CompileFail => "COMPILE_FAIL",
            Self::Success => "SUCCESS",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for OnboardingScreen {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "SCREEN_UNSPECIFIED",
            "WELCOME",
            "PROFILE_CHECK",
            "PROFILE_FOUND",
            "PROFILE_SETUP",
            "LINK_ACCOUNT",
            "DBT_PARSE",
            "PARSE_ERROR_AUTOFIX",
            "DBT_PARSE_RETRY",
            "PARSE_ERROR_FAIL",
            "COMPILE_NO_SA",
            "COMPILE_NO_SA_FAIL",
            "COMPILE",
            "COMPILE_FAIL",
            "SUCCESS",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = OnboardingScreen;

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
                    "SCREEN_UNSPECIFIED" => Ok(OnboardingScreen::ScreenUnspecified),
                    "WELCOME" => Ok(OnboardingScreen::Welcome),
                    "PROFILE_CHECK" => Ok(OnboardingScreen::ProfileCheck),
                    "PROFILE_FOUND" => Ok(OnboardingScreen::ProfileFound),
                    "PROFILE_SETUP" => Ok(OnboardingScreen::ProfileSetup),
                    "LINK_ACCOUNT" => Ok(OnboardingScreen::LinkAccount),
                    "DBT_PARSE" => Ok(OnboardingScreen::DbtParse),
                    "PARSE_ERROR_AUTOFIX" => Ok(OnboardingScreen::ParseErrorAutofix),
                    "DBT_PARSE_RETRY" => Ok(OnboardingScreen::DbtParseRetry),
                    "PARSE_ERROR_FAIL" => Ok(OnboardingScreen::ParseErrorFail),
                    "COMPILE_NO_SA" => Ok(OnboardingScreen::CompileNoSa),
                    "COMPILE_NO_SA_FAIL" => Ok(OnboardingScreen::CompileNoSaFail),
                    "COMPILE" => Ok(OnboardingScreen::Compile),
                    "COMPILE_FAIL" => Ok(OnboardingScreen::CompileFail),
                    "SUCCESS" => Ok(OnboardingScreen::Success),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for PackageInstall {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.enrichment.is_some() {
            len += 1;
        }
        if !self.event_id.is_empty() {
            len += 1;
        }
        if !self.invocation_id.is_empty() {
            len += 1;
        }
        if !self.name.is_empty() {
            len += 1;
        }
        if !self.source.is_empty() {
            len += 1;
        }
        if !self.version.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.events.fusion.PackageInstall", len)?;
        if let Some(v) = self.enrichment.as_ref() {
            struct_ser.serialize_field("enrichment", v)?;
        }
        if !self.event_id.is_empty() {
            struct_ser.serialize_field("eventId", &self.event_id)?;
        }
        if !self.invocation_id.is_empty() {
            struct_ser.serialize_field("invocationId", &self.invocation_id)?;
        }
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if !self.source.is_empty() {
            struct_ser.serialize_field("source", &self.source)?;
        }
        if !self.version.is_empty() {
            struct_ser.serialize_field("version", &self.version)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PackageInstall {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "enrichment",
            "event_id",
            "eventId",
            "invocation_id",
            "invocationId",
            "name",
            "source",
            "version",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Enrichment,
            EventId,
            InvocationId,
            Name,
            Source,
            Version,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
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
                            "enrichment" => Ok(GeneratedField::Enrichment),
                            "eventId" | "event_id" => Ok(GeneratedField::EventId),
                            "invocationId" | "invocation_id" => Ok(GeneratedField::InvocationId),
                            "name" => Ok(GeneratedField::Name),
                            "source" => Ok(GeneratedField::Source),
                            "version" => Ok(GeneratedField::Version),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PackageInstall;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.events.fusion.PackageInstall")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PackageInstall, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut enrichment__ = None;
                let mut event_id__ = None;
                let mut invocation_id__ = None;
                let mut name__ = None;
                let mut source__ = None;
                let mut version__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Enrichment => {
                            if enrichment__.is_some() {
                                return Err(serde::de::Error::duplicate_field("enrichment"));
                            }
                            enrichment__ = map_.next_value()?;
                        }
                        GeneratedField::EventId => {
                            if event_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("eventId"));
                            }
                            event_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::InvocationId => {
                            if invocation_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("invocationId"));
                            }
                            invocation_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Source => {
                            if source__.is_some() {
                                return Err(serde::de::Error::duplicate_field("source"));
                            }
                            source__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Version => {
                            if version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("version"));
                            }
                            version__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(PackageInstall {
                    enrichment: enrichment__,
                    event_id: event_id__.unwrap_or_default(),
                    invocation_id: invocation_id__.unwrap_or_default(),
                    name: name__.unwrap_or_default(),
                    source: source__.unwrap_or_default(),
                    version: version__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.events.fusion.PackageInstall", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ResourceCounts {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.enrichment.is_some() {
            len += 1;
        }
        if !self.event_id.is_empty() {
            len += 1;
        }
        if !self.invocation_id.is_empty() {
            len += 1;
        }
        if self.models != 0 {
            len += 1;
        }
        if self.tests != 0 {
            len += 1;
        }
        if self.snapshots != 0 {
            len += 1;
        }
        if self.analyses != 0 {
            len += 1;
        }
        if self.macros != 0 {
            len += 1;
        }
        if self.operations != 0 {
            len += 1;
        }
        if self.seeds != 0 {
            len += 1;
        }
        if self.sources != 0 {
            len += 1;
        }
        if self.exposures != 0 {
            len += 1;
        }
        if self.metrics != 0 {
            len += 1;
        }
        if self.groups != 0 {
            len += 1;
        }
        if self.unit_tests != 0 {
            len += 1;
        }
        if self.semantic_models != 0 {
            len += 1;
        }
        if self.saved_queries != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.events.fusion.ResourceCounts", len)?;
        if let Some(v) = self.enrichment.as_ref() {
            struct_ser.serialize_field("enrichment", v)?;
        }
        if !self.event_id.is_empty() {
            struct_ser.serialize_field("eventId", &self.event_id)?;
        }
        if !self.invocation_id.is_empty() {
            struct_ser.serialize_field("invocationId", &self.invocation_id)?;
        }
        if self.models != 0 {
            struct_ser.serialize_field("models", &self.models)?;
        }
        if self.tests != 0 {
            struct_ser.serialize_field("tests", &self.tests)?;
        }
        if self.snapshots != 0 {
            struct_ser.serialize_field("snapshots", &self.snapshots)?;
        }
        if self.analyses != 0 {
            struct_ser.serialize_field("analyses", &self.analyses)?;
        }
        if self.macros != 0 {
            struct_ser.serialize_field("macros", &self.macros)?;
        }
        if self.operations != 0 {
            struct_ser.serialize_field("operations", &self.operations)?;
        }
        if self.seeds != 0 {
            struct_ser.serialize_field("seeds", &self.seeds)?;
        }
        if self.sources != 0 {
            struct_ser.serialize_field("sources", &self.sources)?;
        }
        if self.exposures != 0 {
            struct_ser.serialize_field("exposures", &self.exposures)?;
        }
        if self.metrics != 0 {
            struct_ser.serialize_field("metrics", &self.metrics)?;
        }
        if self.groups != 0 {
            struct_ser.serialize_field("groups", &self.groups)?;
        }
        if self.unit_tests != 0 {
            struct_ser.serialize_field("unitTests", &self.unit_tests)?;
        }
        if self.semantic_models != 0 {
            struct_ser.serialize_field("semanticModels", &self.semantic_models)?;
        }
        if self.saved_queries != 0 {
            struct_ser.serialize_field("savedQueries", &self.saved_queries)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ResourceCounts {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "enrichment",
            "event_id",
            "eventId",
            "invocation_id",
            "invocationId",
            "models",
            "tests",
            "snapshots",
            "analyses",
            "macros",
            "operations",
            "seeds",
            "sources",
            "exposures",
            "metrics",
            "groups",
            "unit_tests",
            "unitTests",
            "semantic_models",
            "semanticModels",
            "saved_queries",
            "savedQueries",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Enrichment,
            EventId,
            InvocationId,
            Models,
            Tests,
            Snapshots,
            Analyses,
            Macros,
            Operations,
            Seeds,
            Sources,
            Exposures,
            Metrics,
            Groups,
            UnitTests,
            SemanticModels,
            SavedQueries,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
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
                            "enrichment" => Ok(GeneratedField::Enrichment),
                            "eventId" | "event_id" => Ok(GeneratedField::EventId),
                            "invocationId" | "invocation_id" => Ok(GeneratedField::InvocationId),
                            "models" => Ok(GeneratedField::Models),
                            "tests" => Ok(GeneratedField::Tests),
                            "snapshots" => Ok(GeneratedField::Snapshots),
                            "analyses" => Ok(GeneratedField::Analyses),
                            "macros" => Ok(GeneratedField::Macros),
                            "operations" => Ok(GeneratedField::Operations),
                            "seeds" => Ok(GeneratedField::Seeds),
                            "sources" => Ok(GeneratedField::Sources),
                            "exposures" => Ok(GeneratedField::Exposures),
                            "metrics" => Ok(GeneratedField::Metrics),
                            "groups" => Ok(GeneratedField::Groups),
                            "unitTests" | "unit_tests" => Ok(GeneratedField::UnitTests),
                            "semanticModels" | "semantic_models" => Ok(GeneratedField::SemanticModels),
                            "savedQueries" | "saved_queries" => Ok(GeneratedField::SavedQueries),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ResourceCounts;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.events.fusion.ResourceCounts")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ResourceCounts, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut enrichment__ = None;
                let mut event_id__ = None;
                let mut invocation_id__ = None;
                let mut models__ = None;
                let mut tests__ = None;
                let mut snapshots__ = None;
                let mut analyses__ = None;
                let mut macros__ = None;
                let mut operations__ = None;
                let mut seeds__ = None;
                let mut sources__ = None;
                let mut exposures__ = None;
                let mut metrics__ = None;
                let mut groups__ = None;
                let mut unit_tests__ = None;
                let mut semantic_models__ = None;
                let mut saved_queries__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Enrichment => {
                            if enrichment__.is_some() {
                                return Err(serde::de::Error::duplicate_field("enrichment"));
                            }
                            enrichment__ = map_.next_value()?;
                        }
                        GeneratedField::EventId => {
                            if event_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("eventId"));
                            }
                            event_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::InvocationId => {
                            if invocation_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("invocationId"));
                            }
                            invocation_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Models => {
                            if models__.is_some() {
                                return Err(serde::de::Error::duplicate_field("models"));
                            }
                            models__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Tests => {
                            if tests__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tests"));
                            }
                            tests__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Snapshots => {
                            if snapshots__.is_some() {
                                return Err(serde::de::Error::duplicate_field("snapshots"));
                            }
                            snapshots__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Analyses => {
                            if analyses__.is_some() {
                                return Err(serde::de::Error::duplicate_field("analyses"));
                            }
                            analyses__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Macros => {
                            if macros__.is_some() {
                                return Err(serde::de::Error::duplicate_field("macros"));
                            }
                            macros__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Operations => {
                            if operations__.is_some() {
                                return Err(serde::de::Error::duplicate_field("operations"));
                            }
                            operations__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Seeds => {
                            if seeds__.is_some() {
                                return Err(serde::de::Error::duplicate_field("seeds"));
                            }
                            seeds__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Sources => {
                            if sources__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sources"));
                            }
                            sources__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Exposures => {
                            if exposures__.is_some() {
                                return Err(serde::de::Error::duplicate_field("exposures"));
                            }
                            exposures__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Metrics => {
                            if metrics__.is_some() {
                                return Err(serde::de::Error::duplicate_field("metrics"));
                            }
                            metrics__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Groups => {
                            if groups__.is_some() {
                                return Err(serde::de::Error::duplicate_field("groups"));
                            }
                            groups__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::UnitTests => {
                            if unit_tests__.is_some() {
                                return Err(serde::de::Error::duplicate_field("unitTests"));
                            }
                            unit_tests__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::SemanticModels => {
                            if semantic_models__.is_some() {
                                return Err(serde::de::Error::duplicate_field("semanticModels"));
                            }
                            semantic_models__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::SavedQueries => {
                            if saved_queries__.is_some() {
                                return Err(serde::de::Error::duplicate_field("savedQueries"));
                            }
                            saved_queries__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(ResourceCounts {
                    enrichment: enrichment__,
                    event_id: event_id__.unwrap_or_default(),
                    invocation_id: invocation_id__.unwrap_or_default(),
                    models: models__.unwrap_or_default(),
                    tests: tests__.unwrap_or_default(),
                    snapshots: snapshots__.unwrap_or_default(),
                    analyses: analyses__.unwrap_or_default(),
                    macros: macros__.unwrap_or_default(),
                    operations: operations__.unwrap_or_default(),
                    seeds: seeds__.unwrap_or_default(),
                    sources: sources__.unwrap_or_default(),
                    exposures: exposures__.unwrap_or_default(),
                    metrics: metrics__.unwrap_or_default(),
                    groups: groups__.unwrap_or_default(),
                    unit_tests: unit_tests__.unwrap_or_default(),
                    semantic_models: semantic_models__.unwrap_or_default(),
                    saved_queries: saved_queries__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.events.fusion.ResourceCounts", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RunModel {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.enrichment.is_some() {
            len += 1;
        }
        if !self.event_id.is_empty() {
            len += 1;
        }
        if !self.invocation_id.is_empty() {
            len += 1;
        }
        if self.index != 0 {
            len += 1;
        }
        if self.total != 0 {
            len += 1;
        }
        if self.execution_time != 0. {
            len += 1;
        }
        if !self.run_status.is_empty() {
            len += 1;
        }
        if self.run_skipped {
            len += 1;
        }
        if !self.model_materialization.is_empty() {
            len += 1;
        }
        if !self.model_incremental_strategy.is_empty() {
            len += 1;
        }
        if !self.model_id.is_empty() {
            len += 1;
        }
        if !self.hashed_contents.is_empty() {
            len += 1;
        }
        if !self.language.is_empty() {
            len += 1;
        }
        if self.has_group {
            len += 1;
        }
        if self.contract_enforced {
            len += 1;
        }
        if !self.access.is_empty() {
            len += 1;
        }
        if self.versioned {
            len += 1;
        }
        if !self.run_skipped_reason.is_empty() {
            len += 1;
        }
        if !self.run_model_id.is_empty() {
            len += 1;
        }
        if !self.resource_type.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.events.fusion.RunModel", len)?;
        if let Some(v) = self.enrichment.as_ref() {
            struct_ser.serialize_field("enrichment", v)?;
        }
        if !self.event_id.is_empty() {
            struct_ser.serialize_field("eventId", &self.event_id)?;
        }
        if !self.invocation_id.is_empty() {
            struct_ser.serialize_field("invocationId", &self.invocation_id)?;
        }
        if self.index != 0 {
            struct_ser.serialize_field("index", &self.index)?;
        }
        if self.total != 0 {
            struct_ser.serialize_field("total", &self.total)?;
        }
        if self.execution_time != 0. {
            struct_ser.serialize_field("executionTime", &self.execution_time)?;
        }
        if !self.run_status.is_empty() {
            struct_ser.serialize_field("runStatus", &self.run_status)?;
        }
        if self.run_skipped {
            struct_ser.serialize_field("runSkipped", &self.run_skipped)?;
        }
        if !self.model_materialization.is_empty() {
            struct_ser.serialize_field("modelMaterialization", &self.model_materialization)?;
        }
        if !self.model_incremental_strategy.is_empty() {
            struct_ser.serialize_field("modelIncrementalStrategy", &self.model_incremental_strategy)?;
        }
        if !self.model_id.is_empty() {
            struct_ser.serialize_field("modelId", &self.model_id)?;
        }
        if !self.hashed_contents.is_empty() {
            struct_ser.serialize_field("hashedContents", &self.hashed_contents)?;
        }
        if !self.language.is_empty() {
            struct_ser.serialize_field("language", &self.language)?;
        }
        if self.has_group {
            struct_ser.serialize_field("hasGroup", &self.has_group)?;
        }
        if self.contract_enforced {
            struct_ser.serialize_field("contractEnforced", &self.contract_enforced)?;
        }
        if !self.access.is_empty() {
            struct_ser.serialize_field("access", &self.access)?;
        }
        if self.versioned {
            struct_ser.serialize_field("versioned", &self.versioned)?;
        }
        if !self.run_skipped_reason.is_empty() {
            struct_ser.serialize_field("runSkippedReason", &self.run_skipped_reason)?;
        }
        if !self.run_model_id.is_empty() {
            struct_ser.serialize_field("runModelId", &self.run_model_id)?;
        }
        if !self.resource_type.is_empty() {
            struct_ser.serialize_field("resourceType", &self.resource_type)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RunModel {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "enrichment",
            "event_id",
            "eventId",
            "invocation_id",
            "invocationId",
            "index",
            "total",
            "execution_time",
            "executionTime",
            "run_status",
            "runStatus",
            "run_skipped",
            "runSkipped",
            "model_materialization",
            "modelMaterialization",
            "model_incremental_strategy",
            "modelIncrementalStrategy",
            "model_id",
            "modelId",
            "hashed_contents",
            "hashedContents",
            "language",
            "has_group",
            "hasGroup",
            "contract_enforced",
            "contractEnforced",
            "access",
            "versioned",
            "run_skipped_reason",
            "runSkippedReason",
            "run_model_id",
            "runModelId",
            "resource_type",
            "resourceType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Enrichment,
            EventId,
            InvocationId,
            Index,
            Total,
            ExecutionTime,
            RunStatus,
            RunSkipped,
            ModelMaterialization,
            ModelIncrementalStrategy,
            ModelId,
            HashedContents,
            Language,
            HasGroup,
            ContractEnforced,
            Access,
            Versioned,
            RunSkippedReason,
            RunModelId,
            ResourceType,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
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
                            "enrichment" => Ok(GeneratedField::Enrichment),
                            "eventId" | "event_id" => Ok(GeneratedField::EventId),
                            "invocationId" | "invocation_id" => Ok(GeneratedField::InvocationId),
                            "index" => Ok(GeneratedField::Index),
                            "total" => Ok(GeneratedField::Total),
                            "executionTime" | "execution_time" => Ok(GeneratedField::ExecutionTime),
                            "runStatus" | "run_status" => Ok(GeneratedField::RunStatus),
                            "runSkipped" | "run_skipped" => Ok(GeneratedField::RunSkipped),
                            "modelMaterialization" | "model_materialization" => Ok(GeneratedField::ModelMaterialization),
                            "modelIncrementalStrategy" | "model_incremental_strategy" => Ok(GeneratedField::ModelIncrementalStrategy),
                            "modelId" | "model_id" => Ok(GeneratedField::ModelId),
                            "hashedContents" | "hashed_contents" => Ok(GeneratedField::HashedContents),
                            "language" => Ok(GeneratedField::Language),
                            "hasGroup" | "has_group" => Ok(GeneratedField::HasGroup),
                            "contractEnforced" | "contract_enforced" => Ok(GeneratedField::ContractEnforced),
                            "access" => Ok(GeneratedField::Access),
                            "versioned" => Ok(GeneratedField::Versioned),
                            "runSkippedReason" | "run_skipped_reason" => Ok(GeneratedField::RunSkippedReason),
                            "runModelId" | "run_model_id" => Ok(GeneratedField::RunModelId),
                            "resourceType" | "resource_type" => Ok(GeneratedField::ResourceType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RunModel;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.events.fusion.RunModel")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<RunModel, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut enrichment__ = None;
                let mut event_id__ = None;
                let mut invocation_id__ = None;
                let mut index__ = None;
                let mut total__ = None;
                let mut execution_time__ = None;
                let mut run_status__ = None;
                let mut run_skipped__ = None;
                let mut model_materialization__ = None;
                let mut model_incremental_strategy__ = None;
                let mut model_id__ = None;
                let mut hashed_contents__ = None;
                let mut language__ = None;
                let mut has_group__ = None;
                let mut contract_enforced__ = None;
                let mut access__ = None;
                let mut versioned__ = None;
                let mut run_skipped_reason__ = None;
                let mut run_model_id__ = None;
                let mut resource_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Enrichment => {
                            if enrichment__.is_some() {
                                return Err(serde::de::Error::duplicate_field("enrichment"));
                            }
                            enrichment__ = map_.next_value()?;
                        }
                        GeneratedField::EventId => {
                            if event_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("eventId"));
                            }
                            event_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::InvocationId => {
                            if invocation_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("invocationId"));
                            }
                            invocation_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Index => {
                            if index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("index"));
                            }
                            index__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Total => {
                            if total__.is_some() {
                                return Err(serde::de::Error::duplicate_field("total"));
                            }
                            total__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ExecutionTime => {
                            if execution_time__.is_some() {
                                return Err(serde::de::Error::duplicate_field("executionTime"));
                            }
                            execution_time__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::RunStatus => {
                            if run_status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("runStatus"));
                            }
                            run_status__ = Some(map_.next_value()?);
                        }
                        GeneratedField::RunSkipped => {
                            if run_skipped__.is_some() {
                                return Err(serde::de::Error::duplicate_field("runSkipped"));
                            }
                            run_skipped__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ModelMaterialization => {
                            if model_materialization__.is_some() {
                                return Err(serde::de::Error::duplicate_field("modelMaterialization"));
                            }
                            model_materialization__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ModelIncrementalStrategy => {
                            if model_incremental_strategy__.is_some() {
                                return Err(serde::de::Error::duplicate_field("modelIncrementalStrategy"));
                            }
                            model_incremental_strategy__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ModelId => {
                            if model_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("modelId"));
                            }
                            model_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::HashedContents => {
                            if hashed_contents__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hashedContents"));
                            }
                            hashed_contents__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Language => {
                            if language__.is_some() {
                                return Err(serde::de::Error::duplicate_field("language"));
                            }
                            language__ = Some(map_.next_value()?);
                        }
                        GeneratedField::HasGroup => {
                            if has_group__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hasGroup"));
                            }
                            has_group__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ContractEnforced => {
                            if contract_enforced__.is_some() {
                                return Err(serde::de::Error::duplicate_field("contractEnforced"));
                            }
                            contract_enforced__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Access => {
                            if access__.is_some() {
                                return Err(serde::de::Error::duplicate_field("access"));
                            }
                            access__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Versioned => {
                            if versioned__.is_some() {
                                return Err(serde::de::Error::duplicate_field("versioned"));
                            }
                            versioned__ = Some(map_.next_value()?);
                        }
                        GeneratedField::RunSkippedReason => {
                            if run_skipped_reason__.is_some() {
                                return Err(serde::de::Error::duplicate_field("runSkippedReason"));
                            }
                            run_skipped_reason__ = Some(map_.next_value()?);
                        }
                        GeneratedField::RunModelId => {
                            if run_model_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("runModelId"));
                            }
                            run_model_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ResourceType => {
                            if resource_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resourceType"));
                            }
                            resource_type__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(RunModel {
                    enrichment: enrichment__,
                    event_id: event_id__.unwrap_or_default(),
                    invocation_id: invocation_id__.unwrap_or_default(),
                    index: index__.unwrap_or_default(),
                    total: total__.unwrap_or_default(),
                    execution_time: execution_time__.unwrap_or_default(),
                    run_status: run_status__.unwrap_or_default(),
                    run_skipped: run_skipped__.unwrap_or_default(),
                    model_materialization: model_materialization__.unwrap_or_default(),
                    model_incremental_strategy: model_incremental_strategy__.unwrap_or_default(),
                    model_id: model_id__.unwrap_or_default(),
                    hashed_contents: hashed_contents__.unwrap_or_default(),
                    language: language__.unwrap_or_default(),
                    has_group: has_group__.unwrap_or_default(),
                    contract_enforced: contract_enforced__.unwrap_or_default(),
                    access: access__.unwrap_or_default(),
                    versioned: versioned__.unwrap_or_default(),
                    run_skipped_reason: run_skipped_reason__.unwrap_or_default(),
                    run_model_id: run_model_id__.unwrap_or_default(),
                    resource_type: resource_type__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.events.fusion.RunModel", FIELDS, GeneratedVisitor)
    }
}
