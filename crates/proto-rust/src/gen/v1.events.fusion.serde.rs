// @generated
impl serde::Serialize for CloudInvocation {
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
        if !self.invocation_id.is_empty() {
            len += 1;
        }
        if !self.dbt_cloud_account_identifier.is_empty() {
            len += 1;
        }
        if !self.dbt_cloud_project_id.is_empty() {
            len += 1;
        }
        if !self.dbt_cloud_environment_id.is_empty() {
            len += 1;
        }
        if !self.dbt_cloud_job_id.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.events.fusion.CloudInvocation", len)?;
        if let Some(v) = self.enrichment.as_ref() {
            struct_ser.serialize_field("enrichment", v)?;
        }
        if !self.invocation_id.is_empty() {
            struct_ser.serialize_field("invocationId", &self.invocation_id)?;
        }
        if !self.dbt_cloud_account_identifier.is_empty() {
            struct_ser.serialize_field("dbtCloudAccountIdentifier", &self.dbt_cloud_account_identifier)?;
        }
        if !self.dbt_cloud_project_id.is_empty() {
            struct_ser.serialize_field("dbtCloudProjectId", &self.dbt_cloud_project_id)?;
        }
        if !self.dbt_cloud_environment_id.is_empty() {
            struct_ser.serialize_field("dbtCloudEnvironmentId", &self.dbt_cloud_environment_id)?;
        }
        if !self.dbt_cloud_job_id.is_empty() {
            struct_ser.serialize_field("dbtCloudJobId", &self.dbt_cloud_job_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CloudInvocation {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "enrichment",
            "invocation_id",
            "invocationId",
            "dbt_cloud_account_identifier",
            "dbtCloudAccountIdentifier",
            "dbt_cloud_project_id",
            "dbtCloudProjectId",
            "dbt_cloud_environment_id",
            "dbtCloudEnvironmentId",
            "dbt_cloud_job_id",
            "dbtCloudJobId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Enrichment,
            InvocationId,
            DbtCloudAccountIdentifier,
            DbtCloudProjectId,
            DbtCloudEnvironmentId,
            DbtCloudJobId,
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
                            "enrichment" => Ok(GeneratedField::Enrichment),
                            "invocationId" | "invocation_id" => Ok(GeneratedField::InvocationId),
                            "dbtCloudAccountIdentifier" | "dbt_cloud_account_identifier" => Ok(GeneratedField::DbtCloudAccountIdentifier),
                            "dbtCloudProjectId" | "dbt_cloud_project_id" => Ok(GeneratedField::DbtCloudProjectId),
                            "dbtCloudEnvironmentId" | "dbt_cloud_environment_id" => Ok(GeneratedField::DbtCloudEnvironmentId),
                            "dbtCloudJobId" | "dbt_cloud_job_id" => Ok(GeneratedField::DbtCloudJobId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CloudInvocation;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.events.fusion.CloudInvocation")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CloudInvocation, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut enrichment__ = None;
                let mut invocation_id__ = None;
                let mut dbt_cloud_account_identifier__ = None;
                let mut dbt_cloud_project_id__ = None;
                let mut dbt_cloud_environment_id__ = None;
                let mut dbt_cloud_job_id__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Enrichment => {
                            if enrichment__.is_some() {
                                return Err(serde::de::Error::duplicate_field("enrichment"));
                            }
                            enrichment__ = map_.next_value()?;
                        }
                        GeneratedField::InvocationId => {
                            if invocation_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("invocationId"));
                            }
                            invocation_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DbtCloudAccountIdentifier => {
                            if dbt_cloud_account_identifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dbtCloudAccountIdentifier"));
                            }
                            dbt_cloud_account_identifier__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DbtCloudProjectId => {
                            if dbt_cloud_project_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dbtCloudProjectId"));
                            }
                            dbt_cloud_project_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DbtCloudEnvironmentId => {
                            if dbt_cloud_environment_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dbtCloudEnvironmentId"));
                            }
                            dbt_cloud_environment_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DbtCloudJobId => {
                            if dbt_cloud_job_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dbtCloudJobId"));
                            }
                            dbt_cloud_job_id__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(CloudInvocation {
                    enrichment: enrichment__,
                    invocation_id: invocation_id__.unwrap_or_default(),
                    dbt_cloud_account_identifier: dbt_cloud_account_identifier__.unwrap_or_default(),
                    dbt_cloud_project_id: dbt_cloud_project_id__.unwrap_or_default(),
                    dbt_cloud_environment_id: dbt_cloud_environment_id__.unwrap_or_default(),
                    dbt_cloud_job_id: dbt_cloud_job_id__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.events.fusion.CloudInvocation", FIELDS, GeneratedVisitor)
    }
}
