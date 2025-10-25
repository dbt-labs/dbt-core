impl serde::Serialize for NodeCacheDetail {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_cache_reason != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.events.fusion.node.NodeCacheDetail", len)?;
        if self.node_cache_reason != 0 {
            let v = NodeCacheReason::try_from(self.node_cache_reason)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.node_cache_reason)))?;
            struct_ser.serialize_field("node_cache_reason", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for NodeCacheDetail {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node_cache_reason",
            "nodeCacheReason",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeCacheReason,
            __SkipField__,
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
                            "nodeCacheReason" | "node_cache_reason" => Ok(GeneratedField::NodeCacheReason),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NodeCacheDetail;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.events.fusion.node.NodeCacheDetail")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<NodeCacheDetail, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_cache_reason__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NodeCacheReason => {
                            if node_cache_reason__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeCacheReason"));
                            }
                            node_cache_reason__ = Some(map_.next_value::<NodeCacheReason>()? as i32);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(NodeCacheDetail {
                    node_cache_reason: node_cache_reason__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.events.fusion.node.NodeCacheDetail", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for NodeCacheReason {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::NoChanges => "NODE_CACHE_REASON_NO_CHANGES",
            Self::StillFresh => "NODE_CACHE_REASON_STILL_FRESH",
            Self::UpdateCriteriaNotMet => "NODE_CACHE_REASON_UPDATE_CRITERIA_NOT_MET",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for NodeCacheReason {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "NODE_CACHE_REASON_NO_CHANGES",
            "NODE_CACHE_REASON_STILL_FRESH",
            "NODE_CACHE_REASON_UPDATE_CRITERIA_NOT_MET",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NodeCacheReason;

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
                    "NODE_CACHE_REASON_NO_CHANGES" => Ok(NodeCacheReason::NoChanges),
                    "NODE_CACHE_REASON_STILL_FRESH" => Ok(NodeCacheReason::StillFresh),
                    "NODE_CACHE_REASON_UPDATE_CRITERIA_NOT_MET" => Ok(NodeCacheReason::UpdateCriteriaNotMet),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for NodeCancelReason {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::UserCancelled => "NODE_CANCEL_REASON_USER_CANCELLED",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for NodeCancelReason {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "NODE_CANCEL_REASON_USER_CANCELLED",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NodeCancelReason;

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
                    "NODE_CANCEL_REASON_USER_CANCELLED" => Ok(NodeCancelReason::UserCancelled),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for NodeErrorType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Internal => "NODE_ERROR_TYPE_INTERNAL",
            Self::External => "NODE_ERROR_TYPE_EXTERNAL",
            Self::User => "NODE_ERROR_TYPE_USER",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for NodeErrorType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "NODE_ERROR_TYPE_INTERNAL",
            "NODE_ERROR_TYPE_EXTERNAL",
            "NODE_ERROR_TYPE_USER",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NodeErrorType;

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
                    "NODE_ERROR_TYPE_INTERNAL" => Ok(NodeErrorType::Internal),
                    "NODE_ERROR_TYPE_EXTERNAL" => Ok(NodeErrorType::External),
                    "NODE_ERROR_TYPE_USER" => Ok(NodeErrorType::User),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for NodeEvaluated {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.unique_id.is_empty() {
            len += 1;
        }
        if !self.name.is_empty() {
            len += 1;
        }
        if self.database.is_some() {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        if self.identifier.is_some() {
            len += 1;
        }
        if self.materialization.is_some() {
            len += 1;
        }
        if self.custom_materialization.is_some() {
            len += 1;
        }
        if self.node_type != 0 {
            len += 1;
        }
        if self.node_outcome != 0 {
            len += 1;
        }
        if self.phase != 0 {
            len += 1;
        }
        if self.node_error_type.is_some() {
            len += 1;
        }
        if self.node_cancel_reason.is_some() {
            len += 1;
        }
        if self.node_skip_reason.is_some() {
            len += 1;
        }
        if self.dbt_core_event_code.is_some() {
            len += 1;
        }
        if self.node_outcome_detail.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.events.fusion.node.NodeEvaluated", len)?;
        if !self.unique_id.is_empty() {
            struct_ser.serialize_field("unique_id", &self.unique_id)?;
        }
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if let Some(v) = self.database.as_ref() {
            struct_ser.serialize_field("database", v)?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        if let Some(v) = self.identifier.as_ref() {
            struct_ser.serialize_field("identifier", v)?;
        }
        if let Some(v) = self.materialization.as_ref() {
            let v = NodeMaterialization::try_from(*v)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
            struct_ser.serialize_field("materialization", &v)?;
        }
        if let Some(v) = self.custom_materialization.as_ref() {
            struct_ser.serialize_field("custom_materialization", v)?;
        }
        if self.node_type != 0 {
            let v = NodeType::try_from(self.node_type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.node_type)))?;
            struct_ser.serialize_field("node_type", &v)?;
        }
        if self.node_outcome != 0 {
            let v = NodeOutcome::try_from(self.node_outcome)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.node_outcome)))?;
            struct_ser.serialize_field("node_outcome", &v)?;
        }
        if self.phase != 0 {
            let v = super::phase::ExecutionPhase::try_from(self.phase)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.phase)))?;
            struct_ser.serialize_field("phase", &v)?;
        }
        if let Some(v) = self.node_error_type.as_ref() {
            let v = NodeErrorType::try_from(*v)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
            struct_ser.serialize_field("node_error_type", &v)?;
        }
        if let Some(v) = self.node_cancel_reason.as_ref() {
            let v = NodeCancelReason::try_from(*v)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
            struct_ser.serialize_field("node_cancel_reason", &v)?;
        }
        if let Some(v) = self.node_skip_reason.as_ref() {
            let v = NodeSkipReason::try_from(*v)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
            struct_ser.serialize_field("node_skip_reason", &v)?;
        }
        if let Some(v) = self.dbt_core_event_code.as_ref() {
            struct_ser.serialize_field("dbt_core_event_code", v)?;
        }
        if let Some(v) = self.node_outcome_detail.as_ref() {
            match v {
                node_evaluated::NodeOutcomeDetail::NodeCacheDetail(v) => {
                    struct_ser.serialize_field("node_cache_detail", v)?;
                }
                node_evaluated::NodeOutcomeDetail::NodeTestDetail(v) => {
                    struct_ser.serialize_field("node_test_detail", v)?;
                }
                node_evaluated::NodeOutcomeDetail::NodeFreshnessOutcome(v) => {
                    struct_ser.serialize_field("node_freshness_outcome", v)?;
                }
                node_evaluated::NodeOutcomeDetail::NodeSkipUpstreamDetail(v) => {
                    struct_ser.serialize_field("node_skip_upstream_detail", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for NodeEvaluated {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "unique_id",
            "uniqueId",
            "name",
            "database",
            "schema",
            "identifier",
            "materialization",
            "custom_materialization",
            "customMaterialization",
            "node_type",
            "nodeType",
            "node_outcome",
            "nodeOutcome",
            "phase",
            "node_error_type",
            "nodeErrorType",
            "node_cancel_reason",
            "nodeCancelReason",
            "node_skip_reason",
            "nodeSkipReason",
            "dbt_core_event_code",
            "dbtCoreEventCode",
            "node_cache_detail",
            "nodeCacheDetail",
            "node_test_detail",
            "nodeTestDetail",
            "node_freshness_outcome",
            "nodeFreshnessOutcome",
            "node_skip_upstream_detail",
            "nodeSkipUpstreamDetail",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            UniqueId,
            Name,
            Database,
            Schema,
            Identifier,
            Materialization,
            CustomMaterialization,
            NodeType,
            NodeOutcome,
            Phase,
            NodeErrorType,
            NodeCancelReason,
            NodeSkipReason,
            DbtCoreEventCode,
            NodeCacheDetail,
            NodeTestDetail,
            NodeFreshnessOutcome,
            NodeSkipUpstreamDetail,
            __SkipField__,
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
                            "uniqueId" | "unique_id" => Ok(GeneratedField::UniqueId),
                            "name" => Ok(GeneratedField::Name),
                            "database" => Ok(GeneratedField::Database),
                            "schema" => Ok(GeneratedField::Schema),
                            "identifier" => Ok(GeneratedField::Identifier),
                            "materialization" => Ok(GeneratedField::Materialization),
                            "customMaterialization" | "custom_materialization" => Ok(GeneratedField::CustomMaterialization),
                            "nodeType" | "node_type" => Ok(GeneratedField::NodeType),
                            "nodeOutcome" | "node_outcome" => Ok(GeneratedField::NodeOutcome),
                            "phase" => Ok(GeneratedField::Phase),
                            "nodeErrorType" | "node_error_type" => Ok(GeneratedField::NodeErrorType),
                            "nodeCancelReason" | "node_cancel_reason" => Ok(GeneratedField::NodeCancelReason),
                            "nodeSkipReason" | "node_skip_reason" => Ok(GeneratedField::NodeSkipReason),
                            "dbtCoreEventCode" | "dbt_core_event_code" => Ok(GeneratedField::DbtCoreEventCode),
                            "nodeCacheDetail" | "node_cache_detail" => Ok(GeneratedField::NodeCacheDetail),
                            "nodeTestDetail" | "node_test_detail" => Ok(GeneratedField::NodeTestDetail),
                            "nodeFreshnessOutcome" | "node_freshness_outcome" => Ok(GeneratedField::NodeFreshnessOutcome),
                            "nodeSkipUpstreamDetail" | "node_skip_upstream_detail" => Ok(GeneratedField::NodeSkipUpstreamDetail),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NodeEvaluated;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.events.fusion.node.NodeEvaluated")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<NodeEvaluated, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut unique_id__ = None;
                let mut name__ = None;
                let mut database__ = None;
                let mut schema__ = None;
                let mut identifier__ = None;
                let mut materialization__ = None;
                let mut custom_materialization__ = None;
                let mut node_type__ = None;
                let mut node_outcome__ = None;
                let mut phase__ = None;
                let mut node_error_type__ = None;
                let mut node_cancel_reason__ = None;
                let mut node_skip_reason__ = None;
                let mut dbt_core_event_code__ = None;
                let mut node_outcome_detail__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::UniqueId => {
                            if unique_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uniqueId"));
                            }
                            unique_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Database => {
                            if database__.is_some() {
                                return Err(serde::de::Error::duplicate_field("database"));
                            }
                            database__ = map_.next_value()?;
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map_.next_value()?;
                        }
                        GeneratedField::Identifier => {
                            if identifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("identifier"));
                            }
                            identifier__ = map_.next_value()?;
                        }
                        GeneratedField::Materialization => {
                            if materialization__.is_some() {
                                return Err(serde::de::Error::duplicate_field("materialization"));
                            }
                            materialization__ = map_.next_value::<::std::option::Option<NodeMaterialization>>()?.map(|x| x as i32);
                        }
                        GeneratedField::CustomMaterialization => {
                            if custom_materialization__.is_some() {
                                return Err(serde::de::Error::duplicate_field("customMaterialization"));
                            }
                            custom_materialization__ = map_.next_value()?;
                        }
                        GeneratedField::NodeType => {
                            if node_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeType"));
                            }
                            node_type__ = Some(map_.next_value::<NodeType>()? as i32);
                        }
                        GeneratedField::NodeOutcome => {
                            if node_outcome__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeOutcome"));
                            }
                            node_outcome__ = Some(map_.next_value::<NodeOutcome>()? as i32);
                        }
                        GeneratedField::Phase => {
                            if phase__.is_some() {
                                return Err(serde::de::Error::duplicate_field("phase"));
                            }
                            phase__ = Some(map_.next_value::<super::phase::ExecutionPhase>()? as i32);
                        }
                        GeneratedField::NodeErrorType => {
                            if node_error_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeErrorType"));
                            }
                            node_error_type__ = map_.next_value::<::std::option::Option<NodeErrorType>>()?.map(|x| x as i32);
                        }
                        GeneratedField::NodeCancelReason => {
                            if node_cancel_reason__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeCancelReason"));
                            }
                            node_cancel_reason__ = map_.next_value::<::std::option::Option<NodeCancelReason>>()?.map(|x| x as i32);
                        }
                        GeneratedField::NodeSkipReason => {
                            if node_skip_reason__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeSkipReason"));
                            }
                            node_skip_reason__ = map_.next_value::<::std::option::Option<NodeSkipReason>>()?.map(|x| x as i32);
                        }
                        GeneratedField::DbtCoreEventCode => {
                            if dbt_core_event_code__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dbtCoreEventCode"));
                            }
                            dbt_core_event_code__ = map_.next_value()?;
                        }
                        GeneratedField::NodeCacheDetail => {
                            if node_outcome_detail__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeCacheDetail"));
                            }
                            node_outcome_detail__ = map_.next_value::<::std::option::Option<_>>()?.map(node_evaluated::NodeOutcomeDetail::NodeCacheDetail)
;
                        }
                        GeneratedField::NodeTestDetail => {
                            if node_outcome_detail__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeTestDetail"));
                            }
                            node_outcome_detail__ = map_.next_value::<::std::option::Option<_>>()?.map(node_evaluated::NodeOutcomeDetail::NodeTestDetail)
;
                        }
                        GeneratedField::NodeFreshnessOutcome => {
                            if node_outcome_detail__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeFreshnessOutcome"));
                            }
                            node_outcome_detail__ = map_.next_value::<::std::option::Option<_>>()?.map(node_evaluated::NodeOutcomeDetail::NodeFreshnessOutcome)
;
                        }
                        GeneratedField::NodeSkipUpstreamDetail => {
                            if node_outcome_detail__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeSkipUpstreamDetail"));
                            }
                            node_outcome_detail__ = map_.next_value::<::std::option::Option<_>>()?.map(node_evaluated::NodeOutcomeDetail::NodeSkipUpstreamDetail)
;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(NodeEvaluated {
                    unique_id: unique_id__.unwrap_or_default(),
                    name: name__.unwrap_or_default(),
                    database: database__,
                    schema: schema__,
                    identifier: identifier__,
                    materialization: materialization__,
                    custom_materialization: custom_materialization__,
                    node_type: node_type__.unwrap_or_default(),
                    node_outcome: node_outcome__.unwrap_or_default(),
                    phase: phase__.unwrap_or_default(),
                    node_error_type: node_error_type__,
                    node_cancel_reason: node_cancel_reason__,
                    node_skip_reason: node_skip_reason__,
                    dbt_core_event_code: dbt_core_event_code__,
                    node_outcome_detail: node_outcome_detail__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.events.fusion.node.NodeEvaluated", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for NodeMaterialization {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unknown => "NODE_MATERIALIZATION_UNKNOWN",
            Self::Snapshot => "NODE_MATERIALIZATION_SNAPSHOT",
            Self::Seed => "NODE_MATERIALIZATION_SEED",
            Self::View => "NODE_MATERIALIZATION_VIEW",
            Self::Table => "NODE_MATERIALIZATION_TABLE",
            Self::Incremental => "NODE_MATERIALIZATION_INCREMENTAL",
            Self::MaterializedView => "NODE_MATERIALIZATION_MATERIALIZED_VIEW",
            Self::External => "NODE_MATERIALIZATION_EXTERNAL",
            Self::Test => "NODE_MATERIALIZATION_TEST",
            Self::Ephemeral => "NODE_MATERIALIZATION_EPHEMERAL",
            Self::Unit => "NODE_MATERIALIZATION_UNIT",
            Self::Analysis => "NODE_MATERIALIZATION_ANALYSIS",
            Self::StreamingTable => "NODE_MATERIALIZATION_STREAMING_TABLE",
            Self::DynamicTable => "NODE_MATERIALIZATION_DYNAMIC_TABLE",
            Self::Function => "NODE_MATERIALIZATION_FUNCTION",
            Self::Custom => "NODE_MATERIALIZATION_CUSTOM",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for NodeMaterialization {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "NODE_MATERIALIZATION_UNKNOWN",
            "NODE_MATERIALIZATION_SNAPSHOT",
            "NODE_MATERIALIZATION_SEED",
            "NODE_MATERIALIZATION_VIEW",
            "NODE_MATERIALIZATION_TABLE",
            "NODE_MATERIALIZATION_INCREMENTAL",
            "NODE_MATERIALIZATION_MATERIALIZED_VIEW",
            "NODE_MATERIALIZATION_EXTERNAL",
            "NODE_MATERIALIZATION_TEST",
            "NODE_MATERIALIZATION_EPHEMERAL",
            "NODE_MATERIALIZATION_UNIT",
            "NODE_MATERIALIZATION_ANALYSIS",
            "NODE_MATERIALIZATION_STREAMING_TABLE",
            "NODE_MATERIALIZATION_DYNAMIC_TABLE",
            "NODE_MATERIALIZATION_FUNCTION",
            "NODE_MATERIALIZATION_CUSTOM",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NodeMaterialization;

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
                    "NODE_MATERIALIZATION_UNKNOWN" => Ok(NodeMaterialization::Unknown),
                    "NODE_MATERIALIZATION_SNAPSHOT" => Ok(NodeMaterialization::Snapshot),
                    "NODE_MATERIALIZATION_SEED" => Ok(NodeMaterialization::Seed),
                    "NODE_MATERIALIZATION_VIEW" => Ok(NodeMaterialization::View),
                    "NODE_MATERIALIZATION_TABLE" => Ok(NodeMaterialization::Table),
                    "NODE_MATERIALIZATION_INCREMENTAL" => Ok(NodeMaterialization::Incremental),
                    "NODE_MATERIALIZATION_MATERIALIZED_VIEW" => Ok(NodeMaterialization::MaterializedView),
                    "NODE_MATERIALIZATION_EXTERNAL" => Ok(NodeMaterialization::External),
                    "NODE_MATERIALIZATION_TEST" => Ok(NodeMaterialization::Test),
                    "NODE_MATERIALIZATION_EPHEMERAL" => Ok(NodeMaterialization::Ephemeral),
                    "NODE_MATERIALIZATION_UNIT" => Ok(NodeMaterialization::Unit),
                    "NODE_MATERIALIZATION_ANALYSIS" => Ok(NodeMaterialization::Analysis),
                    "NODE_MATERIALIZATION_STREAMING_TABLE" => Ok(NodeMaterialization::StreamingTable),
                    "NODE_MATERIALIZATION_DYNAMIC_TABLE" => Ok(NodeMaterialization::DynamicTable),
                    "NODE_MATERIALIZATION_FUNCTION" => Ok(NodeMaterialization::Function),
                    "NODE_MATERIALIZATION_CUSTOM" => Ok(NodeMaterialization::Custom),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for NodeOutcome {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "NODE_OUTCOME_UNSPECIFIED",
            Self::Success => "NODE_OUTCOME_SUCCESS",
            Self::Error => "NODE_OUTCOME_ERROR",
            Self::Canceled => "NODE_OUTCOME_CANCELED",
            Self::Skipped => "NODE_OUTCOME_SKIPPED",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for NodeOutcome {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "NODE_OUTCOME_UNSPECIFIED",
            "NODE_OUTCOME_SUCCESS",
            "NODE_OUTCOME_ERROR",
            "NODE_OUTCOME_CANCELED",
            "NODE_OUTCOME_SKIPPED",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NodeOutcome;

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
                    "NODE_OUTCOME_UNSPECIFIED" => Ok(NodeOutcome::Unspecified),
                    "NODE_OUTCOME_SUCCESS" => Ok(NodeOutcome::Success),
                    "NODE_OUTCOME_ERROR" => Ok(NodeOutcome::Error),
                    "NODE_OUTCOME_CANCELED" => Ok(NodeOutcome::Canceled),
                    "NODE_OUTCOME_SKIPPED" => Ok(NodeOutcome::Skipped),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for NodeSkipReason {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "NODE_SKIP_REASON_UNSPECIFIED",
            Self::Upstream => "NODE_SKIP_REASON_UPSTREAM",
            Self::Cached => "NODE_SKIP_REASON_CACHED",
            Self::PhaseDisabled => "NODE_SKIP_REASON_PHASE_DISABLED",
            Self::NoOp => "NODE_SKIP_REASON_NO_OP",
            Self::PhaseSkipped => "NODE_SKIP_REASON_PHASE_SKIPPED",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for NodeSkipReason {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "NODE_SKIP_REASON_UNSPECIFIED",
            "NODE_SKIP_REASON_UPSTREAM",
            "NODE_SKIP_REASON_CACHED",
            "NODE_SKIP_REASON_PHASE_DISABLED",
            "NODE_SKIP_REASON_NO_OP",
            "NODE_SKIP_REASON_PHASE_SKIPPED",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NodeSkipReason;

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
                    "NODE_SKIP_REASON_UNSPECIFIED" => Ok(NodeSkipReason::Unspecified),
                    "NODE_SKIP_REASON_UPSTREAM" => Ok(NodeSkipReason::Upstream),
                    "NODE_SKIP_REASON_CACHED" => Ok(NodeSkipReason::Cached),
                    "NODE_SKIP_REASON_PHASE_DISABLED" => Ok(NodeSkipReason::PhaseDisabled),
                    "NODE_SKIP_REASON_NO_OP" => Ok(NodeSkipReason::NoOp),
                    "NODE_SKIP_REASON_PHASE_SKIPPED" => Ok(NodeSkipReason::PhaseSkipped),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for NodeSkipUpstreamDetail {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.upstream_unique_id.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.events.fusion.node.NodeSkipUpstreamDetail", len)?;
        if !self.upstream_unique_id.is_empty() {
            struct_ser.serialize_field("upstream_unique_id", &self.upstream_unique_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for NodeSkipUpstreamDetail {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "upstream_unique_id",
            "upstreamUniqueId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            UpstreamUniqueId,
            __SkipField__,
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
                            "upstreamUniqueId" | "upstream_unique_id" => Ok(GeneratedField::UpstreamUniqueId),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NodeSkipUpstreamDetail;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.events.fusion.node.NodeSkipUpstreamDetail")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<NodeSkipUpstreamDetail, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut upstream_unique_id__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::UpstreamUniqueId => {
                            if upstream_unique_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("upstreamUniqueId"));
                            }
                            upstream_unique_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(NodeSkipUpstreamDetail {
                    upstream_unique_id: upstream_unique_id__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.events.fusion.node.NodeSkipUpstreamDetail", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for NodeType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "NODE_TYPE_UNSPECIFIED",
            Self::Model => "NODE_TYPE_MODEL",
            Self::Seed => "NODE_TYPE_SEED",
            Self::Snapshot => "NODE_TYPE_SNAPSHOT",
            Self::Source => "NODE_TYPE_SOURCE",
            Self::Test => "NODE_TYPE_TEST",
            Self::UnitTest => "NODE_TYPE_UNIT_TEST",
            Self::Macro => "NODE_TYPE_MACRO",
            Self::DocsMacro => "NODE_TYPE_DOCS_MACRO",
            Self::Analysis => "NODE_TYPE_ANALYSIS",
            Self::Operation => "NODE_TYPE_OPERATION",
            Self::Exposure => "NODE_TYPE_EXPOSURE",
            Self::Metric => "NODE_TYPE_METRIC",
            Self::SavedQuery => "NODE_TYPE_SAVED_QUERY",
            Self::SemanticModel => "NODE_TYPE_SEMANTIC_MODEL",
            Self::Function => "NODE_TYPE_FUNCTION",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for NodeType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "NODE_TYPE_UNSPECIFIED",
            "NODE_TYPE_MODEL",
            "NODE_TYPE_SEED",
            "NODE_TYPE_SNAPSHOT",
            "NODE_TYPE_SOURCE",
            "NODE_TYPE_TEST",
            "NODE_TYPE_UNIT_TEST",
            "NODE_TYPE_MACRO",
            "NODE_TYPE_DOCS_MACRO",
            "NODE_TYPE_ANALYSIS",
            "NODE_TYPE_OPERATION",
            "NODE_TYPE_EXPOSURE",
            "NODE_TYPE_METRIC",
            "NODE_TYPE_SAVED_QUERY",
            "NODE_TYPE_SEMANTIC_MODEL",
            "NODE_TYPE_FUNCTION",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NodeType;

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
                    "NODE_TYPE_UNSPECIFIED" => Ok(NodeType::Unspecified),
                    "NODE_TYPE_MODEL" => Ok(NodeType::Model),
                    "NODE_TYPE_SEED" => Ok(NodeType::Seed),
                    "NODE_TYPE_SNAPSHOT" => Ok(NodeType::Snapshot),
                    "NODE_TYPE_SOURCE" => Ok(NodeType::Source),
                    "NODE_TYPE_TEST" => Ok(NodeType::Test),
                    "NODE_TYPE_UNIT_TEST" => Ok(NodeType::UnitTest),
                    "NODE_TYPE_MACRO" => Ok(NodeType::Macro),
                    "NODE_TYPE_DOCS_MACRO" => Ok(NodeType::DocsMacro),
                    "NODE_TYPE_ANALYSIS" => Ok(NodeType::Analysis),
                    "NODE_TYPE_OPERATION" => Ok(NodeType::Operation),
                    "NODE_TYPE_EXPOSURE" => Ok(NodeType::Exposure),
                    "NODE_TYPE_METRIC" => Ok(NodeType::Metric),
                    "NODE_TYPE_SAVED_QUERY" => Ok(NodeType::SavedQuery),
                    "NODE_TYPE_SEMANTIC_MODEL" => Ok(NodeType::SemanticModel),
                    "NODE_TYPE_FUNCTION" => Ok(NodeType::Function),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for SourceFreshnessDetail {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_freshness_outcome != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.events.fusion.node.SourceFreshnessDetail", len)?;
        if self.node_freshness_outcome != 0 {
            let v = SourceFreshnessOutcome::try_from(self.node_freshness_outcome)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.node_freshness_outcome)))?;
            struct_ser.serialize_field("node_freshness_outcome", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SourceFreshnessDetail {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node_freshness_outcome",
            "nodeFreshnessOutcome",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeFreshnessOutcome,
            __SkipField__,
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
                            "nodeFreshnessOutcome" | "node_freshness_outcome" => Ok(GeneratedField::NodeFreshnessOutcome),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SourceFreshnessDetail;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.events.fusion.node.SourceFreshnessDetail")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SourceFreshnessDetail, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_freshness_outcome__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NodeFreshnessOutcome => {
                            if node_freshness_outcome__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeFreshnessOutcome"));
                            }
                            node_freshness_outcome__ = Some(map_.next_value::<SourceFreshnessOutcome>()? as i32);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(SourceFreshnessDetail {
                    node_freshness_outcome: node_freshness_outcome__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.events.fusion.node.SourceFreshnessDetail", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SourceFreshnessOutcome {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::OutcomePassed => "SOURCE_FRESHNESS_OUTCOME_OUTCOME_PASSED",
            Self::OutcomeWarned => "SOURCE_FRESHNESS_OUTCOME_OUTCOME_WARNED",
            Self::OutcomeFailed => "SOURCE_FRESHNESS_OUTCOME_OUTCOME_FAILED",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for SourceFreshnessOutcome {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "SOURCE_FRESHNESS_OUTCOME_OUTCOME_PASSED",
            "SOURCE_FRESHNESS_OUTCOME_OUTCOME_WARNED",
            "SOURCE_FRESHNESS_OUTCOME_OUTCOME_FAILED",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SourceFreshnessOutcome;

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
                    "SOURCE_FRESHNESS_OUTCOME_OUTCOME_PASSED" => Ok(SourceFreshnessOutcome::OutcomePassed),
                    "SOURCE_FRESHNESS_OUTCOME_OUTCOME_WARNED" => Ok(SourceFreshnessOutcome::OutcomeWarned),
                    "SOURCE_FRESHNESS_OUTCOME_OUTCOME_FAILED" => Ok(SourceFreshnessOutcome::OutcomeFailed),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for TestEvaluationDetail {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.test_outcome != 0 {
            len += 1;
        }
        if self.failing_rows != 0 {
            len += 1;
        }
        if self.diff_table.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.events.fusion.node.TestEvaluationDetail", len)?;
        if self.test_outcome != 0 {
            let v = TestOutcome::try_from(self.test_outcome)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.test_outcome)))?;
            struct_ser.serialize_field("test_outcome", &v)?;
        }
        if self.failing_rows != 0 {
            struct_ser.serialize_field("failing_rows", &self.failing_rows)?;
        }
        if let Some(v) = self.diff_table.as_ref() {
            struct_ser.serialize_field("diff_table", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TestEvaluationDetail {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "test_outcome",
            "testOutcome",
            "failing_rows",
            "failingRows",
            "diff_table",
            "diffTable",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TestOutcome,
            FailingRows,
            DiffTable,
            __SkipField__,
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
                            "testOutcome" | "test_outcome" => Ok(GeneratedField::TestOutcome),
                            "failingRows" | "failing_rows" => Ok(GeneratedField::FailingRows),
                            "diffTable" | "diff_table" => Ok(GeneratedField::DiffTable),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TestEvaluationDetail;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.events.fusion.node.TestEvaluationDetail")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TestEvaluationDetail, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut test_outcome__ = None;
                let mut failing_rows__ = None;
                let mut diff_table__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::TestOutcome => {
                            if test_outcome__.is_some() {
                                return Err(serde::de::Error::duplicate_field("testOutcome"));
                            }
                            test_outcome__ = Some(map_.next_value::<TestOutcome>()? as i32);
                        }
                        GeneratedField::FailingRows => {
                            if failing_rows__.is_some() {
                                return Err(serde::de::Error::duplicate_field("failingRows"));
                            }
                            failing_rows__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::DiffTable => {
                            if diff_table__.is_some() {
                                return Err(serde::de::Error::duplicate_field("diffTable"));
                            }
                            diff_table__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(TestEvaluationDetail {
                    test_outcome: test_outcome__.unwrap_or_default(),
                    failing_rows: failing_rows__.unwrap_or_default(),
                    diff_table: diff_table__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.events.fusion.node.TestEvaluationDetail", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TestOutcome {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Passed => "TEST_OUTCOME_PASSED",
            Self::Warned => "TEST_OUTCOME_WARNED",
            Self::Failed => "TEST_OUTCOME_FAILED",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for TestOutcome {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "TEST_OUTCOME_PASSED",
            "TEST_OUTCOME_WARNED",
            "TEST_OUTCOME_FAILED",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TestOutcome;

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
                    "TEST_OUTCOME_PASSED" => Ok(TestOutcome::Passed),
                    "TEST_OUTCOME_WARNED" => Ok(TestOutcome::Warned),
                    "TEST_OUTCOME_FAILED" => Ok(TestOutcome::Failed),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
