// @generated
impl serde::Serialize for AdapterCommonEventInfo {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        if !self.code.is_empty() {
            len += 1;
        }
        if !self.msg.is_empty() {
            len += 1;
        }
        if !self.level.is_empty() {
            len += 1;
        }
        if !self.invocation_id.is_empty() {
            len += 1;
        }
        if self.pid != 0 {
            len += 1;
        }
        if !self.thread.is_empty() {
            len += 1;
        }
        if self.ts.is_some() {
            len += 1;
        }
        if !self.extra.is_empty() {
            len += 1;
        }
        if !self.category.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.AdapterCommonEventInfo", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if !self.code.is_empty() {
            struct_ser.serialize_field("code", &self.code)?;
        }
        if !self.msg.is_empty() {
            struct_ser.serialize_field("msg", &self.msg)?;
        }
        if !self.level.is_empty() {
            struct_ser.serialize_field("level", &self.level)?;
        }
        if !self.invocation_id.is_empty() {
            struct_ser.serialize_field("invocationId", &self.invocation_id)?;
        }
        if self.pid != 0 {
            struct_ser.serialize_field("pid", &self.pid)?;
        }
        if !self.thread.is_empty() {
            struct_ser.serialize_field("thread", &self.thread)?;
        }
        if let Some(v) = self.ts.as_ref() {
            struct_ser.serialize_field("ts", v)?;
        }
        if !self.extra.is_empty() {
            struct_ser.serialize_field("extra", &self.extra)?;
        }
        if !self.category.is_empty() {
            struct_ser.serialize_field("category", &self.category)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdapterCommonEventInfo {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "code",
            "msg",
            "level",
            "invocation_id",
            "invocationId",
            "pid",
            "thread",
            "ts",
            "extra",
            "category",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Code,
            Msg,
            Level,
            InvocationId,
            Pid,
            Thread,
            Ts,
            Extra,
            Category,
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
                            "name" => Ok(GeneratedField::Name),
                            "code" => Ok(GeneratedField::Code),
                            "msg" => Ok(GeneratedField::Msg),
                            "level" => Ok(GeneratedField::Level),
                            "invocationId" | "invocation_id" => Ok(GeneratedField::InvocationId),
                            "pid" => Ok(GeneratedField::Pid),
                            "thread" => Ok(GeneratedField::Thread),
                            "ts" => Ok(GeneratedField::Ts),
                            "extra" => Ok(GeneratedField::Extra),
                            "category" => Ok(GeneratedField::Category),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdapterCommonEventInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.AdapterCommonEventInfo")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdapterCommonEventInfo, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut code__ = None;
                let mut msg__ = None;
                let mut level__ = None;
                let mut invocation_id__ = None;
                let mut pid__ = None;
                let mut thread__ = None;
                let mut ts__ = None;
                let mut extra__ = None;
                let mut category__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Code => {
                            if code__.is_some() {
                                return Err(serde::de::Error::duplicate_field("code"));
                            }
                            code__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Msg => {
                            if msg__.is_some() {
                                return Err(serde::de::Error::duplicate_field("msg"));
                            }
                            msg__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Level => {
                            if level__.is_some() {
                                return Err(serde::de::Error::duplicate_field("level"));
                            }
                            level__ = Some(map_.next_value()?);
                        }
                        GeneratedField::InvocationId => {
                            if invocation_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("invocationId"));
                            }
                            invocation_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Pid => {
                            if pid__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pid"));
                            }
                            pid__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Thread => {
                            if thread__.is_some() {
                                return Err(serde::de::Error::duplicate_field("thread"));
                            }
                            thread__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Ts => {
                            if ts__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ts"));
                            }
                            ts__ = map_.next_value()?;
                        }
                        GeneratedField::Extra => {
                            if extra__.is_some() {
                                return Err(serde::de::Error::duplicate_field("extra"));
                            }
                            extra__ = Some(
                                map_.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                        GeneratedField::Category => {
                            if category__.is_some() {
                                return Err(serde::de::Error::duplicate_field("category"));
                            }
                            category__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(AdapterCommonEventInfo {
                    name: name__.unwrap_or_default(),
                    code: code__.unwrap_or_default(),
                    msg: msg__.unwrap_or_default(),
                    level: level__.unwrap_or_default(),
                    invocation_id: invocation_id__.unwrap_or_default(),
                    pid: pid__.unwrap_or_default(),
                    thread: thread__.unwrap_or_default(),
                    ts: ts__,
                    extra: extra__.unwrap_or_default(),
                    category: category__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.AdapterCommonEventInfo", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdapterDeprecationWarning {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.old_name.is_empty() {
            len += 1;
        }
        if !self.new_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.AdapterDeprecationWarning", len)?;
        if !self.old_name.is_empty() {
            struct_ser.serialize_field("oldName", &self.old_name)?;
        }
        if !self.new_name.is_empty() {
            struct_ser.serialize_field("newName", &self.new_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdapterDeprecationWarning {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "old_name",
            "oldName",
            "new_name",
            "newName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            OldName,
            NewName,
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
                            "oldName" | "old_name" => Ok(GeneratedField::OldName),
                            "newName" | "new_name" => Ok(GeneratedField::NewName),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdapterDeprecationWarning;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.AdapterDeprecationWarning")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdapterDeprecationWarning, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut old_name__ = None;
                let mut new_name__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::OldName => {
                            if old_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("oldName"));
                            }
                            old_name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::NewName => {
                            if new_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("newName"));
                            }
                            new_name__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(AdapterDeprecationWarning {
                    old_name: old_name__.unwrap_or_default(),
                    new_name: new_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.AdapterDeprecationWarning", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdapterDeprecationWarningMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.AdapterDeprecationWarningMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdapterDeprecationWarningMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdapterDeprecationWarningMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.AdapterDeprecationWarningMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdapterDeprecationWarningMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(AdapterDeprecationWarningMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.AdapterDeprecationWarningMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdapterEventDebug {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_info.is_some() {
            len += 1;
        }
        if !self.name.is_empty() {
            len += 1;
        }
        if !self.base_msg.is_empty() {
            len += 1;
        }
        if self.args.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.AdapterEventDebug", len)?;
        if let Some(v) = self.node_info.as_ref() {
            struct_ser.serialize_field("nodeInfo", v)?;
        }
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if !self.base_msg.is_empty() {
            struct_ser.serialize_field("baseMsg", &self.base_msg)?;
        }
        if let Some(v) = self.args.as_ref() {
            struct_ser.serialize_field("args", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdapterEventDebug {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node_info",
            "nodeInfo",
            "name",
            "base_msg",
            "baseMsg",
            "args",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeInfo,
            Name,
            BaseMsg,
            Args,
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
                            "nodeInfo" | "node_info" => Ok(GeneratedField::NodeInfo),
                            "name" => Ok(GeneratedField::Name),
                            "baseMsg" | "base_msg" => Ok(GeneratedField::BaseMsg),
                            "args" => Ok(GeneratedField::Args),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdapterEventDebug;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.AdapterEventDebug")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdapterEventDebug, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_info__ = None;
                let mut name__ = None;
                let mut base_msg__ = None;
                let mut args__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NodeInfo => {
                            if node_info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeInfo"));
                            }
                            node_info__ = map_.next_value()?;
                        }
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::BaseMsg => {
                            if base_msg__.is_some() {
                                return Err(serde::de::Error::duplicate_field("baseMsg"));
                            }
                            base_msg__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Args => {
                            if args__.is_some() {
                                return Err(serde::de::Error::duplicate_field("args"));
                            }
                            args__ = map_.next_value()?;
                        }
                    }
                }
                Ok(AdapterEventDebug {
                    node_info: node_info__,
                    name: name__.unwrap_or_default(),
                    base_msg: base_msg__.unwrap_or_default(),
                    args: args__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.AdapterEventDebug", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdapterEventDebugMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.AdapterEventDebugMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdapterEventDebugMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdapterEventDebugMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.AdapterEventDebugMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdapterEventDebugMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(AdapterEventDebugMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.AdapterEventDebugMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdapterEventError {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_info.is_some() {
            len += 1;
        }
        if !self.name.is_empty() {
            len += 1;
        }
        if !self.base_msg.is_empty() {
            len += 1;
        }
        if self.args.is_some() {
            len += 1;
        }
        if !self.exc_info.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.AdapterEventError", len)?;
        if let Some(v) = self.node_info.as_ref() {
            struct_ser.serialize_field("nodeInfo", v)?;
        }
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if !self.base_msg.is_empty() {
            struct_ser.serialize_field("baseMsg", &self.base_msg)?;
        }
        if let Some(v) = self.args.as_ref() {
            struct_ser.serialize_field("args", v)?;
        }
        if !self.exc_info.is_empty() {
            struct_ser.serialize_field("excInfo", &self.exc_info)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdapterEventError {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node_info",
            "nodeInfo",
            "name",
            "base_msg",
            "baseMsg",
            "args",
            "exc_info",
            "excInfo",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeInfo,
            Name,
            BaseMsg,
            Args,
            ExcInfo,
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
                            "nodeInfo" | "node_info" => Ok(GeneratedField::NodeInfo),
                            "name" => Ok(GeneratedField::Name),
                            "baseMsg" | "base_msg" => Ok(GeneratedField::BaseMsg),
                            "args" => Ok(GeneratedField::Args),
                            "excInfo" | "exc_info" => Ok(GeneratedField::ExcInfo),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdapterEventError;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.AdapterEventError")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdapterEventError, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_info__ = None;
                let mut name__ = None;
                let mut base_msg__ = None;
                let mut args__ = None;
                let mut exc_info__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NodeInfo => {
                            if node_info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeInfo"));
                            }
                            node_info__ = map_.next_value()?;
                        }
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::BaseMsg => {
                            if base_msg__.is_some() {
                                return Err(serde::de::Error::duplicate_field("baseMsg"));
                            }
                            base_msg__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Args => {
                            if args__.is_some() {
                                return Err(serde::de::Error::duplicate_field("args"));
                            }
                            args__ = map_.next_value()?;
                        }
                        GeneratedField::ExcInfo => {
                            if exc_info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("excInfo"));
                            }
                            exc_info__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(AdapterEventError {
                    node_info: node_info__,
                    name: name__.unwrap_or_default(),
                    base_msg: base_msg__.unwrap_or_default(),
                    args: args__,
                    exc_info: exc_info__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.AdapterEventError", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdapterEventErrorMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.AdapterEventErrorMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdapterEventErrorMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdapterEventErrorMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.AdapterEventErrorMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdapterEventErrorMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(AdapterEventErrorMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.AdapterEventErrorMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdapterEventInfo {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_info.is_some() {
            len += 1;
        }
        if !self.name.is_empty() {
            len += 1;
        }
        if !self.base_msg.is_empty() {
            len += 1;
        }
        if self.args.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.AdapterEventInfo", len)?;
        if let Some(v) = self.node_info.as_ref() {
            struct_ser.serialize_field("nodeInfo", v)?;
        }
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if !self.base_msg.is_empty() {
            struct_ser.serialize_field("baseMsg", &self.base_msg)?;
        }
        if let Some(v) = self.args.as_ref() {
            struct_ser.serialize_field("args", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdapterEventInfo {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node_info",
            "nodeInfo",
            "name",
            "base_msg",
            "baseMsg",
            "args",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeInfo,
            Name,
            BaseMsg,
            Args,
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
                            "nodeInfo" | "node_info" => Ok(GeneratedField::NodeInfo),
                            "name" => Ok(GeneratedField::Name),
                            "baseMsg" | "base_msg" => Ok(GeneratedField::BaseMsg),
                            "args" => Ok(GeneratedField::Args),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdapterEventInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.AdapterEventInfo")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdapterEventInfo, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_info__ = None;
                let mut name__ = None;
                let mut base_msg__ = None;
                let mut args__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NodeInfo => {
                            if node_info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeInfo"));
                            }
                            node_info__ = map_.next_value()?;
                        }
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::BaseMsg => {
                            if base_msg__.is_some() {
                                return Err(serde::de::Error::duplicate_field("baseMsg"));
                            }
                            base_msg__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Args => {
                            if args__.is_some() {
                                return Err(serde::de::Error::duplicate_field("args"));
                            }
                            args__ = map_.next_value()?;
                        }
                    }
                }
                Ok(AdapterEventInfo {
                    node_info: node_info__,
                    name: name__.unwrap_or_default(),
                    base_msg: base_msg__.unwrap_or_default(),
                    args: args__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.AdapterEventInfo", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdapterEventInfoMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.AdapterEventInfoMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdapterEventInfoMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdapterEventInfoMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.AdapterEventInfoMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdapterEventInfoMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(AdapterEventInfoMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.AdapterEventInfoMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdapterEventWarning {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_info.is_some() {
            len += 1;
        }
        if !self.name.is_empty() {
            len += 1;
        }
        if !self.base_msg.is_empty() {
            len += 1;
        }
        if self.args.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.AdapterEventWarning", len)?;
        if let Some(v) = self.node_info.as_ref() {
            struct_ser.serialize_field("nodeInfo", v)?;
        }
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if !self.base_msg.is_empty() {
            struct_ser.serialize_field("baseMsg", &self.base_msg)?;
        }
        if let Some(v) = self.args.as_ref() {
            struct_ser.serialize_field("args", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdapterEventWarning {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node_info",
            "nodeInfo",
            "name",
            "base_msg",
            "baseMsg",
            "args",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeInfo,
            Name,
            BaseMsg,
            Args,
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
                            "nodeInfo" | "node_info" => Ok(GeneratedField::NodeInfo),
                            "name" => Ok(GeneratedField::Name),
                            "baseMsg" | "base_msg" => Ok(GeneratedField::BaseMsg),
                            "args" => Ok(GeneratedField::Args),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdapterEventWarning;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.AdapterEventWarning")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdapterEventWarning, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_info__ = None;
                let mut name__ = None;
                let mut base_msg__ = None;
                let mut args__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NodeInfo => {
                            if node_info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeInfo"));
                            }
                            node_info__ = map_.next_value()?;
                        }
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::BaseMsg => {
                            if base_msg__.is_some() {
                                return Err(serde::de::Error::duplicate_field("baseMsg"));
                            }
                            base_msg__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Args => {
                            if args__.is_some() {
                                return Err(serde::de::Error::duplicate_field("args"));
                            }
                            args__ = map_.next_value()?;
                        }
                    }
                }
                Ok(AdapterEventWarning {
                    node_info: node_info__,
                    name: name__.unwrap_or_default(),
                    base_msg: base_msg__.unwrap_or_default(),
                    args: args__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.AdapterEventWarning", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdapterEventWarningMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.AdapterEventWarningMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdapterEventWarningMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdapterEventWarningMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.AdapterEventWarningMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdapterEventWarningMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(AdapterEventWarningMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.AdapterEventWarningMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdapterImportError {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.exc.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.AdapterImportError", len)?;
        if !self.exc.is_empty() {
            struct_ser.serialize_field("exc", &self.exc)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdapterImportError {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "exc",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Exc,
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
                            "exc" => Ok(GeneratedField::Exc),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdapterImportError;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.AdapterImportError")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdapterImportError, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut exc__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Exc => {
                            if exc__.is_some() {
                                return Err(serde::de::Error::duplicate_field("exc"));
                            }
                            exc__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(AdapterImportError {
                    exc: exc__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.AdapterImportError", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdapterImportErrorMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.AdapterImportErrorMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdapterImportErrorMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdapterImportErrorMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.AdapterImportErrorMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdapterImportErrorMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(AdapterImportErrorMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.AdapterImportErrorMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdapterNodeInfo {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.node_path.is_empty() {
            len += 1;
        }
        if !self.node_name.is_empty() {
            len += 1;
        }
        if !self.unique_id.is_empty() {
            len += 1;
        }
        if !self.resource_type.is_empty() {
            len += 1;
        }
        if !self.materialized.is_empty() {
            len += 1;
        }
        if !self.node_status.is_empty() {
            len += 1;
        }
        if !self.node_started_at.is_empty() {
            len += 1;
        }
        if !self.node_finished_at.is_empty() {
            len += 1;
        }
        if self.meta.is_some() {
            len += 1;
        }
        if self.node_relation.is_some() {
            len += 1;
        }
        if !self.node_checksum.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.AdapterNodeInfo", len)?;
        if !self.node_path.is_empty() {
            struct_ser.serialize_field("nodePath", &self.node_path)?;
        }
        if !self.node_name.is_empty() {
            struct_ser.serialize_field("nodeName", &self.node_name)?;
        }
        if !self.unique_id.is_empty() {
            struct_ser.serialize_field("uniqueId", &self.unique_id)?;
        }
        if !self.resource_type.is_empty() {
            struct_ser.serialize_field("resourceType", &self.resource_type)?;
        }
        if !self.materialized.is_empty() {
            struct_ser.serialize_field("materialized", &self.materialized)?;
        }
        if !self.node_status.is_empty() {
            struct_ser.serialize_field("nodeStatus", &self.node_status)?;
        }
        if !self.node_started_at.is_empty() {
            struct_ser.serialize_field("nodeStartedAt", &self.node_started_at)?;
        }
        if !self.node_finished_at.is_empty() {
            struct_ser.serialize_field("nodeFinishedAt", &self.node_finished_at)?;
        }
        if let Some(v) = self.meta.as_ref() {
            struct_ser.serialize_field("meta", v)?;
        }
        if let Some(v) = self.node_relation.as_ref() {
            struct_ser.serialize_field("nodeRelation", v)?;
        }
        if !self.node_checksum.is_empty() {
            struct_ser.serialize_field("nodeChecksum", &self.node_checksum)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdapterNodeInfo {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node_path",
            "nodePath",
            "node_name",
            "nodeName",
            "unique_id",
            "uniqueId",
            "resource_type",
            "resourceType",
            "materialized",
            "node_status",
            "nodeStatus",
            "node_started_at",
            "nodeStartedAt",
            "node_finished_at",
            "nodeFinishedAt",
            "meta",
            "node_relation",
            "nodeRelation",
            "node_checksum",
            "nodeChecksum",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodePath,
            NodeName,
            UniqueId,
            ResourceType,
            Materialized,
            NodeStatus,
            NodeStartedAt,
            NodeFinishedAt,
            Meta,
            NodeRelation,
            NodeChecksum,
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
                            "nodePath" | "node_path" => Ok(GeneratedField::NodePath),
                            "nodeName" | "node_name" => Ok(GeneratedField::NodeName),
                            "uniqueId" | "unique_id" => Ok(GeneratedField::UniqueId),
                            "resourceType" | "resource_type" => Ok(GeneratedField::ResourceType),
                            "materialized" => Ok(GeneratedField::Materialized),
                            "nodeStatus" | "node_status" => Ok(GeneratedField::NodeStatus),
                            "nodeStartedAt" | "node_started_at" => Ok(GeneratedField::NodeStartedAt),
                            "nodeFinishedAt" | "node_finished_at" => Ok(GeneratedField::NodeFinishedAt),
                            "meta" => Ok(GeneratedField::Meta),
                            "nodeRelation" | "node_relation" => Ok(GeneratedField::NodeRelation),
                            "nodeChecksum" | "node_checksum" => Ok(GeneratedField::NodeChecksum),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdapterNodeInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.AdapterNodeInfo")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdapterNodeInfo, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_path__ = None;
                let mut node_name__ = None;
                let mut unique_id__ = None;
                let mut resource_type__ = None;
                let mut materialized__ = None;
                let mut node_status__ = None;
                let mut node_started_at__ = None;
                let mut node_finished_at__ = None;
                let mut meta__ = None;
                let mut node_relation__ = None;
                let mut node_checksum__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NodePath => {
                            if node_path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodePath"));
                            }
                            node_path__ = Some(map_.next_value()?);
                        }
                        GeneratedField::NodeName => {
                            if node_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeName"));
                            }
                            node_name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::UniqueId => {
                            if unique_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uniqueId"));
                            }
                            unique_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ResourceType => {
                            if resource_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resourceType"));
                            }
                            resource_type__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Materialized => {
                            if materialized__.is_some() {
                                return Err(serde::de::Error::duplicate_field("materialized"));
                            }
                            materialized__ = Some(map_.next_value()?);
                        }
                        GeneratedField::NodeStatus => {
                            if node_status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeStatus"));
                            }
                            node_status__ = Some(map_.next_value()?);
                        }
                        GeneratedField::NodeStartedAt => {
                            if node_started_at__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeStartedAt"));
                            }
                            node_started_at__ = Some(map_.next_value()?);
                        }
                        GeneratedField::NodeFinishedAt => {
                            if node_finished_at__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeFinishedAt"));
                            }
                            node_finished_at__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Meta => {
                            if meta__.is_some() {
                                return Err(serde::de::Error::duplicate_field("meta"));
                            }
                            meta__ = map_.next_value()?;
                        }
                        GeneratedField::NodeRelation => {
                            if node_relation__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeRelation"));
                            }
                            node_relation__ = map_.next_value()?;
                        }
                        GeneratedField::NodeChecksum => {
                            if node_checksum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeChecksum"));
                            }
                            node_checksum__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(AdapterNodeInfo {
                    node_path: node_path__.unwrap_or_default(),
                    node_name: node_name__.unwrap_or_default(),
                    unique_id: unique_id__.unwrap_or_default(),
                    resource_type: resource_type__.unwrap_or_default(),
                    materialized: materialized__.unwrap_or_default(),
                    node_status: node_status__.unwrap_or_default(),
                    node_started_at: node_started_at__.unwrap_or_default(),
                    node_finished_at: node_finished_at__.unwrap_or_default(),
                    meta: meta__,
                    node_relation: node_relation__,
                    node_checksum: node_checksum__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.AdapterNodeInfo", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdapterNodeRelation {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.database.is_empty() {
            len += 1;
        }
        if !self.schema.is_empty() {
            len += 1;
        }
        if !self.alias.is_empty() {
            len += 1;
        }
        if !self.relation_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.AdapterNodeRelation", len)?;
        if !self.database.is_empty() {
            struct_ser.serialize_field("database", &self.database)?;
        }
        if !self.schema.is_empty() {
            struct_ser.serialize_field("schema", &self.schema)?;
        }
        if !self.alias.is_empty() {
            struct_ser.serialize_field("alias", &self.alias)?;
        }
        if !self.relation_name.is_empty() {
            struct_ser.serialize_field("relationName", &self.relation_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdapterNodeRelation {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "database",
            "schema",
            "alias",
            "relation_name",
            "relationName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Database,
            Schema,
            Alias,
            RelationName,
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
                            "database" => Ok(GeneratedField::Database),
                            "schema" => Ok(GeneratedField::Schema),
                            "alias" => Ok(GeneratedField::Alias),
                            "relationName" | "relation_name" => Ok(GeneratedField::RelationName),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdapterNodeRelation;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.AdapterNodeRelation")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdapterNodeRelation, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut database__ = None;
                let mut schema__ = None;
                let mut alias__ = None;
                let mut relation_name__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Database => {
                            if database__.is_some() {
                                return Err(serde::de::Error::duplicate_field("database"));
                            }
                            database__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Alias => {
                            if alias__.is_some() {
                                return Err(serde::de::Error::duplicate_field("alias"));
                            }
                            alias__ = Some(map_.next_value()?);
                        }
                        GeneratedField::RelationName => {
                            if relation_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("relationName"));
                            }
                            relation_name__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(AdapterNodeRelation {
                    database: database__.unwrap_or_default(),
                    schema: schema__.unwrap_or_default(),
                    alias: alias__.unwrap_or_default(),
                    relation_name: relation_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.AdapterNodeRelation", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdapterRegistered {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.adapter_name.is_empty() {
            len += 1;
        }
        if !self.adapter_version.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.AdapterRegistered", len)?;
        if !self.adapter_name.is_empty() {
            struct_ser.serialize_field("adapterName", &self.adapter_name)?;
        }
        if !self.adapter_version.is_empty() {
            struct_ser.serialize_field("adapterVersion", &self.adapter_version)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdapterRegistered {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "adapter_name",
            "adapterName",
            "adapter_version",
            "adapterVersion",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            AdapterName,
            AdapterVersion,
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
                            "adapterName" | "adapter_name" => Ok(GeneratedField::AdapterName),
                            "adapterVersion" | "adapter_version" => Ok(GeneratedField::AdapterVersion),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdapterRegistered;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.AdapterRegistered")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdapterRegistered, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut adapter_name__ = None;
                let mut adapter_version__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::AdapterName => {
                            if adapter_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("adapterName"));
                            }
                            adapter_name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::AdapterVersion => {
                            if adapter_version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("adapterVersion"));
                            }
                            adapter_version__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(AdapterRegistered {
                    adapter_name: adapter_name__.unwrap_or_default(),
                    adapter_version: adapter_version__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.AdapterRegistered", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdapterRegisteredMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.AdapterRegisteredMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdapterRegisteredMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdapterRegisteredMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.AdapterRegisteredMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdapterRegisteredMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(AdapterRegisteredMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.AdapterRegisteredMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BuildingCatalog {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.BuildingCatalog", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BuildingCatalog {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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
                            Err(serde::de::Error::unknown_field(value, FIELDS))
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BuildingCatalog;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.BuildingCatalog")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<BuildingCatalog, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(BuildingCatalog {
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.BuildingCatalog", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BuildingCatalogMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.BuildingCatalogMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BuildingCatalogMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BuildingCatalogMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.BuildingCatalogMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<BuildingCatalogMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(BuildingCatalogMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.BuildingCatalogMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CacheAction {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.action.is_empty() {
            len += 1;
        }
        if self.ref_key.is_some() {
            len += 1;
        }
        if self.ref_key_2.is_some() {
            len += 1;
        }
        if self.ref_key_3.is_some() {
            len += 1;
        }
        if !self.ref_list.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.CacheAction", len)?;
        if !self.action.is_empty() {
            struct_ser.serialize_field("action", &self.action)?;
        }
        if let Some(v) = self.ref_key.as_ref() {
            struct_ser.serialize_field("refKey", v)?;
        }
        if let Some(v) = self.ref_key_2.as_ref() {
            struct_ser.serialize_field("refKey2", v)?;
        }
        if let Some(v) = self.ref_key_3.as_ref() {
            struct_ser.serialize_field("refKey3", v)?;
        }
        if !self.ref_list.is_empty() {
            struct_ser.serialize_field("refList", &self.ref_list)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CacheAction {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "action",
            "ref_key",
            "refKey",
            "ref_key_2",
            "refKey2",
            "ref_key_3",
            "refKey3",
            "ref_list",
            "refList",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Action,
            RefKey,
            RefKey2,
            RefKey3,
            RefList,
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
                            "action" => Ok(GeneratedField::Action),
                            "refKey" | "ref_key" => Ok(GeneratedField::RefKey),
                            "refKey2" | "ref_key_2" => Ok(GeneratedField::RefKey2),
                            "refKey3" | "ref_key_3" => Ok(GeneratedField::RefKey3),
                            "refList" | "ref_list" => Ok(GeneratedField::RefList),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CacheAction;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.CacheAction")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CacheAction, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut action__ = None;
                let mut ref_key__ = None;
                let mut ref_key_2__ = None;
                let mut ref_key_3__ = None;
                let mut ref_list__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Action => {
                            if action__.is_some() {
                                return Err(serde::de::Error::duplicate_field("action"));
                            }
                            action__ = Some(map_.next_value()?);
                        }
                        GeneratedField::RefKey => {
                            if ref_key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("refKey"));
                            }
                            ref_key__ = map_.next_value()?;
                        }
                        GeneratedField::RefKey2 => {
                            if ref_key_2__.is_some() {
                                return Err(serde::de::Error::duplicate_field("refKey2"));
                            }
                            ref_key_2__ = map_.next_value()?;
                        }
                        GeneratedField::RefKey3 => {
                            if ref_key_3__.is_some() {
                                return Err(serde::de::Error::duplicate_field("refKey3"));
                            }
                            ref_key_3__ = map_.next_value()?;
                        }
                        GeneratedField::RefList => {
                            if ref_list__.is_some() {
                                return Err(serde::de::Error::duplicate_field("refList"));
                            }
                            ref_list__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(CacheAction {
                    action: action__.unwrap_or_default(),
                    ref_key: ref_key__,
                    ref_key_2: ref_key_2__,
                    ref_key_3: ref_key_3__,
                    ref_list: ref_list__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.CacheAction", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CacheActionMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.CacheActionMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CacheActionMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CacheActionMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.CacheActionMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CacheActionMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(CacheActionMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.CacheActionMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CacheDumpGraph {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.dump.is_empty() {
            len += 1;
        }
        if !self.before_after.is_empty() {
            len += 1;
        }
        if !self.action.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.CacheDumpGraph", len)?;
        if !self.dump.is_empty() {
            struct_ser.serialize_field("dump", &self.dump)?;
        }
        if !self.before_after.is_empty() {
            struct_ser.serialize_field("beforeAfter", &self.before_after)?;
        }
        if !self.action.is_empty() {
            struct_ser.serialize_field("action", &self.action)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CacheDumpGraph {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "dump",
            "before_after",
            "beforeAfter",
            "action",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Dump,
            BeforeAfter,
            Action,
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
                            "dump" => Ok(GeneratedField::Dump),
                            "beforeAfter" | "before_after" => Ok(GeneratedField::BeforeAfter),
                            "action" => Ok(GeneratedField::Action),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CacheDumpGraph;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.CacheDumpGraph")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CacheDumpGraph, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut dump__ = None;
                let mut before_after__ = None;
                let mut action__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Dump => {
                            if dump__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dump"));
                            }
                            dump__ = Some(
                                map_.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                        GeneratedField::BeforeAfter => {
                            if before_after__.is_some() {
                                return Err(serde::de::Error::duplicate_field("beforeAfter"));
                            }
                            before_after__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Action => {
                            if action__.is_some() {
                                return Err(serde::de::Error::duplicate_field("action"));
                            }
                            action__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(CacheDumpGraph {
                    dump: dump__.unwrap_or_default(),
                    before_after: before_after__.unwrap_or_default(),
                    action: action__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.CacheDumpGraph", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CacheDumpGraphMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.CacheDumpGraphMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CacheDumpGraphMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CacheDumpGraphMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.CacheDumpGraphMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CacheDumpGraphMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(CacheDumpGraphMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.CacheDumpGraphMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CacheMiss {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.conn_name.is_empty() {
            len += 1;
        }
        if !self.database.is_empty() {
            len += 1;
        }
        if !self.schema.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.CacheMiss", len)?;
        if !self.conn_name.is_empty() {
            struct_ser.serialize_field("connName", &self.conn_name)?;
        }
        if !self.database.is_empty() {
            struct_ser.serialize_field("database", &self.database)?;
        }
        if !self.schema.is_empty() {
            struct_ser.serialize_field("schema", &self.schema)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CacheMiss {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "conn_name",
            "connName",
            "database",
            "schema",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ConnName,
            Database,
            Schema,
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
                            "connName" | "conn_name" => Ok(GeneratedField::ConnName),
                            "database" => Ok(GeneratedField::Database),
                            "schema" => Ok(GeneratedField::Schema),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CacheMiss;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.CacheMiss")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CacheMiss, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut conn_name__ = None;
                let mut database__ = None;
                let mut schema__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ConnName => {
                            if conn_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connName"));
                            }
                            conn_name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Database => {
                            if database__.is_some() {
                                return Err(serde::de::Error::duplicate_field("database"));
                            }
                            database__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(CacheMiss {
                    conn_name: conn_name__.unwrap_or_default(),
                    database: database__.unwrap_or_default(),
                    schema: schema__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.CacheMiss", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CacheMissMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.CacheMissMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CacheMissMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CacheMissMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.CacheMissMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CacheMissMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(CacheMissMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.CacheMissMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CannotGenerateDocs {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.CannotGenerateDocs", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CannotGenerateDocs {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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
                            Err(serde::de::Error::unknown_field(value, FIELDS))
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CannotGenerateDocs;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.CannotGenerateDocs")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CannotGenerateDocs, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(CannotGenerateDocs {
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.CannotGenerateDocs", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CannotGenerateDocsMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.CannotGenerateDocsMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CannotGenerateDocsMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CannotGenerateDocsMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.CannotGenerateDocsMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CannotGenerateDocsMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(CannotGenerateDocsMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.CannotGenerateDocsMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CatalogGenerationError {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.exc.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.CatalogGenerationError", len)?;
        if !self.exc.is_empty() {
            struct_ser.serialize_field("exc", &self.exc)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CatalogGenerationError {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "exc",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Exc,
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
                            "exc" => Ok(GeneratedField::Exc),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CatalogGenerationError;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.CatalogGenerationError")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CatalogGenerationError, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut exc__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Exc => {
                            if exc__.is_some() {
                                return Err(serde::de::Error::duplicate_field("exc"));
                            }
                            exc__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(CatalogGenerationError {
                    exc: exc__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.CatalogGenerationError", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CatalogGenerationErrorMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.CatalogGenerationErrorMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CatalogGenerationErrorMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CatalogGenerationErrorMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.CatalogGenerationErrorMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CatalogGenerationErrorMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(CatalogGenerationErrorMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.CatalogGenerationErrorMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CatalogWritten {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.path.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.CatalogWritten", len)?;
        if !self.path.is_empty() {
            struct_ser.serialize_field("path", &self.path)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CatalogWritten {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "path",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Path,
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
                            "path" => Ok(GeneratedField::Path),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CatalogWritten;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.CatalogWritten")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CatalogWritten, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut path__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Path => {
                            if path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("path"));
                            }
                            path__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(CatalogWritten {
                    path: path__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.CatalogWritten", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CatalogWrittenMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.CatalogWrittenMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CatalogWrittenMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CatalogWrittenMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.CatalogWrittenMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CatalogWrittenMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(CatalogWrittenMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.CatalogWrittenMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CodeExecution {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.conn_name.is_empty() {
            len += 1;
        }
        if !self.code_content.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.CodeExecution", len)?;
        if !self.conn_name.is_empty() {
            struct_ser.serialize_field("connName", &self.conn_name)?;
        }
        if !self.code_content.is_empty() {
            struct_ser.serialize_field("codeContent", &self.code_content)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CodeExecution {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "conn_name",
            "connName",
            "code_content",
            "codeContent",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ConnName,
            CodeContent,
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
                            "connName" | "conn_name" => Ok(GeneratedField::ConnName),
                            "codeContent" | "code_content" => Ok(GeneratedField::CodeContent),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CodeExecution;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.CodeExecution")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CodeExecution, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut conn_name__ = None;
                let mut code_content__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ConnName => {
                            if conn_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connName"));
                            }
                            conn_name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::CodeContent => {
                            if code_content__.is_some() {
                                return Err(serde::de::Error::duplicate_field("codeContent"));
                            }
                            code_content__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(CodeExecution {
                    conn_name: conn_name__.unwrap_or_default(),
                    code_content: code_content__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.CodeExecution", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CodeExecutionMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.CodeExecutionMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CodeExecutionMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CodeExecutionMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.CodeExecutionMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CodeExecutionMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(CodeExecutionMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.CodeExecutionMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CodeExecutionStatus {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.status.is_empty() {
            len += 1;
        }
        if self.elapsed != 0. {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.CodeExecutionStatus", len)?;
        if !self.status.is_empty() {
            struct_ser.serialize_field("status", &self.status)?;
        }
        if self.elapsed != 0. {
            struct_ser.serialize_field("elapsed", &self.elapsed)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CodeExecutionStatus {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "elapsed",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Elapsed,
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
                            "status" => Ok(GeneratedField::Status),
                            "elapsed" => Ok(GeneratedField::Elapsed),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CodeExecutionStatus;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.CodeExecutionStatus")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CodeExecutionStatus, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status__ = None;
                let mut elapsed__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Elapsed => {
                            if elapsed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("elapsed"));
                            }
                            elapsed__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(CodeExecutionStatus {
                    status: status__.unwrap_or_default(),
                    elapsed: elapsed__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.CodeExecutionStatus", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CodeExecutionStatusMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.CodeExecutionStatusMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CodeExecutionStatusMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CodeExecutionStatusMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.CodeExecutionStatusMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CodeExecutionStatusMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(CodeExecutionStatusMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.CodeExecutionStatusMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ColTypeChange {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.orig_type.is_empty() {
            len += 1;
        }
        if !self.new_type.is_empty() {
            len += 1;
        }
        if self.table.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.ColTypeChange", len)?;
        if !self.orig_type.is_empty() {
            struct_ser.serialize_field("origType", &self.orig_type)?;
        }
        if !self.new_type.is_empty() {
            struct_ser.serialize_field("newType", &self.new_type)?;
        }
        if let Some(v) = self.table.as_ref() {
            struct_ser.serialize_field("table", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ColTypeChange {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "orig_type",
            "origType",
            "new_type",
            "newType",
            "table",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            OrigType,
            NewType,
            Table,
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
                            "origType" | "orig_type" => Ok(GeneratedField::OrigType),
                            "newType" | "new_type" => Ok(GeneratedField::NewType),
                            "table" => Ok(GeneratedField::Table),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ColTypeChange;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.ColTypeChange")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ColTypeChange, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut orig_type__ = None;
                let mut new_type__ = None;
                let mut table__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::OrigType => {
                            if orig_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("origType"));
                            }
                            orig_type__ = Some(map_.next_value()?);
                        }
                        GeneratedField::NewType => {
                            if new_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("newType"));
                            }
                            new_type__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Table => {
                            if table__.is_some() {
                                return Err(serde::de::Error::duplicate_field("table"));
                            }
                            table__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ColTypeChange {
                    orig_type: orig_type__.unwrap_or_default(),
                    new_type: new_type__.unwrap_or_default(),
                    table: table__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.ColTypeChange", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ColTypeChangeMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.ColTypeChangeMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ColTypeChangeMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ColTypeChangeMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.ColTypeChangeMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ColTypeChangeMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ColTypeChangeMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.ColTypeChangeMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CollectFreshnessReturnSignature {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.CollectFreshnessReturnSignature", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CollectFreshnessReturnSignature {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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
                            Err(serde::de::Error::unknown_field(value, FIELDS))
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CollectFreshnessReturnSignature;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.CollectFreshnessReturnSignature")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CollectFreshnessReturnSignature, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(CollectFreshnessReturnSignature {
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.CollectFreshnessReturnSignature", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CollectFreshnessReturnSignatureMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.CollectFreshnessReturnSignatureMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CollectFreshnessReturnSignatureMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CollectFreshnessReturnSignatureMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.CollectFreshnessReturnSignatureMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CollectFreshnessReturnSignatureMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(CollectFreshnessReturnSignatureMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.CollectFreshnessReturnSignatureMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ConnectionClosed {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_info.is_some() {
            len += 1;
        }
        if !self.conn_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.ConnectionClosed", len)?;
        if let Some(v) = self.node_info.as_ref() {
            struct_ser.serialize_field("nodeInfo", v)?;
        }
        if !self.conn_name.is_empty() {
            struct_ser.serialize_field("connName", &self.conn_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ConnectionClosed {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node_info",
            "nodeInfo",
            "conn_name",
            "connName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeInfo,
            ConnName,
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
                            "nodeInfo" | "node_info" => Ok(GeneratedField::NodeInfo),
                            "connName" | "conn_name" => Ok(GeneratedField::ConnName),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ConnectionClosed;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.ConnectionClosed")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ConnectionClosed, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_info__ = None;
                let mut conn_name__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NodeInfo => {
                            if node_info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeInfo"));
                            }
                            node_info__ = map_.next_value()?;
                        }
                        GeneratedField::ConnName => {
                            if conn_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connName"));
                            }
                            conn_name__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ConnectionClosed {
                    node_info: node_info__,
                    conn_name: conn_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.ConnectionClosed", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ConnectionClosedInCleanup {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.conn_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.ConnectionClosedInCleanup", len)?;
        if !self.conn_name.is_empty() {
            struct_ser.serialize_field("connName", &self.conn_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ConnectionClosedInCleanup {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "conn_name",
            "connName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ConnName,
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
                            "connName" | "conn_name" => Ok(GeneratedField::ConnName),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ConnectionClosedInCleanup;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.ConnectionClosedInCleanup")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ConnectionClosedInCleanup, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut conn_name__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ConnName => {
                            if conn_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connName"));
                            }
                            conn_name__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ConnectionClosedInCleanup {
                    conn_name: conn_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.ConnectionClosedInCleanup", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ConnectionClosedInCleanupMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.ConnectionClosedInCleanupMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ConnectionClosedInCleanupMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ConnectionClosedInCleanupMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.ConnectionClosedInCleanupMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ConnectionClosedInCleanupMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ConnectionClosedInCleanupMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.ConnectionClosedInCleanupMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ConnectionClosedMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.ConnectionClosedMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ConnectionClosedMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ConnectionClosedMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.ConnectionClosedMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ConnectionClosedMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ConnectionClosedMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.ConnectionClosedMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ConnectionLeftOpen {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_info.is_some() {
            len += 1;
        }
        if !self.conn_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.ConnectionLeftOpen", len)?;
        if let Some(v) = self.node_info.as_ref() {
            struct_ser.serialize_field("nodeInfo", v)?;
        }
        if !self.conn_name.is_empty() {
            struct_ser.serialize_field("connName", &self.conn_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ConnectionLeftOpen {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node_info",
            "nodeInfo",
            "conn_name",
            "connName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeInfo,
            ConnName,
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
                            "nodeInfo" | "node_info" => Ok(GeneratedField::NodeInfo),
                            "connName" | "conn_name" => Ok(GeneratedField::ConnName),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ConnectionLeftOpen;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.ConnectionLeftOpen")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ConnectionLeftOpen, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_info__ = None;
                let mut conn_name__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NodeInfo => {
                            if node_info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeInfo"));
                            }
                            node_info__ = map_.next_value()?;
                        }
                        GeneratedField::ConnName => {
                            if conn_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connName"));
                            }
                            conn_name__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ConnectionLeftOpen {
                    node_info: node_info__,
                    conn_name: conn_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.ConnectionLeftOpen", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ConnectionLeftOpenInCleanup {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.conn_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.ConnectionLeftOpenInCleanup", len)?;
        if !self.conn_name.is_empty() {
            struct_ser.serialize_field("connName", &self.conn_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ConnectionLeftOpenInCleanup {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "conn_name",
            "connName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ConnName,
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
                            "connName" | "conn_name" => Ok(GeneratedField::ConnName),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ConnectionLeftOpenInCleanup;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.ConnectionLeftOpenInCleanup")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ConnectionLeftOpenInCleanup, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut conn_name__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ConnName => {
                            if conn_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connName"));
                            }
                            conn_name__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ConnectionLeftOpenInCleanup {
                    conn_name: conn_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.ConnectionLeftOpenInCleanup", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ConnectionLeftOpenInCleanupMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.ConnectionLeftOpenInCleanupMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ConnectionLeftOpenInCleanupMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ConnectionLeftOpenInCleanupMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.ConnectionLeftOpenInCleanupMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ConnectionLeftOpenInCleanupMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ConnectionLeftOpenInCleanupMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.ConnectionLeftOpenInCleanupMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ConnectionLeftOpenMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.ConnectionLeftOpenMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ConnectionLeftOpenMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ConnectionLeftOpenMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.ConnectionLeftOpenMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ConnectionLeftOpenMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ConnectionLeftOpenMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.ConnectionLeftOpenMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ConnectionReused {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.conn_name.is_empty() {
            len += 1;
        }
        if !self.orig_conn_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.ConnectionReused", len)?;
        if !self.conn_name.is_empty() {
            struct_ser.serialize_field("connName", &self.conn_name)?;
        }
        if !self.orig_conn_name.is_empty() {
            struct_ser.serialize_field("origConnName", &self.orig_conn_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ConnectionReused {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "conn_name",
            "connName",
            "orig_conn_name",
            "origConnName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ConnName,
            OrigConnName,
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
                            "connName" | "conn_name" => Ok(GeneratedField::ConnName),
                            "origConnName" | "orig_conn_name" => Ok(GeneratedField::OrigConnName),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ConnectionReused;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.ConnectionReused")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ConnectionReused, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut conn_name__ = None;
                let mut orig_conn_name__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ConnName => {
                            if conn_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connName"));
                            }
                            conn_name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::OrigConnName => {
                            if orig_conn_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("origConnName"));
                            }
                            orig_conn_name__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ConnectionReused {
                    conn_name: conn_name__.unwrap_or_default(),
                    orig_conn_name: orig_conn_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.ConnectionReused", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ConnectionReusedMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.ConnectionReusedMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ConnectionReusedMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ConnectionReusedMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.ConnectionReusedMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ConnectionReusedMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ConnectionReusedMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.ConnectionReusedMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ConnectionUsed {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_info.is_some() {
            len += 1;
        }
        if !self.conn_type.is_empty() {
            len += 1;
        }
        if !self.conn_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.ConnectionUsed", len)?;
        if let Some(v) = self.node_info.as_ref() {
            struct_ser.serialize_field("nodeInfo", v)?;
        }
        if !self.conn_type.is_empty() {
            struct_ser.serialize_field("connType", &self.conn_type)?;
        }
        if !self.conn_name.is_empty() {
            struct_ser.serialize_field("connName", &self.conn_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ConnectionUsed {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node_info",
            "nodeInfo",
            "conn_type",
            "connType",
            "conn_name",
            "connName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeInfo,
            ConnType,
            ConnName,
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
                            "nodeInfo" | "node_info" => Ok(GeneratedField::NodeInfo),
                            "connType" | "conn_type" => Ok(GeneratedField::ConnType),
                            "connName" | "conn_name" => Ok(GeneratedField::ConnName),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ConnectionUsed;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.ConnectionUsed")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ConnectionUsed, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_info__ = None;
                let mut conn_type__ = None;
                let mut conn_name__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NodeInfo => {
                            if node_info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeInfo"));
                            }
                            node_info__ = map_.next_value()?;
                        }
                        GeneratedField::ConnType => {
                            if conn_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connType"));
                            }
                            conn_type__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ConnName => {
                            if conn_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connName"));
                            }
                            conn_name__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ConnectionUsed {
                    node_info: node_info__,
                    conn_type: conn_type__.unwrap_or_default(),
                    conn_name: conn_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.ConnectionUsed", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ConnectionUsedMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.ConnectionUsedMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ConnectionUsedMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ConnectionUsedMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.ConnectionUsedMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ConnectionUsedMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ConnectionUsedMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.ConnectionUsedMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ConstraintNotEnforced {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.constraint.is_empty() {
            len += 1;
        }
        if !self.adapter.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.ConstraintNotEnforced", len)?;
        if !self.constraint.is_empty() {
            struct_ser.serialize_field("constraint", &self.constraint)?;
        }
        if !self.adapter.is_empty() {
            struct_ser.serialize_field("adapter", &self.adapter)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ConstraintNotEnforced {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "constraint",
            "adapter",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Constraint,
            Adapter,
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
                            "constraint" => Ok(GeneratedField::Constraint),
                            "adapter" => Ok(GeneratedField::Adapter),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ConstraintNotEnforced;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.ConstraintNotEnforced")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ConstraintNotEnforced, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut constraint__ = None;
                let mut adapter__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Constraint => {
                            if constraint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("constraint"));
                            }
                            constraint__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Adapter => {
                            if adapter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("adapter"));
                            }
                            adapter__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ConstraintNotEnforced {
                    constraint: constraint__.unwrap_or_default(),
                    adapter: adapter__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.ConstraintNotEnforced", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ConstraintNotEnforcedMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.ConstraintNotEnforcedMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ConstraintNotEnforcedMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ConstraintNotEnforcedMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.ConstraintNotEnforcedMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ConstraintNotEnforcedMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ConstraintNotEnforcedMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.ConstraintNotEnforcedMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ConstraintNotSupported {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.constraint.is_empty() {
            len += 1;
        }
        if !self.adapter.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.ConstraintNotSupported", len)?;
        if !self.constraint.is_empty() {
            struct_ser.serialize_field("constraint", &self.constraint)?;
        }
        if !self.adapter.is_empty() {
            struct_ser.serialize_field("adapter", &self.adapter)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ConstraintNotSupported {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "constraint",
            "adapter",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Constraint,
            Adapter,
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
                            "constraint" => Ok(GeneratedField::Constraint),
                            "adapter" => Ok(GeneratedField::Adapter),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ConstraintNotSupported;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.ConstraintNotSupported")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ConstraintNotSupported, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut constraint__ = None;
                let mut adapter__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Constraint => {
                            if constraint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("constraint"));
                            }
                            constraint__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Adapter => {
                            if adapter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("adapter"));
                            }
                            adapter__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ConstraintNotSupported {
                    constraint: constraint__.unwrap_or_default(),
                    adapter: adapter__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.ConstraintNotSupported", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ConstraintNotSupportedMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.ConstraintNotSupportedMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ConstraintNotSupportedMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ConstraintNotSupportedMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.ConstraintNotSupportedMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ConstraintNotSupportedMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ConstraintNotSupportedMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.ConstraintNotSupportedMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DatabaseErrorRunningHook {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.hook_type.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.DatabaseErrorRunningHook", len)?;
        if !self.hook_type.is_empty() {
            struct_ser.serialize_field("hookType", &self.hook_type)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DatabaseErrorRunningHook {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "hook_type",
            "hookType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            HookType,
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
                            "hookType" | "hook_type" => Ok(GeneratedField::HookType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DatabaseErrorRunningHook;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.DatabaseErrorRunningHook")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<DatabaseErrorRunningHook, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut hook_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::HookType => {
                            if hook_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hookType"));
                            }
                            hook_type__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(DatabaseErrorRunningHook {
                    hook_type: hook_type__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.DatabaseErrorRunningHook", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DatabaseErrorRunningHookMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.DatabaseErrorRunningHookMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DatabaseErrorRunningHookMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DatabaseErrorRunningHookMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.DatabaseErrorRunningHookMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<DatabaseErrorRunningHookMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(DatabaseErrorRunningHookMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.DatabaseErrorRunningHookMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FinishedRunningStats {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.stat_line.is_empty() {
            len += 1;
        }
        if !self.execution.is_empty() {
            len += 1;
        }
        if self.execution_time != 0. {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.FinishedRunningStats", len)?;
        if !self.stat_line.is_empty() {
            struct_ser.serialize_field("statLine", &self.stat_line)?;
        }
        if !self.execution.is_empty() {
            struct_ser.serialize_field("execution", &self.execution)?;
        }
        if self.execution_time != 0. {
            struct_ser.serialize_field("executionTime", &self.execution_time)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FinishedRunningStats {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "stat_line",
            "statLine",
            "execution",
            "execution_time",
            "executionTime",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            StatLine,
            Execution,
            ExecutionTime,
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
                            "statLine" | "stat_line" => Ok(GeneratedField::StatLine),
                            "execution" => Ok(GeneratedField::Execution),
                            "executionTime" | "execution_time" => Ok(GeneratedField::ExecutionTime),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FinishedRunningStats;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.FinishedRunningStats")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<FinishedRunningStats, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut stat_line__ = None;
                let mut execution__ = None;
                let mut execution_time__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::StatLine => {
                            if stat_line__.is_some() {
                                return Err(serde::de::Error::duplicate_field("statLine"));
                            }
                            stat_line__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Execution => {
                            if execution__.is_some() {
                                return Err(serde::de::Error::duplicate_field("execution"));
                            }
                            execution__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ExecutionTime => {
                            if execution_time__.is_some() {
                                return Err(serde::de::Error::duplicate_field("executionTime"));
                            }
                            execution_time__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(FinishedRunningStats {
                    stat_line: stat_line__.unwrap_or_default(),
                    execution: execution__.unwrap_or_default(),
                    execution_time: execution_time__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.FinishedRunningStats", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FinishedRunningStatsMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.FinishedRunningStatsMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FinishedRunningStatsMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FinishedRunningStatsMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.FinishedRunningStatsMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<FinishedRunningStatsMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(FinishedRunningStatsMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.FinishedRunningStatsMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for HooksRunning {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.num_hooks != 0 {
            len += 1;
        }
        if !self.hook_type.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.HooksRunning", len)?;
        if self.num_hooks != 0 {
            struct_ser.serialize_field("numHooks", &self.num_hooks)?;
        }
        if !self.hook_type.is_empty() {
            struct_ser.serialize_field("hookType", &self.hook_type)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for HooksRunning {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "num_hooks",
            "numHooks",
            "hook_type",
            "hookType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NumHooks,
            HookType,
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
                            "numHooks" | "num_hooks" => Ok(GeneratedField::NumHooks),
                            "hookType" | "hook_type" => Ok(GeneratedField::HookType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = HooksRunning;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.HooksRunning")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<HooksRunning, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut num_hooks__ = None;
                let mut hook_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NumHooks => {
                            if num_hooks__.is_some() {
                                return Err(serde::de::Error::duplicate_field("numHooks"));
                            }
                            num_hooks__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::HookType => {
                            if hook_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hookType"));
                            }
                            hook_type__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(HooksRunning {
                    num_hooks: num_hooks__.unwrap_or_default(),
                    hook_type: hook_type__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.HooksRunning", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for HooksRunningMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.HooksRunningMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for HooksRunningMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = HooksRunningMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.HooksRunningMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<HooksRunningMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(HooksRunningMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.HooksRunningMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ListRelations {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.database.is_empty() {
            len += 1;
        }
        if !self.schema.is_empty() {
            len += 1;
        }
        if !self.relations.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.ListRelations", len)?;
        if !self.database.is_empty() {
            struct_ser.serialize_field("database", &self.database)?;
        }
        if !self.schema.is_empty() {
            struct_ser.serialize_field("schema", &self.schema)?;
        }
        if !self.relations.is_empty() {
            struct_ser.serialize_field("relations", &self.relations)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ListRelations {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "database",
            "schema",
            "relations",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Database,
            Schema,
            Relations,
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
                            "database" => Ok(GeneratedField::Database),
                            "schema" => Ok(GeneratedField::Schema),
                            "relations" => Ok(GeneratedField::Relations),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ListRelations;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.ListRelations")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ListRelations, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut database__ = None;
                let mut schema__ = None;
                let mut relations__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Database => {
                            if database__.is_some() {
                                return Err(serde::de::Error::duplicate_field("database"));
                            }
                            database__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Relations => {
                            if relations__.is_some() {
                                return Err(serde::de::Error::duplicate_field("relations"));
                            }
                            relations__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ListRelations {
                    database: database__.unwrap_or_default(),
                    schema: schema__.unwrap_or_default(),
                    relations: relations__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.ListRelations", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ListRelationsMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.ListRelationsMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ListRelationsMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ListRelationsMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.ListRelationsMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ListRelationsMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ListRelationsMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.ListRelationsMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for NewConnection {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_info.is_some() {
            len += 1;
        }
        if !self.conn_type.is_empty() {
            len += 1;
        }
        if !self.conn_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.NewConnection", len)?;
        if let Some(v) = self.node_info.as_ref() {
            struct_ser.serialize_field("nodeInfo", v)?;
        }
        if !self.conn_type.is_empty() {
            struct_ser.serialize_field("connType", &self.conn_type)?;
        }
        if !self.conn_name.is_empty() {
            struct_ser.serialize_field("connName", &self.conn_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for NewConnection {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node_info",
            "nodeInfo",
            "conn_type",
            "connType",
            "conn_name",
            "connName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeInfo,
            ConnType,
            ConnName,
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
                            "nodeInfo" | "node_info" => Ok(GeneratedField::NodeInfo),
                            "connType" | "conn_type" => Ok(GeneratedField::ConnType),
                            "connName" | "conn_name" => Ok(GeneratedField::ConnName),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NewConnection;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.NewConnection")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<NewConnection, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_info__ = None;
                let mut conn_type__ = None;
                let mut conn_name__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NodeInfo => {
                            if node_info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeInfo"));
                            }
                            node_info__ = map_.next_value()?;
                        }
                        GeneratedField::ConnType => {
                            if conn_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connType"));
                            }
                            conn_type__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ConnName => {
                            if conn_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connName"));
                            }
                            conn_name__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(NewConnection {
                    node_info: node_info__,
                    conn_type: conn_type__.unwrap_or_default(),
                    conn_name: conn_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.NewConnection", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for NewConnectionMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.NewConnectionMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for NewConnectionMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NewConnectionMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.NewConnectionMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<NewConnectionMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(NewConnectionMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.NewConnectionMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for NewConnectionOpening {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_info.is_some() {
            len += 1;
        }
        if !self.connection_state.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.NewConnectionOpening", len)?;
        if let Some(v) = self.node_info.as_ref() {
            struct_ser.serialize_field("nodeInfo", v)?;
        }
        if !self.connection_state.is_empty() {
            struct_ser.serialize_field("connectionState", &self.connection_state)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for NewConnectionOpening {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node_info",
            "nodeInfo",
            "connection_state",
            "connectionState",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeInfo,
            ConnectionState,
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
                            "nodeInfo" | "node_info" => Ok(GeneratedField::NodeInfo),
                            "connectionState" | "connection_state" => Ok(GeneratedField::ConnectionState),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NewConnectionOpening;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.NewConnectionOpening")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<NewConnectionOpening, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_info__ = None;
                let mut connection_state__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NodeInfo => {
                            if node_info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeInfo"));
                            }
                            node_info__ = map_.next_value()?;
                        }
                        GeneratedField::ConnectionState => {
                            if connection_state__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectionState"));
                            }
                            connection_state__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(NewConnectionOpening {
                    node_info: node_info__,
                    connection_state: connection_state__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.NewConnectionOpening", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for NewConnectionOpeningMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.NewConnectionOpeningMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for NewConnectionOpeningMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NewConnectionOpeningMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.NewConnectionOpeningMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<NewConnectionOpeningMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(NewConnectionOpeningMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.NewConnectionOpeningMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PluginLoadError {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.exc_info.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.PluginLoadError", len)?;
        if !self.exc_info.is_empty() {
            struct_ser.serialize_field("excInfo", &self.exc_info)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PluginLoadError {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "exc_info",
            "excInfo",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ExcInfo,
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
                            "excInfo" | "exc_info" => Ok(GeneratedField::ExcInfo),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PluginLoadError;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.PluginLoadError")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PluginLoadError, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut exc_info__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ExcInfo => {
                            if exc_info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("excInfo"));
                            }
                            exc_info__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(PluginLoadError {
                    exc_info: exc_info__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.PluginLoadError", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PluginLoadErrorMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.PluginLoadErrorMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PluginLoadErrorMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PluginLoadErrorMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.PluginLoadErrorMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PluginLoadErrorMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(PluginLoadErrorMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.PluginLoadErrorMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ReferenceKeyMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.database.is_empty() {
            len += 1;
        }
        if !self.schema.is_empty() {
            len += 1;
        }
        if !self.identifier.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.ReferenceKeyMsg", len)?;
        if !self.database.is_empty() {
            struct_ser.serialize_field("database", &self.database)?;
        }
        if !self.schema.is_empty() {
            struct_ser.serialize_field("schema", &self.schema)?;
        }
        if !self.identifier.is_empty() {
            struct_ser.serialize_field("identifier", &self.identifier)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ReferenceKeyMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "database",
            "schema",
            "identifier",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Database,
            Schema,
            Identifier,
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
                            "database" => Ok(GeneratedField::Database),
                            "schema" => Ok(GeneratedField::Schema),
                            "identifier" => Ok(GeneratedField::Identifier),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ReferenceKeyMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.ReferenceKeyMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ReferenceKeyMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut database__ = None;
                let mut schema__ = None;
                let mut identifier__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Database => {
                            if database__.is_some() {
                                return Err(serde::de::Error::duplicate_field("database"));
                            }
                            database__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Identifier => {
                            if identifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("identifier"));
                            }
                            identifier__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ReferenceKeyMsg {
                    database: database__.unwrap_or_default(),
                    schema: schema__.unwrap_or_default(),
                    identifier: identifier__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.ReferenceKeyMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Rollback {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_info.is_some() {
            len += 1;
        }
        if !self.conn_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.Rollback", len)?;
        if let Some(v) = self.node_info.as_ref() {
            struct_ser.serialize_field("nodeInfo", v)?;
        }
        if !self.conn_name.is_empty() {
            struct_ser.serialize_field("connName", &self.conn_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Rollback {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node_info",
            "nodeInfo",
            "conn_name",
            "connName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeInfo,
            ConnName,
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
                            "nodeInfo" | "node_info" => Ok(GeneratedField::NodeInfo),
                            "connName" | "conn_name" => Ok(GeneratedField::ConnName),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Rollback;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.Rollback")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Rollback, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_info__ = None;
                let mut conn_name__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NodeInfo => {
                            if node_info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeInfo"));
                            }
                            node_info__ = map_.next_value()?;
                        }
                        GeneratedField::ConnName => {
                            if conn_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connName"));
                            }
                            conn_name__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Rollback {
                    node_info: node_info__,
                    conn_name: conn_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.Rollback", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RollbackFailed {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_info.is_some() {
            len += 1;
        }
        if !self.conn_name.is_empty() {
            len += 1;
        }
        if !self.exc_info.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.RollbackFailed", len)?;
        if let Some(v) = self.node_info.as_ref() {
            struct_ser.serialize_field("nodeInfo", v)?;
        }
        if !self.conn_name.is_empty() {
            struct_ser.serialize_field("connName", &self.conn_name)?;
        }
        if !self.exc_info.is_empty() {
            struct_ser.serialize_field("excInfo", &self.exc_info)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RollbackFailed {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node_info",
            "nodeInfo",
            "conn_name",
            "connName",
            "exc_info",
            "excInfo",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeInfo,
            ConnName,
            ExcInfo,
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
                            "nodeInfo" | "node_info" => Ok(GeneratedField::NodeInfo),
                            "connName" | "conn_name" => Ok(GeneratedField::ConnName),
                            "excInfo" | "exc_info" => Ok(GeneratedField::ExcInfo),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RollbackFailed;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.RollbackFailed")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<RollbackFailed, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_info__ = None;
                let mut conn_name__ = None;
                let mut exc_info__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NodeInfo => {
                            if node_info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeInfo"));
                            }
                            node_info__ = map_.next_value()?;
                        }
                        GeneratedField::ConnName => {
                            if conn_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connName"));
                            }
                            conn_name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ExcInfo => {
                            if exc_info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("excInfo"));
                            }
                            exc_info__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(RollbackFailed {
                    node_info: node_info__,
                    conn_name: conn_name__.unwrap_or_default(),
                    exc_info: exc_info__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.RollbackFailed", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RollbackFailedMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.RollbackFailedMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RollbackFailedMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RollbackFailedMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.RollbackFailedMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<RollbackFailedMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(RollbackFailedMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.RollbackFailedMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RollbackMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.RollbackMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RollbackMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RollbackMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.RollbackMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<RollbackMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(RollbackMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.RollbackMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SqlCommit {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_info.is_some() {
            len += 1;
        }
        if !self.conn_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.SQLCommit", len)?;
        if let Some(v) = self.node_info.as_ref() {
            struct_ser.serialize_field("nodeInfo", v)?;
        }
        if !self.conn_name.is_empty() {
            struct_ser.serialize_field("connName", &self.conn_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SqlCommit {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node_info",
            "nodeInfo",
            "conn_name",
            "connName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeInfo,
            ConnName,
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
                            "nodeInfo" | "node_info" => Ok(GeneratedField::NodeInfo),
                            "connName" | "conn_name" => Ok(GeneratedField::ConnName),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SqlCommit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.SQLCommit")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SqlCommit, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_info__ = None;
                let mut conn_name__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NodeInfo => {
                            if node_info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeInfo"));
                            }
                            node_info__ = map_.next_value()?;
                        }
                        GeneratedField::ConnName => {
                            if conn_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connName"));
                            }
                            conn_name__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(SqlCommit {
                    node_info: node_info__,
                    conn_name: conn_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.SQLCommit", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SqlCommitMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.SQLCommitMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SqlCommitMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SqlCommitMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.SQLCommitMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SqlCommitMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(SqlCommitMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.SQLCommitMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SqlQuery {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_info.is_some() {
            len += 1;
        }
        if !self.conn_name.is_empty() {
            len += 1;
        }
        if !self.sql.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.SQLQuery", len)?;
        if let Some(v) = self.node_info.as_ref() {
            struct_ser.serialize_field("nodeInfo", v)?;
        }
        if !self.conn_name.is_empty() {
            struct_ser.serialize_field("connName", &self.conn_name)?;
        }
        if !self.sql.is_empty() {
            struct_ser.serialize_field("sql", &self.sql)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SqlQuery {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node_info",
            "nodeInfo",
            "conn_name",
            "connName",
            "sql",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeInfo,
            ConnName,
            Sql,
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
                            "nodeInfo" | "node_info" => Ok(GeneratedField::NodeInfo),
                            "connName" | "conn_name" => Ok(GeneratedField::ConnName),
                            "sql" => Ok(GeneratedField::Sql),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SqlQuery;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.SQLQuery")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SqlQuery, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_info__ = None;
                let mut conn_name__ = None;
                let mut sql__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NodeInfo => {
                            if node_info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeInfo"));
                            }
                            node_info__ = map_.next_value()?;
                        }
                        GeneratedField::ConnName => {
                            if conn_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connName"));
                            }
                            conn_name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Sql => {
                            if sql__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sql"));
                            }
                            sql__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(SqlQuery {
                    node_info: node_info__,
                    conn_name: conn_name__.unwrap_or_default(),
                    sql: sql__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.SQLQuery", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SqlQueryMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.SQLQueryMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SqlQueryMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SqlQueryMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.SQLQueryMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SqlQueryMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(SqlQueryMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.SQLQueryMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SqlQueryStatus {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_info.is_some() {
            len += 1;
        }
        if !self.status.is_empty() {
            len += 1;
        }
        if self.elapsed != 0. {
            len += 1;
        }
        if !self.query_id.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.SQLQueryStatus", len)?;
        if let Some(v) = self.node_info.as_ref() {
            struct_ser.serialize_field("nodeInfo", v)?;
        }
        if !self.status.is_empty() {
            struct_ser.serialize_field("status", &self.status)?;
        }
        if self.elapsed != 0. {
            struct_ser.serialize_field("elapsed", &self.elapsed)?;
        }
        if !self.query_id.is_empty() {
            struct_ser.serialize_field("queryId", &self.query_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SqlQueryStatus {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node_info",
            "nodeInfo",
            "status",
            "elapsed",
            "query_id",
            "queryId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeInfo,
            Status,
            Elapsed,
            QueryId,
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
                            "nodeInfo" | "node_info" => Ok(GeneratedField::NodeInfo),
                            "status" => Ok(GeneratedField::Status),
                            "elapsed" => Ok(GeneratedField::Elapsed),
                            "queryId" | "query_id" => Ok(GeneratedField::QueryId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SqlQueryStatus;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.SQLQueryStatus")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SqlQueryStatus, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_info__ = None;
                let mut status__ = None;
                let mut elapsed__ = None;
                let mut query_id__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NodeInfo => {
                            if node_info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeInfo"));
                            }
                            node_info__ = map_.next_value()?;
                        }
                        GeneratedField::Status => {
                            if status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Elapsed => {
                            if elapsed__.is_some() {
                                return Err(serde::de::Error::duplicate_field("elapsed"));
                            }
                            elapsed__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::QueryId => {
                            if query_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("queryId"));
                            }
                            query_id__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(SqlQueryStatus {
                    node_info: node_info__,
                    status: status__.unwrap_or_default(),
                    elapsed: elapsed__.unwrap_or_default(),
                    query_id: query_id__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.SQLQueryStatus", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SqlQueryStatusMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.SQLQueryStatusMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SqlQueryStatusMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SqlQueryStatusMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.SQLQueryStatusMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SqlQueryStatusMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(SqlQueryStatusMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.SQLQueryStatusMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SchemaCreation {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.relation.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.SchemaCreation", len)?;
        if let Some(v) = self.relation.as_ref() {
            struct_ser.serialize_field("relation", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SchemaCreation {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "relation",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Relation,
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
                            "relation" => Ok(GeneratedField::Relation),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SchemaCreation;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.SchemaCreation")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SchemaCreation, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut relation__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Relation => {
                            if relation__.is_some() {
                                return Err(serde::de::Error::duplicate_field("relation"));
                            }
                            relation__ = map_.next_value()?;
                        }
                    }
                }
                Ok(SchemaCreation {
                    relation: relation__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.SchemaCreation", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SchemaCreationMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.SchemaCreationMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SchemaCreationMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SchemaCreationMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.SchemaCreationMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SchemaCreationMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(SchemaCreationMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.SchemaCreationMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SchemaDrop {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.relation.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.SchemaDrop", len)?;
        if let Some(v) = self.relation.as_ref() {
            struct_ser.serialize_field("relation", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SchemaDrop {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "relation",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Relation,
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
                            "relation" => Ok(GeneratedField::Relation),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SchemaDrop;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.SchemaDrop")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SchemaDrop, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut relation__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Relation => {
                            if relation__.is_some() {
                                return Err(serde::de::Error::duplicate_field("relation"));
                            }
                            relation__ = map_.next_value()?;
                        }
                    }
                }
                Ok(SchemaDrop {
                    relation: relation__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.SchemaDrop", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SchemaDropMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.SchemaDropMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SchemaDropMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SchemaDropMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.SchemaDropMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SchemaDropMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(SchemaDropMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.SchemaDropMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TypeCodeNotFound {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.type_code != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.TypeCodeNotFound", len)?;
        if self.type_code != 0 {
            struct_ser.serialize_field("typeCode", &self.type_code)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TypeCodeNotFound {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "type_code",
            "typeCode",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TypeCode,
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
                            "typeCode" | "type_code" => Ok(GeneratedField::TypeCode),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TypeCodeNotFound;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.TypeCodeNotFound")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TypeCodeNotFound, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut type_code__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::TypeCode => {
                            if type_code__.is_some() {
                                return Err(serde::de::Error::duplicate_field("typeCode"));
                            }
                            type_code__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(TypeCodeNotFound {
                    type_code: type_code__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.TypeCodeNotFound", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TypeCodeNotFoundMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.TypeCodeNotFoundMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TypeCodeNotFoundMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TypeCodeNotFoundMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.TypeCodeNotFoundMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TypeCodeNotFoundMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(TypeCodeNotFoundMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.TypeCodeNotFoundMsg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for WriteCatalogFailure {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.num_exceptions != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.WriteCatalogFailure", len)?;
        if self.num_exceptions != 0 {
            struct_ser.serialize_field("numExceptions", &self.num_exceptions)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for WriteCatalogFailure {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "num_exceptions",
            "numExceptions",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NumExceptions,
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
                            "numExceptions" | "num_exceptions" => Ok(GeneratedField::NumExceptions),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WriteCatalogFailure;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.WriteCatalogFailure")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<WriteCatalogFailure, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut num_exceptions__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NumExceptions => {
                            if num_exceptions__.is_some() {
                                return Err(serde::de::Error::duplicate_field("numExceptions"));
                            }
                            num_exceptions__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(WriteCatalogFailure {
                    num_exceptions: num_exceptions__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.WriteCatalogFailure", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for WriteCatalogFailureMsg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.info.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.fields.adapter_types.WriteCatalogFailureMsg", len)?;
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            struct_ser.serialize_field("data", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for WriteCatalogFailureMsg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
            Data,
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
                            "info" => Ok(GeneratedField::Info),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WriteCatalogFailureMsg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.fields.adapter_types.WriteCatalogFailureMsg")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<WriteCatalogFailureMsg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info__ = map_.next_value()?;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(WriteCatalogFailureMsg {
                    info: info__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.fields.adapter_types.WriteCatalogFailureMsg", FIELDS, GeneratedVisitor)
    }
}
