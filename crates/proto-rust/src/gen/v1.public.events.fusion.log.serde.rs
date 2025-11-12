impl serde::Serialize for CompiledCodeInline {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.sql.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.events.fusion.log.CompiledCodeInline", len)?;
        if !self.sql.is_empty() {
            struct_ser.serialize_field("sql", &self.sql)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CompiledCodeInline {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "sql",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Sql,
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
                            "sql" => Ok(GeneratedField::Sql),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CompiledCodeInline;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.events.fusion.log.CompiledCodeInline")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CompiledCodeInline, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut sql__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Sql => {
                            if sql__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sql"));
                            }
                            sql__ = Some(map_.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(CompiledCodeInline {
                    sql: sql__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.events.fusion.log.CompiledCodeInline", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ListItemOutput {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.output_format != 0 {
            len += 1;
        }
        if !self.content.is_empty() {
            len += 1;
        }
        if self.unique_id.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.events.fusion.log.ListItemOutput", len)?;
        if self.output_format != 0 {
            let v = ListOutputFormat::try_from(self.output_format)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.output_format)))?;
            struct_ser.serialize_field("output_format", &v)?;
        }
        if !self.content.is_empty() {
            struct_ser.serialize_field("content", &self.content)?;
        }
        if let Some(v) = self.unique_id.as_ref() {
            struct_ser.serialize_field("unique_id", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ListItemOutput {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "output_format",
            "outputFormat",
            "content",
            "unique_id",
            "uniqueId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            OutputFormat,
            Content,
            UniqueId,
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
                            "outputFormat" | "output_format" => Ok(GeneratedField::OutputFormat),
                            "content" => Ok(GeneratedField::Content),
                            "uniqueId" | "unique_id" => Ok(GeneratedField::UniqueId),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ListItemOutput;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.events.fusion.log.ListItemOutput")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ListItemOutput, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut output_format__ = None;
                let mut content__ = None;
                let mut unique_id__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::OutputFormat => {
                            if output_format__.is_some() {
                                return Err(serde::de::Error::duplicate_field("outputFormat"));
                            }
                            output_format__ = Some(map_.next_value::<ListOutputFormat>()? as i32);
                        }
                        GeneratedField::Content => {
                            if content__.is_some() {
                                return Err(serde::de::Error::duplicate_field("content"));
                            }
                            content__ = Some(map_.next_value()?);
                        }
                        GeneratedField::UniqueId => {
                            if unique_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uniqueId"));
                            }
                            unique_id__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(ListItemOutput {
                    output_format: output_format__.unwrap_or_default(),
                    content: content__.unwrap_or_default(),
                    unique_id: unique_id__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.events.fusion.log.ListItemOutput", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ListOutputFormat {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "LIST_OUTPUT_FORMAT_UNSPECIFIED",
            Self::Json => "LIST_OUTPUT_FORMAT_JSON",
            Self::Selector => "LIST_OUTPUT_FORMAT_SELECTOR",
            Self::Name => "LIST_OUTPUT_FORMAT_NAME",
            Self::Path => "LIST_OUTPUT_FORMAT_PATH",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for ListOutputFormat {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "LIST_OUTPUT_FORMAT_UNSPECIFIED",
            "LIST_OUTPUT_FORMAT_JSON",
            "LIST_OUTPUT_FORMAT_SELECTOR",
            "LIST_OUTPUT_FORMAT_NAME",
            "LIST_OUTPUT_FORMAT_PATH",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ListOutputFormat;

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
                    "LIST_OUTPUT_FORMAT_UNSPECIFIED" => Ok(ListOutputFormat::Unspecified),
                    "LIST_OUTPUT_FORMAT_JSON" => Ok(ListOutputFormat::Json),
                    "LIST_OUTPUT_FORMAT_SELECTOR" => Ok(ListOutputFormat::Selector),
                    "LIST_OUTPUT_FORMAT_NAME" => Ok(ListOutputFormat::Name),
                    "LIST_OUTPUT_FORMAT_PATH" => Ok(ListOutputFormat::Path),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for LogMessage {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.code.is_some() {
            len += 1;
        }
        if self.dbt_core_event_code.is_some() {
            len += 1;
        }
        if self.original_severity_number != 0 {
            len += 1;
        }
        if !self.original_severity_text.is_empty() {
            len += 1;
        }
        if self.unique_id.is_some() {
            len += 1;
        }
        if self.file.is_some() {
            len += 1;
        }
        if self.line.is_some() {
            len += 1;
        }
        if self.phase.is_some() {
            len += 1;
        }
        if self.package_name.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.events.fusion.log.LogMessage", len)?;
        if let Some(v) = self.code.as_ref() {
            struct_ser.serialize_field("code", v)?;
        }
        if let Some(v) = self.dbt_core_event_code.as_ref() {
            struct_ser.serialize_field("dbt_core_event_code", v)?;
        }
        if self.original_severity_number != 0 {
            let v = super::compat::SeverityNumber::try_from(self.original_severity_number)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.original_severity_number)))?;
            struct_ser.serialize_field("original_severity_number", &v)?;
        }
        if !self.original_severity_text.is_empty() {
            struct_ser.serialize_field("original_severity_text", &self.original_severity_text)?;
        }
        if let Some(v) = self.unique_id.as_ref() {
            struct_ser.serialize_field("unique_id", v)?;
        }
        if let Some(v) = self.file.as_ref() {
            struct_ser.serialize_field("file", v)?;
        }
        if let Some(v) = self.line.as_ref() {
            struct_ser.serialize_field("line", v)?;
        }
        if let Some(v) = self.phase.as_ref() {
            let v = super::phase::ExecutionPhase::try_from(*v)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
            struct_ser.serialize_field("phase", &v)?;
        }
        if let Some(v) = self.package_name.as_ref() {
            struct_ser.serialize_field("package_name", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LogMessage {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "code",
            "dbt_core_event_code",
            "dbtCoreEventCode",
            "original_severity_number",
            "originalSeverityNumber",
            "original_severity_text",
            "originalSeverityText",
            "unique_id",
            "uniqueId",
            "file",
            "line",
            "phase",
            "package_name",
            "packageName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Code,
            DbtCoreEventCode,
            OriginalSeverityNumber,
            OriginalSeverityText,
            UniqueId,
            File,
            Line,
            Phase,
            PackageName,
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
                            "code" => Ok(GeneratedField::Code),
                            "dbtCoreEventCode" | "dbt_core_event_code" => Ok(GeneratedField::DbtCoreEventCode),
                            "originalSeverityNumber" | "original_severity_number" => Ok(GeneratedField::OriginalSeverityNumber),
                            "originalSeverityText" | "original_severity_text" => Ok(GeneratedField::OriginalSeverityText),
                            "uniqueId" | "unique_id" => Ok(GeneratedField::UniqueId),
                            "file" => Ok(GeneratedField::File),
                            "line" => Ok(GeneratedField::Line),
                            "phase" => Ok(GeneratedField::Phase),
                            "packageName" | "package_name" => Ok(GeneratedField::PackageName),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LogMessage;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.events.fusion.log.LogMessage")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<LogMessage, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut code__ = None;
                let mut dbt_core_event_code__ = None;
                let mut original_severity_number__ = None;
                let mut original_severity_text__ = None;
                let mut unique_id__ = None;
                let mut file__ = None;
                let mut line__ = None;
                let mut phase__ = None;
                let mut package_name__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Code => {
                            if code__.is_some() {
                                return Err(serde::de::Error::duplicate_field("code"));
                            }
                            code__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::DbtCoreEventCode => {
                            if dbt_core_event_code__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dbtCoreEventCode"));
                            }
                            dbt_core_event_code__ = map_.next_value()?;
                        }
                        GeneratedField::OriginalSeverityNumber => {
                            if original_severity_number__.is_some() {
                                return Err(serde::de::Error::duplicate_field("originalSeverityNumber"));
                            }
                            original_severity_number__ = Some(map_.next_value::<super::compat::SeverityNumber>()? as i32);
                        }
                        GeneratedField::OriginalSeverityText => {
                            if original_severity_text__.is_some() {
                                return Err(serde::de::Error::duplicate_field("originalSeverityText"));
                            }
                            original_severity_text__ = Some(map_.next_value()?);
                        }
                        GeneratedField::UniqueId => {
                            if unique_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uniqueId"));
                            }
                            unique_id__ = map_.next_value()?;
                        }
                        GeneratedField::File => {
                            if file__.is_some() {
                                return Err(serde::de::Error::duplicate_field("file"));
                            }
                            file__ = map_.next_value()?;
                        }
                        GeneratedField::Line => {
                            if line__.is_some() {
                                return Err(serde::de::Error::duplicate_field("line"));
                            }
                            line__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::Phase => {
                            if phase__.is_some() {
                                return Err(serde::de::Error::duplicate_field("phase"));
                            }
                            phase__ = map_.next_value::<::std::option::Option<super::phase::ExecutionPhase>>()?.map(|x| x as i32);
                        }
                        GeneratedField::PackageName => {
                            if package_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("packageName"));
                            }
                            package_name__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(LogMessage {
                    code: code__,
                    dbt_core_event_code: dbt_core_event_code__,
                    original_severity_number: original_severity_number__.unwrap_or_default(),
                    original_severity_text: original_severity_text__.unwrap_or_default(),
                    unique_id: unique_id__,
                    file: file__,
                    line: line__,
                    phase: phase__,
                    package_name: package_name__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.events.fusion.log.LogMessage", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for UserLogMessage {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.is_print {
            len += 1;
        }
        if !self.dbt_core_event_code.is_empty() {
            len += 1;
        }
        if self.unique_id.is_some() {
            len += 1;
        }
        if self.phase.is_some() {
            len += 1;
        }
        if self.package_name.is_some() {
            len += 1;
        }
        if self.line.is_some() {
            len += 1;
        }
        if self.column.is_some() {
            len += 1;
        }
        if self.relative_path.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.events.fusion.log.UserLogMessage", len)?;
        if self.is_print {
            struct_ser.serialize_field("is_print", &self.is_print)?;
        }
        if !self.dbt_core_event_code.is_empty() {
            struct_ser.serialize_field("dbt_core_event_code", &self.dbt_core_event_code)?;
        }
        if let Some(v) = self.unique_id.as_ref() {
            struct_ser.serialize_field("unique_id", v)?;
        }
        if let Some(v) = self.phase.as_ref() {
            let v = super::phase::ExecutionPhase::try_from(*v)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
            struct_ser.serialize_field("phase", &v)?;
        }
        if let Some(v) = self.package_name.as_ref() {
            struct_ser.serialize_field("package_name", v)?;
        }
        if let Some(v) = self.line.as_ref() {
            struct_ser.serialize_field("line", v)?;
        }
        if let Some(v) = self.column.as_ref() {
            struct_ser.serialize_field("column", v)?;
        }
        if let Some(v) = self.relative_path.as_ref() {
            struct_ser.serialize_field("relative_path", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UserLogMessage {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "is_print",
            "isPrint",
            "dbt_core_event_code",
            "dbtCoreEventCode",
            "unique_id",
            "uniqueId",
            "phase",
            "package_name",
            "packageName",
            "line",
            "column",
            "relative_path",
            "relativePath",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            IsPrint,
            DbtCoreEventCode,
            UniqueId,
            Phase,
            PackageName,
            Line,
            Column,
            RelativePath,
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
                            "isPrint" | "is_print" => Ok(GeneratedField::IsPrint),
                            "dbtCoreEventCode" | "dbt_core_event_code" => Ok(GeneratedField::DbtCoreEventCode),
                            "uniqueId" | "unique_id" => Ok(GeneratedField::UniqueId),
                            "phase" => Ok(GeneratedField::Phase),
                            "packageName" | "package_name" => Ok(GeneratedField::PackageName),
                            "line" => Ok(GeneratedField::Line),
                            "column" => Ok(GeneratedField::Column),
                            "relativePath" | "relative_path" => Ok(GeneratedField::RelativePath),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = UserLogMessage;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.events.fusion.log.UserLogMessage")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<UserLogMessage, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut is_print__ = None;
                let mut dbt_core_event_code__ = None;
                let mut unique_id__ = None;
                let mut phase__ = None;
                let mut package_name__ = None;
                let mut line__ = None;
                let mut column__ = None;
                let mut relative_path__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::IsPrint => {
                            if is_print__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isPrint"));
                            }
                            is_print__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DbtCoreEventCode => {
                            if dbt_core_event_code__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dbtCoreEventCode"));
                            }
                            dbt_core_event_code__ = Some(map_.next_value()?);
                        }
                        GeneratedField::UniqueId => {
                            if unique_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uniqueId"));
                            }
                            unique_id__ = map_.next_value()?;
                        }
                        GeneratedField::Phase => {
                            if phase__.is_some() {
                                return Err(serde::de::Error::duplicate_field("phase"));
                            }
                            phase__ = map_.next_value::<::std::option::Option<super::phase::ExecutionPhase>>()?.map(|x| x as i32);
                        }
                        GeneratedField::PackageName => {
                            if package_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("packageName"));
                            }
                            package_name__ = map_.next_value()?;
                        }
                        GeneratedField::Line => {
                            if line__.is_some() {
                                return Err(serde::de::Error::duplicate_field("line"));
                            }
                            line__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::Column => {
                            if column__.is_some() {
                                return Err(serde::de::Error::duplicate_field("column"));
                            }
                            column__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::RelativePath => {
                            if relative_path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("relativePath"));
                            }
                            relative_path__ = map_.next_value()?;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(UserLogMessage {
                    is_print: is_print__.unwrap_or_default(),
                    dbt_core_event_code: dbt_core_event_code__.unwrap_or_default(),
                    unique_id: unique_id__,
                    phase: phase__,
                    package_name: package_name__,
                    line: line__,
                    column: column__,
                    relative_path: relative_path__,
                })
            }
        }
        deserializer.deserialize_struct("v1.public.events.fusion.log.UserLogMessage", FIELDS, GeneratedVisitor)
    }
}
