impl serde::Serialize for OnboardingScreenShown {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.screen != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("v1.public.events.fusion.onboarding.OnboardingScreenShown", len)?;
        if self.screen != 0 {
            let v = super::OnboardingScreen::try_from(self.screen)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.screen)))?;
            struct_ser.serialize_field("screen", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for OnboardingScreenShown {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "screen",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Screen,
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
                            "screen" => Ok(GeneratedField::Screen),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = OnboardingScreenShown;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct v1.public.events.fusion.onboarding.OnboardingScreenShown")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<OnboardingScreenShown, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut screen__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Screen => {
                            if screen__.is_some() {
                                return Err(serde::de::Error::duplicate_field("screen"));
                            }
                            screen__ = Some(map_.next_value::<super::OnboardingScreen>()? as i32);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(OnboardingScreenShown {
                    screen: screen__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("v1.public.events.fusion.onboarding.OnboardingScreenShown", FIELDS, GeneratedVisitor)
    }
}
