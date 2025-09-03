use serde::{Deserialize, Deserializer, Serializer};
use uuid::Uuid;

pub mod required {
    use super::*;
    pub fn serialize<S>(uuid: &Uuid, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&uuid.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Uuid, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Uuid::try_parse(&s).map_err(serde::de::Error::custom)
    }
}

pub mod optional {
    use super::*;
    pub fn serialize<S>(uuid: &Option<Uuid>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match uuid {
            Some(u) => serializer.serialize_str(&u.to_string()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Uuid>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt = Option::<String>::deserialize(deserializer)?;
        match opt {
            Some(s) => Ok(Some(Uuid::try_parse(&s).map_err(serde::de::Error::custom)?)),
            None => Ok(None),
        }
    }
}
