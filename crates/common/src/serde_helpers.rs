use serde::{Deserialize, Deserializer, Serializer};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

pub mod option_rfc3339 {
    use super::*;

    pub fn serialize<S>(
        value: &Option<OffsetDateTime>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(timestamp) => serializer.serialize_some(
                &timestamp
                    .format(&Rfc3339)
                    .map_err(serde::ser::Error::custom)?,
            ),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<OffsetDateTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Option::<String>::deserialize(deserializer)?;
        value
            .map(|raw| OffsetDateTime::parse(&raw, &Rfc3339).map_err(serde::de::Error::custom))
            .transpose()
    }
}
