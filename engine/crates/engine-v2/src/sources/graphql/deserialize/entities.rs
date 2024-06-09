use std::fmt;

use serde::{
    de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor},
    Deserializer,
};

use crate::{plan::PlanWalker, response::ResponsePartMut, sources::ExecutionError};

pub(in crate::sources::graphql) struct EntitiesDataSeed<'a> {
    pub response_part: &'a ResponsePartMut<'a>,
    pub plan: PlanWalker<'a>,
}

impl<'de, 'a> DeserializeSeed<'de> for EntitiesDataSeed<'a> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de, 'a> Visitor<'de> for EntitiesDataSeed<'a> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("data with an entities list")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some(key) = map.next_key::<EntitiesKey>()? {
            match key {
                EntitiesKey::Entities => {
                    map.next_value_seed(EntitiesSeed {
                        response_part: self.response_part,
                        plan: self.plan,
                    })?;
                }
                EntitiesKey::Unknown => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(())
    }
}

#[derive(serde::Deserialize)]
enum EntitiesKey {
    #[serde(rename = "_entities")]
    Entities,
    #[serde(other)]
    Unknown,
}

struct EntitiesSeed<'a> {
    response_part: &'a ResponsePartMut<'a>,
    plan: PlanWalker<'a>,
}

impl<'de, 'a> DeserializeSeed<'de> for EntitiesSeed<'a> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(self)
    }
}

impl<'de, 'a> Visitor<'de> for EntitiesSeed<'a> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a non null entities list")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(seed) = self.response_part.next_seed(self.plan) {
            match seq.next_element_seed(seed) {
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(err) => {
                    // Discarding the rest of the list
                    while seq.next_element::<IgnoredAny>().unwrap_or_default().is_some() {}
                    return Err(err);
                }
            }
        }
        if seq.next_element::<IgnoredAny>()?.is_some() {
            self.response_part.push_error(ExecutionError::Internal(
                "Received more entities than expected".to_string(),
            ));
            while seq.next_element::<IgnoredAny>()?.is_some() {}
        }
        Ok(())
    }
}
