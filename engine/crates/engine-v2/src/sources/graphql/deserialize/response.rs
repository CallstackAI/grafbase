use std::fmt;

use grafbase_tracing::{gql_response_status::GraphqlResponseStatus, span::GqlRecorderSpanExt};
use serde::{
    de::{DeserializeSeed, IgnoredAny, MapAccess, Visitor},
    Deserializer,
};
use tracing::Span;

use super::errors::{ConcreteGraphqlErrorsSeed, GraphqlErrorsSeed};
use crate::response::ResponsePartMut;

pub(in crate::sources::graphql) struct GraphqlResponseSeed<'a, DataSeed> {
    error_path_converter: Box<dyn GraphqlErrorsSeed + 'a>,
    part: &'a ResponsePartMut<'a>,
    graphql_span: Option<Span>,
    data_seed: Option<DataSeed>,
}

impl<'a, DataSeed> GraphqlResponseSeed<'a, DataSeed> {
    pub fn new(
        error_path_converter: impl GraphqlErrorsSeed + 'a,
        part: &'a ResponsePartMut<'a>,
        seed: DataSeed,
    ) -> Self {
        Self {
            error_path_converter: Box::new(error_path_converter),
            part,
            graphql_span: None,
            data_seed: Some(seed),
        }
    }

    pub fn with_graphql_span(self, span: Span) -> Self {
        Self {
            graphql_span: Some(span),
            ..self
        }
    }
}

impl<'resp, 'de, DataSeed> DeserializeSeed<'de> for GraphqlResponseSeed<'resp, DataSeed>
where
    DataSeed: DeserializeSeed<'de, Value = ()>,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'resp, 'de, DataSeed> Visitor<'de> for GraphqlResponseSeed<'resp, DataSeed>
where
    DataSeed: DeserializeSeed<'de, Value = ()>,
{
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a valid GraphQL response")
    }

    fn visit_map<A>(mut self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut data_is_null_result = Ok(true);
        let mut errors = vec![];
        while let Some(key) = map.next_key::<ResponseKey>()? {
            match key {
                ResponseKey::Data => match self.data_seed.take() {
                    Some(seed) => {
                        data_is_null_result = map.next_value_seed(NullableDataSeed { seed });
                    }
                    None => continue,
                },
                ResponseKey::Errors => map.next_value_seed(ConcreteGraphqlErrorsSeed {
                    error_path_converter: self.error_path_converter.as_ref(),
                    errors: &mut errors,
                })?,
                ResponseKey::Unknown => {
                    map.next_value::<IgnoredAny>()?;
                }
            };
        }

        let data_is_present = self.data_seed.is_some();
        if let Some(span) = self.graphql_span {
            let status = if errors.is_empty() {
                GraphqlResponseStatus::Success
            } else if data_is_present {
                GraphqlResponseStatus::FieldError {
                    count: errors.len() as u64,
                    data_is_null: data_is_null_result?,
                }
            } else {
                GraphqlResponseStatus::RequestError {
                    count: errors.len() as u64,
                }
            };
            span.record_gql_status(status);
        }

        self.part.push_errors(errors);
        Ok(())
    }
}

struct NullableDataSeed<Seed> {
    seed: Seed,
}

impl<'de, Seed> DeserializeSeed<'de> for NullableDataSeed<Seed>
where
    Seed: DeserializeSeed<'de, Value = ()>,
{
    type Value = bool;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_option(self)
    }
}

impl<'de, Seed> Visitor<'de> for NullableDataSeed<Seed>
where
    Seed: DeserializeSeed<'de, Value = ()>,
{
    type Value = bool;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a nullable value")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_none()
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(true)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        self.seed.deserialize(deserializer)?;
        Ok(false)
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum ResponseKey {
    Data,
    Errors,
    #[serde(other)]
    Unknown,
}
