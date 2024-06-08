use std::fmt;

use grafbase_tracing::{gql_response_status::GraphqlResponseStatus, span::GqlRecorderSpanExt};
use serde::{
    de::{DeserializeSeed, IgnoredAny, MapAccess, Visitor},
    Deserializer,
};
use tracing::Span;

use super::errors::UpstreamGraphqlErrorsSeed;
use crate::{
    execution::{ExecutionError, ExecutionResult},
    response::ResponsePart,
};

pub fn ingest_deserializer_into_response<'part, 'de, DataSeed, D>(
    part: &'part ResponsePart,
    sugraph_gql_request_span: Option<Span>,
    seed: DataSeed,
    deserializer: D,
) -> ExecutionResult<()>
where
    D: Deserializer<'de>,
    DataSeed: DeserializeSeed<'de, Value = ()>,
{
    GraphqlResponseSeed {
        part,
        sugraph_gql_request_span,
        seed: Some(seed),
    }
    .deserialize(deserializer)
    .map_err(|err| ExecutionError::DeserializationError(err.to_string()))
}

struct GraphqlResponseSeed<'parent, DataSeed> {
    part: &'parent ResponsePart,
    sugraph_gql_request_span: Option<Span>,
    seed: Option<DataSeed>,
}

impl<'part, 'de, DataSeed> DeserializeSeed<'de> for GraphqlResponseSeed<'part, DataSeed>
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

impl<'part, 'de, DataSeed> Visitor<'de> for GraphqlResponseSeed<'part, DataSeed>
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
                ResponseKey::Data => match self.seed.take() {
                    Some(seed) => {
                        data_is_null_result = map.next_value_seed(NullableDataSeed { seed });
                    }
                    None => continue,
                },
                ResponseKey::Errors => map.next_value_seed(UpstreamGraphqlErrorsSeed { errors: &mut errors })?,
                ResponseKey::Unknown => {
                    map.next_value::<IgnoredAny>()?;
                }
            };
        }

        if let Some(span) = self.sugraph_gql_request_span {
            let data_is_present = self.seed.is_some();
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
