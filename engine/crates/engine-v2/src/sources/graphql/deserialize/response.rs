use std::fmt;

use grafbase_tracing::gql_response_status::GraphqlResponseStatus;
use serde::{
    de::{DeserializeSeed, IgnoredAny, MapAccess, Visitor},
    Deserializer,
};

use super::errors::UpstreamGraphqlErrorsSeed;
use crate::response::{GraphqlError, SeedContext};

pub fn ingest_deserializer_into_response<'ctx, 'de, DataSeed, D>(
    ctx: &SeedContext<'ctx>,
    seed: DataSeed,
    deserializer: D,
) -> Option<GraphqlResponseStatus>
where
    D: Deserializer<'de>,
    DataSeed: DeserializeSeed<'de, Value = ()>,
{
    let result = GraphqlResponseSeed { ctx, seed: Some(seed) }.deserialize(deserializer);
    match result {
        Ok(status) => Some(status),
        Err(err) => {
            report_error_if_no_others(ctx, format!("Error decoding response from upstream: {err}"));
            None
        }
    }
}

struct GraphqlResponseSeed<'ctx, 'parent, DataSeed> {
    ctx: &'parent SeedContext<'ctx>,
    seed: Option<DataSeed>,
}

impl<'ctx, 'parent, 'de, DataSeed> DeserializeSeed<'de> for GraphqlResponseSeed<'ctx, 'parent, DataSeed>
where
    DataSeed: DeserializeSeed<'de, Value = ()>,
{
    type Value = GraphqlResponseStatus;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'ctx, 'parent, 'de, DataSeed> Visitor<'de> for GraphqlResponseSeed<'ctx, 'parent, DataSeed>
where
    DataSeed: DeserializeSeed<'de, Value = ()>,
{
    type Value = GraphqlResponseStatus;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a valid GraphQL response")
    }

    fn visit_map<A>(mut self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut data_result = DataSeedResult {
            is_null: true,
            err: None,
        };
        let mut errors = vec![];
        while let Some(key) = map.next_key::<ResponseKey>()? {
            match key {
                ResponseKey::Data => match self.seed.take() {
                    Some(seed) => {
                        data_result = map
                            .next_value_seed(InfaillibleNullableSeed { seed })
                            .expect("Infaillible by design.");
                    }
                    None => continue,
                },
                ResponseKey::Errors => map.next_value_seed(UpstreamGraphqlErrorsSeed { errors: &mut errors })?,
                ResponseKey::Unknown => {
                    map.next_value::<IgnoredAny>()?;
                }
            };
        }
        let data_is_present = self.seed.is_some();
        let errors_count = {
            let mut part = self.ctx.borrow_mut_response_part();
            if !errors.is_empty() {
                // Replacing the any serde errors if the data is null/isn't present, they're not relevant anymore
                // since our request completely failed
                if data_result.is_null {
                    part.replace_errors(errors);
                } else {
                    part.push_errors(errors);
                }
            } else if data_result.is_null {
                part.push_error(GraphqlError {
                    message: "Data is null or missing".to_string(),
                    ..Default::default()
                });
            }

            part.errors_count()
        };

        Ok(if errors_count > 0 {
            if data_is_present {
                GraphqlResponseStatus::FieldError {
                    count: errors_count as u64,
                    data_is_null: data_result.is_null,
                }
            } else {
                GraphqlResponseStatus::RequestError {
                    count: errors_count as u64,
                }
            }
        } else {
            GraphqlResponseStatus::Success
        })
    }
}

struct InfaillibleNullableSeed<Seed> {
    seed: Seed,
}

struct DataSeedResult {
    is_null: bool,
    err: Option<String>,
}

impl<'de, Seed> DeserializeSeed<'de> for InfaillibleNullableSeed<Seed>
where
    Seed: DeserializeSeed<'de, Value = ()>,
{
    type Value = DataSeedResult;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_option(self)
    }
}

impl<'de, Seed> Visitor<'de> for InfaillibleNullableSeed<Seed>
where
    Seed: DeserializeSeed<'de, Value = ()>,
{
    type Value = DataSeedResult;

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
        Ok(DataSeedResult {
            is_null: true,
            err: None,
        })
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(DataSeedResult {
            is_null: false,
            err: self.seed.deserialize(deserializer).map_err(|err| err.to_string()).err(),
        })
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

fn report_error_if_no_others(ctx: &SeedContext<'_>, message: String) {
    let mut response_part = ctx.borrow_mut_response_part();
    // Only adding this if no other more precise errors were added.
    if !response_part.has_errors() {
        response_part.push_error(GraphqlError {
            message,
            ..Default::default()
        });
    }
}
