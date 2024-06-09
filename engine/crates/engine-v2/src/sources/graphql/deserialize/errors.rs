use std::collections::BTreeMap;

use serde::{de::DeserializeSeed, Deserializer};

use crate::response::{GraphqlError, ResponseKeys, ResponsePartMut, ResponsePath, UnpackedResponseEdge};

#[derive(serde::Deserialize)]
pub(super) struct SubgraphGraphqlError {
    pub message: String,
    #[serde(default)]
    pub locations: serde_json::Value,
    #[serde(default)]
    pub path: serde_json::Value,
    #[serde(default)]
    pub extensions: serde_json::Value,
}

pub(super) struct ConcreteGraphqlErrorsSeed<T>(T);

pub(in crate::sources::graphql) trait GraphqlErrorsSeed<'a> {
    fn part(&self) -> &ResponsePartMut<'a>;
    fn convert(&self, path: &serde_json::Value) -> Option<ResponsePath>;
}

pub struct RootErrorPathConverter<'a> {
    part: &'a ResponsePartMut<'a>,
    response_keys: &'a ResponseKeys,
}

impl<'a> RootErrorPathConverter<'a> {
    pub fn new(part: &'a ResponsePartMut<'a>, response_keys: &'a ResponseKeys) -> Self {
        Self { part, response_keys }
    }
}

impl<'a> GraphqlErrorsSeed<'a> for RootErrorPathConverter<'a> {
    fn part(&self) -> &ResponsePartMut<'a> {
        self.part
    }

    fn convert(&self, path: &serde_json::Value) -> Option<ResponsePath> {
        let mut out = ResponsePath::default();
        for edge in path.as_array()? {
            if let Some(index) = edge.as_u64() {
                out.push(index as usize);
            } else {
                let key = edge.as_str()?;
                let response_key = self.response_keys.get(key)?;
                // We need this path for two reasons only:
                // - To report nicely in the error message
                // - To know whether an error exist if we're missing the appropriate data for a
                //   response object.
                // For the latter we only check whether there is an error at all, not if it's one
                // that could actually propagate up to the root response object. That's a lot more
                // work and very likely useless.
                // So, currently, we'll never read those fields and treat them as extra field
                // to cram them into an ResponseEdge.
                out.push(UnpackedResponseEdge::ExtraFieldResponseKey(response_key.into()))
            }
        }
        Some(out)
    }
}

impl<T, 'de, 'a> DeserializeSeed<'de> for T
where
    T: GraphqlErrorsSeed<'a>,
{
    type Value = usize;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let errors = <Vec<SubgraphGraphqlError> as serde::Deserialize>::deserialize(deserializer)?;
        let errors_count = errors.len();
        self.part().push_errors(errors.into_iter().map(|error| {
            let mut extensions = BTreeMap::new();
            if !error.locations.is_null() {
                extensions.insert("upstream_locations".to_string(), error.locations);
            }
            let path = self.convert(&error.path);
            if path.is_none() && !error.path.is_null() {
                extensions.insert("upstream_path".to_string(), error.path);
            }
            if !error.extensions.is_null() {
                extensions.insert("upstream_extensions".to_string(), error.extensions);
            }
            GraphqlError {
                message: format!("Upstream error: {}", error.message),
                path,
                extensions,
                ..Default::default()
            }
        }));
        Ok(errors_count)
    }
}
