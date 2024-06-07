use std::collections::BTreeMap;

use engine::ErrorCode;

use super::ResponsePath;

#[derive(Debug, Default)]
pub(crate) struct GraphqlError {
    pub message: String,
    pub locations: Vec<crate::operation::Location>,
    pub path: Option<ResponsePath>,
    pub extensions: BTreeMap<String, serde_json::Value>,
    // pub extensions: GraphqlErrorExtensions,
}

// #[derive(Debug, Default)]
// pub(crate) struct GraphqlErrorExtensions {
//     pub code: Option<ErrorCode>,
//     // ensures consistent ordering for tests
//     pub other: BTreeMap<String, serde_json::Value>,
// }

impl GraphqlError {
    pub fn new(message: impl Into<String>) -> Self {
        GraphqlError {
            message: message.into(),
            ..Default::default()
        }
    }

    pub fn with_error_code(mut self, code: ErrorCode) -> Self {
        self.extensions
            .insert("code".to_string(), serde_json::Value::String(code.to_string()));
        self
    }

    pub fn internal_server_error() -> Self {
        GraphqlError::new("Internal server error").with_error_code(ErrorCode::InternalServerError)
    }
}

// impl GraphqlErrorExtensions {
//     pub fn is_empty(&self) -> bool {
//         self.code.is_none() && self.other.is_empty()
//     }
// }
//
// impl serde::Serialize for GraphqlErrorExtensions {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer,
//     {
//         use serde::ser::{SerializeMap, SerializeSeq};
//         let mut map = serializer.serialize_map(Some(self.other.len() + self.code.is_some() as usize))?;
//         if let Some(code) = &self.code {
//             map.serialize_entry("code", code)?;
//         }
//         for (key, value) in &self.other {
//             map.serialize_entry(key, value)?;
//         }
//         map.end()
//     }
// }
