use std::borrow::Cow;

use url::Url;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, Hash, PartialEq, Eq)]
pub struct Resolver {
    /// A unique name for the given GraphQL resolver instance.
    name: String,

    /// The name of this GraphQL resolver instance.
    ///
    /// Each instance is expected to have a unique name, as the name of the instance is used as the
    /// field name within which the root upstream fields are exposed.
    pub namespace: Option<String>,

    /// The prefix for this GraphQL resolver if any.
    ///
    /// If not present this will default to the namespace above, mostly for backwards
    /// compatability reasons.
    ///
    /// This is used by the serializer to make sure there is no collision between global
    /// types. E.g. if a `User` type exists, it won't be overwritten by the same type of the
    /// upstream server, as it'll be prefixed as `MyPrefixUser`.
    pub type_prefix: Option<String>,

    /// The URL of the upstream GraphQL API.
    ///
    /// This should point to the actual query endpoint, not a publicly available playground or any
    /// other destination.
    pub url: Url,
}

impl Resolver {
    #[must_use]
    pub fn new(name: String, url: Url, namespace: Option<String>, type_prefix: Option<String>) -> Self {
        Self {
            name,
            url,
            namespace,
            type_prefix,
        }
    }

    #[must_use]
    pub fn name(&self) -> Cow<'_, String> {
        Cow::Borrowed(&self.name)
    }

    #[cfg(test)]
    pub fn stub(name: &str, namespace: impl AsRef<str>, url: impl AsRef<str>) -> Self {
        let namespace = match namespace.as_ref() {
            "" => None,
            v => Some(v.to_owned()),
        };

        Self {
            name: name.to_string(),
            type_prefix: namespace.clone(),
            namespace,
            url: Url::parse(url.as_ref()).expect("valid url"),
        }
    }
}
