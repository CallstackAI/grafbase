use http::Response;
use http_body::Body;

use crate::gql_response_status::GraphqlResponseStatus;

/// Tracing target for logging
pub const GRAFBASE_TARGET: &str = "grafbase";
pub(crate) const SCOPE: &str = "grafbase";
pub(crate) const SCOPE_VERSION: &str = "1.0";

/// Cache span
pub mod cache;
/// GraphQL span
pub mod gql;
/// Request span
pub mod request;
/// Resolver span
pub mod resolver;
/// Subgraph span
pub mod subgraph;

/// Extension trait to record http response attributes
pub trait HttpRecorderSpanExt {
    /// Recording response attributes in the span
    fn record_response<B: Body>(&self, response: &Response<B>);
    /// Record response failure in the span
    fn record_failure(&self, error: String);
    /// Record response failure in the span
    fn record_status_code(&self, status_code: http::StatusCode);
}

/// Extension trait to record gql request attributes
pub trait GqlRecorderSpanExt {
    /// Record GraphQL request attributes in the span
    fn record_gql_request(&self, attributes: GqlRequestAttributes);
    /// Record GraphQL response attributes in the span
    fn record_gql_response(&self, attributes: GqlResponseAttributes);

    fn record_gql_status(&self, status: GraphqlResponseStatus) {
        self.record_gql_response(GqlResponseAttributes { status })
    }

    /// Record that the response has a field error
    fn record_field_errors(&self, count: usize, data_is_null: bool) {
        assert!(count > 0, "field errors count must be greater than 0");
        self.record_gql_response(GqlResponseAttributes {
            status: GraphqlResponseStatus::FieldError {
                count: count as u64,
                data_is_null,
            },
        })
    }

    /// Record that the response has a request error
    fn record_request_errors(&self, count: usize) {
        assert!(count > 0, "request errors count must be greater than 0");
        self.record_gql_response(GqlResponseAttributes {
            status: GraphqlResponseStatus::RequestError { count: count as u64 },
        })
    }
}

/// Wraps attributes of a graphql request intended to be recorded
#[derive(Debug)]
pub struct GqlRequestAttributes {
    /// GraphQL operation type
    pub operation_type: &'static str,
    /// GraphQL operation name
    pub operation_name: Option<String>,
}

/// Wraps attributes of a graphql response intended to be recorded
pub struct GqlResponseAttributes {
    pub status: GraphqlResponseStatus,
}

/// Extension trait to record resolver invocation attributes
pub trait ResolverInvocationRecorderSpanExt {
    /// Recording error details in the span
    fn record_failure(&self, error: &str);
}

/// Extension trait to record cache operation attributes
pub trait CacheRecorderSpanExt {
    /// Recording cache status in the span
    fn record_status(&self, value: http::HeaderValue);
    /// Recording cached operation as error
    fn record_error(&self);
}
