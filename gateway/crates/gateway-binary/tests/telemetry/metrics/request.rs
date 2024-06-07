use std::time::Duration;

use super::{with_gateway, ExponentialHistogramRow};

#[test]
fn basic() {
    with_gateway(|service_name, start_time_unix, gateway, clickhouse| async move {
        let resp = gateway.gql::<serde_json::Value>("{ __typename }").send().await;
        insta::assert_json_snapshot!(resp, @r###"
        {
          "data": {
            "__typename": "Query"
          }
        }
        "###);
        tokio::time::sleep(Duration::from_secs(2)).await;

        let row = clickhouse
            .query(
                r#"
                SELECT Count, Attributes
                FROM otel_metrics_exponential_histogram
                WHERE ServiceName = ? AND StartTimeUnix >= ?
                    AND ScopeName = 'grafbase'
                    AND MetricName = 'request_latency'
                "#,
            )
            .bind(&service_name)
            .bind(start_time_unix)
            .fetch_one::<ExponentialHistogramRow>()
            .await
            .unwrap();
        insta::assert_json_snapshot!(row, @r###"
        {
          "Count": 1,
          "Attributes": {}
        }
        "###);
    });
}

#[test]
fn request_error() {
    with_gateway(|service_name, start_time_unix, gateway, clickhouse| async move {
        let resp = gateway.gql::<serde_json::Value>("{ __typ__ename }").send().await;
        insta::assert_json_snapshot!(resp, @r###"
        {
          "errors": [
            {
              "message": "Query does not have a field named '__typ__ename'",
              "locations": [
                {
                  "line": 1,
                  "column": 3
                }
              ]
            }
          ]
        }
        "###);
        tokio::time::sleep(Duration::from_secs(2)).await;

        let row = clickhouse
            .query(
                r#"
                SELECT Count, Attributes
                FROM otel_metrics_exponential_histogram
                WHERE ServiceName = ? AND StartTimeUnix >= ?
                    AND ScopeName = 'grafbase'
                    AND MetricName = 'request_latency'
                "#,
            )
            .bind(&service_name)
            .bind(start_time_unix)
            .fetch_one::<ExponentialHistogramRow>()
            .await
            .unwrap();
        insta::assert_json_snapshot!(row, @r###"
        {
          "Count": 1,
          "Attributes": {
            "gql.response.status": "REQUEST_ERROR"
          }
        }
        "###);
    });
}

#[test]
fn field_error() {
    with_gateway(|service_name, start_time_unix, gateway, clickhouse| async move {
        let resp = gateway
            .gql::<serde_json::Value>("{ __typename me { id } }")
            .send()
            .await;
        insta::assert_json_snapshot!(resp, @r###"
        {
          "data": {
            "__typename": "Query"
          },
          "errors": [
            {
              "message": "error sending request for url (http://127.0.0.1:46697/)"
            }
          ]
        }
        "###);
        tokio::time::sleep(Duration::from_secs(2)).await;

        let row = clickhouse
            .query(
                r#"
                SELECT Count, Attributes
                FROM otel_metrics_exponential_histogram
                WHERE ServiceName = ? AND StartTimeUnix >= ?
                    AND ScopeName = 'grafbase'
                    AND MetricName = 'request_latency'
                "#,
            )
            .bind(&service_name)
            .bind(start_time_unix)
            .fetch_one::<ExponentialHistogramRow>()
            .await
            .unwrap();
        insta::assert_json_snapshot!(row, @r###"
        {
          "Count": 1,
          "Attributes": {
            "gql.response.status": "FIELD_ERROR"
          }
        }
        "###);
    });
}

#[test]
fn field_error_data_null() {
    with_gateway(|service_name, start_time_unix, gateway, clickhouse| async move {
        let resp = gateway.gql::<serde_json::Value>("{ me { id } }").send().await;
        insta::assert_json_snapshot!(resp, @r###"
        {
          "data": {},
          "errors": [
            {
              "message": "error sending request for url (http://127.0.0.1:46697/)"
            }
          ]
        }
        "###);
        tokio::time::sleep(Duration::from_secs(2)).await;

        let row = clickhouse
            .query(
                r#"
                SELECT Count, Attributes
                FROM otel_metrics_exponential_histogram
                WHERE ServiceName = ? AND StartTimeUnix >= ?
                    AND ScopeName = 'grafbase'
                    AND MetricName = 'request_latency'
                "#,
            )
            .bind(&service_name)
            .bind(start_time_unix)
            .fetch_one::<ExponentialHistogramRow>()
            .await
            .unwrap();
        insta::assert_json_snapshot!(row, @r###"
        {
          "Count": 1,
          "Attributes": {
            "gql.response.status": "FIELD_ERROR"
          }
        }
        "###);
    });
}

#[test]
fn client() {
    with_gateway(|service_name, start_time_unix, gateway, clickhouse| async move {
        let resp = gateway
            .gql::<serde_json::Value>("{ __typename }")
            .header("x-grafbase-client-name", "test")
            .header("x-grafbase-client-version", "1.0.0")
            .send()
            .await;
        insta::assert_json_snapshot!(resp, @r###"
        {
          "data": {
            "__typename": "Query"
          }
        }
        "###);
        tokio::time::sleep(Duration::from_secs(2)).await;

        //
        // Unknown client
        //
        let row = clickhouse
            .query(
                r#"
                SELECT Count, Attributes
                FROM otel_metrics_exponential_histogram
                WHERE ServiceName = ? AND StartTimeUnix >= ?
                    AND ScopeName = 'grafbase'
                    AND MetricName = 'request_latency'
                    AND Attributes['http.headers.x-grafbase-client-name'] = 'unknown'
                "#,
            )
            .bind(&service_name)
            .bind(start_time_unix)
            .fetch_optional::<ExponentialHistogramRow>()
            .await
            .unwrap();
        assert_eq!(row, None);

        //
        // Unknown version
        //
        let row = clickhouse
            .query(
                r#"
                SELECT Count, Attributes
                FROM otel_metrics_exponential_histogram
                WHERE ServiceName = ? AND StartTimeUnix >= ?
                    AND ScopeName = 'grafbase'
                    AND MetricName = 'request_latency'
                    AND Attributes['http.headers.x-grafbase-client-name'] = 'test'
                    AND Attributes['http.headers.x-grafbase-client-version'] = 'unknown'
                "#,
            )
            .bind(&service_name)
            .bind(start_time_unix)
            .fetch_optional::<ExponentialHistogramRow>()
            .await
            .unwrap();
        assert_eq!(row, None);

        //
        // Known client & version
        //
        let row = clickhouse
            .query(
                r#"
                SELECT Count, Attributes
                FROM otel_metrics_exponential_histogram
                WHERE ServiceName = ? AND StartTimeUnix >= ?
                    AND ScopeName = 'grafbase'
                    AND MetricName = 'request_latency'
                    AND Attributes['http.headers.x-grafbase-client-name'] = 'test'
                    AND Attributes['http.headers.x-grafbase-client-version'] = '1.0.0'
                "#,
            )
            .bind(&service_name)
            .bind(start_time_unix)
            .fetch_one::<ExponentialHistogramRow>()
            .await
            .unwrap();
        insta::assert_json_snapshot!(row, @r###"
        {
          "Count": 1,
          "Attributes": {
            "http.headers.x-grafbase-client-name": "test",
            "http.headers.x-grafbase-client-version": "1.0.0"
          }
        }
        "###);
    });
}
