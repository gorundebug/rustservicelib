use servicelib::api::{CallSemantics, DataConnectorImplementation, ProgrammingLanguage, StreamApp};

#[test]
fn framework_api_deserializes_the_canonical_wire_shape() {
    let app: StreamApp = serde_json::from_value(serde_json::json!({
        "settings": {"name": "mixed-language-example"},
        "services": [{
            "id": 1,
            "name": "Rust Service",
            "programmingLanguage": 4,
            "modulePath": "github.com/gorundebug/rustservice",
            "defaultCallSemantics": 2,
            "httpHost": "0.0.0.0",
            "httpPort": 9091,
            "statusHandler": "/status",
            "metricsHandler": "/metrics",
            "grpcHost": "0.0.0.0",
            "grpcPort": 9201,
            "defaultGrpcTimeout": 5000,
            "color": "#4A90D9",
            "shutdownTimeout": 5000,
            "environment": "local"
        }],
        "streams": [],
        "links": [],
        "types": [],
        "dataConnectors": [{
            "id": 1,
            "name": "HTTP",
            "type": 1,
            "rustImplementation": "rust/axum"
        }],
        "endpoints": [],
        "pools": []
    }))
    .unwrap();

    assert_eq!(
        app.services[0].programming_language,
        ProgrammingLanguage::Rust
    );
    assert_eq!(
        app.services[0].default_call_semantics,
        CallSemantics::FunctionCall
    );
    assert_eq!(
        app.data_connectors[0].rust_implementation,
        Some(DataConnectorImplementation::RustAxum)
    );
}
