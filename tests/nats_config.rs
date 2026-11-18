use servicelib::runtime::config::NatsDataConnectorConfig;
#[test]
fn shared_connection_profile_with_runtime_secret_and_zero_default_exceptions(){
    let cases:serde_json::Value=serde_json::from_str(include_str!("fixtures/nats-connection-validation.json")).unwrap();
    for case in cases.as_array().unwrap(){
        let mut config=case["config"].clone();
        if config.get("servers").is_none(){config["servers"]=serde_json::json!(["nats://broker:4222"]);}
        let parsed=serde_json::from_value::<NatsDataConnectorConfig>(config);
        let invalid=case["invalidFields"].as_array().unwrap();
        let expected=invalid.iter().any(|v|!matches!(v.as_str(),Some("username"|"password"|"token")));
        // Runtime zero means use default; authored zero is rejected by topology validation.
        let zero=case["config"]["connectTimeoutMs"]==0 || case["config"]["operationTimeoutMs"]==0;
        let valid=parsed.is_ok_and(|config|config.validate().is_ok());
        assert_eq!(valid,!expected||zero,"{}",case["name"]);
    }
}
#[test]
fn runtime_authentication_is_exclusive_and_paired(){
    let mut c=NatsDataConnectorConfig{servers:vec!["nats://broker:4222".into()],username:"u".into(),password:"p".into(),..Default::default()};
    assert!(c.validate().is_ok());c.token="t".into();assert!(c.validate().is_err());c.token.clear();c.password.clear();assert!(c.validate().is_err());
}
