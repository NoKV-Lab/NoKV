//! Black-box characterization of `nokv-agent`'s public surface.

#[test]
fn index_error_display_is_user_facing() {
    let rendered =
        nokv_agent::AgentIndexError::InvalidArgument("limit must be positive".to_owned())
            .to_string();
    assert_eq!(
        rendered,
        "invalid agent index argument: limit must be positive"
    );
}

#[test]
fn holt_store_is_exported_as_the_backend() {
    fn assert_store<S: nokv_agent::AgentStore>() {}
    assert_store::<nokv_agent::HoltAgentStore>();
}
