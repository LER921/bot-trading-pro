use uuid::Uuid;

pub fn new_id() -> String {
    Uuid::new_v4().simple().to_string()
}

pub fn new_correlation_id() -> String {
    format!("corr-{}", new_id())
}

pub fn new_request_id() -> String {
    format!("req-{}", new_id())
}

pub fn new_client_order_id(prefix: &str) -> String {
    let short = new_id();
    format!("{prefix}-{}", &short[..20])
}
