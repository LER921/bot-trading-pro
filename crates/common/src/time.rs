pub type Timestamp = time::OffsetDateTime;

pub fn now_utc() -> Timestamp {
    time::OffsetDateTime::now_utc()
}

pub fn unix_timestamp_ms(timestamp: Timestamp) -> i128 {
    timestamp.unix_timestamp_nanos() / 1_000_000
}
