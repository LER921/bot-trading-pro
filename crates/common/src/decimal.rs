pub use rust_decimal::Decimal;

pub fn zero() -> Decimal {
    Decimal::ZERO
}

pub fn abs(value: Decimal) -> Decimal {
    if value.is_sign_negative() {
        -value
    } else {
        value
    }
}

pub fn clamp(value: Decimal, min: Decimal, max: Decimal) -> Decimal {
    if value < min {
        min
    } else if value > max {
        max
    } else {
        value
    }
}
