use std::env;
use std::fmt::Display;
use std::str::FromStr;

/// Reads a required environment variable as a [`String`].
///
/// # Panics
///
/// Panics if `key` is not set.
pub(in crate::config) fn env_var(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| panic!("{key} not set"))
}

/// Reads an optional environment variable as a [`String`].
///
/// Returns [`None`] if `key` is not set.
pub(in crate::config) fn env_var_opt(key: &str) -> Option<String> {
    env::var(key).ok()
}

/// Reads an environment variable as a [`String`], or returns `default` if unset.
pub(in crate::config) fn env_var_or(key: &str, default: impl Into<String>) -> String {
    env::var(key).unwrap_or_else(|_| default.into())
}

/// Reads a required environment variable and parses it as `T`.
///
/// # Panics
///
/// Panics if `key` is not set, or if the value cannot be parsed as `T`.
pub(in crate::config) fn env_var_parse<T>(key: &str) -> T
where
    T: FromStr,
    T::Err: Display,
{
    env::var(key)
        .map(|v| {
            v.parse().unwrap_or_else(|e| {
                panic!(
                    "{key} could not be parsed to {}: {e}",
                    std::any::type_name::<T>()
                )
            })
        })
        .expect("{key} not set")
}

/// Reads an environment variable and parses it as `T`, or returns `default` if unset.
///
/// # Panics
///
/// Panics if `key` is set but the value cannot be parsed as `T`.
pub(in crate::config) fn env_var_parse_or<T>(key: &str, default: T) -> T
where
    T: FromStr,
    T::Err: Display,
{
    env::var(key)
        .map(|v| {
            v.parse().unwrap_or_else(|e| {
                panic!(
                    "{key} could not be parsed to {}: {e}",
                    std::any::type_name::<T>()
                )
            })
        })
        .unwrap_or(default)
}
