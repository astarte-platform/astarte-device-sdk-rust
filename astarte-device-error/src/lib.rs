// This file is part of Astarte.
//
// Copyright 2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Generic error type to store a context and message information.
//!
//! # Example
//!
//! This shows how to create and error that wraps [`ErrorKind`](std::io::ErrorKind).
//!
//! ```
//! #[derive(Debug, Clone, Copy, PartialEq, Eq)]
//! enum ErrorKind {
//!     Io(std::io::ErrorKind),
//!     _Other,
//!     _Errors,
//! }
//!
//! impl std::fmt::Display for ErrorKind {
//!     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//!         match self {
//!             ErrorKind::_Other => write!(f, "some error"),
//!             ErrorKind::_Errors => write!(f, "other error"),
//!             ErrorKind::Io(error_kind) => write!(f, "io error {error_kind}"),
//!         }
//!     }
//! }
//!
//! let err = astarte_device_error::Error::with(ErrorKind::Io(std::io::ErrorKind::NotFound), "while reading")
//!     .set_ctx(std::path::Path::new("/foo/bar").display())
//!     .set_source(std::io::Error::from(std::io::ErrorKind::NotFound));
//!
//! let display = err.to_string();
//!
//! let exp = "io error entity not found while reading /foo/bar";
//!
//! assert_eq!(display, exp);
//! ```

#![warn(
    clippy::dbg_macro,
    clippy::todo,
    missing_docs,
    rustdoc::missing_crate_level_docs
)]

use std::fmt::{Debug, Display};

trait DebugDisplay: Display + Debug + Send + Sync + 'static {}

impl<T> DebugDisplay for T where T: Display + Debug + Send + Sync + 'static {}

/// Generic error struct to store the information.
///
/// It provides a way to store:
///
/// - An error kind
/// - A static error message
/// - A dynamic display context
/// - A generic error source
#[derive(Debug)]
#[must_use]
pub struct Error<K> {
    /// Error kind like, used as error code.
    ///
    /// Used with PartialEq or match to handle and differentiate the errors.
    kind: K,
    message: Option<&'static str>,
    ctx: Option<Box<dyn DebugDisplay>>,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl<K> Error<K> {
    /// Create a new error for the kind
    pub fn new(kind: K) -> Self
    where
        K: Display + Debug + PartialEq,
    {
        Self {
            kind,
            message: None,
            ctx: None,
            source: None,
        }
    }

    /// Create a new error for the kind and the given context
    pub fn with(kind: K, message: &'static str) -> Self
    where
        K: Display + Debug + PartialEq,
    {
        Self {
            kind,
            message: Some(message),
            ctx: None,
            source: None,
        }
    }

    /// Sets the message for the error
    pub fn set_ctx<T>(mut self, message: T) -> Self
    where
        T: Display + Debug + Send + Sync + 'static,
    {
        self.ctx = Some(Box::new(message));
        self
    }

    /// Sets the error source
    pub fn set_source<T>(mut self, source: T) -> Self
    where
        T: std::error::Error + Send + Sync + 'static,
    {
        self.source = Some(Box::new(source));
        self
    }

    /// Returns the error kind.
    pub fn kind(&self) -> &K {
        &self.kind
    }
}

impl<K> Display for Error<K>
where
    K: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            kind,
            message,
            ctx,
            source,
        } = self;

        write!(f, "{kind}")?;

        if let Some(ctx) = message {
            write!(f, " {ctx}")?;
        }

        if let Some(ctx) = ctx {
            write!(f, " {ctx}")?;
        }

        if let Some(source) = source
            && f.alternate()
        {
            write!(f, ": {source}")?;
        }

        Ok(())
    }
}

impl<K> std::error::Error for Error<K>
where
    K: Debug + Display,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.source {
            Some(source) => Some(source.as_ref()),
            None => None,
        }
    }
}

/// Extension trait for [`Result`] with [`Error`] as error.
pub trait ResultExt<T, K> {
    /// Maps the kind of [`Error<T>`] to [`Error<U>`] when [`Result::Err`].
    ///
    /// It will call the closure if the [`Result`] is an [`Err`] and convert the error kind.
    fn map_kind<F, U>(self, f: F) -> Result<T, Error<U>>
    where
        F: FnOnce(K) -> U;
}

impl<T, K> ResultExt<T, K> for Result<T, Error<K>> {
    fn map_kind<F, U>(self, f: F) -> Result<T, Error<U>>
    where
        F: FnOnce(K) -> U,
    {
        match self {
            Ok(value) => Ok(value),
            Err(error) => {
                let Error {
                    kind,
                    message,
                    ctx,
                    source,
                } = error;

                let kind = (f)(kind);

                Err(Error {
                    kind,
                    message,
                    ctx,
                    source,
                })
            }
        }
    }
}

/// Extension trait to wrap results.
///
/// Methods implemented on [`Result`] to give context to error messages.
pub trait WrapError<T, E>: Sized {
    /// Wrap an [`Result::Err`] with the given error kind.
    ///
    /// It will construct an [`Error`] with the given kind if the [`Result`] is an [`Err`] and set
    /// the [`Error`] source to the previous error.
    fn wrap_err<K>(self, kind: K) -> Result<T, Error<K>>
    where
        K: std::fmt::Display + std::fmt::Debug + PartialEq;

    /// Wrap an [`Result::Err`] with the given error kind and context.
    ///
    /// It will construct an [`Error`] with the given kind and context if the [`Result`] is an
    /// [`Err`] and set the [`Error`] source to the previous error.
    fn wrap_err_msg<K>(self, kind: K, msg: &'static str) -> Result<T, Error<K>>
    where
        K: std::fmt::Display + std::fmt::Debug + PartialEq;

    /// Wrap an [`Result::Err`] with the given closure.
    ///
    /// It will call the closure if the [`Result`] is an [`Err`] and set the [`Error`] source to the
    /// previous error.
    fn wrap_err_with<F, K>(self, f: F) -> Result<T, Error<K>>
    where
        F: FnOnce(&E) -> Error<K>;
}

impl<T, E> WrapError<T, E> for Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn wrap_err<K>(self, kind: K) -> Result<T, Error<K>>
    where
        K: std::fmt::Display + std::fmt::Debug + PartialEq,
    {
        match self {
            Ok(value) => Ok(value),
            Err(error) => {
                let error = Error::new(kind).set_source(error);

                Err(error)
            }
        }
    }

    fn wrap_err_msg<K>(self, kind: K, ctx: &'static str) -> Result<T, Error<K>>
    where
        K: std::fmt::Display + std::fmt::Debug + PartialEq,
    {
        match self {
            Ok(value) => Ok(value),
            Err(error) => {
                let error = Error::with(kind, ctx).set_source(error);

                Err(error)
            }
        }
    }

    fn wrap_err_with<F, O>(self, f: F) -> Result<T, Error<O>>
    where
        F: FnOnce(&E) -> Error<O>,
    {
        match self {
            Ok(value) => Ok(value),
            Err(error) => {
                let error = (f)(&error).set_source(error);

                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::path::Path;

    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ErrorKind {
        Simple,
        Io(io::ErrorKind),
    }

    impl Display for ErrorKind {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                ErrorKind::Simple => write!(f, "simple error kind"),
                ErrorKind::Io(error_kind) => write!(f, "io error {error_kind}"),
            }
        }
    }

    #[test]
    fn should_display() {
        let err = Error::with(ErrorKind::Io(io::ErrorKind::NotFound), "while reading")
            .set_ctx(Path::new("/foo/bar").display())
            .set_source(io::Error::from(io::ErrorKind::NotFound));

        let display = err.to_string();

        let exp = "io error entity not found while reading /foo/bar";

        assert_eq!(display, exp);
    }

    #[test]
    fn should_display_alternate() {
        let err = Error::with(ErrorKind::Io(io::ErrorKind::NotFound), "while reading")
            .set_ctx(Path::new("/foo/bar").display())
            .set_source(io::Error::from(io::ErrorKind::NotFound));

        let display = format!("{err:#}");

        let exp = "io error entity not found while reading /foo/bar: entity not found";

        assert_eq!(display, exp);
    }

    #[test]
    fn should_check_size() {
        let kind = ErrorKind::Io(io::ErrorKind::NotFound);
        let err = Error::with(kind, "while reading")
            .set_ctx(Path::new("/foo/bar").display())
            .set_source(io::Error::from(io::ErrorKind::NotFound));

        let full_size = size_of_val(&err);

        assert_eq!(full_size, 56);

        let kind_size = size_of_val(&kind);

        assert_eq!(kind_size, 1);

        let err_size = full_size - kind_size;

        assert_eq!(err_size, 55);
    }

    #[test]
    fn should_wrap_err() {
        let res: io::Result<()> = Err(io::Error::from(io::ErrorKind::NotFound));

        let Error {
            kind,
            message: ctx,
            ctx: message,
            source,
        } = res.wrap_err(ErrorKind::Simple).unwrap_err();

        assert_eq!(kind, ErrorKind::Simple);
        assert!(ctx.is_none());
        assert!(message.is_none());
        assert!(source.is_some());
    }

    #[test]
    fn should_wrap_err_ctx() {
        let res: io::Result<()> = Err(io::Error::from(io::ErrorKind::NotFound));

        let exp_ctx = "some io error";
        let Error {
            kind,
            message: ctx,
            ctx: message,
            source,
        } = res.wrap_err_msg(ErrorKind::Simple, exp_ctx).unwrap_err();

        assert_eq!(kind, ErrorKind::Simple);
        assert_eq!(ctx.unwrap(), exp_ctx);
        assert!(message.is_none());
        assert!(source.is_some());
    }

    #[test]
    fn should_wrap_err_with() {
        let res: io::Result<()> = Err(io::Error::from(io::ErrorKind::NotFound));

        let exp_ctx = "some io error";
        let exp_msg = "some message";
        let Error {
            kind,
            message: ctx,
            ctx: message,
            source,
        } = res
            .wrap_err_with(|error| {
                Error::with(ErrorKind::Io(error.kind()), exp_ctx).set_ctx(exp_msg)
            })
            .unwrap_err();

        assert_eq!(kind, ErrorKind::Io(io::ErrorKind::NotFound));
        assert_eq!(ctx.unwrap(), exp_ctx);
        assert_eq!(message.unwrap().to_string(), exp_msg);
        assert!(source.is_some());
    }
}
