use anyhow::Error as AnyhowError;
use std::borrow::Cow;
use std::fmt;

#[derive(Debug, Clone, Copy)]
pub enum ErrorKind {
    Database,
    InvalidInput,
    Forbidden,
    NotFound,
    Unauthorized,
    Unknown,
    Upstream,
}

pub struct LibError {
    pub kind: ErrorKind,
    pub public: Cow<'static, str>,
    pub source: AnyhowError,
}

impl LibError {
    pub fn new(
        kind: ErrorKind,
        public: impl Into<Cow<'static, str>>,
        source: impl Into<AnyhowError>,
    ) -> Self {
        Self {
            kind,
            public: public.into(),
            source: source.into(),
        }
    }

    pub fn database(public: impl Into<Cow<'static, str>>, source: impl Into<AnyhowError>) -> Self {
        Self::new(ErrorKind::Database, public, source)
    }

    pub fn invalid(public: impl Into<Cow<'static, str>>, source: impl Into<AnyhowError>) -> Self {
        Self::new(ErrorKind::InvalidInput, public, source)
    }

    pub fn forbidden(public: impl Into<Cow<'static, str>>, source: impl Into<AnyhowError>) -> Self {
        Self::new(ErrorKind::Forbidden, public, source)
    }

    pub fn not_found(public: impl Into<Cow<'static, str>>, source: impl Into<AnyhowError>) -> Self {
        Self::new(ErrorKind::NotFound, public, source)
    }

    pub fn unauthorized(
        public: impl Into<Cow<'static, str>>,
        source: impl Into<AnyhowError>,
    ) -> Self {
        Self::new(ErrorKind::Unauthorized, public, source)
    }

    pub fn upstream(public: impl Into<Cow<'static, str>>, source: impl Into<AnyhowError>) -> Self {
        Self::new(ErrorKind::Upstream, public, source)
    }

    pub fn unknown(public: impl Into<Cow<'static, str>>, source: impl Into<AnyhowError>) -> Self {
        Self::new(ErrorKind::Unknown, public, source)
    }

    pub fn context(self, msg: impl fmt::Display) -> Self {
        Self {
            source: self.source.context(msg.to_string()),
            ..self
        }
    }
}

impl fmt::Display for LibError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.public, self.source)
    }
}

impl fmt::Debug for LibError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LibError")
            .field("kind", &self.kind)
            .field("public", &self.public)
            .field("source", &self.source)
            .finish()
    }
}

impl std::error::Error for LibError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

pub type Result<T, E = LibError> = core::result::Result<T, E>;
