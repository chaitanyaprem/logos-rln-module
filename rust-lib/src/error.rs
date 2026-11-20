#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ErrorKind {
    UnknownRegistry,
    UnknownMembership,
    ProviderFailure,
    Locked,
    BadPassword,
    NoUsableMembership,
    AmbiguousSelection,
    InvalidArgument,
    KeychainUnavailable,
    Internal,
    NotReady,
    Transient,
    BudgetExhausted,
    Permanent,
}

impl ErrorKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            ErrorKind::UnknownRegistry => "unknown_registry",
            ErrorKind::UnknownMembership => "unknown_membership",
            ErrorKind::ProviderFailure => "provider_failure",
            ErrorKind::Locked => "locked",
            ErrorKind::BadPassword => "bad_password",
            ErrorKind::NoUsableMembership => "no_usable_membership",
            ErrorKind::AmbiguousSelection => "ambiguous_selection",
            ErrorKind::InvalidArgument => "invalid_argument",
            ErrorKind::KeychainUnavailable => "keychain_unavailable",
            ErrorKind::Internal => "internal",
            ErrorKind::NotReady => "not_ready",
            ErrorKind::Transient => "transient",
            ErrorKind::BudgetExhausted => "budget_exhausted",
            ErrorKind::Permanent => "permanent",
        }
    }

    pub(crate) fn class(self) -> &'static str {
        match self {
            ErrorKind::NotReady | ErrorKind::Locked => "not_ready",
            ErrorKind::Transient | ErrorKind::ProviderFailure => "transient",
            ErrorKind::BudgetExhausted => "budget_exhausted",
            ErrorKind::UnknownRegistry
            | ErrorKind::UnknownMembership
            | ErrorKind::BadPassword
            | ErrorKind::NoUsableMembership
            | ErrorKind::AmbiguousSelection
            | ErrorKind::InvalidArgument
            | ErrorKind::KeychainUnavailable
            | ErrorKind::Internal
            | ErrorKind::Permanent => "permanent",
        }
    }
}

#[derive(Debug)]
pub(crate) struct ApiError {
    pub(crate) kind: ErrorKind,
    pub(crate) message: String,
}

impl ApiError {
    pub(crate) fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        ApiError {
            kind,
            message: message.into(),
        }
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        ApiError::new(ErrorKind::Internal, message)
    }

    pub(crate) fn body(&self) -> serde_json::Value {
        serde_json::to_value(crate::views::ErrorBody::new(
            self.kind.class(),
            self.kind.as_str(),
            &self.message,
        ))
        .unwrap_or(serde_json::Value::Null)
    }

    pub(crate) fn to_json(&self) -> String {
        serde_json::json!({ "error": self.body() }).to_string()
    }
}
