use crate::error::{ApiError, ErrorKind};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DelegatedOptions {
    pub(crate) discover: bool,
    pub(crate) allocator_peer_id: String,
    pub(crate) allocator_multiaddr: String,
    pub(crate) auth_type: Option<String>,
    pub(crate) auth_payload: Option<String>,
    pub(crate) auth_provider: Option<String>,
    pub(crate) auth_args: Option<String>,
}

const OPTION_PAIR_KEYS: (&str, &str) = ("allocator_peer_id", "allocator_multiaddr");
const BLOCK_PAIR_KEYS: (&str, &str) = ("peer_id", "multiaddr");

fn validate_delegated(
    d: &DelegatedOptions,
    (peer_key, multiaddr_key): (&str, &str),
) -> Result<(), ApiError> {
    if d.allocator_peer_id.is_empty() != d.allocator_multiaddr.is_empty() {
        return Err(ApiError::new(
            ErrorKind::InvalidArgument,
            format!("{peer_key} and {multiaddr_key} go together"),
        ));
    }
    if !d.discover && d.allocator_peer_id.is_empty() {
        return Err(ApiError::new(
            ErrorKind::InvalidArgument,
            format!("delegated registration needs discover or {peer_key} and {multiaddr_key}"),
        ));
    }
    if d.auth_type.is_none()
        && (d.auth_payload.is_some() || d.auth_provider.is_some() || d.auth_args.is_some())
    {
        return Err(ApiError::new(
            ErrorKind::InvalidArgument,
            "auth_payload/auth_provider/auth_args need auth_type",
        ));
    }
    if d.auth_payload.is_some() && d.auth_provider.is_some() {
        return Err(ApiError::new(
            ErrorKind::InvalidArgument,
            "auth_payload and auth_provider are mutually exclusive",
        ));
    }
    if let Some(p) = &d.auth_payload {
        let h = p.trim_start_matches("0x");
        if h.is_empty() || h.len() % 2 != 0 || !h.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(ApiError::new(
                ErrorKind::InvalidArgument,
                "auth_payload must be hex-encoded bytes",
            ));
        }
    }
    Ok(())
}

impl DelegatedOptions {
    pub(crate) fn from_allocator_block(
        v: Option<&serde_json::Value>,
        what: &str,
    ) -> Result<Option<Self>, ApiError> {
        let obj = match v {
            None | Some(serde_json::Value::Null) => return Ok(None),
            Some(serde_json::Value::Object(o)) => o,
            Some(_) => {
                return Err(ApiError::new(
                    ErrorKind::InvalidArgument,
                    format!("{what} must be an object {{discover?, peer_id?, multiaddr?, auth_type?, auth_payload?|auth_provider?, auth_args?}}"),
                ))
            }
        };
        let discover = match obj.get("discover") {
            None | Some(serde_json::Value::Null) => true,
            Some(serde_json::Value::Bool(b)) => *b,
            Some(other) => {
                return Err(ApiError::new(
                    ErrorKind::InvalidArgument,
                    format!("{what}.discover must be a boolean, got {other}"),
                ))
            }
        };
        let text = |key: &str| -> Result<Option<String>, ApiError> {
            match obj.get(key) {
                None | Some(serde_json::Value::Null) => Ok(None),
                Some(serde_json::Value::String(s)) if s.trim().is_empty() => Ok(None),
                Some(serde_json::Value::String(s)) => Ok(Some(s.trim().to_string())),
                Some(other) => Err(ApiError::new(
                    ErrorKind::InvalidArgument,
                    format!("{what}.{key} must be a string, got {other}"),
                )),
            }
        };
        let d = Self {
            discover,
            allocator_peer_id: text("peer_id")?.unwrap_or_default(),
            allocator_multiaddr: text("multiaddr")?.unwrap_or_default(),
            auth_type: text("auth_type")?,
            auth_payload: text("auth_payload")?,
            auth_provider: text("auth_provider")?,
            auth_args: text("auth_args")?,
        };
        validate_delegated(&d, BLOCK_PAIR_KEYS)
            .map_err(|e| ApiError::new(e.kind, format!("{what}: {}", e.message)))?;
        Ok(Some(d))
    }

    pub(crate) fn from_registry_options(
        mut opts: serde_json::Value,
    ) -> Result<Option<Self>, ApiError> {
        if !flat_bool_option(&opts, "delegated") {
            if ["auth_type", "auth_payload", "auth_provider", "auth_args"]
                .iter()
                .any(|k| opts.get(*k).is_some())
            {
                return Err(ApiError::new(
                    ErrorKind::InvalidArgument,
                    "auth_* options apply to delegated registration only — set \"delegated\":\"true\"",
                ));
            }
            return Ok(None);
        }
        let d = Self {
            discover: match opts.get("discover") {
                None => true,
                Some(_) => flat_bool_option(&opts, "discover"),
            },
            allocator_peer_id: flat_str_option(&mut opts, OPTION_PAIR_KEYS.0).unwrap_or_default(),
            allocator_multiaddr: flat_str_option(&mut opts, OPTION_PAIR_KEYS.1).unwrap_or_default(),
            auth_type: flat_str_option(&mut opts, "auth_type"),
            auth_payload: flat_str_option(&mut opts, "auth_payload"),
            auth_provider: flat_str_option(&mut opts, "auth_provider"),
            auth_args: flat_str_option(&mut opts, "auth_args"),
        };
        validate_delegated(&d, OPTION_PAIR_KEYS)?;
        Ok(Some(d))
    }
}

pub(crate) fn flat_bool_option(options: &serde_json::Value, key: &str) -> bool {
    options.get(key).and_then(|v| v.as_str()) == Some("true")
}

pub(crate) fn flat_str_option(options: &mut serde_json::Value, key: &str) -> Option<String> {
    match options.get_mut(key).map(serde_json::Value::take) {
        Some(serde_json::Value::String(s)) if !s.is_empty() => Some(s),
        _ => None,
    }
}

pub(crate) fn delegated_request_args(
    d: &DelegatedOptions,
    commitment_hex: &str,
    rate_limit: u64,
    registry_account: &str,
) -> String {
    let mut args = serde_json::json!({
        "discover": d.discover,
        "config": registry_account,
        "identityCommitment": commitment_hex,
        "rate": rate_limit,
    });
    if !d.allocator_peer_id.is_empty() {
        args["allocatorPeerId"] = d.allocator_peer_id.as_str().into();
        args["allocatorMultiaddr"] = d.allocator_multiaddr.as_str().into();
    }
    if let Some(t) = &d.auth_type {
        args["authType"] = t.as_str().into();
    }
    if let Some(p) = &d.auth_payload {
        args["authPayload"] = p.as_str().into();
    }
    if let Some(module) = &d.auth_provider {
        args["authProvider"] = module.as_str().into();
    }
    if let Some(extra) = &d.auth_args {
        args["authArgs"] = extra.as_str().into();
    }
    args.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delegated_request_args_pass_auth_selection_through_verbatim() {
        let mut d = DelegatedOptions {
            discover: true,
            allocator_peer_id: "p".into(),
            allocator_multiaddr: "/ip4/127.0.0.1/tcp/1".into(),
            auth_type: None,
            auth_payload: None,
            auth_provider: None,
            auth_args: None,
        };
        let v: serde_json::Value =
            serde_json::from_str(&delegated_request_args(&d, "ab", 300, "cf")).unwrap();
        for key in ["authType", "authPayload", "authProvider", "authArgs"] {
            assert!(
                v.get(key).is_none(),
                "unset {key} must stay absent, got: {v}"
            );
        }
        assert_eq!(v["config"], "cf", "the scope's registry rides along: {v}");
        assert_eq!(v["identityCommitment"], "ab", "got: {v}");
        assert_eq!(v["rate"], 300, "got: {v}");
        assert_eq!(v["allocatorPeerId"], "p", "got: {v}");
        assert_eq!(v["allocatorMultiaddr"], "/ip4/127.0.0.1/tcp/1", "got: {v}");

        d.auth_type = Some("voucher-v1".into());
        d.auth_provider = Some("voucher_module".into());
        d.auth_args = Some(r#"{"campaign":"launch"}"#.into());
        let v: serde_json::Value =
            serde_json::from_str(&delegated_request_args(&d, "ab", 300, "cf")).unwrap();
        assert_eq!(v["authType"], "voucher-v1", "got: {v}");
        assert_eq!(v["authProvider"], "voucher_module", "got: {v}");
        assert_eq!(v["authArgs"], r#"{"campaign":"launch"}"#, "got: {v}");
        assert!(v.get("authPayload").is_none(), "got: {v}");

        d.auth_provider = None;
        d.auth_args = None;
        d.auth_payload = Some("deadbeef".into());
        let v: serde_json::Value =
            serde_json::from_str(&delegated_request_args(&d, "ab", 300, "cf")).unwrap();
        assert_eq!(v["authPayload"], "deadbeef", "got: {v}");
    }

    #[test]
    fn an_auth_type_alone_is_valid_and_sent_alone() {
        let block = DelegatedOptions::from_allocator_block(
            Some(&serde_json::json!({ "auth_type": "peer-any" })),
            "x",
        )
        .unwrap()
        .unwrap();
        assert_eq!(block.auth_type.as_deref(), Some("peer-any"));
        let (_, flat) = crate::register::parse_registry_options(
            &serde_json::json!([
                { "key": "delegated", "value": "true" },
                { "key": "auth_type", "value": "peer-any" },
            ])
            .to_string(),
        )
        .unwrap();
        assert_eq!(
            DelegatedOptions::from_registry_options(flat)
                .unwrap()
                .as_ref(),
            Some(&block)
        );
        let v: serde_json::Value =
            serde_json::from_str(&delegated_request_args(&block, "ab", 300, "cf")).unwrap();
        assert_eq!(v["authType"], "peer-any", "got: {v}");
        for key in [
            "authPayload",
            "authProvider",
            "authArgs",
            "allocatorPeerId",
            "allocatorMultiaddr",
        ] {
            assert!(v.get(key).is_none(), "{key} must stay absent, got: {v}");
        }
    }

    #[test]
    fn pair_errors_name_the_callers_keys() {
        let e = DelegatedOptions::from_allocator_block(
            Some(&serde_json::json!({ "peer_id": "p" })),
            "config_json.allocator",
        )
        .unwrap_err();
        assert_eq!(
            e.message,
            "config_json.allocator: peer_id and multiaddr go together"
        );
        let (_, flat) = crate::register::parse_registry_options(
            &serde_json::json!([
                { "key": "delegated", "value": "true" },
                { "key": "discover", "value": "false" },
            ])
            .to_string(),
        )
        .unwrap();
        assert_eq!(
            DelegatedOptions::from_registry_options(flat)
                .unwrap_err()
                .message,
            "delegated registration needs discover or allocator_peer_id and allocator_multiaddr"
        );
    }
}
