use std::sync::LazyLock;

use crate::registry_id::CanonicalRegistryId;

const TABLE: &str = include_str!("../allocator-networks.json");
const FORMAT: &str = "rln-allocator-networks";
const VERSION: u64 = 1;
pub(crate) const GIVEN_TABLE_ENV: &str = "RLN_ALLOCATOR_NETWORKS";

#[derive(serde::Deserialize)]
struct Table {
    format: String,
    version: u64,
    networks: Vec<Network>,
}

#[derive(serde::Deserialize)]
struct Network {
    reference: String,
    bootstrap_peers: Vec<String>,
}

fn parse(text: &str) -> Result<Vec<Network>, String> {
    let table: Table = serde_json::from_str(text).map_err(|e| e.to_string())?;
    if table.format != FORMAT || table.version != VERSION {
        return Err(format!(
            "expected {FORMAT} version {VERSION}, got {} version {}",
            table.format, table.version
        ));
    }
    Ok(table.networks)
}

fn given_table() -> Option<String> {
    let raw = std::env::var(GIVEN_TABLE_ENV).ok()?;
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    if raw.starts_with('{') {
        return Some(raw.to_owned());
    }
    match std::fs::read_to_string(raw) {
        Ok(text) => Some(text),
        Err(e) => {
            eprintln!(
                "allocator networks: {GIVEN_TABLE_ENV} names {raw}, which cannot be read: {e}"
            );
            None
        }
    }
}

static BUILT_IN: LazyLock<Vec<Network>> = LazyLock::new(|| {
    parse(TABLE).unwrap_or_else(|e| {
        eprintln!("allocator networks: built-in table unusable: {e}");
        Vec::new()
    })
});

fn given() -> Vec<Network> {
    let Some(text) = given_table() else {
        return Vec::new();
    };
    parse(&text).unwrap_or_else(|e| {
        eprintln!("allocator networks: {GIVEN_TABLE_ENV} ignored: {e}");
        Vec::new()
    })
}

fn peers_on(reference: &str, given: &[Network], built_in: &[Network]) -> Vec<String> {
    given
        .iter()
        .chain(built_in)
        .find(|n| n.reference.eq_ignore_ascii_case(reference))
        .map(|n| n.bootstrap_peers.clone())
        .unwrap_or_default()
}

pub(crate) fn bootstrap_peers(registry: &CanonicalRegistryId) -> Vec<String> {
    if registry.namespace != "logos" {
        return Vec::new();
    }
    peers_on(&registry.reference, &given(), &BUILT_IN)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry_id;

    #[test]
    fn the_built_in_table_names_peers_by_network() {
        let networks = parse(TABLE).expect("the built-in table parses");
        assert!(!networks.is_empty());
        for network in &networks {
            assert!(
                !network.bootstrap_peers.is_empty(),
                "{} lists no peer",
                network.reference
            );
            assert_eq!(
                networks
                    .iter()
                    .filter(|n| n.reference == network.reference)
                    .count(),
                1,
                "{} is listed twice",
                network.reference
            );
            for peer in &network.bootstrap_peers {
                let (addr, peer_id) = peer.rsplit_once("/p2p/").expect("a /p2p/ multiaddr");
                assert!(addr.starts_with('/') && !peer_id.is_empty() && !peer_id.contains('/'));
            }
        }
        let on = |raw: &str| bootstrap_peers(&registry_id::parse(raw).unwrap());
        let account = "ab".repeat(32);
        assert_eq!(on(&format!("logos:testnet:{account}")).len(), 6);
        assert_eq!(on(&format!("logos:TESTNET:{account}")).len(), 6);
        assert!(on(&format!("logos:local:{account}")).is_empty());
        assert!(on("fakens:testnet:x").is_empty());
    }

    #[test]
    fn a_given_table_replaces_a_network_and_adds_another() {
        let given = parse(
            r#"{"format":"rln-allocator-networks","version":1,"networks":[
                {"reference":"TESTNET","bootstrap_peers":[]},
                {"reference":"local","bootstrap_peers":["/ip4/127.0.0.1/tcp/20001/p2p/PeerL"]}]}"#,
        )
        .unwrap();
        let built_in = parse(TABLE).unwrap();
        assert!(peers_on("testnet", &given, &built_in).is_empty());
        assert_eq!(
            peers_on("local", &given, &built_in),
            ["/ip4/127.0.0.1/tcp/20001/p2p/PeerL"]
        );
        assert_eq!(peers_on("testnet", &[], &built_in).len(), 6);
        assert!(peers_on("local", &[], &built_in).is_empty());
    }

    #[test]
    fn another_format_or_version_is_refused() {
        assert!(parse(r#"{"format":"other","version":1,"networks":[]}"#).is_err());
        assert!(parse(r#"{"format":"rln-allocator-networks","version":2,"networks":[]}"#).is_err());
        assert!(parse("not json").is_err());
    }
}
