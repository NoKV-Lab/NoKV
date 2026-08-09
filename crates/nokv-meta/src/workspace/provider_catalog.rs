//! Workspace-owned catalog for opaque ordered provider spaces.
//!
//! The public provider SPI sees only stable numeric identifiers. Workspace
//! semantics, durable family tags, and built-in Holt tree names stay here.

use crate::provider::v1::OrderedSpaceId;

use super::codec::{
    CHANGE_EVENT_TREE, COMMAND_DEDUPE_TREE, HISTORY_TREE, RECOVERY_OUTBOX_TREE, ROOT_FENCE_TREE,
    SYSTEM_TREE,
};
use super::engine::MetadataFamily;

pub(super) const SYSTEM_SPACE: OrderedSpaceId = OrderedSpaceId::new(0x0101);
pub(super) const ROOT_FENCE_SPACE: OrderedSpaceId = OrderedSpaceId::new(0x0102);
pub(super) const COMMAND_DEDUPE_SPACE: OrderedSpaceId = OrderedSpaceId::new(0x0103);
pub(super) const CHANGE_EVENT_SPACE: OrderedSpaceId = OrderedSpaceId::new(0x0104);
pub(super) const HISTORY_SPACE: OrderedSpaceId = OrderedSpaceId::new(0x0105);
pub(super) const RECOVERY_OUTBOX_SPACE: OrderedSpaceId = OrderedSpaceId::new(0x0106);

pub(super) const fn domain_space(family: MetadataFamily) -> OrderedSpaceId {
    OrderedSpaceId::new(0x0200 | family.history_tag() as u16)
}

pub(super) fn domain_family(space: OrderedSpaceId) -> Option<MetadataFamily> {
    MetadataFamily::ALL
        .into_iter()
        .find(|family| domain_space(*family) == space)
}

pub(super) fn all_ordered_spaces() -> Vec<OrderedSpaceId> {
    let mut spaces = vec![
        SYSTEM_SPACE,
        ROOT_FENCE_SPACE,
        COMMAND_DEDUPE_SPACE,
        CHANGE_EVENT_SPACE,
        HISTORY_SPACE,
        RECOVERY_OUTBOX_SPACE,
    ];
    spaces.extend(MetadataFamily::ALL.into_iter().map(domain_space));
    spaces
}

pub(super) fn logical_state_spaces() -> Vec<OrderedSpaceId> {
    let mut spaces = vec![
        ROOT_FENCE_SPACE,
        COMMAND_DEDUPE_SPACE,
        CHANGE_EVENT_SPACE,
        HISTORY_SPACE,
        RECOVERY_OUTBOX_SPACE,
    ];
    spaces.extend(MetadataFamily::ALL.into_iter().map(domain_space));
    spaces
}

pub(super) fn diagnostic_name(space: OrderedSpaceId) -> Option<String> {
    match space {
        SYSTEM_SPACE => Some("system".to_owned()),
        ROOT_FENCE_SPACE => Some("root_fence".to_owned()),
        COMMAND_DEDUPE_SPACE => Some("command_dedupe".to_owned()),
        CHANGE_EVENT_SPACE => Some("change_event".to_owned()),
        HISTORY_SPACE => Some("history".to_owned()),
        RECOVERY_OUTBOX_SPACE => Some("recovery_outbox".to_owned()),
        _ => domain_family(space).map(|family| family.tree_name().to_owned()),
    }
}

pub(super) fn holt_tree_name(space: OrderedSpaceId) -> Option<&'static str> {
    match space {
        SYSTEM_SPACE => Some(SYSTEM_TREE),
        ROOT_FENCE_SPACE => Some(ROOT_FENCE_TREE),
        COMMAND_DEDUPE_SPACE => Some(COMMAND_DEDUPE_TREE),
        CHANGE_EVENT_SPACE => Some(CHANGE_EVENT_TREE),
        HISTORY_SPACE => Some(HISTORY_TREE),
        RECOVERY_OUTBOX_SPACE => Some(RECOVERY_OUTBOX_TREE),
        _ => domain_family(space).map(MetadataFamily::tree_name),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn ordered_space_ids_have_one_frozen_complete_mapping() {
        let actual = all_ordered_spaces()
            .into_iter()
            .map(OrderedSpaceId::get)
            .collect::<Vec<_>>();
        let expected = vec![
            0x0101, 0x0102, 0x0103, 0x0104, 0x0105, 0x0106, 0x0202, 0x0203, 0x0204, 0x0205, 0x0206,
            0x0207, 0x0208, 0x0209, 0x020a, 0x020b, 0x020c, 0x020d, 0x020e, 0x020f, 0x0211, 0x0212,
            0x0213, 0x0215, 0x0216, 0x0217,
        ];
        assert_eq!(actual, expected);
        assert_eq!(actual.iter().copied().collect::<BTreeSet<_>>().len(), 26);
        assert!(!actual.contains(&0x0210));
        assert!(!actual.contains(&0x0214));
    }

    #[test]
    fn every_ordered_space_has_one_holt_catalog_entry() {
        let trees = all_ordered_spaces()
            .into_iter()
            .map(|space| holt_tree_name(space).unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(trees.len(), 26);
    }
}
