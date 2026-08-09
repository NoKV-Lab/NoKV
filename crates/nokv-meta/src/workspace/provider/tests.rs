use super::{
    AtomicCommitOutcome, AtomicOp, AtomicPlan, MetadataProvider, MetadataReadView, OrderedSpaceId,
    ProviderErrorKind, ProviderScan, ProviderScanItem, ProviderTransactionModel,
    ProviderVersionModel, ReadScope,
};
use crate::workspace::engine::MetadataFamily;
use nokv_types::LogicalShardId;

use super::HoltProvider;

fn shard() -> LogicalShardId {
    LogicalShardId::from_bytes([0x31; 16])
}

type ProviderFactory = fn() -> Box<dyn MetadataProvider>;

fn holt_memory_provider() -> Box<dyn MetadataProvider> {
    Box::new(HoltProvider::open_memory(shard()).unwrap())
}

const CONTRACT_PROVIDERS: &[(&str, ProviderFactory)] = &[("holt-memory", holt_memory_provider)];

fn for_each_provider(test: impl Fn(&dyn MetadataProvider)) {
    for (_, factory) in CONTRACT_PROVIDERS {
        let provider = factory();
        test(provider.as_ref());
    }
}

fn for_each_provider_factory(test: impl Fn(ProviderFactory)) {
    for (_, factory) in CONTRACT_PROVIDERS {
        test(*factory);
    }
}

fn put(provider: &dyn MetadataProvider, space: OrderedSpaceId, key: &[u8], value: &[u8]) {
    let transaction = provider.begin_write().unwrap();
    let plan = AtomicPlan {
        operations: vec![AtomicOp::Put {
            space,
            key: key.to_vec(),
            value: value.to_vec(),
        }],
    };
    assert_eq!(
        transaction.commit(plan).unwrap(),
        AtomicCommitOutcome::Committed
    );
}

fn value(provider: &dyn MetadataProvider, space: OrderedSpaceId, key: &[u8]) -> Option<Vec<u8>> {
    provider.get(space, key).unwrap().map(|record| record.value)
}

#[test]
fn provider_traits_are_object_safe() {
    fn accept_provider(_: &dyn MetadataProvider) {}
    fn accept_read_view(_: &dyn MetadataReadView) {}

    let provider = HoltProvider::open_memory(shard()).unwrap();
    accept_provider(&provider);
    let view = provider
        .begin_read(&[ReadScope {
            space: crate::workspace::provider_catalog::domain_space(MetadataFamily::PathCurrent),
            prefix: Vec::new(),
        }])
        .unwrap();
    accept_read_view(view.as_ref());
}

#[test]
fn provider_contract_view_scan_is_ordered_delimited_and_exclusive() {
    for_each_provider(|provider| {
        let transaction = provider.begin_write().unwrap();
        let mut plan = super::AtomicPlan::default();
        for (key, value) in [
            (b"root/a".to_vec(), b"a".to_vec()),
            (b"root/dir/one".to_vec(), b"one".to_vec()),
            (b"root/dir/two".to_vec(), b"two".to_vec()),
            (b"root/z".to_vec(), b"z".to_vec()),
        ] {
            plan.operations.push(super::AtomicOp::Put {
                space: crate::workspace::provider_catalog::domain_space(
                    MetadataFamily::PathCurrent,
                ),
                key,
                value,
            });
        }
        assert_eq!(
            transaction.commit(plan).unwrap(),
            super::AtomicCommitOutcome::Committed
        );

        let view = provider
            .begin_read(&[ReadScope {
                space: crate::workspace::provider_catalog::domain_space(
                    MetadataFamily::PathCurrent,
                ),
                prefix: b"root/".to_vec(),
            }])
            .unwrap();
        let page = view
            .scan(&ProviderScan {
                space: crate::workspace::provider_catalog::domain_space(
                    MetadataFamily::PathCurrent,
                ),
                prefix: b"root/".to_vec(),
                start_after: Some(b"root/a".to_vec()),
                delimiter: Some(b'/'),
                limit: 2,
            })
            .unwrap();
        assert_eq!(
            page.items,
            vec![
                super::ProviderScanItem::CommonPrefix(b"root/dir/".to_vec()),
                super::ProviderScanItem::Key {
                    key: b"root/z".to_vec(),
                    value: b"z".to_vec(),
                },
            ]
        );

        let first_page = view
            .scan(&ProviderScan {
                space: crate::workspace::provider_catalog::domain_space(
                    MetadataFamily::PathCurrent,
                ),
                prefix: b"root/".to_vec(),
                start_after: Some(b"root/a".to_vec()),
                delimiter: Some(b'/'),
                limit: 1,
            })
            .unwrap();
        assert_eq!(
            first_page.items,
            vec![ProviderScanItem::CommonPrefix(b"root/dir/".to_vec())]
        );
        let second_page = view
            .scan(&ProviderScan {
                space: crate::workspace::provider_catalog::domain_space(
                    MetadataFamily::PathCurrent,
                ),
                prefix: b"root/".to_vec(),
                start_after: Some(b"root/dir/".to_vec()),
                delimiter: Some(b'/'),
                limit: 1,
            })
            .unwrap();
        assert_eq!(
            second_page.items,
            vec![ProviderScanItem::Key {
                key: b"root/z".to_vec(),
                value: b"z".to_vec(),
            }]
        );
    });
}

#[test]
fn holt_capabilities_and_preflight_limits_are_exact() {
    let provider = HoltProvider::open_memory(shard()).unwrap();
    let capabilities = provider.capabilities();
    assert_eq!(
        capabilities.transaction_model,
        ProviderTransactionModel::CrossSpaceAtomicBatch
    );
    assert_eq!(
        capabilities.version_model,
        ProviderVersionModel::OpaqueRecordWitness
    );
    assert!(capabilities.consistent_cross_space_reads);
    assert!(capabilities.all_ambiguous_commit_outcomes_settled_before_return);
    assert!(capabilities.commit_resolution_reads_causally_current);
    assert_eq!(capabilities.max_key_bytes, u16::MAX as usize);
    assert_eq!(capabilities.max_value_bytes, u16::MAX as usize);
    assert_eq!(capabilities.max_transaction_bytes, u32::MAX as usize);
    assert_eq!(capabilities.max_read_view_duration, None);
    assert_eq!(capabilities.max_scan_items, None);

    let oversized = vec![0; u16::MAX as usize + 1];
    let plan = AtomicPlan {
        operations: vec![AtomicOp::Put {
            space: crate::workspace::provider_catalog::SYSTEM_SPACE,
            key: b"key".to_vec(),
            value: oversized,
        }],
    };
    assert_eq!(plan.logical_footprint(), 3 + u16::MAX as usize + 1);
    let error = provider.begin_write().unwrap().commit(plan).unwrap_err();
    assert_eq!(error.kind(), ProviderErrorKind::TransactionTooLarge);
    assert_eq!(
        error.limit(),
        Some(crate::provider::v1::ProviderTransactionLimit {
            affected_bytes: u16::MAX as usize + 1,
            max_bytes: u16::MAX as usize,
        })
    );
}

#[test]
fn cross_space_batch_commits_all_mutations_together() {
    for_each_provider(|provider| {
        let transaction = provider.begin_write().unwrap();
        let plan = AtomicPlan {
            operations: vec![
                AtomicOp::Put {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: b"system".to_vec(),
                    value: b"one".to_vec(),
                },
                AtomicOp::Put {
                    space: crate::workspace::provider_catalog::domain_space(
                        MetadataFamily::Operation,
                    ),
                    key: b"domain".to_vec(),
                    value: b"two".to_vec(),
                },
            ],
        };
        assert_eq!(
            transaction.commit(plan).unwrap(),
            AtomicCommitOutcome::Committed
        );
        assert_eq!(
            value(
                provider,
                crate::workspace::provider_catalog::SYSTEM_SPACE,
                b"system"
            ),
            Some(b"one".to_vec())
        );
        assert_eq!(
            value(
                provider,
                crate::workspace::provider_catalog::domain_space(MetadataFamily::Operation),
                b"domain",
            ),
            Some(b"two".to_vec())
        );
    });
}

#[test]
fn absence_guard_is_atomic_on_conflict_and_ordered_on_success() {
    for_each_provider(|provider| {
        let domain = crate::workspace::provider_catalog::domain_space(MetadataFamily::Operation);
        put(provider, domain, b"occupied", b"original");

        let conflict = AtomicPlan {
            operations: vec![
                AtomicOp::AssertAbsent {
                    space: domain,
                    key: b"occupied".to_vec(),
                },
                AtomicOp::Put {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: b"must-not-commit".to_vec(),
                    value: b"value".to_vec(),
                },
            ],
        };
        assert_eq!(
            provider.begin_write().unwrap().commit(conflict).unwrap(),
            AtomicCommitOutcome::Conflict
        );
        assert_eq!(
            value(provider, domain, b"occupied"),
            Some(b"original".to_vec())
        );
        assert_eq!(
            value(
                provider,
                crate::workspace::provider_catalog::SYSTEM_SPACE,
                b"must-not-commit"
            ),
            None
        );

        let success = AtomicPlan {
            operations: vec![
                AtomicOp::AssertAbsent {
                    space: domain,
                    key: b"new".to_vec(),
                },
                AtomicOp::Put {
                    space: domain,
                    key: b"new".to_vec(),
                    value: b"final".to_vec(),
                },
            ],
        };
        assert_eq!(
            provider.begin_write().unwrap().commit(success).unwrap(),
            AtomicCommitOutcome::Committed
        );
        assert_eq!(value(provider, domain, b"new"), Some(b"final".to_vec()));
    });
}

#[test]
fn prefix_empty_guard_observes_order_and_rolls_back_cross_space_conflicts() {
    for_each_provider(|provider| {
        let domain = crate::workspace::provider_catalog::domain_space(MetadataFamily::Operation);
        put(provider, domain, b"dir/child", b"child");

        let conflict = AtomicPlan {
            operations: vec![
                AtomicOp::AssertPrefixEmpty {
                    space: domain,
                    prefix: b"dir/".to_vec(),
                },
                AtomicOp::Put {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: b"must-not-commit".to_vec(),
                    value: b"value".to_vec(),
                },
            ],
        };
        assert_eq!(
            provider.begin_write().unwrap().commit(conflict).unwrap(),
            AtomicCommitOutcome::Conflict
        );
        assert_eq!(
            value(
                provider,
                crate::workspace::provider_catalog::SYSTEM_SPACE,
                b"must-not-commit"
            ),
            None
        );

        let ordered = AtomicPlan {
            operations: vec![
                AtomicOp::Delete {
                    space: domain,
                    key: b"dir/child".to_vec(),
                },
                AtomicOp::AssertPrefixEmpty {
                    space: domain,
                    prefix: b"dir/".to_vec(),
                },
                AtomicOp::Put {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: b"committed".to_vec(),
                    value: b"yes".to_vec(),
                },
            ],
        };
        assert_eq!(
            provider.begin_write().unwrap().commit(ordered).unwrap(),
            AtomicCommitOutcome::Committed
        );
        assert_eq!(value(provider, domain, b"dir/child"), None);
        assert_eq!(
            value(
                provider,
                crate::workspace::provider_catalog::SYSTEM_SPACE,
                b"committed"
            ),
            Some(b"yes".to_vec())
        );
    });
}

#[test]
fn stale_witness_conflict_is_all_or_nothing() {
    for_each_provider(|provider| {
        put(
            provider,
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            b"guard",
            b"one",
        );
        let stale = provider
            .get(crate::workspace::provider_catalog::SYSTEM_SPACE, b"guard")
            .unwrap()
            .unwrap()
            .witness;
        put(
            provider,
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            b"guard",
            b"two",
        );

        let plan = AtomicPlan {
            operations: vec![
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: b"guard".to_vec(),
                    witness: stale,
                },
                AtomicOp::Put {
                    space: crate::workspace::provider_catalog::domain_space(
                        MetadataFamily::Operation,
                    ),
                    key: b"must-not-commit".to_vec(),
                    value: b"value".to_vec(),
                },
            ],
        };
        assert_eq!(
            provider.begin_write().unwrap().commit(plan).unwrap(),
            AtomicCommitOutcome::Conflict
        );
        assert_eq!(
            value(
                provider,
                crate::workspace::provider_catalog::SYSTEM_SPACE,
                b"guard"
            ),
            Some(b"two".to_vec())
        );
        assert_eq!(
            value(
                provider,
                crate::workspace::provider_catalog::domain_space(MetadataFamily::Operation),
                b"must-not-commit"
            ),
            None
        );
    });
}

#[test]
fn foreign_witness_fails_with_typed_authority_error_before_mutation() {
    for_each_provider_factory(|factory| {
        let first = factory();
        let second = factory();
        put(
            first.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            b"guard",
            b"one",
        );
        put(
            second.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            b"guard",
            b"two",
        );
        let foreign = first
            .get(crate::workspace::provider_catalog::SYSTEM_SPACE, b"guard")
            .unwrap()
            .unwrap()
            .witness;
        let plan = AtomicPlan {
            operations: vec![AtomicOp::CompareAndPut {
                space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                key: b"guard".to_vec(),
                witness: foreign,
                value: b"changed".to_vec(),
            }],
        };
        assert_eq!(
            second
                .begin_write()
                .unwrap()
                .commit(plan)
                .unwrap_err()
                .kind(),
            ProviderErrorKind::AuthorityMismatch
        );
        assert_eq!(
            value(
                second.as_ref(),
                crate::workspace::provider_catalog::SYSTEM_SPACE,
                b"guard"
            ),
            Some(b"two".to_vec())
        );

        let foreign = first
            .get(crate::workspace::provider_catalog::SYSTEM_SPACE, b"guard")
            .unwrap()
            .unwrap()
            .witness;
        let plan = AtomicPlan {
            operations: vec![
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: b"guard".to_vec(),
                    witness: foreign,
                },
                AtomicOp::Put {
                    space: crate::workspace::provider_catalog::domain_space(
                        MetadataFamily::Operation,
                    ),
                    key: b"must-not-commit".to_vec(),
                    value: b"bad".to_vec(),
                },
            ],
        };
        assert_eq!(
            second
                .begin_write()
                .unwrap()
                .commit(plan)
                .unwrap_err()
                .kind(),
            ProviderErrorKind::AuthorityMismatch
        );
        assert_eq!(
            value(
                second.as_ref(),
                crate::workspace::provider_catalog::domain_space(MetadataFamily::Operation),
                b"must-not-commit"
            ),
            None
        );
    });
}

#[test]
fn delete_and_reinsert_never_revalidates_an_old_witness() {
    for_each_provider(|provider| {
        let space = crate::workspace::provider_catalog::SYSTEM_SPACE;
        put(provider, space, b"aba", b"same");
        let stale = provider.get(space, b"aba").unwrap().unwrap().witness;
        assert_eq!(
            provider
                .begin_write()
                .unwrap()
                .commit(AtomicPlan {
                    operations: vec![
                        AtomicOp::Delete {
                            space,
                            key: b"aba".to_vec(),
                        },
                        AtomicOp::Put {
                            space,
                            key: b"aba".to_vec(),
                            value: b"same".to_vec(),
                        },
                    ],
                })
                .unwrap(),
            AtomicCommitOutcome::Committed
        );
        assert_eq!(
            provider
                .begin_write()
                .unwrap()
                .commit(AtomicPlan {
                    operations: vec![AtomicOp::AssertUnchanged {
                        space,
                        key: b"aba".to_vec(),
                        witness: stale,
                    }],
                })
                .unwrap(),
            AtomicCommitOutcome::Conflict
        );
    });
}

#[test]
fn captured_view_is_consistent_across_spaces_after_a_later_commit() {
    for_each_provider(|provider| {
        let domain = crate::workspace::provider_catalog::domain_space(MetadataFamily::Operation);
        let initial = AtomicPlan {
            operations: vec![
                AtomicOp::Put {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: b"version".to_vec(),
                    value: b"one".to_vec(),
                },
                AtomicOp::Put {
                    space: domain,
                    key: b"record".to_vec(),
                    value: b"one".to_vec(),
                },
            ],
        };
        assert_eq!(
            provider.begin_write().unwrap().commit(initial).unwrap(),
            AtomicCommitOutcome::Committed
        );
        let view = provider
            .begin_read(&[
                ReadScope {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    prefix: b"version".to_vec(),
                },
                ReadScope {
                    space: domain,
                    prefix: b"record".to_vec(),
                },
            ])
            .unwrap();

        let later = AtomicPlan {
            operations: vec![
                AtomicOp::Put {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: b"version".to_vec(),
                    value: b"two".to_vec(),
                },
                AtomicOp::Put {
                    space: domain,
                    key: b"record".to_vec(),
                    value: b"two".to_vec(),
                },
            ],
        };
        assert_eq!(
            provider.begin_write().unwrap().commit(later).unwrap(),
            AtomicCommitOutcome::Committed
        );
        assert_eq!(
            view.get(crate::workspace::provider_catalog::SYSTEM_SPACE, b"version")
                .unwrap()
                .unwrap()
                .value,
            b"one"
        );
        assert_eq!(view.get(domain, b"record").unwrap().unwrap().value, b"one");
        assert_eq!(
            value(
                provider,
                crate::workspace::provider_catalog::SYSTEM_SPACE,
                b"version"
            ),
            Some(b"two".to_vec())
        );
        assert_eq!(value(provider, domain, b"record"), Some(b"two".to_vec()));
    });
}

#[test]
fn scan_limit_is_a_hard_ordered_page_bound() {
    for_each_provider(|provider| {
        let domain = crate::workspace::provider_catalog::domain_space(MetadataFamily::Operation);
        for key in [b"k/1", b"k/2", b"k/3"] {
            put(provider, domain, key, key);
        }
        let view = provider
            .begin_read(&[ReadScope {
                space: domain,
                prefix: b"k/".to_vec(),
            }])
            .unwrap();
        let page = view
            .scan(&ProviderScan {
                space: domain,
                prefix: b"k/".to_vec(),
                start_after: None,
                delimiter: None,
                limit: 2,
            })
            .unwrap();
        assert_eq!(
            page.items,
            vec![
                ProviderScanItem::Key {
                    key: b"k/1".to_vec(),
                    value: b"k/1".to_vec(),
                },
                ProviderScanItem::Key {
                    key: b"k/2".to_vec(),
                    value: b"k/2".to_vec(),
                },
            ]
        );
    });
}
