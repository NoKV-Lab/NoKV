# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

import json
import pathlib
import subprocess
import sys
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from directional_similar_runs import DirectionalIndex, DirectionalSearchError  # noqa: E402


FEATURES = ["direction_x", "direction_y"]


def record(run_id, after, *, before=None, features=None, provenance=None, outcome=None):
    return {
        "run_id": run_id,
        "features": FEATURES if features is None else features,
        "before": [0, 0] if before is None else before,
        "after": after,
        "provenance": {
            "source": "tests/fixture.json",
            "reference": run_id,
        }
        if provenance is None
        else provenance,
        "outcome": {"label": outcome} if outcome is not None else None,
    }


class DirectionalIndexTests(unittest.TestCase):
    def test_same_direction_under_scale_is_near_one(self):
        index = DirectionalIndex(
            [record("query", [1, 2]), record("scaled", [10, 20])]
        )
        result = index.search("query", top_k=1)[0]
        self.assertAlmostEqual(result["cosine_similarity"], 1.0, places=12)
        self.assertEqual(index.fingerprint("query")["delta"], [1.0, 2.0])
        self.assertAlmostEqual(index.get("scaled").magnitude, (500**0.5), places=12)

    def test_opposite_and_orthogonal_directions_rank_correctly(self):
        index = DirectionalIndex(
            [
                record("query", [1, 0]),
                record("same", [2, 0]),
                record("orthogonal", [0, 3]),
                record("opposite", [-4, 0]),
            ]
        )
        results = index.search("query", top_k=3)
        self.assertEqual(
            [result["run_id"] for result in results],
            ["same", "orthogonal", "opposite"],
        )
        self.assertAlmostEqual(results[0]["cosine_similarity"], 1.0, places=12)
        self.assertAlmostEqual(results[1]["cosine_similarity"], 0.0, places=12)
        self.assertAlmostEqual(results[2]["cosine_similarity"], -1.0, places=12)

    def test_zero_delta_is_explicit_and_never_divides_or_ranks(self):
        index = DirectionalIndex(
            [
                record("flat", [4, -2], before=[4, -2]),
                record("moving", [1, 0]),
            ]
        )
        self.assertEqual(index.get("flat").magnitude, 0.0)
        self.assertIsNone(index.get("flat").direction)
        self.assertEqual(index.fingerprint("flat")["direction"], None)
        self.assertEqual(index.search("flat", top_k=5), [])
        self.assertEqual([r["run_id"] for r in index.search("moving", top_k=5)], [])

    def test_before_after_dimension_mismatch_is_rejected(self):
        with self.assertRaisesRegex(DirectionalSearchError, "dimensions differ"):
            DirectionalIndex([record("bad", [1], before=[0, 0])])

    def test_feature_vector_dimension_mismatch_is_rejected(self):
        with self.assertRaisesRegex(DirectionalSearchError, "features and vector"):
            DirectionalIndex([record("bad", [1, 2], features=["x"])])

    def test_feature_semantics_and_order_must_match(self):
        with self.assertRaisesRegex(DirectionalSearchError, "feature semantics/order"):
            DirectionalIndex(
                [
                    record("first", [1, 0]),
                    record("second", [1, 0], features=["direction_y", "direction_x"]),
                ]
            )

    def test_nan_and_infinity_are_rejected(self):
        for bad in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(bad=bad):
                with self.assertRaisesRegex(DirectionalSearchError, "finite"):
                    DirectionalIndex([record("bad", [bad, 0])])

    def test_duplicate_run_ids_are_rejected(self):
        with self.assertRaisesRegex(DirectionalSearchError, "duplicate run_id"):
            DirectionalIndex([record("same", [1, 0]), record("same", [0, 1])])

    def test_ties_are_deterministic_and_independent_of_input_order(self):
        query = record("query", [1, 0])
        first = DirectionalIndex(
            [query, record("zeta", [1, 1]), record("alpha", [1, 1])]
        )
        second = DirectionalIndex(
            [query, record("alpha", [1, 1]), record("zeta", [1, 1])]
        )
        self.assertEqual(
            [r["run_id"] for r in first.search("query", top_k=2)], ["alpha", "zeta"]
        )
        self.assertEqual(first.search("query", top_k=2), second.search("query", top_k=2))

    def test_outcome_label_cannot_leak_into_fingerprint_or_ranking(self):
        records_a = [
            record("query", [1, 0], outcome="one"),
            record("candidate", [1, 1], outcome="good"),
        ]
        records_b = [
            record("query", [1, 0], outcome="completely-different"),
            record("candidate", [1, 1], outcome={"unexpected": "shape"}),
        ]
        index_a = DirectionalIndex(records_a)
        index_b = DirectionalIndex(records_b)
        self.assertEqual(index_a.fingerprint("candidate"), index_b.fingerprint("candidate"))
        self.assertEqual(index_a.search("query", top_k=1), index_b.search("query", top_k=1))

    def test_top_k_and_explicit_self_exclusion(self):
        index = DirectionalIndex(
            [record("query", [1, 0]), record("zz-other", [1, 0]), record("third", [0, 1])]
        )
        self.assertEqual([r["run_id"] for r in index.search("query", top_k=1)], ["zz-other"])
        self.assertEqual(
            [r["run_id"] for r in index.search("query", top_k=1, exclude_self=False)],
            ["query"],
        )
        self.assertEqual(index.search("query", top_k=0), [])

    def test_provenance_is_preserved_in_results(self):
        provenance = {
            "source": "frozen/raw.jsonl",
            "reference": {"offset": 17, "digest": "abc"},
            "producer": "offline-fixture",
        }
        index = DirectionalIndex(
            [record("query", [1, 0]), record("candidate", [1, 0], provenance=provenance)]
        )
        result = index.search("query", top_k=1)[0]
        self.assertEqual(result["provenance"], provenance)
        result["provenance"]["reference"]["offset"] = 999
        self.assertEqual(index.get("candidate").provenance, provenance)

    def test_cli_reads_example_and_emits_disposable_results(self):
        example = ROOT / "examples" / "runs.json"
        completed = subprocess.run(
            [
                sys.executable,
                str(ROOT / "directional_similar_runs.py"),
                "--records",
                str(example),
                "--query-run-id",
                "toy-east",
                "--top-k",
                "2",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        output = json.loads(completed.stdout)
        self.assertTrue(output["exclude_self"])
        self.assertEqual([r["run_id"] for r in output["results"]], ["toy-east-scaled", "toy-north"])
        self.assertEqual(
            output["results"][0]["provenance"]["reference"], "toy-east-scaled"
        )


if __name__ == "__main__":
    unittest.main()
