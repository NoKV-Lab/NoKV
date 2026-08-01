# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

import copy
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import directional_similar_runs as directional_module  # noqa: E402
from directional_similar_runs import (  # noqa: E402
    DirectionalIndex,
    DirectionalSearchError,
    INPUT_SCHEMA,
    RESULT_SCHEMA,
)


FEATURES = [
    {"name": "direction_x", "scale": 1.0, "unit": "normalized"},
    {"name": "direction_y", "scale": 1.0, "unit": "normalized"},
]
_UNSET = object()


def record(run_id, after, *, before=None, provenance=None, outcome=_UNSET):
    value = {
        "run_id": run_id,
        "before": [0, 0] if before is None else before,
        "after": after,
        "provenance": {
            "source": "tests/fixture.json",
            "reference": run_id,
        }
        if provenance is None
        else provenance,
    }
    if outcome is not _UNSET:
        value["outcome"] = outcome
    return value


def dataset(*runs, feature_space_id="test-direction-v1", features=None, schema=INPUT_SCHEMA):
    return {
        "schema": schema,
        "feature_space": {
            "id": feature_space_id,
            "features": copy.deepcopy(FEATURES if features is None else features),
        },
        "runs": list(runs),
    }


def nested_provenance(depth):
    value = "leaf"
    for _ in range(depth):
        value = {"next": value}
    return {"source": "tests/fixture.json", "nested": value}


class DirectionalIndexTests(unittest.TestCase):
    def test_same_scaled_direction_is_near_one(self):
        index = DirectionalIndex(
            dataset(record("query", [1, 2]), record("scaled", [10, 20]))
        )
        result = index.search("query", top_k=1)[0]
        self.assertAlmostEqual(result["cosine_similarity"], 1.0, places=12)
        fingerprint = index.fingerprint("scaled")
        self.assertEqual(fingerprint["raw_delta"], [10.0, 20.0])
        self.assertEqual(fingerprint["scaled_delta"], [10.0, 20.0])
        self.assertAlmostEqual(
            fingerprint["scaled_magnitude"], (500**0.5), places=12
        )

    def test_unit_conversion_with_matching_scales_preserves_direction_and_ranking(self):
        seconds = dataset(
            record("query", [2, 4]),
            record("same", [4, 8]),
            record("orthogonal", [0, 4]),
            features=[
                {"name": "latency", "scale": 2, "unit": "seconds"},
                {"name": "duration", "scale": 4, "unit": "seconds"},
            ],
        )
        milliseconds = dataset(
            record("query", [2000, 4000]),
            record("same", [4000, 8000]),
            record("orthogonal", [0, 4000]),
            features=[
                {"name": "latency", "scale": 2000, "unit": "milliseconds"},
                {"name": "duration", "scale": 4000, "unit": "milliseconds"},
            ],
        )
        seconds_index = DirectionalIndex(seconds)
        milliseconds_index = DirectionalIndex(milliseconds)

        self.assertEqual(
            seconds_index.fingerprint("query")["scaled_delta"],
            milliseconds_index.fingerprint("query")["scaled_delta"],
        )
        self.assertEqual(
            seconds_index.fingerprint("query")["direction"],
            milliseconds_index.fingerprint("query")["direction"],
        )
        self.assertNotEqual(
            seconds_index.fingerprint("query")["feature_space_commitment"],
            milliseconds_index.fingerprint("query")["feature_space_commitment"],
        )
        self.assertEqual(
            [result["run_id"] for result in seconds_index.search("query", 2)],
            [result["run_id"] for result in milliseconds_index.search("query", 2)],
        )
        self.assertEqual(
            [result["cosine_similarity"] for result in seconds_index.search("query", 2)],
            [
                result["cosine_similarity"]
                for result in milliseconds_index.search("query", 2)
            ],
        )

    def test_scale_values_change_ranking(self):
        runs = [
            record("query", [1, 1]),
            record("x-axis", [1, 0]),
            record("y-axis", [0, 1]),
        ]
        x_weighted = DirectionalIndex(
            dataset(
                *runs,
                features=[
                    {"name": "x", "scale": 1, "unit": "raw"},
                    {"name": "y", "scale": 10, "unit": "raw"},
                ],
            )
        )
        y_weighted = DirectionalIndex(
            dataset(
                *runs,
                features=[
                    {"name": "x", "scale": 10, "unit": "raw"},
                    {"name": "y", "scale": 1, "unit": "raw"},
                ],
            )
        )
        self.assertEqual(x_weighted.search("query", 1)[0]["run_id"], "x-axis")
        self.assertEqual(y_weighted.search("query", 1)[0]["run_id"], "y-axis")
        self.assertNotEqual(
            x_weighted.fingerprint("query")["feature_space_commitment"],
            y_weighted.fingerprint("query")["feature_space_commitment"],
        )

    def test_opposite_and_orthogonal_directions_rank_correctly(self):
        index = DirectionalIndex(
            dataset(
                record("query", [1, 0]),
                record("same", [2, 0]),
                record("orthogonal", [0, 3]),
                record("opposite", [-4, 0]),
            )
        )
        results = index.search("query", top_k=3)
        self.assertEqual(
            [result["run_id"] for result in results],
            ["same", "orthogonal", "opposite"],
        )
        self.assertAlmostEqual(results[0]["cosine_similarity"], 1.0, places=12)
        self.assertAlmostEqual(results[1]["cosine_similarity"], 0.0, places=12)
        self.assertAlmostEqual(results[2]["cosine_similarity"], -1.0, places=12)

    def test_zero_delta_is_explicit_and_never_ranks(self):
        index = DirectionalIndex(
            dataset(
                record("flat", [4, -2], before=[4, -2]),
                record("moving", [1, 0]),
            )
        )
        fingerprint = index.fingerprint("flat")
        self.assertEqual(fingerprint["raw_delta"], [0.0, 0.0])
        self.assertEqual(fingerprint["scaled_delta"], [0.0, 0.0])
        self.assertEqual(fingerprint["scaled_magnitude"], 0.0)
        self.assertIsNone(fingerprint["direction"])
        self.assertEqual(index.search("flat", top_k=5), [])
        self.assertEqual(index.search("moving", top_k=5), [])

    def test_legacy_array_unknown_schema_and_empty_feature_space_id_are_rejected(self):
        with self.assertRaisesRegex(DirectionalSearchError, "legacy run array"):
            DirectionalIndex([record("legacy", [1, 0])])
        with self.assertRaisesRegex(DirectionalSearchError, "unsupported schema"):
            DirectionalIndex(dataset(record("bad", [1, 0]), schema="unknown/v1"))
        with self.assertRaisesRegex(DirectionalSearchError, "feature_space.id"):
            DirectionalIndex(dataset(record("bad", [1, 0]), feature_space_id=""))

    def test_invalid_feature_scales_are_rejected(self):
        invalid_scales = [(_UNSET, "numeric"), (True, "numeric"), (0, "greater"), (-1, "greater")]
        invalid_scales.extend(
            [(float("nan"), "finite"), (float("inf"), "finite")]
        )
        for scale, message in invalid_scales:
            with self.subTest(scale=scale):
                feature = {"name": "x", "unit": "raw"}
                if scale is not _UNSET:
                    feature["scale"] = scale
                with self.assertRaisesRegex(DirectionalSearchError, message):
                    DirectionalIndex(
                        dataset(record("bad", [1]), features=[feature])
                    )

    def test_duplicate_features_and_vector_dimension_mismatch_are_rejected(self):
        duplicate_features = [
            {"name": "x", "scale": 1, "unit": "raw"},
            {"name": "x", "scale": 2, "unit": "raw"},
        ]
        with self.assertRaisesRegex(DirectionalSearchError, "duplicate feature"):
            DirectionalIndex(
                dataset(record("bad", [1, 2]), features=duplicate_features)
            )
        with self.assertRaisesRegex(DirectionalSearchError, "feature space and vector"):
            DirectionalIndex(dataset(record("bad", [1], before=[0])))
        with self.assertRaisesRegex(DirectionalSearchError, "dimensions differ"):
            DirectionalIndex(dataset(record("bad", [1], before=[0, 0])))

    def test_nonfinite_vectors_and_derived_values_are_rejected(self):
        for bad in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(bad=bad):
                with self.assertRaisesRegex(DirectionalSearchError, "finite"):
                    DirectionalIndex(dataset(record("bad", [bad, 0])))

        with self.assertRaisesRegex(DirectionalSearchError, "raw delta"):
            DirectionalIndex(
                dataset(record("bad", [1e308, 0], before=[-1e308, 0]))
            )
        with self.assertRaisesRegex(DirectionalSearchError, "scaled delta"):
            DirectionalIndex(
                dataset(
                    record("bad", [1e308], before=[0]),
                    features=[{"name": "x", "scale": 1e-308, "unit": "raw"}],
                )
            )
        with self.assertRaisesRegex(DirectionalSearchError, "scaled magnitude"):
            DirectionalIndex(
                dataset(
                    record("bad", [1.7e308, 1.7e308]),
                    features=[
                        {"name": "x", "scale": 1, "unit": "raw"},
                        {"name": "y", "scale": 1, "unit": "raw"},
                    ],
                )
            )
        with self.assertRaisesRegex(DirectionalSearchError, "underflow"):
            DirectionalIndex(
                dataset(
                    record("bad", [5e-324], before=[0]),
                    features=[{"name": "x", "scale": 1e308, "unit": "raw"}],
                )
            )

    def test_duplicate_run_ids_are_rejected(self):
        with self.assertRaisesRegex(DirectionalSearchError, "duplicate run_id"):
            DirectionalIndex(
                dataset(record("same", [1, 0]), record("same", [0, 1]))
            )

    def test_ties_are_deterministic_and_independent_of_input_order(self):
        query = record("query", [1, 0])
        first = DirectionalIndex(
            dataset(query, record("zeta", [1, 1]), record("alpha", [1, 1]))
        )
        second = DirectionalIndex(
            dataset(query, record("alpha", [1, 1]), record("zeta", [1, 1]))
        )
        self.assertEqual(
            [result["run_id"] for result in first.search("query", top_k=2)],
            ["alpha", "zeta"],
        )
        self.assertEqual(first.search("query", 2), second.search("query", 2))

    def test_outcome_is_validated_but_not_stored_or_scored(self):
        records_a = [
            record("query", [1, 0], outcome={"label": "one"}),
            record("candidate", [1, 1], outcome={"label": "good"}),
        ]
        records_b = [
            record("query", [1, 0], outcome={"label": "different"}),
            record("candidate", [1, 1], outcome=["another", "shape"]),
        ]
        index_a = DirectionalIndex(dataset(*records_a))
        index_b = DirectionalIndex(dataset(*records_b))
        self.assertEqual(
            index_a.fingerprint("candidate"), index_b.fingerprint("candidate")
        )
        self.assertEqual(index_a.search("query", 1), index_b.search("query", 1))
        with self.assertRaisesRegex(DirectionalSearchError, "JSON-compatible"):
            DirectionalIndex(
                dataset(record("bad", [1, 0], outcome=object()))
            )

    def test_non_string_keys_and_cyclic_metadata_are_rejected_cleanly(self):
        invalid = dataset(record("bad", [1, 0]))
        invalid[1] = "not a JSON object key"
        with self.assertRaisesRegex(DirectionalSearchError, "unknown fields"):
            DirectionalIndex(invalid)

        cyclic = {"source": "tests/fixture.json"}
        cyclic["cycle"] = cyclic
        with self.assertRaisesRegex(DirectionalSearchError, "acyclic JSON"):
            DirectionalIndex(
                dataset(record("bad", [1, 0], provenance=cyclic))
            )

    def test_metadata_depth_is_bounded_with_a_contract_error(self):
        at_limit = nested_provenance(directional_module._MAX_METADATA_DEPTH - 1)
        DirectionalIndex(dataset(record("ok", [1, 0], provenance=at_limit)))

        over_limit = nested_provenance(directional_module._MAX_METADATA_DEPTH)
        with self.assertRaisesRegex(DirectionalSearchError, "maximum JSON depth"):
            DirectionalIndex(
                dataset(record("bad", [1, 0], provenance=over_limit))
            )

    def test_identity_text_rejects_boundary_whitespace_and_lone_surrogates(self):
        invalid_values = []

        invalid_id = dataset(record("run", [1, 0]))
        invalid_id["feature_space"]["id"] = " space"
        invalid_values.append(invalid_id)

        invalid_name = dataset(record("run", [1, 0]))
        invalid_name["feature_space"]["features"][0]["name"] = "x "
        invalid_values.append(invalid_name)

        invalid_unit = dataset(record("run", [1, 0]))
        invalid_unit["feature_space"]["features"][0]["unit"] = " raw"
        invalid_values.append(invalid_unit)

        invalid_run = dataset(record(" run", [1, 0]))
        invalid_values.append(invalid_run)

        invalid_source = dataset(record("run", [1, 0]))
        invalid_source["runs"][0]["provenance"]["source"] = "source "
        invalid_values.append(invalid_source)

        for invalid in invalid_values:
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(DirectionalSearchError, "whitespace"):
                    DirectionalIndex(invalid)

        lone_surrogate = dataset(record("run", [1, 0]))
        lone_surrogate["feature_space"]["id"] = "\ud800"
        with self.assertRaisesRegex(DirectionalSearchError, "UTF-8"):
            DirectionalIndex(lone_surrogate)

    def test_public_api_has_no_legacy_mutable_accessors(self):
        self.assertEqual(
            set(directional_module.__all__),
            {
                "DirectionalIndex",
                "DirectionalSearchError",
                "INPUT_SCHEMA",
                "RESULT_SCHEMA",
                "load_dataset",
            },
        )
        index = DirectionalIndex(dataset(record("run", [1, 0])))
        self.assertFalse(hasattr(index, "get"))
        self.assertFalse(hasattr(index, "runs"))
        self.assertFalse(hasattr(directional_module, "load_records"))
        self.assertFalse(hasattr(directional_module, "RunVector"))
        self.assertFalse(hasattr(directional_module, "SimilarRun"))

    def test_top_k_self_exclusion_and_unknown_run_validation(self):
        index = DirectionalIndex(
            dataset(
                record("query", [1, 0]),
                record("zz-other", [1, 0]),
                record("third", [0, 1]),
            )
        )
        self.assertEqual(index.search("query", 1)[0]["run_id"], "zz-other")
        self.assertEqual(
            index.search("query", 1, exclude_self=False)[0]["run_id"], "query"
        )
        self.assertEqual(index.search("query", 0), [])
        with self.assertRaisesRegex(DirectionalSearchError, "unknown run_id"):
            index.fingerprint("missing")
        with self.assertRaisesRegex(DirectionalSearchError, "run_id"):
            index.fingerprint([])
        with self.assertRaisesRegex(DirectionalSearchError, "top_k"):
            index.search("query", True)

    def test_input_and_result_mutation_cannot_change_index_state(self):
        provenance = {
            "source": "frozen/raw.jsonl",
            "reference": {"offset": 17, "tags": ["a", "b"]},
        }
        source = dataset(
            record("query", [1, 0]),
            record("candidate", [1, 0], provenance=provenance),
        )
        index = DirectionalIndex(source)
        provenance["reference"]["offset"] = 999
        provenance["reference"]["tags"].append("mutated")
        source["feature_space"]["id"] = "mutated-space"
        source["runs"][1]["after"][0] = -1

        first_result = index.search("query", 1)
        first_fingerprint = index.fingerprint("candidate")
        self.assertEqual(first_result[0]["provenance"]["reference"]["offset"], 17)
        self.assertEqual(first_result[0]["provenance"]["reference"]["tags"], ["a", "b"])
        self.assertEqual(
            first_result[0]["feature_space"]["id"], "test-direction-v1"
        )
        self.assertEqual(first_fingerprint["raw_delta"], [1.0, 0.0])

        first_result[0]["provenance"]["reference"]["offset"] = 404
        first_result[0]["raw_delta"][0] = 404
        first_fingerprint["scaled_delta"][0] = 404
        first_fingerprint["feature_space"]["features"][0]["scale"] = 404
        self.assertEqual(index.search("query", 1)[0]["raw_delta"], [1.0, 0.0])
        self.assertEqual(
            index.search("query", 1)[0]["provenance"]["reference"]["offset"],
            17,
        )
        self.assertEqual(index.fingerprint("candidate")["scaled_delta"], [1.0, 0.0])
        self.assertEqual(
            index.fingerprint("candidate")["feature_space"]["features"][0]["scale"],
            1.0,
        )

    def test_fingerprint_and_results_identify_the_feature_space(self):
        index = DirectionalIndex(
            dataset(
                record("query", [1, 0]),
                record("candidate", [1, 0]),
                feature_space_id="frozen-space-2026-08",
            )
        )
        fingerprint = index.fingerprint("query")
        self.assertEqual(fingerprint["feature_space"]["id"], "frozen-space-2026-08")
        self.assertRegex(fingerprint["feature_space_commitment"], r"^sha256:[0-9a-f]{64}$")
        result = index.search("query", 1)[0]
        self.assertEqual(result["feature_space"], fingerprint["feature_space"])
        self.assertEqual(
            result["feature_space_commitment"],
            fingerprint["feature_space_commitment"],
        )

    def test_cli_reads_versioned_example_and_emits_disposable_results(self):
        example = ROOT / "examples" / "runs.json"
        completed = subprocess.run(
            [
                sys.executable,
                str(ROOT / "directional_similar_runs.py"),
                "--dataset",
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
        expected_commitment = (
            "sha256:e3200e1254056d8148e98ef460fc4fe1c2ef6e9af3152873abd25604f96beb60"
        )
        self.assertEqual(output["schema"], RESULT_SCHEMA)
        self.assertEqual(output["feature_space"]["id"], "toy-direction-v1")
        self.assertEqual(output["feature_space_commitment"], expected_commitment)
        self.assertEqual(
            output["query_fingerprint"]["feature_space_commitment"],
            expected_commitment,
        )
        self.assertEqual(
            output["query_fingerprint"]["feature_space"], output["feature_space"]
        )
        self.assertTrue(output["exclude_self"])
        self.assertEqual(
            [result["run_id"] for result in output["results"]],
            ["toy-east-scaled", "toy-north"],
        )
        self.assertEqual(
            output["results"][0]["provenance"]["reference"],
            "toy-east-scaled",
        )
        for result in output["results"]:
            self.assertEqual(result["feature_space"], output["feature_space"])
            self.assertEqual(
                result["feature_space_commitment"], expected_commitment
            )

    def test_cli_rejects_the_legacy_array_with_exit_two(self):
        with tempfile.TemporaryDirectory() as directory:
            legacy = pathlib.Path(directory) / "legacy.json"
            legacy.write_text(json.dumps([record("legacy", [1, 0])]), encoding="utf-8")
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "directional_similar_runs.py"),
                    "--dataset",
                    str(legacy),
                    "--query-run-id",
                    "legacy",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
        self.assertEqual(completed.returncode, 2)
        self.assertIn("versioned dataset object", completed.stderr)

    def test_cli_rejects_non_utf8_input_without_a_traceback(self):
        with tempfile.TemporaryDirectory() as directory:
            invalid = pathlib.Path(directory) / "invalid.json"
            invalid.write_bytes(b"\xff\xfe")
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "directional_similar_runs.py"),
                    "--dataset",
                    str(invalid),
                    "--query-run-id",
                    "run",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
        self.assertEqual(completed.returncode, 2)
        self.assertIn("valid UTF-8 JSON", completed.stderr)
        self.assertNotIn("Traceback", completed.stderr)


if __name__ == "__main__":
    unittest.main()
