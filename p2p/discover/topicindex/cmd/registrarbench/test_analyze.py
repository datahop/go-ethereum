"""Mode isolation and measured-operation filtering, without generating figures."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import analyze


def fixture(mode="simulated", rate=10, audit=False):
    run = analyze.Run(Path("fixture.jsonl"), "fixture")
    run.metadata = {"mode": mode, "registration_semantics": "total-attempt-scheduled-arrivals-v1",
                    "config": {"Mode": mode, "RegRate": rate, "QueryRate": 10,
                               "Advertisers": 100, "Topics": 30, "Alpha": 1,
                               "CacheSize": 50, "Lifetime": 1000000000},
                    "dataset": {"source": "synthetic"}, "allocation_audit": audit}
    run.summary = {"performance_valid": not audit, "measurement_start_ns": 100,
                   "measurement_seconds": 1, "allocation_audit": audit}
    return run


class AnalysisTests(unittest.TestCase):
    def test_population_flags(self):
        for any_limited, measured_limited in ((True, False), (True, True), (False, False)):
            for legacy in (False, True):
                with self.subTest(any=any_limited, measurement=measured_limited, legacy=legacy):
                    run = fixture()
                    run.summary.update({"population_limited" if legacy else "population_limited_any": any_limited,
                                        "measured_population_limited" if legacy else "population_limited_measurement": measured_limited})
                    row = analyze.summary_row(run)
                    self.assertIs(row["population_limited_any"], any_limited)
                    self.assertIs(row["population_limited_measurement"], measured_limited)
        self.assertIsNone(analyze.population_flags({"population_limited": True})["population_limited_measurement"])

    def test_mode_and_audit_isolation(self):
        simulated, realtime, audit = fixture(), fixture("realtime"), fixture("realtime", audit=True)
        self.assertTrue(simulated.valid)
        self.assertFalse(simulated.realtime_valid)
        self.assertTrue(realtime.realtime_valid)
        self.assertFalse(audit.valid)
        self.assertFalse(audit.realtime_valid)
        self.assertEqual(len({r.group for r in (simulated, realtime, audit)}), 3)
        legacy = fixture()
        del legacy.metadata["registration_semantics"]
        self.assertNotEqual(legacy.group, simulated.group)
        legacy.metadata["registration_semantics"] = "total-attempt-paced-cap-v1"
        self.assertNotEqual(legacy.group, simulated.group)

    def test_simulated_load_comparisons_excluded(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(analyze, "save") as save:
            notes = []
            analyze.cross_run_plots([fixture(rate=10), fixture(rate=20)], Path(tmp), notes)
            save.assert_not_called()
            self.assertTrue(any("simulated" in note for note in notes))

    def test_read_filters_warmup_and_handles_expiry(self):
        run = fixture()
        records = [dict(run.metadata, type="metadata")]
        for kind in ("registration", "query", "expiry"):
            for started in (50, 100, 150):
                records.append(dict(type="operation", operation=kind, started_ns=started,
                                    latency_ns=20, utilisation=.5, topic_rank=1,
                                    topic_population=10, topic_cache_size=2))
        records.append(dict(type="retry_expired"))
        records.append(dict(type="population_scarcity"))
        records.append(dict(run.summary, type="summary"))
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "run.jsonl"
            path.write_text("\n".join(json.dumps(r) for r in records))
            actual = analyze.read_run(path, 1)
        for kind in actual.operations:
            self.assertEqual(list(actual.values(kind)[0]), [100, 150])


if __name__ == "__main__":
    unittest.main()
