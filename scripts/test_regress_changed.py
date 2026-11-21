#!/usr/bin/env python3
"""Tests for scripts/regress-changed.py. Run: python3 -I scripts/test_regress_changed.py"""

import importlib.util
import os
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("regress_changed", os.path.join(HERE, "regress-changed.py"))
rc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rc)


def plan_for(*paths):
    return rc.build_plan(list(paths), rc.load_map())


class Opts:
    no_msrv = no_cases = no_emulators = False


class MapTests(unittest.TestCase):
    def test_map_parses_and_every_action_is_known(self):
        for glob, actions in rc.load_map():
            p = rc.Plan()
            for a in actions:
                p.add(a, glob)

    def test_docs_need_nothing(self):
        p = plan_for("docs/review/rubric.md")
        self.assertFalse(p.wide)
        self.assertEqual(rc.commands(p, Opts())[0], [])

    def test_crate_gets_its_tests_msrv_and_cases(self):
        p = plan_for("crates/fs-amiga/src/sfs.rs")
        self.assertEqual(p.crates, {"fs-amiga"})
        self.assertEqual(p.msrv, {"fs-amiga"})
        self.assertIn("sfs", p.cases)
        self.assertFalse(p.wide)

    def test_writer_crate_selects_its_emulator_scenarios(self):
        self.assertIn("ntfs", plan_for("crates/fs-pc/src/ntfs_fsck.rs").emu)

    def test_src_module_becomes_a_lib_filter(self):
        self.assertEqual(rc.module_filter("src/controller/restore/mod.rs"), "controller::restore")
        self.assertEqual(rc.module_filter("src/model/job.rs"), "model::job")
        p = plan_for("src/controller/restore/source.rs")
        self.assertIn("controller::restore::source", p.lib)
        self.assertIn("ui_scenarios", p.tests)
        self.assertIn("flow.restore", p.cases)

    def test_central_and_unknown_paths_escalate(self):
        self.assertTrue(plan_for("src/fs/mod.rs").wide)
        self.assertTrue(plan_for("crates/disk-core/src/bits.rs").wide)
        self.assertTrue(plan_for("no/such/area.txt").wide)

    def test_case_manifest_selects_its_own_ids(self):
        p = plan_for("regression-tests/cases/tier1/new-volume.toml")
        self.assertTrue(any(c.startswith("fs.new-volume.") for c in p.cases))

    def test_integration_target_from_tests_dir(self):
        self.assertIn("cli_suite", plan_for("tests/cli_suite/cli_hfs.rs").tests)
        self.assertIn("doc_parity", plan_for("tests/doc_parity.rs").tests)

    def test_frontend_gated_by_host(self):
        cmds, notes = rc.commands(plan_for("frontends/win32/src/restore.rs"), Opts())
        labels = [l for l, _ in cmds]
        if rc.platform.system() != "Windows":
            self.assertNotIn("win32 front-end tests", labels)
            self.assertTrue(any("win32" in n for n in notes))

    def test_one_cargo_invocation_per_kind(self):
        p = plan_for("crates/fs-amiga/src/sfs.rs", "crates/fs-xfs/src/xfs.rs")
        cmds, _ = rc.commands(p, Opts())
        crate_cmds = [a for l, a in cmds if l == "crate tests"]
        self.assertEqual(len(crate_cmds), 1)
        self.assertIn("fs-xfs", crate_cmds[0])


class LintTests(unittest.TestCase):
    def test_flags_post_173_apis(self):
        hits = rc.lint_post_173([("src/x.rs", "let e = io::Error::other(msg);"),
                                 ("src/x.rs", "let v: Vec<_> = std::iter::repeat_n(0, 4).collect();")])
        self.assertEqual(len(hits), 2)

    def test_allows_the_shims_and_comments(self):
        hits = rc.lint_post_173([("src/x.rs", "crate::compat::repeat_n(0u8, 4)"),
                                 ("src/x.rs", "// io::Error::other( is 1.74")])
        self.assertEqual(hits, [])


if __name__ == "__main__":
    unittest.main()
