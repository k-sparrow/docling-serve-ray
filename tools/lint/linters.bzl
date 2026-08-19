"""Lint aspects, mirroring Artemis's tools/lint/linters.bzl -- minus flake8
(see MODULE.bazel's LINT & FORMAT section for why: it needs
py_console_script_binary, which needs a newer rules_python than this repo's
pin allows). ruff alone covers the same ground.

Usage: `bazel build --aspects=//tools/lint:linters.bzl%ruff
--output_groups=rules_lint_human --@aspect_rules_lint//lint:fail_on_violation //...`
"""

load("@aspect_rules_lint//lint:lint_test.bzl", "lint_test")
load("@aspect_rules_lint//lint:ruff.bzl", "lint_ruff_aspect")

ruff = lint_ruff_aspect(
    binary = "@multitool//tools/ruff",
    configs = ["@@//:.ruff.toml"],
)

ruff_test = lint_test(aspect = ruff)
