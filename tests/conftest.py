"""Make a run that could not execute the bridge tests say so.

Three modules skip themselves at collection when the Reticulum stack
(rns/lxmf/staticmap/dotenv) is absent. That is the right behaviour on a laptop
without the radio stack installed -- but it means a run with those dependencies
missing reports success having executed none of the bridge tests: the map labels,
the command handling, the soil formatting, the farmer-facing wording. The summary
line says "passed" either way, and the only difference is a skip count nobody
reads.

This is not hypothetical. cd60737 changed the map labels and every command
confirmation and was verified against a suite that was silently skipping the exact
files covering it. The gap surfaced only when the dependencies were installed
deliberately and the skip count dropped to zero.

Two things happen here. Every run that skipped a module gets a loud terminal
section naming what did not run, so "these tests passed" is distinguishable from
"these tests did not run" without anyone parsing a skip count. And setting
NAVAMESH_REQUIRE_FULL_TESTS=1 turns those skips into a failed run, which is what
CI and any pre-deployment check should set -- an env var is something CI cannot
satisfy by accident, unlike a dependency that is merely usually present.

Deliberately reports what *actually* skipped rather than probing for the stack
itself: importing reticulum_bridge to find out has side effects (it exits at
import when its config is absent, which is why the guards catch SystemExit too),
and a list of dependency names here would be a fourth copy to keep in sync. This
hook sees whatever the modules really did, including any module guarded later.
"""
import os

import pytest

REQUIRE_ENV = "NAVAMESH_REQUIRE_FULL_TESTS"

# Collector-level skips seen this session: (nodeid, reason).
_skipped_modules: list = []


def _required() -> bool:
    return os.environ.get(REQUIRE_ENV, "").strip().lower() in ("1", "true", "yes")


@pytest.hookimpl(trylast=True)
def pytest_collectreport(report):
    """A module that skips itself at collection reports here, not as a test."""
    if report.skipped:
        reason = ""
        if isinstance(getattr(report, "longrepr", None), tuple) and len(report.longrepr) == 3:
            reason = report.longrepr[2]      # (path, lineno, message)
        _skipped_modules.append((report.nodeid, reason))


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    if not _skipped_modules:
        return

    write = terminalreporter.write_line
    terminalreporter.section("modules that did not run", sep="=", red=True, bold=True)
    write(
        f"{len(_skipped_modules)} test module(s) skipped themselves at collection. "
        "Their tests did not execute -- this run does not cover them:"
    )
    for nodeid, reason in _skipped_modules:
        write(f"  - {nodeid}" + (f"  ({reason})" if reason else ""))
    write("")
    if _required():
        write(
            f"{REQUIRE_ENV} is set, so this counts as a failure. Install the bridge "
            "stack: pip install rns lxmf staticmap pillow python-dotenv"
        )
    else:
        write(
            f"Set {REQUIRE_ENV}=1 to make this a failed run instead of a passing one "
            "(CI and any pre-deployment check should)."
        )


def pytest_sessionfinish(session, exitstatus):
    """Fail the run when modules were skipped and the caller said they must not be.

    Only escalates -- an already-failing run keeps its own exit status, since a
    real test failure is more informative than this one.
    """
    if _skipped_modules and _required() and exitstatus == 0:
        session.exitstatus = 1
