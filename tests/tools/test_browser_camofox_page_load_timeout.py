"""Regression tests: Camofox calls that load a page get the page-load budget.

The first ``browser_navigate`` of a session creates the tab *on* the target
URL, and that tab-creation POST was budgeted with the generic 30s command
timeout — while navigating an *existing* tab used an explicit 60s. The
expensive call (tab create + full page load, cold) had half the deadline of
the cheap one, so the first navigate to a heavy site (Taobao behind its
captcha) timed out while a retry against the now-warm tab succeeded. Same
shape as the agent-browser cold-start bug in a628a4d.

Click and key-press share the root cause: both can trigger a navigation —
pressing Enter in a search box is the documented workaround for sites whose
sort params do not survive direct URL navigation — and both were budgeted as
instant commands.
"""

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def fake_session():
    return {
        "user_id": "hermes_test123",
        "tab_id": None,
        "session_key": "task_my-session",
        "managed": False,
        "adopt_existing_tab": False,
    }


def _timeout_of(mock_call):
    """Pull the timeout kwarg off a requests call, however it was passed."""
    return mock_call.kwargs.get("timeout") or mock_call[1].get("timeout")


class TestEnsureTabBudget:
    """Tab creation on a real URL is a page load, not a command."""

    def test_tab_created_on_a_url_gets_the_page_load_budget(self, fake_session):
        from tools import browser_camofox as mod

        resp = MagicMock()
        resp.json.return_value = {"tabId": "tab-42"}
        resp.raise_for_status = MagicMock()

        with patch.object(mod, "_get_session", return_value=fake_session), \
             patch.object(mod, "get_camofox_url", return_value="http://localhost:9377"), \
             patch.object(mod, "_get_command_timeout", return_value=30), \
             patch("tools.browser_camofox.requests.post", return_value=resp) as post:
            mod._ensure_tab("test-task", url="https://item.taobao.com/item.htm?id=1")

        assert _timeout_of(post.call_args) >= 60, (
            "the first navigate of a session creates the tab AND loads the "
            "page — it must not get a shorter deadline than a later navigate"
        )

    def test_blank_tab_stays_on_the_command_budget(self, fake_session):
        """An about:blank tab loads nothing; no reason to wait 60s on it."""
        from tools import browser_camofox as mod

        resp = MagicMock()
        resp.json.return_value = {"tabId": "tab-42"}
        resp.raise_for_status = MagicMock()

        with patch.object(mod, "_get_session", return_value=fake_session), \
             patch.object(mod, "get_camofox_url", return_value="http://localhost:9377"), \
             patch.object(mod, "_get_command_timeout", return_value=30), \
             patch("tools.browser_camofox.requests.post", return_value=resp) as post:
            mod._ensure_tab("test-task")

        assert _timeout_of(post.call_args) == 30

    def test_first_navigate_is_never_stingier_than_a_later_one(self, fake_session):
        """The asymmetry itself, asserted directly: cold >= warm."""
        from tools import browser_camofox as mod

        resp = MagicMock()
        resp.json.return_value = {"tabId": "tab-42"}
        resp.raise_for_status = MagicMock()

        with patch.object(mod, "_get_session", return_value=fake_session), \
             patch.object(mod, "get_camofox_url", return_value="http://localhost:9377"), \
             patch.object(mod, "_get_command_timeout", return_value=30), \
             patch("tools.browser_camofox.requests.post", return_value=resp) as post:
            mod._ensure_tab("test-task", url="https://example.com")
            cold = _timeout_of(post.call_args)

        with patch.object(mod, "_get_command_timeout", return_value=30):
            warm = mod._page_load_timeout()

        assert cold >= warm


class TestPageLoadTimeoutHelper:
    """The helper is a floor, not a cap."""

    def test_floor_is_sixty(self):
        from tools import browser_camofox as mod

        with patch.object(mod, "_get_command_timeout", return_value=30):
            assert mod._page_load_timeout() == 60

    def test_a_larger_configured_timeout_wins(self):
        """Raising browser.command_timeout must raise the page-load budget
        too — a user who configured 120s meant it."""
        from tools import browser_camofox as mod

        with patch.object(mod, "_get_command_timeout", return_value=120):
            assert mod._page_load_timeout() == 120


class TestNavigationTriggeringActions:
    """Click and press can navigate; they were budgeted as instant."""

    @pytest.fixture
    def live_session(self):
        return {
            "user_id": "hermes_test123",
            "tab_id": "tab-42",
            "session_key": "task_my-session",
            "managed": False,
            "adopt_existing_tab": False,
        }

    def test_click_gets_the_page_load_budget(self, live_session):
        from tools import browser_camofox as mod

        with patch.object(mod, "_get_session", return_value=live_session), \
             patch.object(mod, "_camofox_private_page_block", return_value=None), \
             patch.object(mod, "_get_command_timeout", return_value=30), \
             patch.object(mod, "_post", return_value={"url": "x"}) as post:
            mod.camofox_click("@e1", "test-task")

        assert post.call_args.kwargs.get("timeout") >= 60

    def test_press_gets_the_page_load_budget(self, live_session):
        """Enter in a search box is a navigation."""
        from tools import browser_camofox as mod

        with patch.object(mod, "_get_session", return_value=live_session), \
             patch.object(mod, "_camofox_private_page_block", return_value=None), \
             patch.object(mod, "_get_command_timeout", return_value=30), \
             patch.object(mod, "_post", return_value={}) as post:
            mod.camofox_press("Enter", "test-task")

        assert post.call_args.kwargs.get("timeout") >= 60
