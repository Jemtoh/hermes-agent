"""Read-only running display for fresh one-shot claims (retained fork fix)."""
from datetime import datetime, timedelta, timezone

import pytest
from cron.jobs import create_job, get_job, load_jobs, pause_job, save_jobs


@pytest.fixture
def tmp_cron_dir(tmp_path, monkeypatch):
    monkeypatch.setattr("cron.jobs.CRON_DIR", tmp_path / "cron")
    monkeypatch.setattr("cron.jobs.JOBS_FILE", tmp_path / "cron" / "jobs.json")
    monkeypatch.setattr("cron.jobs.OUTPUT_DIR", tmp_path / "cron" / "output")
    return tmp_path


class TestRunningStateDisplay:
    """A one-shot mid-run stores state="scheduled" until mark_job_run — its
    run_claim is the only persisted evidence of the run. Reading the stored
    state mid-run looks identical to "never fired", which sent a live
    incident down the wrong path (a 16-minute run diagnosed as a missed
    tick). get_job/list_jobs derive "running" from a fresh claim; nothing
    persists it."""

    def _claimed_job(self, claimed_at: datetime):
        job = create_job(prompt="One shot", schedule="2030-01-01T12:00:00")
        jobs = load_jobs()
        for j in jobs:
            if j["id"] == job["id"]:
                j["run_claim"] = {
                    "at": claimed_at.isoformat(),
                    "by": "test-host:1",
                }
        save_jobs(jobs)
        return job

    def test_fresh_claim_reads_as_running(self, tmp_cron_dir, monkeypatch):
        now = datetime(2026, 7, 26, 14, 50, 0, tzinfo=timezone.utc)
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)
        job = self._claimed_job(now - timedelta(minutes=5))
        assert get_job(job["id"])["state"] == "running"

    def test_expired_claim_reads_as_scheduled(self, tmp_cron_dir, monkeypatch):
        """A claim past its TTL is a dead run (gateway killed mid-flight) —
        showing "running" forever would hide exactly the failure the TTL
        exists to recover from."""
        now = datetime(2026, 7, 26, 14, 50, 0, tzinfo=timezone.utc)
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)
        job = self._claimed_job(now - timedelta(hours=2))
        assert get_job(job["id"])["state"] == "scheduled"

    def test_malformed_claim_keeps_stored_state(self, tmp_cron_dir):
        job = create_job(prompt="One shot", schedule="2030-01-01T12:00:00")
        jobs = load_jobs()
        for j in jobs:
            if j["id"] == job["id"]:
                j["run_claim"] = {"at": "not-a-date", "by": "test-host:1"}
        save_jobs(jobs)
        assert get_job(job["id"])["state"] == "scheduled"

    def test_derived_state_is_not_persisted(self, tmp_cron_dir, monkeypatch):
        """The stored record must keep state="scheduled" — "running" is a
        read-path derivation, not a state-machine transition."""
        now = datetime(2026, 7, 26, 14, 50, 0, tzinfo=timezone.utc)
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)
        job = self._claimed_job(now - timedelta(minutes=5))
        assert get_job(job["id"])["state"] == "running"
        raw = [j for j in load_jobs() if j["id"] == job["id"]][0]
        assert raw["state"] == "scheduled"

    def test_paused_job_with_claim_stays_paused(self, tmp_cron_dir, monkeypatch):
        """Only state="scheduled" derives to running — a paused job's stored
        state outranks a leftover claim."""
        now = datetime(2026, 7, 26, 14, 50, 0, tzinfo=timezone.utc)
        monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)
        job = self._claimed_job(now - timedelta(minutes=5))
        pause_job(job["id"], reason="test")
        assert get_job(job["id"])["state"] == "paused"
