"""Shared optional-text and reasoning normalization for authored cron fields."""
from typing import Any, Optional


def _normalize_job_optional_text(
    value: Any, *, strip_trailing_slash: bool = False
) -> Optional[str]:
    if not isinstance(value, str):
        return None
    return (value.strip().rstrip("/") if strip_trailing_slash else value.strip()) or None


def _normalize_reasoning_effort(value: Any) -> Optional[str]:
    """Spelling-only validation via the shared parser (cron knob never stricter/looser than
    config.yaml); model capability is deliberately NOT checked (model unknowable at create time,
    transports clamp at send time). None for unset, lowercase level, or ValueError."""
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    from hermes_constants import parse_reasoning_effort

    if parse_reasoning_effort(text) is None:
        raise ValueError(
            f"Invalid reasoning_effort {value!r}. Valid levels: "
            "none, minimal, low, medium, high, xhigh, max, ultra "
            "(empty string clears the override).")
    if text in {"false", "disabled"}:
        return "none"
    return text


def validate_job_modes(
    monitor_script: Optional[str],
    monitor_url: Optional[str],
    no_agent: bool,
    script: Optional[str],
    script_output_format: Optional[str] = None,
) -> None:
    """Execution-mode invariants shared by create_job and update_job (no bypass via the update
    door)."""
    from cron.artifact_delivery import validate_job_format
    from cron.jobs_invariants import _validate_job_mode_invariants

    validate_job_format(script_output_format, no_agent=no_agent, script=script)
    _validate_job_mode_invariants(monitor_script, monitor_url, no_agent, script)
