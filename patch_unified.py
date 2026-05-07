"""
Retired one-off patch script.

This file previously rewrote `Job_pipeline/preprocessing/unified_preprocessor.py`
in place to apply a Gemini fallback migration. That behavior was brittle,
non-idempotent, and dangerous because it modified repository source files based
on exact string matches and executed at import time.

The patch is intentionally disabled and retained only as a historical marker.
If a future migration is needed, implement it as explicit internal tooling with
clear documentation, versioning, and a non-default execution path.
"""

if __name__ == "__main__":
    raise SystemExit(
        "patch_unified.py has been retired and no longer rewrites source files."
    )
