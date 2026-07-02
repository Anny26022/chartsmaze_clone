# Public Release Checklist

Use this checklist before publishing the repository or cutting a public release.

## Required

- Confirm the `LICENSE` choice is acceptable for the project.
- Run `python3 -m unittest discover -s tests -v`.
- Run `python3 -m compileall -q .`.
- Run `python3 -m pip install -e . --dry-run` after package metadata changes.
- Run a full live refresh when changing endpoint logic, generated schemas, or pipeline ordering.
- Verify generated artifact counts and note the run date in release notes.
- Review `pipeline_report.json` after a live refresh; final artifact validation should be clean before publishing.
- Confirm no generated intermediates are staged except the intended tracked outputs.
- Review `docs/DATA_LIMITATIONS.md` for current known gaps.

## Recommended

- Consider renaming the pipeline folder in a breaking v2 release. It is currently preserved for path compatibility.
- Publish large generated artifacts through GitHub Releases if repository size becomes a problem.
- Add endpoint health metrics if upstream reliability becomes a recurring issue.
- Add full schema snapshots for the final JSON and breadth artifacts once consumers are stable.
