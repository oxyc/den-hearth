# Repository agent instructions

## What deserves fixing

Den is a household app: one owner, a few family members, one Apple TV, a homelab box. There is no
adversary inside the household, and diagnostics go to our own already-redacted log collector. Judge
every finding against that, not against a bank.

**Worth fixing (and filing, if you can't fix it now):**
- A person can hit it in normal use and sees something wrong, missing or stuck: wrong audio
  language, skip-credits at the wrong time, no next episode, an empty row, a frozen overlay.
- Data people care about is lost or wrong: watch progress, seen marks, watchlist, library.
- A security hole reachable from the public internet (den-edge public names, public media listener).
- Slowness a person can feel, with a measurement or a trace showing it.
- A failing or flaky gate (`make verify`, Playwright, the dataset daily run).

**Not worth an issue, a PR or a comment** — don't file these, don't fix them in passing:
- Hypothetical failures of local storage: Keychain/SQLite/UserDefaults/filesystem refusing a write,
  "verify by readback", "fail closed when unreadable", rollback-of-a-failed-rollback, quarantine.
- Numeric edge cases nobody hits: overflow, saturation, non-finite values, exhausted clocks/epochs.
- Races and "fence to the exact profile/authority" work without a reproduced user-visible symptom.
- Removing identifiers from our own diagnostics.
- Parity for its own sake. Platforms may differ internally; only a user-visible wrong result counts.
- Localizing rare error or recovery copy, "qualify on hardware" chores, audit/tracker issues.
- Performance "opportunities" without a measurement of a felt problem.
- Bundle-size bookkeeping. Don't write size records into this file or into PRs.

**How to work:**
- Do the task you were given, then stop. Don't audit neighbouring code looking for more.
- Note anything else you noticed in one line in your final report. File an issue only if it meets
  the bar above; at most three per session, after searching for an existing one. Never file batches.
- Smallest fix for the reported symptom, one PR per issue. A PR over ~400 changed lines, or one
  touching more than one client, needs the owner's go-ahead first.
- Don't leave draft PRs behind: finish it to mergeable or close it.
- Fix the same bug on another client only when that client shows the same user-visible symptom.
- Tests prove the symptom is fixed (E2E/integration preferred). Don't add tests for the cases in the
  "not worth it" list.
