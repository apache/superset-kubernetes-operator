---
name: release-rc
description: >
  Guide the release manager through cutting a new Apache Superset Kubernetes
  Operator release candidate: changelog review, running release-rc.sh and
  release-source.sh, pushing the branch/tag, staging to dist.apache.org/dev,
  and drafting the [VOTE] email. Use when starting a new release or a new RC
  for an in-progress release. Never runs git push, svn, or sends email itself
  — it tells the release manager the exact command to run.
---

# Cutting a release candidate

This skill is a guided checklist for the first half of an ASF release: from changelog review through opening the PMC vote. The full authoritative runbook is `docs/contributing/releasing.md` — this skill walks the release manager (RM) through it, filling in real values and doing read-only verification, but never executes any critical/irreversible step itself.

## Ground rules

- **Never run**: `git push`, `svn add`/`svn commit`, `git tag`, `scripts/release-rc.sh`, `scripts/release-source.sh`, or anything that sends email. For each of these, print the exact, fully-substituted command (no placeholders) and wait for the RM to confirm they ran it before moving on.
- **Safe to run directly**: read-only git/gh inspection (`git status`, `git tag -l`, `git branch -a`, `git log`, `gh run list`, `gh api .../check-runs`), grepping `docs/reference/releases.md` / `Makefile`, and `scripts/release-email.sh vote` (pure stdout template generation, sends nothing).
- **Safe to edit directly**: `docs/reference/releases.md` for the changelog review pass — a normal reversible doc edit. Still leave the `git commit` of that edit to the RM's own command.
- If the RM says they already completed a step in a prior session, verify it read-only (e.g. does the tag exist, does `Makefile` VERSION match) rather than re-explaining it from scratch.

## Steps

### 1. Establish what's being released

Confirm the RM is an Apache Superset **committer** — this is what grants push access to `apache/superset-kubernetes-operator`. Without it, everything up through building the signed tarball still works locally, but step 5 (pushing the branch/tag) will fail. Committer status is separate from PMC membership: a non-PMC committer can drive the whole RC/vote mechanics, but the vote itself still needs a binding `+1` from a PMC member.

Ask the RM which version is being cut (e.g. `0.3.0`) and whether this is the first RC for a new `<major>.<minor>` or a follow-up RC for one already in flight. Cross-check with:

```sh
git tag -l 'v<version>-rc*'
git branch -a | grep '<major>.<minor>'
git status
```

Use this to suggest the next RC number and the branch flow (first RC creates the `<major>.<minor>` branch from `main`; subsequent RCs reuse the existing branch). Flag any mismatch against what the RM stated (e.g. they say "rc1" but `v<version>-rc1` already exists).

### 2. Changelog review

Check whether `docs/reference/releases.md` already has a `## <version>` heading — `release-rc.sh` will refuse to run without one.

If missing, help the RM do the review pass described in `docs/contributing/releasing.md` ("Reviewing the Changelog"):

1. Summarize `git log v<previous>..HEAD` into candidate Keep a Changelog bullets (`Added`/`Changed`/`Deprecated`/`Removed`/`Fixed`/`Security`), dropping non-noteworthy entries (typo fixes, internal refactors).
2. Ask the RM to confirm or adjust the summary.
3. Edit `docs/reference/releases.md` yourself: rename `## Unreleased` to `## <version>` (no date yet — that's added after the vote passes), add a fresh empty `## Unreleased` above it, and place the reviewed bullets.
4. Tell the RM to review the diff and commit it themselves (and, for a first RC, to do this on the new `<major>.<minor>` branch, not `main`).

### 3. Run release-rc.sh

Print, filled in:

```sh
scripts/release-rc.sh <version> --expect-rc <n>
```

This bumps `VERSION`/`Chart.yaml`, runs lint/test/docs/helm checks, commits, and creates the annotated RC tag locally. Wait for the RM to confirm it ran, then verify read-only: `git tag -l v<version>-rc<n>`, `grep VERSION Makefile`, `git branch --show-current`.

Creating the tag here is fine even though CI hasn't run yet — a git tag is just a local pointer to a commit and doesn't touch the remote. It's the *push* of the tag (step 5) that has to wait for CI, not its creation.

### 4. Build and sign the source tarball

Print:

```sh
scripts/release-source.sh
```

This produces `dist/<version>-rc<n>/apache-superset-kubernetes-operator-<version>-rc<n>.tar.gz{,.asc,.sha512}`, signed with the RM's own Apache-registered PGP key, and self-verifies.

### 5. Push branch, wait for CI, push tag

First identify the right remote, read-only — don't assume `origin`, since a release manager developing from a personal fork typically has `origin` pointing at their fork and a separate remote (commonly `upstream`) pointing at `git@github.com:apache/superset-kubernetes-operator.git`:

```sh
git remote -v | grep apache/superset-kubernetes-operator
```

Use whatever remote name that resolves to in every command below. If it's ambiguous (e.g. more than one remote matches, or none do), ask the RM.

Print the two-step sequence, and explain *why* it's two steps: the release workflow's publish gate (`scripts/verify-release-ci.sh`) reads required checks off the tagged commit, so pushing the tag before CI is green just makes the gate wait (or fail) — pushing the branch first lets CI run while the RM stages other artifacts.

```sh
git push <remote> <major>.<minor>
# wait for CI to go green on that branch, then:
git push <remote> v<version>-rc<n>
```

If asked, check CI status read-only:

```sh
gh run list --branch <major>.<minor> --limit 5
```

If the branch or tag ends up pushed to the wrong remote (e.g. a personal fork) by mistake, that's harmless and easy to undo — print `git push <wrong-remote> --delete <branch-or-tag>` for the RM to run.

### 6. Stage to dist.apache.org/dev

Print, with real values substituted (no `${VERSION}`/`${RC}` left as literal shell variables unless the RM specifically wants a reusable script):

```sh
cd ~/asf/dev-superset
mkdir kubernetes-operator-<version>-rc<n>
cp /path/to/dist/<version>-rc<n>/apache-superset-kubernetes-operator-<version>-rc<n>.tar.gz{,.asc,.sha512} \
   kubernetes-operator-<version>-rc<n>/
svn add kubernetes-operator-<version>-rc<n>
svn commit -m "Stage Superset Kubernetes Operator <version>-rc<n>"
```

Ask the RM for their actual local `dist/` output path and SVN checkout path if they differ from the defaults, and substitute those instead.

### 7. Summarize what's new

Read the finalized `## <version>` section of `docs/reference/releases.md` and write:

1. A one-sentence, high-level summary of the release (the overall theme — e.g. "mostly a security/reliability hardening release" vs. "mostly new features").
2. A short highlights list grouped by theme (not by Keep a Changelog heading — group however best conveys what changed, e.g. "Helm/HA", "Kubernetes support", "security hardening", "bug fixes"). **Explicitly call out every `**Breaking:**` bullet** — PMC voters need those most.

Only summarize what's actually in the changelog section — don't pull in internal/dependency-bump commits that were correctly excluded during the changelog review (step 2); those aren't part of the release notes voters see.

Show both to the RM for review.

### 8. Draft the vote email

Run directly (this only prints a template, sends nothing):

```sh
scripts/release-email.sh vote
```

Insert the step 7 summary into the draft — right after the "This is a vote to release..." line and before the artifact/verification block — so voters get the highlights inline instead of having to click through to the changelog. Show the assembled draft to the RM. Remind them:

- Send by hand to `dev@superset.apache.org` with `[VOTE]` in the subject.
- The vote must stay open **at least 72 hours**.
- Needs **at least 3 binding (PMC) +1 votes** and **no -1 votes**.

### 9. Close out

Tell the RM the vote is now open, and that once it closes (passing or not) they should come back and run `/release-final` to continue.
