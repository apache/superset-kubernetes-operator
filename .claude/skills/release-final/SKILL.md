---
name: release-final
description: >
  Guide the release manager through finalizing an Apache Superset Kubernetes
  Operator release after a PMC vote has passed: tallying votes, drafting the
  [RESULT] email, running release-finalize.sh, pushing the final tag, adding
  the changelog date, promoting artifacts to dist.apache.org/release, and
  drafting the [ANNOUNCE] email. Use once a release vote has closed. Never
  runs git push, svn, or sends email itself — it tells the release manager
  the exact command to run.
---

# Finalizing a release

This skill is a guided checklist for the second half of an ASF release: from PMC vote tally through the `[ANNOUNCE]` email. It starts with no memory of whatever `/release-rc` session opened the vote — this session asks for whatever context it needs, since git tags/branches are the durable source of truth and vote tallies only exist in email. The full authoritative runbook is `docs/contributing/releasing.md`.

## Ground rules

- **Never run**: `git push`, `svn add`/`svn commit`, `scripts/release-finalize.sh`, or anything that sends email. Print the exact, fully-substituted command and wait for the RM to confirm they ran it.
- **Safe to run directly**: read-only git/gh inspection, and `scripts/release-email.sh {result,announce}` (pure stdout template generation, sends nothing).
- **Safe to edit directly**: `docs/reference/releases.md` to add the release date. Leave the commit/push to the RM.
- Nothing here executes `scripts/release-source.sh` promotion or SVN commands — always print them.

## Steps

### 1. Identify the release being finalized

Confirm the RM is an Apache Superset **committer** (push access to `apache/superset-kubernetes-operator` is required for steps 5 and 6 below) — if `/release-rc` already confirmed this for the same RM, no need to re-ask.

Ask which version/RC is being finalized. Help by listing candidates read-only:

```sh
git tag -l 'v*-rc*'
git tag -l 'v*' | grep -v -- '-rc'
```

Suggest RC tags that don't yet have a matching final tag, but let the RM confirm or override.

### 2. Tally the vote

Ask the RM for the vote tally: who voted, whether they're a binding PMC member, and +1/0/-1. Also ask when the vote email was sent (or confirm it's been open ≥72 hours).

Check against ASF release-vote rules:

- At least 3 binding (PMC) +1 votes.
- No -1 votes.
- Vote open at least 72 hours.

If the tally doesn't yet qualify, say so clearly — but this is advisory only; don't block the RM from proceeding if they say it's fine (e.g. they've already accounted for something you don't have visibility into).

### 3. Draft the result email

Run directly:

```sh
scripts/release-email.sh result
```

Show the draft, remind the RM to send it by hand to `dev@superset.apache.org` with `[RESULT][VOTE]` in the subject.

### 4. Finalize the tag

`release-finalize.sh` refuses a dirty working tree, and untracked files count. Check `git status --porcelain` first. If it isn't empty, have the RM stash the changes (`git stash push -u`) and restore them on a branch off `main` after the release rather than committing them to the release branch.

Print, filled in:

```sh
# From the <major>.<minor> branch
scripts/release-finalize.sh <version>
```

This tags the exact voted RC commit as the final version — it will refuse if `HEAD` isn't that commit. Remind the RM not to add any polish commits first; anything not in the voted RC must not end up under the final tag.

### 5. Push the final tag

Identify the right remote, read-only — don't assume `origin`, since a release manager developing from a personal fork typically has `origin` pointing at their fork and a separate remote (commonly `upstream`) pointing at `git@github.com:apache/superset-kubernetes-operator.git`:

```sh
git remote -v | grep apache/superset-kubernetes-operator
```

Use that remote name below; ask the RM if it's ambiguous.

Print:

```sh
git push <remote> v<version>
```

This triggers the release workflow that publishes `<version>` and `latest` images/chart to GHCR.

### 6. Add the release date to the changelog

On `main` (not the release branch — the docs site builds from `main`, and the release branch's changelog should stay undated to match the voted source), edit `docs/reference/releases.md`: change `## <version>` to `## <version> - <date>`.

`main` requires a review before merging (branch protection), so this needs a PR, not a direct push. Print the commands for the RM to run themselves:

```sh
git checkout main
git pull <remote>
git checkout -b docs/<version>-release-date
# (after the edit)
git add docs/reference/releases.md
git commit -m "docs: add release date for <version>"
git push <remote> docs/<version>-release-date
gh pr create --title "docs: add release date for <version>" --fill
```

### 7. Promote source artifacts to dist.apache.org/release

Once the binary release workflow (step 5) has finished successfully, print the following, run from the `<major>.<minor>` branch with the final tag on `HEAD`:

```sh
scripts/release-source.sh
```

This detects the final tag on `HEAD`, reuses the voted RC's signed tarball bytes under the final filename (preserving signature validity), and regenerates the SHA-512 checksum. Then print the SVN promotion, with real paths substituted:

```sh
cd ~/asf/release-superset
svn up
mkdir kubernetes-operator-<version>
cp /path/to/dist/<version>/apache-superset-kubernetes-operator-<version>.tar.gz{,.asc,.sha512} \
   kubernetes-operator-<version>/
svn add kubernetes-operator-<version>
svn commit -m "Release Apache Superset Kubernetes Operator <version>"
```

No wait is needed before this step — it should happen as soon as the binary workflow is done.

### 8. Wait for the changelog PR, then announce

The announcement links to the release notes on the docs site, so the step 6 PR must be merged and the docs deployed first. Once it is, run directly from the `<major>.<minor>` branch (the script reads the final tag from `HEAD`):

```sh
scripts/release-email.sh announce
```

Show the draft, remind the RM to send it to `announce@apache.org` and `dev@superset.apache.org`.

### 9. Close out

Confirm all steps are done: final tag pushed, changelog dated on `main`, artifacts in `release/superset`, announcement sent. The release is complete.
