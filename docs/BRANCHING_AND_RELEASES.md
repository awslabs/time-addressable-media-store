# Branching and releases

This solution implements the [TAMS API](https://github.com/bbc/tams) specification.
Each release implements one **published** TAMS spec version. The `main` branch also
follows the spec's own `main` branch, so it can contain changes that are not yet in
a published spec version. This document explains how branches, pull requests and
releases are managed so that both of these hold.

## Branches

- **`main`** is the only long-lived branch. All pull requests target `main`. It
  follows upstream TAMS `main` as closely as possible, so at any time it may
  contain changes that implement unreleased parts of the spec.
- **`release/X.x`** (e.g. `release/7.x`) is a short-lived branch. It is created
  from the latest release tag only while a release is being prepared, and deleted
  once the release is tagged.

Release tags (e.g. `v7.1.0`) keep the exact code of every release, so no branch is
needed to preserve them.

## Types of change

Every change is merged to `main` first. When a release is prepared, each change is
treated as one of two types:

| Type | Examples | In the next release? |
|---|---|---|
| **Spec-dependent** | Changes to `api/build/openapi.yaml` or `api/TAMS_UPSTREAM_SHA`; new or changed endpoints, fields or behaviour that implement an unreleased spec change | Only once the TAMS spec version that contains them is published |
| **Everything else** | Bug fixes, dependency updates, infrastructure changes, internal behaviour, documentation | Yes |

Pull requests for spec-dependent changes should say so in their description and
carry the `spec-unreleased` label, so they are easy to leave out when a release is
prepared.

## Pull requests

1. Branch from `main`.
2. Open the pull request against `main`.
3. Pull requests are squash-merged, so each change is a single commit on `main`.

Do not rebase `main`. Do not merge a release branch into `main`.

## Preparing a release

How a release is made depends on whether `main` contains unreleased spec changes.

### When `main` contains no unreleased spec changes

This is the case, for example, after a TAMS spec version is published that covers
everything on `main`.

1. Open a pull request against `main` that bumps `ServiceVersion` in
   `template.yaml` (`Mappings` → `Solution` → `Constants`). If the spec version
   has changed, also update `ApiVersion`.
2. After merging, tag the release on `main` and create the GitHub release.

### When `main` contains unreleased spec changes

The release is built on a short-lived release branch from the latest release tag,
with the releasable commits from `main` cherry-picked onto it. The example below
prepares `v7.2.0` from `v7.1.0`.

1. List the commits on `main` that are not yet released:

   ```bash
   git fetch --tags
   git cherry -v v7.1.0 main
   ```

   Commits marked `+` are not yet released. Commits marked `-` are already in the
   release (git recognises cherry-picked commits). From the `+` commits, choose
   all except the spec-dependent ones.

2. Create the release branch from the latest tag and push it:

   ```bash
   git switch -c release/7.x v7.1.0
   git push -u origin release/7.x
   ```

3. Create a backport branch from it and cherry-pick the chosen commits, oldest
   first:

   ```bash
   git switch -c backport/v7.2.0 release/7.x
   git cherry-pick <sha> <sha> ...
   ```

   If a commit conflicts, resolve it and run `git cherry-pick --continue`.

4. Bump `ServiceVersion` in `template.yaml` and commit it.

5. Run the unit and functional tests and the linters (see
   [Local development](LOCAL_DEVELOPMENT.md)).

6. Push and open a pull request **against the release branch**. GitHub defaults
   new pull requests to `main`, so set the base explicitly:

   ```bash
   git push -u origin backport/v7.2.0
   gh pr create --base release/7.x --head backport/v7.2.0 --title "Release 7.2.0"
   ```

   The description should list every included change and every spec-dependent
   change that was left out.

7. Merge with **Rebase and merge** (not squash), so each change stays a separate
   commit on the release branch.

8. Tag the release and push the tag:

   ```bash
   git switch release/7.x
   git pull
   git tag v7.2.0
   git push origin v7.2.0
   ```

9. Create the GitHub release from the tag. The release notes should state which
   TAMS spec version the release implements, and list any spec-dependent changes
   on `main` that were intentionally left out.

10. Delete the release and backport branches:

    ```bash
    git switch main
    git branch -D release/7.x backport/v7.2.0
    git push origin --delete release/7.x
    git fetch --prune
    ```

## Version numbers

Versions follow [semantic versioning](https://semver.org/):

- **Patch** (`7.1.1`): bug fixes only.
- **Minor** (`7.2.0`): new behaviour or features that are not breaking, including
  support for a new spec version that is not breaking.
- **Major** (`8.0.0`): breaking changes, including those required by a new spec
  version.

The release notes for every release state the TAMS spec version it implements.
