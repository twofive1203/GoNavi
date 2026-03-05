# Contributing Guide

Thank you for contributing to this project.

This repository follows a release-first workflow: `main` is the default public branch, while releases are prepared through `release/*` branches.

---

## Branch Model

- `main`: stable release branch and default branch
- `dev`: day-to-day integration branch for maintainers
- `release/*`: release preparation branches for maintainers
- Recommended branch names for external contributors:
  - `fix/*`: bug fixes
  - `feature/*`: new features or enhancements

Maintainer release flow:

```text
feature/* / fix/* -> dev -> release/* -> main -> tag(vX.Y.Z)
```

---

## How External Contributors Should Open Pull Requests

Whether your branch is `fix/*` or `feature/*`, external contributors should **open pull requests directly against `main`**.

Reasons:

- `main` is the default branch, so the PR entry point is clearer
- merged contributions are immediately visible on the default branch
- maintainers can handle downstream sync and release preparation in one place

Recommended flow:

1. Fork this repository
2. Create a branch in your fork (`fix/*` or `feature/*` is recommended)
3. Make your changes and perform basic self-checks
4. Push the branch to your fork
5. Open a pull request against the `main` branch of this repository

---

## Pull Request Requirements

Please keep each pull request focused, reviewable, and easy to validate.

Recommended expectations:

- one pull request should address one logical change
- use a clear title that explains the purpose
- include the following in the description:
  - background and problem statement
  - key changes
  - impact scope
  - validation method
- include screenshots or recordings for UI changes when helpful
- explicitly mention risk and rollback notes for compatibility, data, or build-chain changes

---

## Merge Strategy for Maintainers

Pull requests merged into `main` should generally use **Squash and merge**.

Reasons:

- keeps `main` history clean and linear
- maps each PR to a single commit on `main`
- reduces release, audit, and rollback complexity

---

## Maintainer Sync Rules

Because external pull requests are merged directly into `main`, maintainers must sync `main` back to development and release branches to avoid branch drift.

### 1. Sync `main` -> `dev` (required)

Every change merged into `main` must be synced into `dev`:

```bash
git checkout dev
git pull
git merge main
git push
```

### 2. Create `release/*` from `dev`

Before a release, create a release branch from `dev`, for example:

```bash
git checkout dev
git pull
git checkout -b release/v0.6.0
git push -u origin release/v0.6.0
```

### 3. Release from `release/*` back to `main`

When release preparation is complete, merge the release branch back into `main` and create a tag:

```bash
git checkout main
git pull
git merge release/v0.6.0
git push
git tag v0.6.0
git push origin v0.6.0
```

### 4. Sync `main` back to `dev` after release

After the release, sync `main` back into `dev` again:

```bash
git checkout dev
git pull
git merge main
git push
```

---

## Commit Message Recommendation

Keep commit messages clear and easy to audit.

Recommended format:

```text
emoji type(scope): concise description
```

Examples:

```text
🔧 fix(ci): fix DuckDB driver toolchain on Windows AMD64
✨ feat(redis): add Stream data browsing support
♻️ refactor(datagrid): optimize large-table horizontal scrolling and rendering
```

---

## Additional Notes

- Please include validation results for documentation, build-chain, or driver compatibility changes
- For larger changes, opening an issue or draft PR first is recommended
- Maintainers may ask contributors to narrow the scope if the change conflicts with the current project direction

Thank you for contributing.
