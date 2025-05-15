# Conventional Commits Configuration

This project follows [Conventional Commits](https://conventionalcommits.org/) specification for automated semantic versioning and changelog generation.

## Commit Message Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types

- `feat`: A new feature (minor version bump)
- `fix`: A bug fix (patch version bump)
- `docs`: Documentation only changes
- `style`: Changes that do not affect the meaning of the code
- `refactor`: A code change that neither fixes a bug nor adds a feature
- `perf`: A code change that improves performance
- `test`: Adding missing tests or correcting existing tests
- `chore`: Changes to the build process or auxiliary tools

### Breaking Changes

To trigger a major version bump, add `BREAKING CHANGE:` in the footer or use `!` after the type:

```
feat!: remove deprecated API endpoint

BREAKING CHANGE: The `/api/v1/wines` endpoint has been removed. Use `/api/v2/wines` instead.
```

### Examples

```bash
# Feature (minor version bump)
feat(api): add wine search functionality

# Bug fix (patch version bump)
fix(parser): handle empty CSV lines correctly

# Breaking change (major version bump)
feat!: redesign wine record structure

BREAKING CHANGE: Wine records now use 'id' instead of 'wineId' field
```

## Semantic Release Behavior

- `feat`: 🆕 Minor version bump (1.0.0 → 1.1.0)
- `fix`: 🐛 Patch version bump (1.0.0 → 1.0.1)
- `BREAKING CHANGE`: 💥 Major version bump (1.0.0 → 2.0.0)
- Other types: No version bump, but included in changelog