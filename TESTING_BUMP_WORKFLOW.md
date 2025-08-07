# Testing the Bump Version Workflow

This document explains how to test the bump version workflow to ensure it properly updates both Python and Rust dependencies.

## 🧪 Local Testing (Recommended First)

### 1. Use the Test Script

We've created `test-bump-workflow.sh` to simulate the workflow logic locally:

```bash
# Test with sentry-arroyo (exists in both Python and Rust)
./test-bump-workflow.sh sentry-arroyo 3.0.0

# Test with sentry_usage_accountant (exists in both, different naming)
./test-bump-workflow.sh sentry_usage_accountant 0.2.0

# Test with any package name and version
./test-bump-workflow.sh <package-name> <version>
```

### 2. What the Test Script Checks

- ✅ Regex pattern generation for package names
- ✅ Python requirements.txt updates
- ✅ Rust Cargo.toml updates (both simple and complex formats)
- ✅ Cargo update command compatibility
- ✅ File diffs to show exactly what changed

### 3. Manual Testing Commands

You can also test individual components manually:

```bash
# Test regex pattern generation
PACKAGE="sentry-arroyo"
re="$(sed 's/[_-]/[_-]/g' <<< "$PACKAGE")"
echo "Pattern: $re"

# Test Python requirements update
sed "s/^\($re\)==.*/\1==3.0.0/g" requirements.txt

# Test Cargo.toml update
sed "s/^\($re\) = \"[^\"]*\"/\1 = \"3.0.0\"/g" rust_snuba/Cargo.toml

# Test cargo update
cd rust_snuba
cargo update --dry-run --package "sentry_arroyo"
```

## 🚀 GitHub Workflow Testing

### Option 1: Test on a Fork (Safest)

1. Fork the repository to your personal GitHub account
2. Enable GitHub Actions on your fork
3. Add the required secrets (`GETSENTRY_BOT_REVERT_TOKEN`)
4. Trigger the workflow manually

### Option 2: Test Branch Method (If you have repository access)

1. **Create a test branch:**
   ```bash
   git checkout -b test-bump-workflow
   git push origin test-bump-workflow
   ```

2. **Trigger the workflow manually:**
   - Go to GitHub Actions tab
   - Select "Bump a dependency" workflow
   - Click "Run workflow"
   - Choose your test branch
   - Enter package name: `sentry-arroyo`
   - Enter version: `2.29.0` (or latest)
   - Click "Run workflow"

3. **Monitor the workflow:**
   - Watch the workflow execution in real-time
   - Check that all steps complete successfully
   - Verify a PR is created with the expected changes

4. **Verify the PR:**
   - Check that both `requirements.txt` and `rust_snuba/Cargo.toml` are updated
   - Check that `rust_snuba/Cargo.lock` is updated
   - Review the commit message format
   - Test merge the PR to ensure it works

### Option 3: Dry Run Testing

You can also test the workflow logic without making actual changes by modifying the workflow temporarily:

1. Add `--dry-run` flags to git commands
2. Add `echo` before destructive operations
3. Use `git diff` instead of `git commit`

## 📋 Test Scenarios to Cover

### Package Name Variations
- ✅ `sentry-arroyo` (hyphen in Python, underscore in Rust)
- ✅ `sentry_usage_accountant` (underscore in both)
- ✅ Packages that exist in only one file
- ✅ Non-existent packages (should not break)

### Version Formats
- ✅ Specific versions: `2.28.4`
- ✅ Latest version: `latest` (pulls from PyPI)

### Dependency Formats in Cargo.toml
- ✅ Simple: `package = "version"`
- ✅ With features: `package = { version = "version", features = [...] }`

### Edge Cases
- ✅ Packages not found in either file
- ✅ Cargo update failures (falls back to full update)
- ✅ No changes detected (workflow should exit cleanly)

## 🔍 What to Look For

### Successful Test Results
- [ ] Python requirements.txt updated with correct version
- [ ] Rust Cargo.toml updated with correct version
- [ ] Rust Cargo.lock updated with resolved dependencies
- [ ] PR created with descriptive title and body
- [ ] Commit message follows expected format
- [ ] All CI checks pass on the PR

### Common Issues to Check
- [ ] Package name mismatches between Python and Rust
- [ ] Regex escaping problems with special characters
- [ ] Cargo update failures due to package name format
- [ ] Missing Rust toolchain in CI environment
- [ ] Git authentication issues
- [ ] PR creation failures

## 🛠️ Troubleshooting

### If the workflow fails:

1. **Check the logs:** Look at the GitHub Actions logs for specific error messages
2. **Test locally first:** Use the test script to verify logic works locally
3. **Check package names:** Ensure the package exists in the expected files
4. **Verify secrets:** Ensure `GETSENTRY_BOT_REVERT_TOKEN` is properly configured
5. **Check permissions:** Ensure the token has the right repository permissions

### Common fixes:
- Package not found: Check if it exists in requirements.txt or Cargo.toml
- Cargo update fails: The workflow will fall back to full cargo update
- Git push fails: Check token permissions and branch protection rules

## 📝 Example Test Run

```bash
$ ./test-bump-workflow.sh sentry-arroyo 3.0.0

🧪 Testing bump workflow logic for package: sentry-arroyo, version: 3.0.0
==================================================
📝 Original versions:
Python requirements:
sentry-arroyo==2.28.4
Rust Cargo.toml:
sentry_arroyo = { version = "2.28.4", features = ["ssl"] }

🔄 Applying version bump logic...
Generated regex pattern: sentry[_-]arroyo

📊 Results after update:
Python requirements:
sentry-arroyo==3.0.0
Rust Cargo.toml:
sentry_arroyo = { version = "3.0.0", features = ["ssl"] }

✅ cargo update with underscores works: sentry_arroyo
✅ Test completed!
```

This confirms the workflow logic works correctly for both Python and Rust dependency updates!
