#!/bin/bash
set -euo pipefail

# Test script to simulate the bump-version workflow logic
# Usage: ./test-bump-workflow.sh <package> <version>

PACKAGE="${1:-sentry-arroyo}"
VERSION="${2:-2.29.0}"

echo "🧪 Testing bump workflow logic for package: $PACKAGE, version: $VERSION"
echo "=================================================="

# Create temporary copies of files to test against
cp requirements.txt requirements.txt.test
cp rust_snuba/Cargo.toml rust_snuba/Cargo.toml.test

echo "📝 Original versions:"
echo "Python requirements:"
grep -E "^$PACKAGE" requirements.txt.test || echo "  Package not found in requirements.txt"

echo "Rust Cargo.toml:"
cd rust_snuba
grep -E "^$PACKAGE" Cargo.toml.test || echo "  Package not found in Cargo.toml"
cd ..

echo ""
echo "🔄 Applying version bump logic..."

# Simulate the workflow logic
re="$(sed 's/[_-]/[_-]/g' <<< "$PACKAGE")"
echo "Generated regex pattern: $re"

# Update Python requirements
echo "Updating Python requirements..."
sed -i.bak "s/^\($re\)==.*/\1==$VERSION/g" requirements.txt.test

# Update Cargo.toml dependencies
echo "Updating Cargo.toml..."
sed -i.bak "s/^\($re\) = \"[^\"]*\"/\1 = \"$VERSION\"/g" rust_snuba/Cargo.toml.test
sed -i.bak2 "s/^\($re\) = { version = \"[^\"]*\"/\1 = { version = \"$VERSION\"/g" rust_snuba/Cargo.toml.test

echo ""
echo "📊 Results after update:"
echo "Python requirements:"
grep -E "^$re" requirements.txt.test || echo "  No matches found"

echo "Rust Cargo.toml:"
grep -E "^$re" rust_snuba/Cargo.toml.test || echo "  No matches found"

echo ""
echo "🔍 Checking for differences..."
echo "Python requirements diff:"
diff requirements.txt requirements.txt.test || echo "  No changes made"

echo ""
echo "Rust Cargo.toml diff:"
diff rust_snuba/Cargo.toml rust_snuba/Cargo.toml.test || echo "  No changes made"

# Test Cargo.lock update logic
echo ""
echo "🦀 Testing Cargo update logic..."
cd rust_snuba

# Check if Cargo.toml was modified
if ! diff Cargo.toml Cargo.toml.test > /dev/null 2>&1; then
    echo "Cargo.toml was modified, testing cargo update commands..."

    # Try updating with underscores (cargo prefers underscores in package names)
    CARGO_PACKAGE="$(echo "$PACKAGE" | sed 's/-/_/g')"
    echo "Testing: cargo update --dry-run --package \"$CARGO_PACKAGE\""

    if cargo update --dry-run --package "$CARGO_PACKAGE" 2>/dev/null; then
        echo "✅ cargo update with underscores works: $CARGO_PACKAGE"
    else
        echo "❌ cargo update with underscores failed: $CARGO_PACKAGE"

        echo "Testing: cargo update --dry-run --package \"$PACKAGE\""
        if cargo update --dry-run --package "$PACKAGE" 2>/dev/null; then
            echo "✅ cargo update with original name works: $PACKAGE"
        else
            echo "❌ cargo update with original name failed: $PACKAGE"
            echo "⚠️  Would fall back to full cargo update"
        fi
    fi
else
    echo "No changes to Cargo.toml, skipping cargo update test"
fi

cd ..

echo ""
echo "🧹 Cleaning up test files..."
rm -f requirements.txt.test requirements.txt.test.bak
rm -f rust_snuba/Cargo.toml.test rust_snuba/Cargo.toml.test.bak rust_snuba/Cargo.toml.test.bak2

echo "✅ Test completed!"
