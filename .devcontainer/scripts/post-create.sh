#!/bin/bash
# post-create.sh - One-time setup when container is created
set -e

echo "=== Snuba devcontainer post-create setup ==="

cd /workspace

# Fix ownership of mounted volumes (they may be created as root)
echo "Fixing volume permissions..."
sudo chown -R vscode:vscode /workspace/.venv /home/vscode/.cargo /home/vscode/.rustup /home/vscode/.cache/uv /commandhistory /home/vscode/.claude 2>/dev/null || true

# Mark workspace as safe for git (ownership differs between host and container)
git config --global --add safe.directory /workspace

# Create .venv if it doesn't exist (volume mount provides persistence)
if [ ! -f ".venv/bin/python" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv .venv
fi

# Activate venv for this script
source .venv/bin/activate

# Install Python dependencies
echo "Installing Python dependencies with uv..."
uv sync --frozen

# Set up Rust environment
echo "Setting up Rust environment..."

# Initialize cargo config if volume is empty
if [ ! -f "/home/vscode/.cargo/config.toml" ]; then
    echo "Initializing cargo config..."
    mkdir -p /home/vscode/.cargo
    cat > /home/vscode/.cargo/config.toml << 'EOF'
[net]
git-fetch-with-cli = true
[registries.crates-io]
protocol = "sparse"
EOF
fi

# Install rustup if not present (fresh volume)
if ! command -v rustup &> /dev/null; then
    echo "Installing rustup..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain none -y
    source "$HOME/.cargo/env"
fi

cd rust_snuba

# Ensure the toolchain from rust-toolchain.toml is installed
echo "Installing Rust toolchain..."
rustup show active-toolchain || rustup toolchain install

    # Build rust_snuba with maturin
    echo "Building rust_snuba with maturin..."

    # Set up PyO3 environment variables
    export SNUBA_TEST_PYTHONPATH="$(python -c 'import sys; print(":".join(sys.path))')"
    export SNUBA_TEST_PYTHONEXECUTABLE="$(python -c 'import sys; print(sys.executable)')"
    export PYTHONHOME="$(dirname $(dirname $(realpath "/workspace/.venv/bin/python")))"

    uvx maturin develop

cd /workspace

# Install pre-commit hooks
if [ -f ".pre-commit-config.yaml" ]; then
    echo "Installing pre-commit hooks..."
    .venv/bin/pre-commit install
fi

# Build admin UI if yarn is available
if command -v yarn &> /dev/null && [ -d "snuba/admin" ]; then
    echo "Building admin UI..."
    cd snuba/admin
    yarn install
    yarn run build
    cd /workspace
fi

echo "=== post-create setup complete ==="
