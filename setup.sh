#!/usr/bin/env bash

# This script installs Rye and updates the shell configuration file.
# It also initializes current project and syncs the dependencies.
# It is intended to be run on a new system to quickly set up Rye.
# It is recommended to review the script before running it.

export SHELL_RC=$(echo "$HOME/.${SHELL##*/}rc")

if ! command -v cargo &>/dev/null; then
    echo "Cargo is not installed. Installing..."
    curl https://sh.rustup.rs -sSf | sh
    echo "Updating $SHELL_RC"
    echo "source $HOME/.cargo/bin" >>$SHELL_RC
else
    echo "Cargo is installed."
fi
source $SHELL_RC

if ! command -v rye &>/dev/null; then
    echo "Rye is not installed. Installing..."
    cargo install --git https://github.com/astral-sh/rye --tag 0.38.0 rye
    echo "Updating $SHELL_RC"
    echo "source $HOME/.rye/env" >>$SHELL_RC
else
    echo "Rye is already installed."
fi

if ! command -v uv &>/dev/null; then
    echo "uv is not installed. Installing..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
else
    echo "uv is already installed."
fi
source $SHELL_RC
which rye
echo $PATH
cat $HOME/.rye/env
rye sync
rye run pre-commit install --hook-type commit-msg --hook-type pre-commit

echo "Done"
