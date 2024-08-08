#!/usr/bin/env bash

# This script installs Rye and updates the shell configuration file.
# It also initializes current project and syncs the dependencies.
# It is intended to be run on a new system to quickly set up Rye.
# It is recommended to review the script before running it.


export SHELL_ZSH=$(echo "$HOME/.${SHELL##*/}rc")

if ! command -v rye &> /dev/null; then
	echo "Rye is not installed. Installing..."
	curl -sSf https://rye.astral.sh/get | bash
	echo "Updating $SHELL_ZSH"
	echo "source $HOME/.rye/env" >> $SHELL_ZSH
else
	echo "Rye is already installed."
fi

if ! command -v uv &> /dev/null; then
	echo "uv is not installed. Installing..."
	curl -LsSf https://astral.sh/uv/install.sh | sh
else
	echo "uv is already installed."
fi
rye sync
echo "Done"
