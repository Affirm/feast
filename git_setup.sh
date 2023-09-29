#!/bin/bash

# Ensure necessary packages installed
if [ "$(command -v git)" ]; then
  git_version="$(git --version)"
  echo "Git is installed. Version: $git_version" && echo
else
  echo "Git is missing. Please install it."
  exit 1
fi


# Collect user information
GITHUB_PAT=""
GITHUB_EMAIL=""
GITHUB_USERNAME=""

read -rp "Enter your Github Personal Access Token: " GITHUB_PAT

read -rp "Enter your Github email address: " GITHUB_EMAIL

read -rp "Enter your Github username: " GITHUB_USERNAME


# Run git setup commands
git config --global user.email "$GITHUB_EMAIL"
git config --global user.name "$GITHUB_USERNAME"

git config --global credential.helper 'store --file ~/.git-credentials'

git credential approve <<<"protocol=https
host=github.com
username=token
password=$GITHUB_PAT"

echo && echo "Git has been successfully setup!"