#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Decrypt the file
# https://docs.github.com/en/actions/reference/encrypted-secrets#limits-for-secrets
mkdir $HOME/secrets
# --batch to prevent interactive command
# --yes to assume "yes" for questions
gpg --quiet --batch --yes --decrypt --passphrase="$GCLOUD_KEY_PASSPHRASE" \
  --output "$HOME/secrets/gcloud-service-key.json" \
  "$DIR/gcloud-service-key.json.gpg"
