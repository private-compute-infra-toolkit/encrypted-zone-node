#!/bin/bash
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset

# --- Function Definitions ---

# Define character limits for commit messages.
SUBJECT_CHAR_LIMIT=72
BODY_WRAP_LIMIT=80

install_hook() {
  HOOK_DIR=$(git rev-parse --git-dir)/hooks
  HOOK_SOURCE="tools/commit_wrapper_hook/prepare-commit-msg-hook"
  HOOK_DEST="${HOOK_DIR}/prepare-commit-msg"

  if [[ -d "${HOOK_DIR}" ]]; then
    echo "Installing prepare-commit-msg hook to ${HOOK_DEST}"
    cp "${HOOK_SOURCE}" "${HOOK_DEST}"
    chmod +x "${HOOK_DEST}"
    echo "Installation successful."
  else
    echo "Error: .git/hooks directory not found. Are you in a git repository?" >&2
    exit 1
  fi
}

wrap_commit_message() {
  COMMIT_MSG_FILE="$1"

  # Read the first line (subject) and the rest of the message from the file.
  first_line=$(head -n 1 "${COMMIT_MSG_FILE}")
  rest_of_message=$(tail -n +2 "${COMMIT_MSG_FILE}")

  # Check if the first line (subject) exceeds 50 characters.
  if [[ ${#first_line} -gt "${SUBJECT_CHAR_LIMIT}" ]]; then
    echo "Error: Commit subject line exceeds ${SUBJECT_CHAR_LIMIT} characters." >&2
    echo "Please shorten the subject line to ${SUBJECT_CHAR_LIMIT} characters or less." >&2
    echo "Current length: ${#first_line}" >&2
    echo "Subject: \"${first_line}\"" >&2
    exit 1
  fi

  # Write the wrapped first line to a temporary file.
  printf "%s\n" "${first_line}" >"${COMMIT_MSG_FILE}.tmp"

  # Check if there is a body to the commit message.
  if [[ -n "${rest_of_message}" ]]; then
    # --- Body and Footer Processing ---

    # Find the line number of the first line that looks like a footer.
    # This is for special lines like "Change-Id:" or "Bug:" that should not be wrapped.
    # `grep -n` adds line numbers, `-m 1` stops after the first match.
    # `cut` extracts just the line number.
    first_footer_line_num=$(echo "${rest_of_message}" | grep -n -m 1 -E "^(Change-Id:|Bug:)" | cut -d: -f1)

    body_to_wrap=""
    footers_to_keep=""

    # If a footer was found, split the message into body and footers.
    if [[ -n "${first_footer_line_num}" ]]; then
      # The body is everything up to the line before the first footer.
      body_to_wrap=$(echo "${rest_of_message}" | head -n $((first_footer_line_num - 1)))
      # The footers are everything from the first footer line to the end.
      footers_to_keep=$(echo "${rest_of_message}" | tail -n +"${first_footer_line_num}")
    else
      # If no footers were found, the entire rest of the message is the body.
      body_to_wrap="${rest_of_message}"
    fi

    # Ensure there's a blank line between the subject and the body,
    # but only if a body exists.
    if [[ -n "${body_to_wrap}" ]]; then
      printf "\n" >>"${COMMIT_MSG_FILE}.tmp"
    fi

    # Wrap the body part of the message at 72 characters and append it.
    if [[ -n "${body_to_wrap}" ]]; then
      printf "%s\n" "${body_to_wrap}" | fold -s -w "${BODY_WRAP_LIMIT}" >>"${COMMIT_MSG_FILE}.tmp"
    fi

    # Append the footers without wrapping them.
    if [[ -n "${footers_to_keep}" ]]; then
      # Add a newline before the footers to separate them from the body,
      # but only if a body was present.
      if [[ -n "${body_to_wrap}" ]]; then
        printf "\n" >>"${COMMIT_MSG_FILE}.tmp"
      fi
      printf "%s\n" "${footers_to_keep}" >>"${COMMIT_MSG_FILE}.tmp"
    fi
  fi

  # Replace the original commit message file with the wrapped one.
  mv "${COMMIT_MSG_FILE}.tmp" "${COMMIT_MSG_FILE}"
}

# --- Main Script Logic ---

# Check the first argument to decide what to do.
case "$1" in
"install")
  install_hook
  ;;
"hook-impl")
  # Called by the git hook itself.
  # The commit message file is passed as the second argument ($2).
  wrap_commit_message "$2"
  ;;
*)
  echo "Usage: $0 {install|hook-impl}" >&2
  exit 1
  ;;
esac
