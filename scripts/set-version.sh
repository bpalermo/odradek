#!/usr/bin/env bash
# Sets the version of everything this repository releases (ADR 4): one number
# for the server image, the Java artifacts and the Clojure and Python clients.
#
#   scripts/set-version.sh 1.2.3
#
# The release workflow runs it on a fresh checkout before building and never
# commits the result. On main every file below stays at 0.0.0-dev, and
# //scripts:version_test fails if they disagree.
set -euo pipefail

version="${1:?usage: set-version.sh X.Y.Z}"
if ! [[ "${version}" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
	echo "not a release version (X.Y.Z): ${version}" >&2
	exit 1
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# replace FILE SED_EXPRESSION: applies it and fails if nothing changed, so a file
# that moved or changed shape is noticed here and not after publishing.
replace() {
	local file="${root}/$1" before
	before="$(cat "${file}")"
	sed -i -E "$2" "${file}"
	if [ "${before}" = "$(cat "${file}")" ]; then
		echo "no version found to replace in $1" >&2
		exit 1
	fi
}

replace bazel/version.bzl "s/^VERSION = \"[^\"]+\"$/VERSION = \"${version}\"/"
replace client/clojure/src/odradek/client/version.edn "s/\\{:version \"[^\"]+\"\\}/{:version \"${version}\"}/"
# The Clojure client's pom depends on the Java artifact of the same release.
replace client/clojure/deps.edn "s|(io\\.github\\.bpalermo/odradek-client \\{:mvn/version )\"[^\"]+\"|\\1\"${version}\"|"

# What the Python client tells the server. Its wheel takes the version from
# bazel/version.bzl.
replace client/python/odradek/_stream.py "s/^VERSION = \"[^\"]+\"$/VERSION = \"${version}\"/"

echo "version set to ${version}"
