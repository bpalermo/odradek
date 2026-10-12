#!/usr/bin/env bash
# Every file that carries the release version agrees, and scripts/set-version.sh
# changes all of them. A client added without teaching set-version.sh about its
# version file would be released under the wrong number.
set -euo pipefail

files=(
	bazel/version.bzl
	client/clojure/src/odradek/client/version.edn
	client/clojure/deps.edn
	client/python/odradek/_stream.py
)

work="${TEST_TMPDIR}/repo"
mkdir -p "${work}/scripts"
cp scripts/set-version.sh "${work}/scripts/"
for file in "${files[@]}"; do
	mkdir -p "${work}/$(dirname "${file}")"
	cp "${file}" "${work}/${file}"
done

# 0.0.0-dev on main; the release version in the release workflow's own test run.
current="$(sed -nE 's/^VERSION = "([^"]+)"$/\1/p' "${work}/bazel/version.bzl")"
if [ -z "${current}" ]; then
	echo "could not read VERSION from bazel/version.bzl" >&2
	exit 1
fi
for file in "${files[@]}"; do
	if ! grep -qF "\"${current}\"" "${work}/${file}"; then
		echo "${file} does not carry the version ${current}" >&2
		exit 1
	fi
done

"${work}/scripts/set-version.sh" 12.34.56

for file in "${files[@]}"; do
	if ! grep -qF '"12.34.56"' "${work}/${file}" || grep -qF "\"${current}\"" "${work}/${file}"; then
		echo "set-version.sh did not set the version in ${file}" >&2
		exit 1
	fi
done

for bad in v1.2.3 1.2 1.2.3-rc1 01.2.3 ''; do
	if "${work}/scripts/set-version.sh" "${bad}" >/dev/null 2>&1; then
		echo "set-version.sh accepted '${bad}'" >&2
		exit 1
	fi
done
echo ok
