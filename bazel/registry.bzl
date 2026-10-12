"""Where odradek's image is published: ONE setting.

The image is `<IMAGE_REGISTRY>/<IMAGE_NAMESPACE>/odradek`, i.e.
`quay.io/odradek/odradek`. Quay has no nested repositories: everything is
`<host>/<namespace>/<name>`. The workflows read these two assignments, so keep
each on ONE line, exactly `NAME = "value"`.
"""

# Registry host.
IMAGE_REGISTRY = "quay.io"

# Organization under the host.
IMAGE_NAMESPACE = "odradek"

# Where cosign's keyless signature lands: "referrer" (an OCI 1.1 referrer and no
# tag, as quay.io serves) or "tag" (cosign 3's sha256-<hex> fallback tag, as
# ghcr.io needs). Asserted by //bazel/cosign:verify_image.
SIGNATURE_LAYOUT = "referrer"

def image_repository(name):
    """Repository path (no host) of an image.

    Args:
      name: image name, "odradek".

    Returns:
      e.g. "odradek/odradek".
    """
    return "{}/{}".format(IMAGE_NAMESPACE, name)
