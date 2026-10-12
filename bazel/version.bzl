"""The version of everything this repository releases (ADR 4).

One number for the server image, the Java artifacts and the other clients. On
main it stays at the development value; the release workflow rewrites this file
from the Git tag before it builds, and does not commit the change.
"""

VERSION = "0.0.0-dev"
