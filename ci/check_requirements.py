"""Check the repository's pinned requirements without invoking pip or the network."""

import importlib.metadata
import sys
from pathlib import Path


def mismatches(requirements):
    result = []
    raw = Path(requirements).read_bytes()
    encoding = "utf-16" if raw.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8-sig"
    for line in raw.decode(encoding).splitlines():
        pin = line.partition("#")[0].strip()
        if not pin:
            continue
        name, separator, expected = pin.partition("==")
        if not separator or not expected:
            raise ValueError("CI requirements must contain exact version pins")
        try:
            actual = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            actual = None
        if actual != expected:
            result.append(name)
    return result


if __name__ == "__main__":
    missing = mismatches(sys.argv[1])
    print("Requirements:", "conformant" if not missing else ", ".join(missing))
    sys.exit(bool(missing))
