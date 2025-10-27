from __future__ import annotations

import struct
from pathlib import Path


def png_to_ico(png_path: Path, ico_path: Path) -> None:
    data = png_path.read_bytes()
    if data[:8] != b"\x89PNG\r\n\x1a\n":
        raise ValueError(f"{png_path} is not a valid PNG file")

    width, height = struct.unpack(">II", data[16:24])
    width_byte = width if 0 < width < 256 else 0
    height_byte = height if 0 < height < 256 else 0

    header = struct.pack("<HHH", 0, 1, 1)
    entry = struct.pack(
        "<BBBBHHII",
        width_byte,
        height_byte,
        0,
        0,
        1,
        32,
        len(data),
        6 + 16,
    )

    ico_path.write_bytes(header + entry + data)


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    png = root / "static" / "icons" / "bagholder.png"
    ico = root / "static" / "icons" / "bagholder.ico"
    png_to_ico(png, ico)
    print(f"Created {ico.relative_to(root)} from {png.relative_to(root)}")
