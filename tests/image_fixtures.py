"""Small, distinct PNG fixtures shared by the real CLI and mock assertions."""

import base64
import struct
import zlib


def png_bytes(rgb):
    def chunk(kind, data):
        return (
            struct.pack(">I", len(data))
            + kind
            + data
            + struct.pack(">I", zlib.crc32(kind + data))
        )

    width = height = 64
    pixels = (b"\0" + bytes(rgb) * width) * height
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(pixels))
        + chunk(b"IEND", b"")
    )


IMAGES = [png_bytes((255, 0, 0)), png_bytes((0, 0, 255))]


def image_blocks(count):
    blocks = [{"type": "text", "text": "IMAGE_E2E_BEGIN"}]
    for index, data in enumerate(IMAGES[:count]):
        blocks.append(
            {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/png",
                    "data": base64.b64encode(data).decode(),
                },
            }
        )
        blocks.append(
            {
                "type": "text",
                "text": "IMAGE_E2E_BETWEEN"
                if index + 1 < count
                else f"IMAGE_E2E_END count={count}. Reply exactly IMAGE_OK_{count}.",
            }
        )
    return blocks
