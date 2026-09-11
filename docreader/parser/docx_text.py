"""Keep secondary Word stories and text boxes omitted by HTML converters."""
import collections
import base64
import hashlib
import html
import io
import math
import re
import xml.etree.ElementTree as ET
import zipfile

_WORD = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
_ESCAPE = re.compile(r"\\([!\"#$%&'()*+,\-./:;<=>?@\[\\\]^_`{|}~])")


def _text(value: str) -> str:
    return "".join(c for c in html.unescape(value) if not c.isspace())


def _docx_svg_png(data: bytes) -> bytes:
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return data  # An existing reference can keep its original .svg name.
    source = data.decode("utf-8-sig")
    if len(data) > 10 << 20 or "<!DOCTYPE" in source.upper() or "<!ENTITY" in source.upper():
        raise ValueError("DOCX_SVG_UNSAFE")
    root = ET.fromstring(source)
    if root.tag != "{http://www.w3.org/2000/svg}svg":
        raise ValueError("DOCX_SVG_INVALID")
    for node in root.iter():
        if node.tag not in {"{http://www.w3.org/2000/svg}" + name for name in ("svg", "g", "path", "rect", "circle", "ellipse", "line", "polyline", "polygon")}:
            raise ValueError("DOCX_SVG_EXTERNAL_CONTENT")
        for name, value in node.attrib.items():
            if name.split("}")[-1] in ("href", "src", "base"):
                raise ValueError("DOCX_SVG_EXTERNAL_CONTENT")
        style = " ".join(node.attrib.values()) + (node.text or "")
        if "\\" in style or "@" in style or re.search(r"url\s*\(", style, re.I):
            raise ValueError("DOCX_SVG_EXTERNAL_CONTENT")
    viewbox = root.get("viewBox")
    if viewbox:
        dimensions = [float(v) for v in re.split(r"[ ,]+", viewbox.strip())]
        if len(dimensions) != 4 or not all(math.isfinite(v) for v in dimensions):
            raise ValueError("DOCX_SVG_DIMENSIONS_INVALID")
        width, height = dimensions[2:]
    else:
        width, height = [float(root.get(key, "0").removesuffix("px")) for key in ("width", "height")]
        root.set("viewBox", f"0 0 {width} {height}")
    if not all(math.isfinite(v) and 0 < v <= 1e9 for v in (width, height)):
        raise ValueError("DOCX_SVG_DIMENSIONS_INVALID")
    scale = min(1, 4096 / max(width, height))
    root.set("width", str(max(1, round(width * scale))))
    root.set("height", str(max(1, round(height * scale))))
    # Reuse the installed converter, after rejecting external resources and
    # bounding raster dimensions. The encrypted DOCX keeps the original SVG.
    from docreader.parser.pptx_media import rasterize_media_bytes
    result = rasterize_media_bytes("source.svg", ET.tostring(root))
    if not result or not result.startswith(b"\x89PNG\r\n\x1a\n"):
        raise ValueError("DOCX_SVG_RASTERIZATION_FAILED")
    # ImageMagick adds timestamps; omit them so repeated parsing deduplicates.
    from PIL import Image
    with Image.open(io.BytesIO(result)) as image:
        output = io.BytesIO()
        image.save(output, format="PNG")
        return output.getvalue()


def complete_docx_stories(content: bytes, markdown: str, images: dict[str, str] | None = None) -> str:
    expected = collections.Counter()
    paragraphs = []
    for key, value in (images or {}).items():
        if key.lower().endswith(".svg"):
            images[key] = base64.b64encode(_docx_svg_png(base64.b64decode(value, validate=True))).decode()
    present_images = collections.Counter(hashlib.sha256(base64.b64decode(value, validate=True)).hexdigest() for value in (images or {}).values())
    image_links = []
    expanded = 0
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        if len(archive.infolist()) > 10000:
            raise ValueError("DOCX_ARCHIVE_ENTRY_LIMIT")
        names = set()
        for entry in archive.infolist():
            if entry.filename in names:
                raise ValueError("DOCX_ARCHIVE_PATH_INVALID")
            names.add(entry.filename)
            is_media = entry.filename.startswith("word/media/") and not entry.is_dir()
            if not is_media and (not entry.filename.startswith("word/") or not entry.filename.endswith(".xml")):
                continue
            limit = (200 << 20) - expanded
            if entry.file_size > limit:
                raise ValueError("DOCX_EXPANDED_SIZE_EXCEEDED")
            with archive.open(entry) as stream:
                body = stream.read(limit + 1)
            expanded += len(body)
            if expanded > 200 << 20:
                raise ValueError("DOCX_EXPANDED_SIZE_EXCEEDED")
            if is_media:
                if images is not None and body:
                    if entry.filename.lower().endswith(".svg"):
                        body = _docx_svg_png(body)
                    digest = hashlib.sha256(body).hexdigest()
                    if present_images[digest]:
                        present_images[digest] -= 1
                    else:
                        suffix = entry.filename.rsplit(".", 1)[-1].lower()
                        if not re.fullmatch(r"[a-z0-9]{1,10}", suffix):
                            raise ValueError("DOCX_IMAGE_FORMAT_INVALID")
                        key = "images/docx-" + hashlib.sha256((entry.filename + digest).encode()).hexdigest()[:24] + "." + suffix
                        images[key] = base64.b64encode(body).decode()
                        image_links.append(f"![image]({key})")
                continue
            root = ET.fromstring(body)
            expected.update(_text(node.text or "") for node in root.iter(_WORD + "t") if _text(node.text or ""))
            parents = {child: parent for parent in root.iter() for child in parent}
            for paragraph in root.iter(_WORD + "p"):
                ancestor = parents.get(paragraph)
                in_textbox = False
                while ancestor is not None:
                    in_textbox |= ancestor.tag == _WORD + "txbxContent"
                    ancestor = parents.get(ancestor)
                if entry.filename == "word/document.xml" and not in_textbox:
                    continue
                # Nested text-box paragraphs own their text; never duplicate it
                # inside the outer paragraph that contains the drawing.
                runs = []
                for node in paragraph.iter(_WORD + "t"):
                    owner = parents.get(node)
                    while owner is not None and owner.tag != _WORD + "p":
                        owner = parents.get(owner)
                    if owner is paragraph:
                        runs.append(node.text or "")
                if runs:
                    paragraphs.append(runs)

    coverage = _text(_ESCAPE.sub(r"\1", markdown))
    additions = []
    for runs in paragraphs:
        if any(coverage.count(value) < expected[value] for run in runs if (value := _text(run))):
            paragraph = "".join(runs)
            # ponytail: secondary stories follow the main body; preserve source
            # paragraph order here, add spatial anchors only for layout export.
            additions.append(re.sub(r"([!\"#$%&'()*+,\-./:;<=>?@\[\\\]^_`{|}~])", r"\\\1", paragraph))
            coverage += _text(paragraph)
    additions.extend(image_links)
    return markdown + ("\n\n" + "\n\n".join(additions) if additions else "")
