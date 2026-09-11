import io
import base64
import unittest
import zipfile

from docreader.parser.docx_text import complete_docx_stories, _docx_svg_png


class DOCXStoriesTest(unittest.TestCase):
    def test_svg_rasterization_bounds_pixels_and_rejects_external_resources(self):
        from PIL import Image
        svg = b'<svg xmlns="http://www.w3.org/2000/svg" width="8192" height="4096" viewBox="0 0 8192 4096"><path fill="red" d="M0 0H8192V4096H0Z"/></svg>'
        png = _docx_svg_png(svg)
        with Image.open(io.BytesIO(png)) as image:
            self.assertEqual(image.size, (4096, 2048))
            self.assertEqual(image.convert("RGB").getpixel((2048,1024)), (255,0,0))
        self.assertEqual(_docx_svg_png(png), png)
        source = io.BytesIO()
        with zipfile.ZipFile(source, "w") as archive:
            archive.writestr("word/media/vector.svg", svg)
        images = {}
        markdown = complete_docx_stories(source.getvalue(), "", images)
        self.assertEqual(complete_docx_stories(source.getvalue(), markdown, images), markdown)
        self.assertEqual(len(images), 1)
        for body in [b'<image href="file:///etc/passwd"/>', b'<path style="fill:url(https://example.invalid/image)"/>', b'<style>@import "remote"</style>', b'<path style="fill:u\\72l(remote)"/>']:
            with self.assertRaises(ValueError):
                _docx_svg_png(b'<svg xmlns="http://www.w3.org/2000/svg" width="1" height="1">'+body+b'</svg>')

    def test_secondary_stories_textboxes_and_duplicates(self):
        word = 'xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"'
        paragraph = lambda text: f"<w:p><w:r><w:t>{text}</w:t></w:r></w:p>"
        source = io.BytesIO()
        with zipfile.ZipFile(source, "w") as archive:
            archive.writestr("word/document.xml", f'<w:document {word}><w:body>{paragraph("Main unchanged")}<w:p><w:r><w:drawing><w:txbxContent>{paragraph("Box [literal] * text")}</w:txbxContent></w:drawing></w:r></w:p></w:body></w:document>')
            archive.writestr("word/footer1.xml", f'<w:ftr {word}>{paragraph("Footer repeat")}</w:ftr>')
            archive.writestr("word/footer2.xml", f'<w:ftr {word}>{paragraph("Footer repeat")}</w:ftr>')
            archive.writestr("word/footnotes.xml", f'<w:footnotes {word}><w:footnote>{paragraph("Footnote retained")}</w:footnote></w:footnotes>')
        original = "Main unchanged\n\n![image](images/existing.png)"
        result = complete_docx_stories(source.getvalue(), original)
        self.assertTrue(result.startswith(original))
        self.assertIn(r"Box \[literal\] \* text", result)
        self.assertEqual(result.count("Footer repeat"), 2)
        self.assertIn("Footnote retained", result)
        self.assertEqual(complete_docx_stories(source.getvalue(), result), result)
        # Missing ordinary body text must still fail the Go coverage check;
        # this repair must not manufacture a fallback transcript for all text.
        self.assertNotIn("Main unchanged", complete_docx_stories(source.getvalue(), ""))

    def test_missing_images_keep_original_bytes_without_duplicating_existing_images(self):
        original, missing = b"original-image-bytes", b"footer-image-bytes"
        source = io.BytesIO()
        with zipfile.ZipFile(source, "w") as archive:
            archive.writestr("word/media/one.png", original)
            archive.writestr("word/media/two.png", missing)
            archive.writestr("word/media/three.png", missing)
        images = {"images/kept.png": base64.b64encode(original).decode()}
        result = complete_docx_stories(source.getvalue(), "![kept](images/kept.png)", images)
        self.assertEqual(len(images), 3)
        self.assertEqual(result.count("![image]"), 2)
        self.assertEqual(complete_docx_stories(source.getvalue(), result, images), result)
        self.assertEqual(sorted(base64.b64decode(v) for v in images.values()), sorted([original, missing, missing]))


if __name__ == "__main__":
    unittest.main()
