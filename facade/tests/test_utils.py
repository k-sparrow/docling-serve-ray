from facade.tests.fakes import FakeS3Client
from facade.utils import (
    build_export_document_response,
    build_zip_archive,
    fetch_artifacts_by_stem,
    parse_artifact_key,
)


class TestParseArtifactKey:
    def test_matches_md_extension_under_a_hash_directory(self):
        # Real Ray-orchestrator layout has no per-format subfolder -- format
        # comes from the leaf filename's extension. Regression test for the
        # bug caught during implementation (see project memory): a first
        # draft assumed a `{format}/{stem}.{ext}` folder structure that
        # doesn't actually exist, and failed silently (all-null fields).
        result = parse_artifact_key("out/req-1/", "out/req-1/abc123hash/35013.md")
        assert result == (".md", "md_content", "35013")

    def test_prefers_the_longer_doctags_suffix_over_plain_txt(self):
        result = parse_artifact_key("out/req-1/", "out/req-1/abc123hash/35013.doctags.txt")
        assert result == (".doctags.txt", "doctags_content", "35013")

    def test_returns_none_for_a_key_outside_the_prefix(self):
        assert parse_artifact_key("out/req-1/", "out/req-2/abc123hash/35013.md") is None

    def test_returns_none_for_an_unrecognized_extension(self):
        assert parse_artifact_key("out/req-1/", "out/req-1/abc123hash/35013.pdf") is None


class TestFetchArtifactsByStem:
    def test_groups_multiple_formats_for_the_same_document(self):
        s3 = FakeS3Client()
        s3.put_object(Bucket="out", Key="req-1/h/35013.md", Body=b"# hello")
        s3.put_object(Bucket="out", Key="req-1/h/35013.json", Body=b'{"a": 1}')
        s3.put_object(Bucket="out", Key="req-1/h/other.md", Body=b"# other doc")

        grouped = fetch_artifacts_by_stem(s3, bucket="out", prefix="req-1/")

        assert set(grouped.keys()) == {"35013", "other"}
        assert grouped["35013"]["md_content"] == (".md", b"# hello")
        assert grouped["35013"]["json_content"] == (".json", b'{"a": 1}')


class TestBuildExportDocumentResponse:
    def test_decodes_json_content_and_passes_through_text_content(self):
        doc = build_export_document_response(
            "35013.pdf",
            {"md_content": (".md", b"# hello"), "json_content": (".json", b'{"a": 1}')},
        )
        assert doc.filename == "35013.pdf"
        assert doc.md_content == "# hello"
        assert doc.json_content == {"a": 1}
        assert doc.html_content is None


class TestBuildZipArchive:
    def test_names_entries_by_stem_not_original_filename(self):
        import zipfile
        from io import BytesIO

        # Regression test for the naming bug caught during implementation:
        # first draft used the original uploaded filename ("35013.pdf") as
        # the zip entry stem, producing "35013.pdf.md" instead of matching
        # docling-serve's own native "35013.md" naming.
        data = build_zip_archive([("35013", {"md_content": (".md", b"content")})])
        with zipfile.ZipFile(BytesIO(data)) as zf:
            assert zf.namelist() == ["35013.md"]
            assert zf.read("35013.md") == b"content"
