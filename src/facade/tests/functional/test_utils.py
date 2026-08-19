from facade.tests.functional.fakes import FakeS3Client
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
        result = parse_artifact_key(
            "out/req-1/", "out/req-1/abc123hash/35013.doctags.txt"
        )
        assert result == (".doctags.txt", "doctags_content", "35013")

    def test_returns_none_for_a_key_outside_the_prefix(self):
        assert parse_artifact_key("out/req-1/", "out/req-2/abc123hash/35013.md") is None

    def test_returns_none_for_an_unrecognized_extension(self):
        assert (
            parse_artifact_key("out/req-1/", "out/req-1/abc123hash/35013.pdf") is None
        )

    def test_works_with_zero_directory_nesting(self):
        # The hash directory the Ray orchestrator inserts isn't guaranteed by
        # anything we control -- format is derived purely from the leaf
        # filename's extension, so this must also work with no intermediate
        # directory at all between the prefix and the file.
        result = parse_artifact_key("out/req-1/", "out/req-1/35013.md")
        assert result == (".md", "md_content", "35013")

    def test_returns_none_for_a_filename_with_no_extension(self):
        assert parse_artifact_key("out/req-1/", "out/req-1/abc123hash/README") is None


class TestFetchArtifactsByStem:
    async def test_groups_multiple_formats_for_the_same_document(self):
        s3 = FakeS3Client()
        s3.put_object(Bucket="out", Key="req-1/h/35013.md", Body=b"# hello")
        s3.put_object(Bucket="out", Key="req-1/h/35013.json", Body=b'{"a": 1}')
        s3.put_object(Bucket="out", Key="req-1/h/other.md", Body=b"# other doc")

        grouped = await fetch_artifacts_by_stem(s3, bucket="out", prefix="req-1/")

        assert set(grouped.keys()) == {"35013", "other"}
        assert grouped["35013"]["md_content"] == (".md", b"# hello")
        assert grouped["35013"]["json_content"] == (".json", b'{"a": 1}')

    async def test_returns_empty_for_a_prefix_with_nothing_under_it(self):
        s3 = FakeS3Client()
        assert await fetch_artifacts_by_stem(s3, bucket="out", prefix="req-1/") == {}


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

    def test_builds_a_bare_response_from_no_artifacts(self):
        doc = build_export_document_response("35013.pdf", {})
        assert doc.filename == "35013.pdf"
        assert doc.md_content is None
        assert doc.json_content is None


class TestBuildZipArchive:
    def test_names_entries_by_stem_not_original_filename(self):
        import zipfile
        from io import BytesIO

        # Regression test for the naming bug caught during implementation:
        # first draft used the original uploaded filename ("35013.pdf") as
        # the zip entry stem, producing "35013.pdf.md" instead of matching
        # docling-serve's own native "35013.md" naming.
        result = build_zip_archive(
            [("35013.pdf", "35013", {"md_content": (".md", b"content")})]
        )
        with zipfile.ZipFile(BytesIO(result.content)) as zf:
            assert "35013.md" in zf.namelist()
            assert zf.read("35013.md") == b"content"

    def test_multiple_documents_each_get_their_own_entries(self):
        import zipfile
        from io import BytesIO

        result = build_zip_archive(
            [
                ("a.pdf", "a", {"md_content": (".md", b"doc a")}),
                ("b.pdf", "b", {"md_content": (".md", b"doc b")}),
            ]
        )
        with zipfile.ZipFile(BytesIO(result.content)) as zf:
            assert zf.read("a.md") == b"doc a"
            assert zf.read("b.md") == b"doc b"
        assert [s.status for s in result.document_statuses] == ["success", "success"]
        assert result.has_failures is False

    def test_a_document_with_no_artifacts_is_marked_failed_and_gets_no_entries(self):
        # A document can individually fail conversion while its siblings in
        # the same batch succeed -- native docling-serve's own zip response
        # has no way to express this at all (ZipArchiveResult is raw bytes,
        # nothing else); this manifest is a facade-only addition, not
        # something a native client would already be relying on.
        import zipfile
        from io import BytesIO

        result = build_zip_archive(
            [
                ("a.pdf", "a", {"md_content": (".md", b"doc a")}),
                ("b.pdf", "b", {}),
            ]
        )
        with zipfile.ZipFile(BytesIO(result.content)) as zf:
            names = zf.namelist()
        assert "a.md" in names
        assert not any(n.startswith("b.") for n in names)
        statuses = {s.filename: s.status for s in result.document_statuses}
        assert statuses == {"a.pdf": "success", "b.pdf": "failed"}
        assert result.has_failures is True

    def test_status_manifest_is_included_in_the_zip(self):
        import json
        import zipfile
        from io import BytesIO

        result = build_zip_archive([("a.pdf", "a", {"md_content": (".md", b"doc a")})])
        with zipfile.ZipFile(BytesIO(result.content)) as zf:
            manifest = json.loads(zf.read("_status.json"))
        assert manifest == {"documents": [{"filename": "a.pdf", "status": "success"}]}

    def test_empty_document_list_produces_a_valid_empty_zip(self):
        import zipfile
        from io import BytesIO

        result = build_zip_archive([])
        with zipfile.ZipFile(BytesIO(result.content)) as zf:
            assert zf.namelist() == ["_status.json"]
        assert result.document_statuses == []
        assert result.has_failures is False
