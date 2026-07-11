import pytest
from pathlib import Path
from config import MergerConfig
from core.deduplicator import SQLiteDeduplicator
from core.parser import XMLFeedParser
from core.writer import XMLStreamWriter
from core.validator import XMLValidator
from core.merger import FeedMerger


@pytest.fixture
def temp_dir(tmp_path) -> Path:
    return tmp_path


def test_sqlite_deduplicator(temp_dir):
    db_path = temp_dir / "test_dupes.db"
    fields = ["title", "company"]
    
    with SQLiteDeduplicator(db_path, fields) as dedupe:
        job1 = {"title": "Software Engineer", "company": "Acme", "description": "Good job"}
        job2 = {"title": "Software Engineer", "company": "Acme", "description": "Different desc"}
        job3 = {"title": "Data Scientist", "company": "Acme", "description": "Another job"}

        assert not dedupe.seen(job1)
        # Duplicate checks of title/company matches
        assert dedupe.seen(job2)
        assert not dedupe.seen(job3)


def test_xml_stream_writer(temp_dir):
    output_path = temp_dir / "output.xml"
    with XMLStreamWriter(output_path) as writer:
        writer.write_element({"title": "Job 1", "description": "Details"})
        writer.write_element({"title": "Job 2", "description": "Details 2"})

    content = output_path.read_text(encoding="utf-8")
    assert "<source>" in content
    assert "<job>" in content
    assert "<title><![CDATA[Job 1]]></title>" in content
    assert "</source>" in content


def test_xml_validator(temp_dir):
    valid_path = temp_dir / "valid.xml"
    valid_path.write_text("<source><job><title>Test</title></job></source>", encoding="utf-8")
    
    invalid_path = temp_dir / "invalid.xml"
    invalid_path.write_text("<source><job><title>Test</title></job>", encoding="utf-8")

    validator = XMLValidator()
    assert validator.validate_file(valid_path)
    
    with pytest.raises(Exception):
        validator.validate_file(invalid_path)


def test_parser_and_release(temp_dir):
    xml_path = temp_dir / "feed.xml"
    xml_path.write_text(
        "<source><job><title>Job 1</title><company>C1</company></job>"
        "<job><title>Job 2</title><company>C2</company></job></source>",
        encoding="utf-8"
    )
    
    config = MergerConfig()
    parser = XMLFeedParser(config)
    jobs = list(parser.iter_jobs(xml_path))

    assert len(jobs) == 2
    assert jobs[0]["title"] == "Job 1"
    assert jobs[0]["company"] == "C1"
    assert jobs[1]["title"] == "Job 2"
    assert jobs[1]["company"] == "C2"


@pytest.mark.anyio
async def test_feed_merger_run(temp_dir):
    # Setup files
    feed_path = temp_dir / "sample_feed.xml"
    output_file = temp_dir / "merged_output.xml"
    db_path = temp_dir / "dupes.db"

    feed_path.write_text(
        "<source><job><title>Python Dev</title><company>Google</company></job></source>",
        encoding="utf-8"
    )

    config = MergerConfig(
        output_file=output_file,
        duplicate_db=db_path,
        reset_duplicate_db=True,
    )
    
    merger = FeedMerger(config)
    await merger.run([str(feed_path)])

    assert output_file.exists()
    assert db_path.exists()
    content = output_file.read_text(encoding="utf-8")
    assert "Python Dev" in content
