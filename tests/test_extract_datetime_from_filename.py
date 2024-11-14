import pytest
from datetime import datetime

from main import extract_datetime_from_filename


@pytest.fixture(autouse=True)
def caplog(caplog):
    """Fixture to capture log output."""
    yield caplog


def test_extract_datetime_success(caplog):
    filename = "CHARGINGCDR_4008-BROCC3-20240901-234125-56908.ber"
    regex_pattern = r"CHARGINGCDR_.*-(\d{8})-.*.ber"
    datetime_format = "%Y%m%d"

    result = extract_datetime_from_filename(filename, regex_pattern, datetime_format)

    assert result == datetime(2024, 9, 1)
    assert "no match found" not in caplog.text
    assert "fail to convert date" not in caplog.text


def test_extract_datetime_success_multi_group(caplog):
    filename = "CHARGINGCDR_4008-BRCCNH-CCNCDR44-01-Blk0Blk-8421-20221231-100511-78.ccn"
    regex_pattern = r"^CHARGINGCDR_.*-(\d{8})-?(\d{2}).*"
    datetime_format = "%Y%m%d%H"

    result = extract_datetime_from_filename(filename, regex_pattern, datetime_format)

    assert result == datetime(2022, 12, 31, 10, 0)
    assert "no match found" not in caplog.text
    assert "fail to convert date" not in caplog.text


def test_extract_datetime_no_match(caplog):
    filename = "report_wrong_format.txt"
    regex_pattern = r"(\d{4})-(\d{2})-(\d{2})"
    datetime_format = "%Y%m%d"

    result = extract_datetime_from_filename(filename, regex_pattern, datetime_format)

    assert result is None
    assert "no match found" in caplog.text


def test_extract_datetime_value_error(caplog):
    filename = "report_2022-30-12.txt"
    regex_pattern = r"(\d{4})-(\d{2})-(\d{2})"
    datetime_format = "%Y%m%d"

    result = extract_datetime_from_filename(filename, regex_pattern, datetime_format)

    assert result is None
    assert "fail to convert date" in caplog.text


def test_extract_datetime_empty_groups(caplog):
    filename = "report_0000-00-00.txt"
    regex_pattern = r"(\d{4})-(\d{2})-(\d{2})"
    datetime_format = "%Y%m%d"

    result = extract_datetime_from_filename(filename, regex_pattern, datetime_format)

    assert result is None
    assert "fail to convert date" in caplog.text
