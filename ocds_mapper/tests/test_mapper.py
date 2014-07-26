import json
import mock
import ocds_mapper.mapper

def test_is_url_returns_true_for_urls():
    assert (
        ocds_mapper.mapper.is_url('http://example.com'),
        "Should be True but was False"
    )

def test_is_url_returns_false_for_file_paths():
    assert (
        not ocds_mapper.mapper.is_url('../data/canada/foo.csv'),
        "Should be False but was True"
    )

def test_process_creates_compatible_json_using_input_data_and_mapping():
    with mock.patch('uuid.uuid4', return_value='UUID'):
        result = ocds_mapper.mapper.process(
            'ocds_mapper/tests/test_data.csv',
            'ocds_mapper/tests/test_mapping.json',
            'John Doe', '2014-07-26')
    assert json.loads(result) == {
        "publisher": {"name": "John Doe"},
        "publishingMeta": {"date": "2014-07-26"},
        "releases": [
            {"releaseMeta": {
                "locale": "English", "ocid": "PW-$KIN-650-6155",
                "releaseID": "John Doe-2014-07-26-UUID"}},
            {"releaseMeta": {
                "locale": "English", "ocid": "PW-$VIC-242-6289",
                "releaseID": "John Doe-2014-07-26-UUID"}},
            {"releaseMeta": {
                "locale": "English", "ocid": "PW-$$XL-122-26346",
                "releaseID": "John Doe-2014-07-26-UUID"}}
        ]
    }
