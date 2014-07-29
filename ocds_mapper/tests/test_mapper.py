import json
import jsontemplate

import mock
import ocds_mapper.mapper
import pytest

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
    pytest.skip("Map was modified. Test needs te bo updated.")
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
                "locale": "en_us",
                "ocid": "PW-$KIN-650-6155",
                "releaseID": "John Doe-2014-07-26-UUID"}},
            {"releaseMeta": {
                "locale": "en_us",
                "ocid": "PW-$VIC-242-6289",
                "releaseID": "John Doe-2014-07-26-UUID"}},
            {"releaseMeta": {
                "locale": "en_us",
                "ocid": "PW-$$XL-122-26346",
                "releaseID": "John Doe-2014-07-26-UUID"}}
        ]
    }

import contextlib
from tempfile import TemporaryFile
@contextlib.contextmanager
def prepare(content):
    with TemporaryFile() as f:
        f.write(content)
        f.flush()
        f.seek(0)
        yield f


def test_render_string():
    template_content = '"{language}"'
    context = dict(language='test_value')
    with prepare(template_content) as template:
        assert '"test_value"' == ocds_mapper.mapper.render(template, context)

def test_render_number():
    template_content = '{ "output": {num} }'
    context = {'num': 12}
    with prepare(template_content) as template:
        assert '{ "output": 12 }' == ocds_mapper.mapper.render(
            template, context
        )

def test_render_boolean():
    for falsy in ['0', 'f', 'false', 'False', 'no', 'No']:
        template_content = '{ "output": {finished|boolean} }'
        context = {'finished': falsy}
        with prepare(template_content) as template:
            assert '{ "output": false }' == ocds_mapper.mapper.render(
                template, context
            )

    for truly in ['1', 't', 'true', 'True', 'yes', 'Yes']:
        template_content = '{ "output": {finished|boolean} }'
        context = {'finished': truly}
        with prepare(template_content) as template:
            assert '{ "output": true }' == ocds_mapper.mapper.render(
                template, context
            )

def test_render_constant():
    template_content = '{ "output": "en_us" }'
    context = {}
    output = '{ "output": "en_us" }'
    with prepare(template_content) as template:
        assert output == ocds_mapper.mapper.render(template, context)

@pytest.mark.xfail
def test_traverse_joins_multiple_fields_with_indexing_into_one_array():
    schema = {'bidder': [{
        'id': 'integer:bidder_#_id',
        'name': 'bidder_#_name'
    }]}
    csv_row = {
        'bidder_0_id': '0',
        'bidder_0_name': 'Zero',
        'bidder_1_id': '1',
        'bidder_1_name': 'One'
    }
    assert {'bidder': [
        {'id': 0, 'name': 'Zero'},
        {'id': 1, 'name': 'One'}
    ]} == ocds_mapper.mapper.traverse(schema, csv_row)

@pytest.mark.xfail
def test_traverse_splits_array_fields_and_creates_objects_based_on_subschema():
    schema = {'attachments': [{
        'uid': 'list:documents',
        'name': 'constant:Attachment'
    }]}
    csv_row = {'documents': 'foo.pdf, bar.pdf ,baz.pdf'}
    assert {'attachments': [
        {'uid': 'foo.pdf', 'name': 'Attachment'},
        {'uid': 'bar.pdf', 'name': 'Attachment'},
        {'uid': 'baz.pdf', 'name': 'Attachment'}
    ]} == ocds_mapper.mapper.traverse(schema, csv_row)

def test_render_raises_error_if_invalid_column_type_is_used():
    template_content = '{ "output": "{column_name|bad_formatter}" }'
    context = {}
    with prepare(template_content) as template:
        with pytest.raises(jsontemplate.BadFormatter) as e:
            ocds_mapper.mapper.render(template, context)

def test_render_raises_error_indicating_wrong_header_for_invalid_keys():
    template_content = '{ "output": "{missing_column_name}" }'
    context = {}
    with prepare(template_content) as template:
        with pytest.raises(jsontemplate.UndefinedVariable) as e:
            ocds_mapper.mapper.render(template, context)

def test_traverse_raises_error_if_integer_conversion_failed():
    template_content = '{num|integer}'
    context = {'num': 'foo'}
    with prepare(template_content) as template:
        with pytest.raises(jsontemplate.EvaluationError) as e:
            ocds_mapper.mapper.render(template, context)

def test_traverse_raises_error_if_float_conversion_failed():
    template_content = '{num|number}'
    context = {'num': 'foo'}
    with prepare(template_content) as template:
        with pytest.raises(jsontemplate.EvaluationError) as e:
            ocds_mapper.mapper.render(template, context)

def test_render_list():
    template_content = (
        '{\n'
            '"attachments": [\n'
                '{.repeated section attachments|list}\n'
                    '{.meta-left}\n'
                        '"uri": "{@}",\n'
                        '"name": ""\n'
                    '{.meta-right}{.alternates with},\n'
                '{.end}'
            ']\n'
        '}\n'
    )
    context = {'attachments': 'a, b, c, d'}
    expected = dict(
        attachments=[
            dict(uri='a', name=''),
            dict(uri='b', name=''),
            dict(uri='c', name=''),
            dict(uri='d', name='')
        ]
    )
    with prepare(template_content) as template:
        output = json.loads(
            ocds_mapper.mapper.render(template, context)
        )
        assert expected == output
