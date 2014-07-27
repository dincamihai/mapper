#!/usr/bin/env python
import argparse
import contextlib
import copy
import csv
import json
import urllib2
import urlparse
import uuid


def is_url(file_path_or_url):
    return urlparse.urlparse(file_path_or_url).scheme != ''


@contextlib.contextmanager
def open_file_path_or_url(file_path_or_url):
    if is_url(file_path_or_url):
        with contextlib.closing(urllib2.urlopen(file_path_or_url)) as f:
            yield f
    else:
        with open(file_path_or_url, 'rb') as f:
            yield f


def get_csv_data(csv_row, key):
    try:
        return csv_row[key].decode('utf-8')
    except KeyError as error:
        raise KeyError(
            'Mapping uses invalid CSV header "{}"'.format(error.message))


def decompose_schema(schema, csv_row):
    result = schema.split(':', 1)  # split at most once, i.e. only first colon
    if len(result) == 1:
        return get_csv_data(csv_row, schema)

    column_type, value = result
    if 'string' == column_type:
        return get_csv_data(csv_row, value)
    elif 'constant' == column_type:
        return value
    elif 'integer' == column_type:
        try:
            return int(get_csv_data(csv_row, value))
        except ValueError:
            raise ValueError(
                '"{}" is not an integer. Maybe mapping "{}" is invalid.'
                .format(get_csv_data(csv_row, value), schema))
    elif 'number' == column_type:
        try:
            return float(get_csv_data(csv_row, value))
        except ValueError:
            raise ValueError(
                '"{}" is not a float. Maybe mapping "{}" is invalid.'
                .format(get_csv_data(csv_row, value), schema))
    elif 'boolean' == column_type:
        return get_csv_data(csv_row, value).lower() in [
            '1', 't', 'true', 'yes']

    raise ValueError('Invalid column type "{}:" -- valid column types are: '
                     'string, constant, integer, boolean.'.format(column_type))


def traverse(schema, csv_row):
    if isinstance(schema, (str, unicode)):
        if schema:
            return decompose_schema(schema, csv_row)
        else:
            return schema
    elif isinstance(schema, dict):
        result = {}
        for key, value in schema.items():
            result[key] = traverse(value, csv_row)
        return result
    elif isinstance(schema, list):
        result = []
        for value in schema:
            result.append(traverse(value, csv_row))
        return result
    else:
        copy.deepcopy(schema)


def process(csv_path, mapping_path, publisher_name, publish_date):
    with open_file_path_or_url(mapping_path) as mapping_file:
        content = mapping_file.read()
        result = json.loads(content)

    release_schema = result['releases'][0]
    result['releases'] = []
    result['publisher']['name'] = publisher_name
    result['publishingMeta']['date'] = publish_date

    with open_file_path_or_url(csv_path) as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            release = traverse(release_schema, csv_row=row)
            if 'releaseID' not in release['releaseMeta']:
                release['releaseMeta']['releaseID'] = "{}-{}-{}".format(
                    publisher_name, publish_date, str(uuid.uuid4()))
            result['releases'].append(release)

    return json.dumps(result, indent=4, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser(
        description='Convert CSV files to the OpenContracting format using '
                    'a given mapping.')
    parser.add_argument('--csv-file', metavar='data.csv', type=str,
                        required=True, help='the csv file to convert')
    parser.add_argument(
        '--mapping-file', metavar='mapping.json', type=str, required=True,
        help='the mapping used to convert the csv file')
    parser.add_argument(
        '--publisher-name', type=str, required=True,
        help='name of the organization that published the csv file')
    parser.add_argument(
        '--publish-date', type=str, required=True,
        help='ISO date when the csv file was published')

    options = parser.parse_args()

    result = process(
        options.csv_file, options.mapping_file,
        options.publisher_name, options.publish_date)
    print(result.encode('utf-8'))


if __name__ == '__main__':
    main()
