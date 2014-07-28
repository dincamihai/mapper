#!/usr/bin/env python
import argparse
import contextlib
import copy
import csv
import json
import urllib2
import urlparse
import uuid
import jsontemplate


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


def process(csv_path, mapping_path, publisher_name, publish_date):
    with open_file_path_or_url(mapping_path) as mapping_file, \
         open_file_path_or_url(csv_path) as csv_file:
        reader = csv.DictReader(csv_file)
        def list_formatter(value, **kwargs):
            return map(lambda it: it.strip(), value.split(','))
        template = jsontemplate.FromFile(
            mapping_file,
            more_formatters=dict(list=list_formatter)
        )
        csv_rows = list(reader)
        for row in csv_rows:
            row['releaseID'] = "{}-{}-{}".format(
                publisher_name, publish_date, str(uuid.uuid4())
            )
        return template.expand(
            name=publisher_name or "null",
            date=publish_date or "null",
            csv_rows=csv_rows
        )


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
