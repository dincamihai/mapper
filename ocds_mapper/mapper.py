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


def add_release_ids(rows, publisher_name, publish_date):
    for row in rows:
        row['releaseID'] = "{}-{}-{}".format(
            publisher_name, publish_date, str(uuid.uuid4())
        )


def process_csv(csv_file, publisher_name, publish_date):
    reader = csv.DictReader(csv_file)
    csv_rows = list(reader)
    add_release_ids(csv_rows, publisher_name, publish_date)
    return csv_rows


def render(mapping_file, context):

    def list_formatter(value):
        return map(lambda it: it.strip(), value.split(','))

    def boolean_formatter(value):
        if value in ['0', 'f', 'false', 'False', 'no', 'No']:
            return 'false'
        elif value in ['1', 't', 'true', 'True', 'yes', 'Yes']:
            return 'true'

    def integer_formatter(value):
        return int(value)

    def number_formatter(value):
        return float(value)

    template = jsontemplate.FromFile(
        mapping_file,
        more_formatters=dict(
            list=list_formatter,
            boolean=boolean_formatter,
            integer=integer_formatter,
            number=number_formatter
        )
    )
    return template.expand(context)


def process(csv_path, mapping_path, publisher_name, publish_date):
    context = dict(
        name=publisher_name,
        date=publish_date
    )
    with open_file_path_or_url(csv_path) as csv_file:
        context['csv_rows'] = process_csv(
            csv_file, publisher_name, publish_date
        )
    with open_file_path_or_url(mapping_path) as mapping_file:
        return render(mapping_file, context)


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
        options.publisher_name, options.publish_date
    )
    print(result.encode('utf-8'))


if __name__ == '__main__':
    main()
