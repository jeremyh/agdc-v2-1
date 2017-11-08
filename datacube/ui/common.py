# coding=utf-8
"""
Common methods for UI code.
"""
from __future__ import absolute_import

import csv
import datetime
import sys
from collections import OrderedDict
from decimal import Decimal
from pathlib import Path

import click
import yaml
import yaml.resolver
from yaml import Node

from datacube.model import Range

try:
    from typing import Iterable
except ImportError:
    pass

from datacube.index._api import Index
from datacube.model import Dataset
from datacube.utils import is_supported_document_type


def get_metadata_path(dataset_path):
    """
    Find a metadata path for a given input/dataset path.

    :type dataset_path: pathlib.Path
    :rtype: Path
    """

    # They may have given us a metadata file directly.
    if dataset_path.is_file() and is_supported_document_type(dataset_path):
        return dataset_path

    # Otherwise there may be a sibling file with appended suffix '.agdc-md.yaml'.
    expected_name = dataset_path.parent.joinpath('{}.agdc-md'.format(dataset_path.name))
    found = find_any_metadata_suffix(expected_name)
    if found:
        return found

    # Otherwise if it's a directory, there may be an 'agdc-metadata.yaml' file describing all contained datasets.
    if dataset_path.is_dir():
        expected_name = dataset_path.joinpath('agdc-metadata')
        found = find_any_metadata_suffix(expected_name)
        if found:
            return found

    raise ValueError('No metadata found for input %r' % dataset_path)


def find_any_metadata_suffix(path):
    """
    Find any supported metadata files that exist with the given file path stem.
    (supported suffixes are tried on the name)

    Eg. searching for '/tmp/ga-metadata' will find if any files such as '/tmp/ga-metadata.yaml' or
    '/tmp/ga-metadata.json', or '/tmp/ga-metadata.yaml.gz' etc that exist: any suffix supported by read_documents()

    :type path: pathlib.Path
    """
    existing_paths = list(filter(is_supported_document_type, path.parent.glob(path.name + '*')))
    if not existing_paths:
        return None

    if len(existing_paths) > 1:
        raise ValueError('Multiple matched metadata files: {!r}'.format(existing_paths))

    return existing_paths[0]


def build_dataset_info(index, dataset, show_sources=False, show_derived=False, depth=1, max_depth=99):
    # type: (Index, Dataset, bool) -> dict

    info = OrderedDict((
        ('id', str(dataset.id)),
        ('product', dataset.type.name),
        ('status', 'archived' if dataset.is_archived else 'active')
    ))

    # Optional when loading a dataset.
    if dataset.indexed_time is not None:
        info['indexed'] = dataset.indexed_time

    info['locations'] = dataset.uris
    info['fields'] = dataset.metadata.search_fields

    if depth < max_depth:
        if show_sources:
            info['sources'] = {key: build_dataset_info(index, source,
                                                       show_sources=True, show_derived=False,
                                                       depth=depth + 1, max_depth=max_depth)
                               for key, source in dataset.sources.items()}

        if show_derived:
            info['derived'] = [build_dataset_info(index, derived,
                                                  show_sources=False, show_derived=True,
                                                  depth=depth + 1, max_depth=max_depth)
                               for derived in index.datasets.get_derived(dataset.id)]

    return info


def _write_csv(infos):
    writer = csv.DictWriter(sys.stdout, ['id', 'status', 'product', 'location'], extrasaction='ignore')
    writer.writeheader()

    def add_first_location(row):
        locations_ = row['locations']
        row['location'] = locations_[0] if locations_ else None
        return row

    writer.writerows(add_first_location(row) for row in infos)


def _write_yaml(infos):
    """
    Dump yaml data with support for OrderedDicts.

    Allows for better human-readability of output: such as dataset ID field first, sources last.

    (Ordered dicts are output identically to normal yaml dicts: their order is purely for readability)
    """

    # We can't control how many ancestors this dumper API uses.
    # pylint: disable=too-many-ancestors
    class OrderedDumper(yaml.SafeDumper):
        pass

    def _dict_representer(dumper, data):
        return dumper.represent_mapping(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, data.items())

    def _range_representer(dumper, data):
        # type: (yaml.Dumper, Range) -> Node
        begin, end = data

        # pyyaml doesn't output timestamps in flow style as timestamps(?)
        if isinstance(begin, datetime.datetime):
            begin = begin.isoformat()
        if isinstance(end, datetime.datetime):
            end = end.isoformat()

        return dumper.represent_mapping(
            yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
            (('begin', begin), ('end', end)),
            flow_style=True
        )

    def _reduced_accuracy_decimal_representer(dumper, data):
        # type: (yaml.Dumper, Decimal) -> Node
        return dumper.represent_float(
            float(data)
        )

    OrderedDumper.add_representer(OrderedDict, _dict_representer)
    OrderedDumper.add_representer(Range, _range_representer)
    OrderedDumper.add_representer(Decimal, _reduced_accuracy_decimal_representer)
    return yaml.dump_all(infos, sys.stdout, OrderedDumper, default_flow_style=False, indent=4)


_OUTPUT_WRITERS = {
    'csv': _write_csv,
    'yaml': _write_yaml,
}

OUTPUT_FORMAT_OPT = click.option('-f', 'format_', help='Output format',
                                 type=click.Choice(_OUTPUT_WRITERS.keys()), default='yaml', show_default=True)


def write_datasets(index, format_: str, datasets: Iterable[Dataset], **dataset_opts):
    _OUTPUT_WRITERS[format_](
        build_dataset_info(index, dataset, **dataset_opts)
        for dataset in datasets
    )
