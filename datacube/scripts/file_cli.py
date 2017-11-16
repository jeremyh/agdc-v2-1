from __future__ import absolute_import

import logging
from pathlib import Path

import click
import sys

from datacube.index._api import Index
from datacube.ui import click as ui
from datacube.ui.click import cli
from datacube.ui.common import write_datasets, OUTPUT_FORMAT_OPT

try:
    from typing import Iterable
except ImportError:
    pass

_LOG = logging.getLogger('datacube-dataset')


class BadMatch(Exception):
    pass


@cli.group(name='file', help='Manage file-based dataset locations')
def file_cmd():
    pass


@file_cmd.command('info', help="Display known datasets at location")
@click.option('--show-sources', help='Also show source datasets', is_flag=True, default=False)
@click.option('--show-derived', help='Also show derived datasets', is_flag=True, default=False)
@OUTPUT_FORMAT_OPT
@click.option('--max-depth',
              help='Maximum sources/derived depth to travel',
              type=int,
              # Unlikely to be hit, but will avoid total-death by circular-references.
              default=99)
@click.argument('files', click.Path(exists=False), nargs=-1)
@ui.pass_index()
def info_cmd(index, show_sources, show_derived, format_, max_depth, files):
    # type: (Index, bool, bool, str, int, Iterable[str]) -> None

    # Using an array wrapper to get around the lack of "nonlocal" in py2
    missing_paths = [0]

    def get_datasets(file_location):
        for file_ in file_location:

            file_uri = _full_uri(file_)
            datasets = index.datasets.get_datasets_for_location(file_uri)
            count = 0
            for dataset in datasets:
                count += 1
                yield dataset

            if count == 0:
                missing_paths[0] += 1

    write_datasets(
        index,
        format_,
        get_datasets(files),
        show_sources=show_sources,
        show_derived=show_derived,
        max_depth=max_depth
    )

    # Error code: number of inputs that weren't found.
    sys.exit(missing_paths[0])


@file_cmd.command('archive', help="Archive file locations")
@click.option('--dry-run', help="Don't archive. Display datasets that would get archived",
              is_flag=True, default=False)
@click.argument('files', click.Path(exists=False), nargs=-1)
@ui.pass_index()
def archive_cmd(index, dry_run, files):
    # type: (Index, bool, Iterable[str]) -> None

    not_found_count = 0

    for file_ in files:
        archive_count = 0

        file_uri = _full_uri(file_)

        to_process = index.datasets.get_datasets_for_location(file_uri)

        for d in to_process:
            click.echo('archiving location %s %s %s' % (d.type.name, d.id, file_uri))
            if not dry_run:
                was_archived = index.datasets.archive_location(d.id, file_uri)
                if was_archived:
                    archive_count += 1

        if archive_count == 0:
            not_found_count += 1

    sys.exit(not_found_count)


@file_cmd.command('restore', help="Restore file locations")
@click.option('--dry-run', help="Don't restore. Display datasets that would get restored",
              is_flag=True, default=False)
@click.argument('files', click.Path(exists=False), nargs=-1)
@ui.pass_index()
def restore_cmd(index, dry_run, files):
    # type: (Index, bool, Iterable[str]) -> None

    not_found_count = 0

    for file_ in files:
        restore_count = 0

        file_uri = _full_uri(file_)
        to_process = index.datasets.get_datasets_for_location(file_uri)

        for d in to_process:
            click.echo('restoring %s %s %s' % (d.type.name, d.id, d.local_uri))

            if not dry_run:
                was_restored = index.datasets.restore_location(d.id, file_uri)
                if was_restored:
                    restore_count += 1
            if restore_count == 0:
                not_found_count += 1

    sys.exit(not_found_count)


def _full_uri(file_):
    # type: (str) -> str
    """
    Get the full uri for the given file, even if it doesn't exist
    """
    path = Path(file_)
    path = path.resolve() if path.exists() else path.absolute()
    file_uri = path.as_uri()
    return file_uri
