# coding=utf-8
"""
Module
"""
from __future__ import absolute_import

import csv
import datetime
import io
import uuid
from decimal import Decimal
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner
from dateutil import tz
from psycopg2._range import NumericRange

import datacube.scripts.cli_app
import datacube.scripts.search_tool
from datacube.index._api import Index
from datacube.model import Dataset
from datacube.model import DatasetType
from datacube.model import MetadataType
from datacube.model import Range
from datacube.scripts import dataset as dataset_script

_EXAMPLE_LS7_NBAR_DATASET_FILE = Path(__file__).parent.joinpath('ls7-nbar-example.yaml')


@pytest.fixture
def pseudo_telemetry_type(index, default_metadata_type):
    index.products.add_document({
        'name': 'ls8_telemetry',
        'description': 'telemetry test',
        'metadata': {
            'product_type': 'pseudo_telemetry_data',
            'platform': {
                'code': 'LANDSAT_8'
            },
            'instrument': {
                'name': 'OLI_TIRS'
            },
            'format': {
                'name': 'PSEUDOMD'
            }
        },
        # We're actually using 'eo' because we do lat/lon searches below...
        'metadata_type': default_metadata_type.name
    })
    return index.products.get_by_name('ls8_telemetry')


@pytest.fixture
def pseudo_telemetry_dataset(index, db, pseudo_telemetry_type):
    id_ = str(uuid.uuid4())
    with db.connect() as connection:
        was_inserted = connection.insert_dataset(
            {
                'id': id_,
                'product_type': 'pseudo_telemetry_data',
                'checksum_path': 'package.sha1',
                'ga_label': 'LS8_OLITIRS_STD-MD_P00_LC81160740742015089ASA00_'
                            '116_074_20150330T022553Z20150330T022657',

                'ga_level': 'P00',
                'size_bytes': 637660782,
                'platform': {
                    'code': 'LANDSAT_8'
                },
                # We're unlikely to have extent info for a raw dataset, we'll use it for search tests.
                'extent': {
                    'from_dt': datetime.datetime(2014, 7, 26, 23, 48, 0, 343853),
                    'to_dt': datetime.datetime(2014, 7, 26, 23, 52, 0, 343853),
                    'coord': {
                        'll': {'lat': -31.33333, 'lon': 149.78434},
                        'lr': {'lat': -31.37116, 'lon': 152.20094},
                        'ul': {'lat': -29.23394, 'lon': 149.85216},
                        'ur': {'lat': -29.26873, 'lon': 152.21782}
                    }
                },
                'image': {
                    'satellite_ref_point_start': {'x': 116, 'y': 74},
                    'satellite_ref_point_end': {'x': 116, 'y': 84},
                },
                'creation_dt': datetime.datetime(2015, 4, 22, 6, 32, 4),
                'instrument': {'name': 'OLI_TIRS'},
                'format': {
                    'name': 'PSEUDOMD'
                },
                'lineage': {
                    'source_datasets': {}
                }
            },
            id_,
            pseudo_telemetry_type.id
        )
    assert was_inserted
    d = index.datasets.get(id_)
    # The dataset should have been matched to the telemetry type.
    assert d.type.id == pseudo_telemetry_type.id

    return d


@pytest.fixture
def pseudo_telemetry_dataset2(index, db, pseudo_telemetry_type):
    # Like the previous dataset, but a day later in time.
    id_ = str(uuid.uuid4())
    with db.connect() as connection:
        was_inserted = connection.insert_dataset(
            {
                'id': id_,
                'product_type': 'pseudo_telemetry_data',
                'checksum_path': 'package.sha1',
                'ga_label': 'LS8_OLITIRS_STD-MD_P00_LC81160740742015089ASA00_'
                            '116_074_20150330T022553Z20150330T022657',

                'ga_level': 'P00',
                'size_bytes': 637660782,
                'platform': {
                    'code': 'LANDSAT_8'
                },
                # We're unlikely to have extent info for a raw dataset, we'll use it for search tests.
                'extent': {
                    'from_dt': datetime.datetime(2014, 7, 27, 23, 48, 0, 343853),
                    'to_dt': datetime.datetime(2014, 7, 27, 23, 52, 0, 343853),
                    'coord': {
                        'll': {'lat': -31.33333, 'lon': 149.78434},
                        'lr': {'lat': -31.37116, 'lon': 152.20094},
                        'ul': {'lat': -29.23394, 'lon': 149.85216},
                        'ur': {'lat': -29.26873, 'lon': 152.21782}
                    }
                },
                'creation_dt': datetime.datetime(2015, 4, 22, 6, 32, 4),
                'instrument': {'name': 'OLI_TIRS'},
                'format': {
                    'name': 'PSEUDOMD'
                },
                'lineage': {
                    'source_datasets': {}
                }
            },
            id_,
            pseudo_telemetry_type.id
        )
    assert was_inserted
    d = index.datasets.get(id_)
    # The dataset should have been matched to the telemetry type.
    assert d.type.id == pseudo_telemetry_type.id

    return d


@pytest.fixture
def ls5_dataset_w_children(index, example_ls5_dataset_path, indexed_ls5_scene_dataset_types):
    # type: (Index, Path, DatasetType) -> Dataset
    # TODO: We need a higher-level API for indexing paths, rather than reaching inside the cli script
    datasets = list(
        dataset_script.load_datasets(
            [example_ls5_dataset_path],
            dataset_script.load_rules_from_types(index)
        )
    )
    assert len(datasets) == 1
    d = index.datasets.add(datasets[0])
    return index.datasets.get(d.id, include_sources=True)


@pytest.fixture
def ls5_dataset_nbar_type(ls5_dataset_w_children, indexed_ls5_scene_dataset_types):
    # type: (Dataset, List[DatasetType]) -> DatasetType
    for dataset_type in indexed_ls5_scene_dataset_types:
        if dataset_type.name == ls5_dataset_w_children.type.name:
            return dataset_type
    else:
        raise RuntimeError("LS5 type was not among types")


def test_search_dataset_equals(index, pseudo_telemetry_dataset):
    """
    :type index: datacube.index._api.Index
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """
    datasets = index.datasets.search_eager(
        platform='LANDSAT_8'
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id

    datasets = index.datasets.search_eager(
        platform='LANDSAT_8',
        instrument='OLI_TIRS'
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id

    # Wrong sensor name
    datasets = index.datasets.search_eager(
        platform='LANDSAT-8',
        instrument='TM',
    )
    assert len(datasets) == 0


def test_search_dataset_by_metadata(index, pseudo_telemetry_dataset):
    """
    :type index: datacube.index._api.Index
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """
    datasets = index.datasets.search_by_metadata(
        {"platform": {"code": "LANDSAT_8"}, "instrument": {"name": "OLI_TIRS"}}
    )
    datasets = list(datasets)
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id

    datasets = index.datasets.search_by_metadata(
        {"platform": {"code": "LANDSAT_5"}, "instrument": {"name": "TM"}}
    )
    datasets = list(datasets)
    assert len(datasets) == 0


def test_search_dataset_ranges(index, pseudo_telemetry_dataset):
    """
    :type index: datacube.index._api.Index
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """

    # In the lat bounds.
    datasets = index.datasets.search_eager(
        lat=Range(-30.5, -29.5),
        time=Range(
            datetime.datetime(2014, 7, 26, 23, 0, 0),
            datetime.datetime(2014, 7, 26, 23, 59, 0)
        )
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id

    # Out of the lat bounds.
    datasets = index.datasets.search_eager(
        lat=Range(28, 32),
        time=Range(
            datetime.datetime(2014, 7, 26, 23, 48, 0),
            datetime.datetime(2014, 7, 26, 23, 50, 0)
        )
    )
    assert len(datasets) == 0

    # Out of the time bounds
    datasets = index.datasets.search_eager(
        lat=Range(-30.5, -29.5),
        time=Range(
            datetime.datetime(2014, 7, 26, 21, 48, 0),
            datetime.datetime(2014, 7, 26, 21, 50, 0)
        )
    )
    assert len(datasets) == 0

    # A dataset that overlaps but is not fully contained by the search bounds.
    # TODO: Do we want overlap as the default behaviour?
    # Should we distinguish between 'contains' and 'overlaps'?
    datasets = index.datasets.search_eager(
        lat=Range(-40, -30)
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id

    # Single point search
    datasets = index.datasets.search_eager(
        lat=-30.0,
        time=Range(
            datetime.datetime(2014, 7, 26, 23, 0, 0),
            datetime.datetime(2014, 7, 26, 23, 59, 0)
        )
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id

    datasets = index.datasets.search_eager(
        lat=30.0,
        time=Range(
            datetime.datetime(2014, 7, 26, 23, 0, 0),
            datetime.datetime(2014, 7, 26, 23, 59, 0)
        )
    )
    assert len(datasets) == 0

    # Single timestamp search
    datasets = index.datasets.search_eager(
        lat=Range(-30.5, -29.5),
        time=datetime.datetime(2014, 7, 26, 23, 50, 0)
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id

    datasets = index.datasets.search_eager(
        lat=Range(-30.5, -29.5),
        time=datetime.datetime(2014, 7, 26, 23, 30, 0)
    )
    assert len(datasets) == 0


def test_search_globally(index, pseudo_telemetry_dataset):
    """
    :type index: datacube.index._api.Index
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """
    # Insert dataset. It should be matched to the telemetry collection.
    # No expressions means get all.
    results = list(index.datasets.search())
    assert len(results) == 1


def test_search_by_product(index, pseudo_telemetry_type, pseudo_telemetry_dataset, indexed_ls5_scene_dataset_types,
                           ls5_dataset_w_children):
    """
    :type index: datacube.index._api.Index
    """
    # Expect one product with our one dataset.
    products = list(index.datasets.search_by_product(
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert len(products) == 1
    product, datasets = products[0]
    assert product.id == pseudo_telemetry_type.id
    assert next(datasets).id == pseudo_telemetry_dataset.id


def test_search_or_expressions(index,
                               pseudo_telemetry_type, pseudo_telemetry_dataset,
                               ls5_dataset_nbar_type, ls5_dataset_w_children,
                               telemetry_metadata_type):
    # type: (Index, DatasetType, Dataset, List[DatasetType], Dataset) -> None

    # Four datasets:
    # Our standard LS8
    # - type=ls8_telemetry
    # LS5 with children:
    # - type=ls5_nbar_scene
    # - type=ls5_level1_scene
    # - type=ls5_satellite_telemetry_data

    all_datasets = index.datasets.search_eager()
    assert len(all_datasets) == 4
    all_ids = set(dataset.id for dataset in all_datasets)

    # OR all platforms: should return all datasets
    datasets = index.datasets.search_eager(
        platform=['LANDSAT_5', 'LANDSAT_7', 'LANDSAT_8']
    )
    assert len(datasets) == 4
    ids = set(dataset.id for dataset in datasets)
    assert ids == all_ids

    # OR expression with only one clause.
    datasets = index.datasets.search_eager(
        platform=['LANDSAT_8']
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id

    # OR two products: return two
    datasets = index.datasets.search_eager(
        product=[pseudo_telemetry_type.name, ls5_dataset_nbar_type.name]
    )
    assert len(datasets) == 2
    ids = set(dataset.id for dataset in datasets)
    assert ids == {pseudo_telemetry_dataset.id, ls5_dataset_w_children.id}

    # eo OR telemetry: return all
    datasets = index.datasets.search_eager(
        metadata_type=[
            telemetry_metadata_type.name,
            pseudo_telemetry_type.metadata_type.name
        ]
    )
    assert len(datasets) == 4
    ids = set(dataset.id for dataset in datasets)
    assert ids == all_ids

    # Redundant ORs should have no effect.
    datasets = index.datasets.search_eager(
        product=[pseudo_telemetry_type.name, pseudo_telemetry_type.name, pseudo_telemetry_type.name]
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id


def test_search_returning(index, pseudo_telemetry_type, pseudo_telemetry_dataset, indexed_ls5_scene_dataset_types):
    """
    :type index: datacube.index._api.Index
    """

    # Expect one product with our one dataset.
    results = list(index.datasets.search_returning(
        ('id', 'sat_path', 'sat_row'),
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert len(results) == 1
    id, path_range, sat_range = results[0]
    assert id == pseudo_telemetry_dataset.id
    # TODO: output nicer types?
    assert path_range == NumericRange(Decimal('116'), Decimal('116'), '[]')
    assert sat_range == NumericRange(Decimal('74'), Decimal('84'), '[]')


def test_search_returning_rows(index, pseudo_telemetry_type,
                               pseudo_telemetry_dataset, pseudo_telemetry_dataset2,
                               indexed_ls5_scene_dataset_types):
    dataset = pseudo_telemetry_dataset

    # If returning a field like uri, there will be one result per location.

    # No locations
    results = list(index.datasets.search_returning(
        ('id', 'uri'),
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert len(results) == 0

    # Add a location to the dataset and we should get one result
    test_uri = 'file:///tmp/test1'
    index.datasets.add_location(dataset, test_uri)
    results = list(index.datasets.search_returning(
        ('id', 'uri'),
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert len(results) == 1
    assert results == [(dataset.id, test_uri)]

    # Add a second location and we should get two results
    test_uri2 = 'file:///tmp/test2'
    index.datasets.add_location(dataset, test_uri2)
    results = set(index.datasets.search_returning(
        ('id', 'uri'),
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert len(results) == 2
    assert results == {
        (dataset.id, test_uri),
        (dataset.id, test_uri2)
    }

    # A second dataset now has a location too:
    test_uri3 = 'mdss://c10/tmp/something'
    index.datasets.add_location(pseudo_telemetry_dataset2, test_uri3)
    # Datasets and locations should still correctly match up...
    results = set(index.datasets.search_returning(
        ('id', 'uri'),
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert len(results) == 3
    assert results == {
        (dataset.id, test_uri),
        (dataset.id, test_uri2),
        (pseudo_telemetry_dataset2.id, test_uri3),
    }


def test_searches_only_type(index, pseudo_telemetry_type, pseudo_telemetry_dataset, ls5_nbar_gtiff_type):
    """
    :type index: datacube.index._api.Index
    :type pseudo_telemetry_type: datacube.model.DatasetType
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """
    # The dataset should have been matched to the telemetry type.
    assert pseudo_telemetry_dataset.type.id == pseudo_telemetry_type.id
    assert index.datasets.search_eager()

    # One result in the telemetry type
    datasets = index.datasets.search_eager(
        product=pseudo_telemetry_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id

    # One result in the metadata type
    datasets = index.datasets.search_eager(
        metadata_type=pseudo_telemetry_type.metadata_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id

    # No results when searching for a different dataset type.
    datasets = index.datasets.search_eager(
        product=ls5_nbar_gtiff_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS'
    )
    assert len(datasets) == 0

    # One result when no types specified.
    datasets = index.datasets.search_eager(
        platform='LANDSAT_8',
        instrument='OLI_TIRS'
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id

    # No results for different metadata type.
    datasets = index.datasets.search_eager(
        metadata_type='telemetry',
        platform='LANDSAT_8',
        instrument='OLI_TIRS'
    )
    assert len(datasets) == 0


def test_search_special_fields(index, pseudo_telemetry_type, pseudo_telemetry_dataset,
                               example_ls5_dataset):
    """
    :type index: datacube.index._api.Index
    :type pseudo_telemetry_type: datacube.model.DatasetType
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """

    # 'product' is a special case
    datasets = index.datasets.search_eager(
        product=pseudo_telemetry_type.name
    )
    assert len(datasets) == 1
    assert datasets[0].id == pseudo_telemetry_dataset.id

    # Unknown field: no results
    datasets = index.datasets.search_eager(
        platform='LANDSAT_8',
        flavour='chocolate',
    )
    assert len(datasets) == 0


def test_search_conflicting_types(index, pseudo_telemetry_dataset, pseudo_telemetry_type):
    # Should return no results.
    datasets = index.datasets.search_eager(
        product=pseudo_telemetry_type.name,
        # The telemetry type is not of type storage_unit.
        metadata_type='storage_unit'
    )
    assert len(datasets) == 0


def test_fetch_all_of_md_type(index, pseudo_telemetry_dataset):
    """
    :type index: datacube.index._api.Index
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """
    # Get every dataset of the md type.
    results = index.datasets.search_eager(
        metadata_type='eo'
    )
    assert len(results) == 1
    assert results[0].id == pseudo_telemetry_dataset.id
    # Get every dataset of the type.
    results = index.datasets.search_eager(
        product=pseudo_telemetry_dataset.type.name
    )
    assert len(results) == 1
    assert results[0].id == pseudo_telemetry_dataset.id

    # No results for another.
    results = index.datasets.search_eager(
        metadata_type='telemetry'
    )
    assert len(results) == 0


def test_count_searches(index, pseudo_telemetry_type, pseudo_telemetry_dataset, ls5_nbar_gtiff_type):
    """
    :type index: datacube.index._api.Index
    :type pseudo_telemetry_type: datacube.model.DatasetType
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """
    # The dataset should have been matched to the telemetry type.
    assert pseudo_telemetry_dataset.type.id == pseudo_telemetry_type.id
    assert index.datasets.search_eager()

    # One result in the telemetry type
    datasets = index.datasets.count(
        product=pseudo_telemetry_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    )
    assert datasets == 1

    # One result in the metadata type
    datasets = index.datasets.count(
        metadata_type=pseudo_telemetry_type.metadata_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    )
    assert datasets == 1

    # No results when searching for a different dataset type.
    datasets = index.datasets.count(
        product=ls5_nbar_gtiff_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS'
    )
    assert datasets == 0

    # One result when no types specified.
    datasets = index.datasets.count(
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    )
    assert datasets == 1

    # No results for different metadata type.
    datasets = index.datasets.count(
        metadata_type='telemetry',
        platform='LANDSAT_8',
        instrument='OLI_TIRS'
    )
    assert datasets == 0


def test_count_by_product_searches(index, pseudo_telemetry_type, pseudo_telemetry_dataset, ls5_nbar_gtiff_type):
    """
    :type index: datacube.index._api.Index
    :type pseudo_telemetry_type: datacube.model.DatasetType
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """
    # The dataset should have been matched to the telemetry type.
    assert pseudo_telemetry_dataset.type.id == pseudo_telemetry_type.id
    assert index.datasets.search_eager()

    # One result in the telemetry type
    products = tuple(index.datasets.count_by_product(
        product=pseudo_telemetry_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert products == ((pseudo_telemetry_type, 1),)

    # One result in the metadata type
    products = tuple(index.datasets.count_by_product(
        metadata_type=pseudo_telemetry_type.metadata_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert products == ((pseudo_telemetry_type, 1),)

    # No results when searching for a different dataset type.
    products = tuple(index.datasets.count_by_product(
        product=ls5_nbar_gtiff_type.name,
        platform='LANDSAT_8',
        instrument='OLI_TIRS'
    ))
    assert products == ()

    # One result when no types specified.
    products = tuple(index.datasets.count_by_product(
        platform='LANDSAT_8',
        instrument='OLI_TIRS',
    ))
    assert products == ((pseudo_telemetry_type, 1),)

    # Only types with datasets should be returned (these params match ls5_gtiff too)
    products = tuple(index.datasets.count_by_product())
    assert products == ((pseudo_telemetry_type, 1),)

    # No results for different metadata type.
    products = tuple(index.datasets.count_by_product(
        metadata_type='telemetry',
    ))
    assert products == ()


def test_count_time_groups(index, pseudo_telemetry_type, pseudo_telemetry_dataset):
    """
    :type index: datacube.index._api.Index
    """

    # 'from_dt': datetime.datetime(2014, 7, 26, 23, 48, 0, 343853),
    # 'to_dt': datetime.datetime(2014, 7, 26, 23, 52, 0, 343853),
    timeline = list(index.datasets.count_product_through_time(
        '1 day',
        product=pseudo_telemetry_type.name,
        time=Range(
            datetime.datetime(2014, 7, 25, tzinfo=tz.tzutc()),
            datetime.datetime(2014, 7, 27, tzinfo=tz.tzutc())
        )
    ))

    assert len(timeline) == 2
    assert timeline == [
        (
            Range(datetime.datetime(2014, 7, 25, tzinfo=tz.tzutc()),
                  datetime.datetime(2014, 7, 26, tzinfo=tz.tzutc())),
            0
        ),
        (
            Range(datetime.datetime(2014, 7, 26, tzinfo=tz.tzutc()),
                  datetime.datetime(2014, 7, 27, tzinfo=tz.tzutc())),
            1
        )
    ]


@pytest.mark.usefixtures('default_metadata_type',
                         'indexed_ls5_scene_dataset_types')
def test_source_filter(global_integration_cli_args, index, example_ls5_dataset_path, ls5_nbar_ingest_config):
    opts = list(global_integration_cli_args)
    opts.extend(
        [
            '-v',
            'dataset',
            'add',
            '--auto-match',
            str(example_ls5_dataset_path)
        ]
    )
    result = CliRunner().invoke(
        datacube.scripts.cli_app.cli,
        opts,
        catch_exceptions=False
    )

    all_nbar = index.datasets.search_eager(product='ls5_nbar_scene')
    assert len(all_nbar) == 1

    dss = index.datasets.search_eager(product='ls5_nbar_scene', source_filter={'product': 'ls5_level1_scene',
                                                                               'gsi': 'ASA'})
    assert dss == all_nbar
    dss = index.datasets.search_eager(product='ls5_nbar_scene', source_filter={'product': 'ls5_level1_scene',
                                                                               'gsi': 'GREG'})
    assert dss == []

    with pytest.raises(RuntimeError):
        dss = index.datasets.search_eager(product='ls5_nbar_scene', source_filter={'gsi': 'ASA'})


def test_count_time_groups_cli(global_integration_cli_args, pseudo_telemetry_type, pseudo_telemetry_dataset):
    # type: (list, DatasetType, Dataset) -> None

    opts = list(global_integration_cli_args)
    opts.extend(
        [
            'product-counts',
            '1 day',
            '2014-07-25 < time < 2014-07-27'
        ]
    )

    runner = CliRunner()
    result = runner.invoke(
        datacube.scripts.search_tool.cli,
        opts,
        catch_exceptions=False
    )
    assert result.exit_code == 0

    expected_out = (
        '{}\n'
        '    2014-07-25: 0\n'
        '    2014-07-26: 1\n'
    ).format(pseudo_telemetry_type.name)

    assert result.output == expected_out


def test_search_cli_basic(global_integration_cli_args, telemetry_metadata_type, pseudo_telemetry_dataset):
    """
    Search datasets using the cli.
    :type global_integration_cli_args: tuple[str]
    :type telemetry_metadata_type: datacube.model.MetadataType
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """
    opts = list(global_integration_cli_args)
    opts.extend(
        [
            # No search arguments: return all datasets.
            'datasets'
        ]
    )

    runner = CliRunner()
    result = runner.invoke(
        datacube.scripts.search_tool.cli,
        opts
    )
    assert str(pseudo_telemetry_dataset.id) in result.output
    assert str(telemetry_metadata_type.name) in result.output

    assert result.exit_code == 0


def test_cli_info(index, global_integration_cli_args, pseudo_telemetry_dataset):
    # type: (Index, tuple, MetadataType, Dataset) -> None
    """
    Search datasets using the cli.
    :type index: datacube.index._api.Index
    :type global_integration_cli_args: tuple[str]
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """
    index.datasets.add_location(pseudo_telemetry_dataset, 'file:///tmp/location1')
    index.datasets.add_location(pseudo_telemetry_dataset, 'file:///tmp/location2')

    opts = list(global_integration_cli_args)
    opts.extend(
        [
            'dataset', 'info', str(pseudo_telemetry_dataset.id)
        ]
    )

    runner = CliRunner()
    result = runner.invoke(
        datacube.scripts.cli_app.cli,
        opts,
        catch_exceptions=False
    )

    assert result.exit_code == 0

    output_doc = yaml.safe_load(result.output)
    assert output_doc == {
        # Newer location first
        'locations': ['file:///tmp/location2', 'file:///tmp/location1'],
        'id': str(pseudo_telemetry_dataset.id),
        'product': 'ls8_telemetry',
        'derived': [],
        'sources': {}
    }


def test_csv_search_via_cli(global_integration_cli_args, pseudo_telemetry_type, pseudo_telemetry_dataset,
                            pseudo_telemetry_dataset2):
    """
    Search datasets via the cli with csv output
    :type global_integration_cli_args: tuple[str]
    :type pseudo_telemetry_dataset: datacube.model.Dataset
    """
    # Test dataset is:
    # platform: LANDSAT_8
    # from: 2014-7-26  23:48:00
    # to:   2014-7-26  23:52:00
    # coords:
    #     ll: (-31.33333, 149.78434)
    #     lr: (-31.37116, 152.20094)
    #     ul: (-29.23394, 149.85216)
    #     ur: (-29.26873, 152.21782)

    # Dataset 2 is the same but on day 2014-7-27

    rows = _cli_csv_search(['datasets', ' -40 < lat < -10'], global_integration_cli_args)
    assert len(rows) == 2
    assert {rows[0]['id'], rows[1]['id']} == {str(pseudo_telemetry_dataset.id), str(pseudo_telemetry_dataset2.id)}

    rows = _cli_csv_search(['datasets', 'product=' + pseudo_telemetry_type.name], global_integration_cli_args)
    assert len(rows) == 2
    assert {rows[0]['id'], rows[1]['id']} == {str(pseudo_telemetry_dataset.id), str(pseudo_telemetry_dataset2.id)}

    # Don't return on a mismatch
    rows = _cli_csv_search(['datasets', '150<lat<160'], global_integration_cli_args)
    assert len(rows) == 0

    # Match only a single dataset using multiple fields
    rows = _cli_csv_search(['datasets', 'platform=LANDSAT_8', '2014-07-24<time<2014-07-27'],
                           global_integration_cli_args)
    assert len(rows) == 1
    assert rows[0]['id'] == str(pseudo_telemetry_dataset.id)

    # One matching field, one non-matching
    rows = _cli_csv_search(['datasets', '2014-07-24<time<2014-07-27', 'platform=LANDSAT_5'],
                           global_integration_cli_args)
    assert len(rows) == 0


# Headers are currently in alphabetical order.
_EXPECTED_OUTPUT_HEADER = 'dataset_type_id,gsi,id,instrument,lat,lon,metadata_type,metadata_type_id,orbit,' \
                          'platform,product,product_type,sat_path,sat_row,time,uri'


def test_csv_structure(global_integration_cli_args, pseudo_telemetry_type, ls5_nbar_gtiff_type,
                       pseudo_telemetry_dataset, pseudo_telemetry_dataset2):
    output = _csv_search_raw(['datasets', ' -40 < lat < -10'], global_integration_cli_args)
    lines = [line.strip() for line in output.split('\n') if line]
    # A header and two dataset rows
    assert len(lines) == 3

    assert lines[0] == _EXPECTED_OUTPUT_HEADER


def _cli_csv_search(args, global_integration_cli_args):
    # Do a CSV search from the cli, returning results as a list of dictionaries
    output = _csv_search_raw(args, global_integration_cli_args)
    return list(csv.DictReader(io.StringIO(output)))


def _csv_search_raw(args, global_integration_cli_args):
    # Do a CSV search from the cli, returning output as a string
    global_opts = list(global_integration_cli_args)
    global_opts.extend(['-f', 'csv'])
    result = _cli_search(args, global_opts)
    assert result.exit_code == 0, result.output
    return result.output


def _cli_search(args, global_integration_cli_args):
    opts = list(global_integration_cli_args)
    opts.extend(args)
    runner = CliRunner()
    result = runner.invoke(
        datacube.scripts.search_tool.cli,
        opts,
        catch_exceptions=False
    )
    return result
