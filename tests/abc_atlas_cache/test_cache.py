import datetime
import pytest
from pathlib import Path
import boto3
from moto import mock_s3
import json

from abc_atlas_access.abc_atlas_cache.cloud_cache import S3CloudCache


@mock_s3
def test_list_all_manifests(tmpdir):
    """
    Test that S3CloudCache.list_al_manifests() returns the correct result
    """

    test_bucket_name = 'list_manifest_bucket'

    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=test_bucket_name)

    client = boto3.client('s3', region_name='us-east-1')
    client.put_object(Bucket=test_bucket_name,
                      Key='releases/20230101/manifest.json',
                      Body=b'123456')
    client.put_object(Bucket=test_bucket_name,
                      Key='releases/20240101/manifest.json',
                      Body=b'123456')
    client.put_object(Bucket=test_bucket_name,
                      Key='junk.txt',
                      Body=b'123456')

    cache = S3CloudCache(cache_dir=Path(tmpdir),
                         bucket_name=test_bucket_name)

    assert cache.manifest_file_names == ['releases/20230101/manifest.json',
                                         'releases/20240101/manifest.json']


@mock_s3
def test_list_all_manifests_many(tmpdir):
    """
    Test the extreme case when there are more manifests than list_objects_v2
    can return at a time
    """

    test_bucket_name = 'list_manifest_bucket'

    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=test_bucket_name)

    client = boto3.client('s3', region_name='us-east-1')
    expected = []
    for day in range(2000):
        date = datetime.date(year=2020, month=1, day=1) \
               + datetime.timedelta(days=day)
        manifest_string = f'releases/{date.strftime("%Y%m%d")}/manifest.json'
        expected.append(manifest_string)
        client.put_object(
            Bucket=test_bucket_name,
            Key=manifest_string,
            Body=b'123456'
        )

    client.put_object(Bucket=test_bucket_name,
                      Key='junk.txt',
                      Body=b'123456')

    cache = S3CloudCache(Path(tmpdir), test_bucket_name)
    expected.sort()
    assert cache.manifest_file_names == expected


def create_manifest_dict(version: str,
                         test_bucket_name: str) -> dict:
    """
    Create a manifest dictionary for testing.

    Parameters
    ----------
    version: str
        The version of the test manifest.
    test_bucket_name: str
        The name of the test bucket.

    Returns
    -------
    test_manifest: dict
        Dictionary of test manifest values.
    """
    test_manifest = {
        "version": version,
        "resource_uri": f"s3://{test_bucket_name}/",
        "directory_listing": {
            "test_directory": {
                "directories": {
                    "expression_matrices": {
                        "version": version,
                        "relative_path": f"expression_matrices/test_directory/{version}",
                        "url": f"https://{test_bucket_name}.s3.us-west-2.amazonaws.com/expression_matrices/test_directory/{version}/",
                        "view_link": f"https://{test_bucket_name}.s3.us-west-2.amazonaws.com/index.html#expression_matrices/test_directory/{version}/",
                        "total_size": 1234
                    },
                    "metadata": {
                        "version": version,
                        "relative_path": f"metadata/test_directory/{version}",
                        "url": f"https://{test_bucket_name}.s3.us-west-2.amazonaws.com/metadata/test_directory/{version}/",
                        "view_link": f"https://{test_bucket_name}.s3.us-west-2.amazonaws.com/index.html#metadata/test_directory/{version}/",
                        "total_size": 5678
                    }
                }
            }
        },
        "file_listing": {
            "test_directory": {
                "expression_matrices": {
                    "junk_file": {
                        "log2": {
                            "files": {
                                "h5ad": {
                                    "version": version,
                                    "relative_path": f"expression_matrices/test_directory/{version}/junk_file.h5ad",
                                    "url": f"https://{test_bucket_name}.s3.us-west-2.amazonaws.com/expression_matrices/test_directory/{version}/junk_file.h5ad",
                                    "size": 1234,
                                    "file_hash": f"abcd{version}"
                                }
                            }
                        }
                    }
                },
                "metadata": {
                    "junk_metadata": {
                        "files": {
                            "csv": {
                                "version": version,
                                "relative_path": f"metadata/test_directory/{version}/junk_metadata.csv",
                                "url": f"https://{test_bucket_name}.s3.us-west-2.amazonaws.com/metadata/test_directory/{version}/junk_metadata.csv",
                                "size": 5678,
                                "file_hash": f"efgh{version}"
                            }
                        }
                    }
                }
            }
        }
    }
    return test_manifest


@mock_s3
def test_loading_manifest(tmpdir):
    """
    Test loading manifests with S3CloudCache
    """

    test_bucket_name = 'list_manifest_bucket'

    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=test_bucket_name, ACL='public-read')

    client = boto3.client('s3', region_name='us-west-2')

    manifest_1 = create_manifest_dict(version='20230101',
                                      test_bucket_name=test_bucket_name)

    manifest_2 = create_manifest_dict(version='20240101',
                                      test_bucket_name=test_bucket_name)

    client.put_object(Bucket=test_bucket_name,
                      Key='releases/20230101/manifest.json',
                      Body=bytes(json.dumps(manifest_1), 'utf-8'))

    client.put_object(Bucket=test_bucket_name,
                      Key='releases/20240101/manifest.json',
                      Body=bytes(json.dumps(manifest_2), 'utf-8'))

    cache = S3CloudCache(Path(tmpdir), test_bucket_name)
    assert cache.current_manifest is None
    cache.load_manifest('releases/20230101/manifest.json')
    assert cache._manifest._data == manifest_1
    assert cache.version == '20230101'
    assert cache.current_manifest == 'releases/20230101/manifest.json'

    cache.load_manifest('releases/20240101/manifest.json')
    assert cache._manifest._data == manifest_2
    assert cache.version == '20240101'
    assert cache.current_manifest == 'releases/20240101/manifest.json'

    with pytest.raises(ValueError) as context:
        cache.load_manifest('releases/20200101/manifest.json')
    msg = 'is not one of the valid manifest names'
    assert msg in context.value.args[0]