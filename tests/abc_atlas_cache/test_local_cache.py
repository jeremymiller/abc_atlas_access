import pathlib
import boto3
import json
from moto import mock_aws
from pathlib import Path
import tempfile
import unittest
from abc_atlas_access.abc_atlas_cache.cloud_cache import (
    S3CloudCache,
    LocalCache
)
import hashlib
from .utils import create_bucket, create_manifest_dict


@mock_aws
class TestLocalCache(unittest.TestCase):

    def setUp(self):
        self.test_bucket_name = 'abc_atlas_test_bucket'
        self.tmpdir = tempfile.TemporaryDirectory()
        self.cache_dir = Path(self.tmpdir.name).resolve()
        self._region = 'us-east-1'
        self.client = create_bucket(region_name=self._region,
                                    bucket_name=self.test_bucket_name)

    def test_local_cache_file_access(self):
        """
        Create a cache; download some, but not all of the files
        with S3CloudCache; verify that we can access the files
        with LocalCache
        """
        test_directory = "test_directory"

        for version in ['20200101', '20210101', '20220101']:
            hasher = hashlib.md5()
            data = bytes(f'11235813kjlssergwesvsdd{version}',
                         encoding='utf-8')
            hasher.update(data)
            true_checksum = hasher.hexdigest()
            manifest, metadata_path, data_path = create_manifest_dict(
                test_directory=test_directory,
                version=version,
                test_bucket_name=self.test_bucket_name,
                file_hash=true_checksum
            )
            self.client.put_object(Bucket=self.test_bucket_name,
                                   Key=f'releases/{version}/manifest.json',
                                   Body=bytes(json.dumps(manifest), 'utf-8'))
            self.client.put_object(Bucket=self.test_bucket_name,
                                   Key=data_path,
                                   Body=data)
            self.client.put_object(Bucket=self.test_bucket_name,
                                   Key=metadata_path,
                                   Body=data)

        cloud_cache = S3CloudCache(cache_dir=self.cache_dir,
                                   bucket_name=self.test_bucket_name)
        for version in ['20200101', '20220101']:
            cloud_cache.load_manifest(f'releases/{version}/manifest.json')
            cloud_cache.download_data(directory=test_directory,
                                      file_name='data_file/log2')
            cloud_cache.download_metadata(directory=test_directory,
                                          file_name='metadata_file')
        del cloud_cache

        local_cache = LocalCache(self.cache_dir)

        manifest_set = set(local_cache.manifest_file_names)
        assert manifest_set == {'releases/20200101/manifest.json',
                                'releases/20220101/manifest.json'}

        local_cache.load_manifest('releases/20200101/manifest.json')
        attr = local_cache.data_path(directory=test_directory,
                                     file_name='data_file/log2')
        assert attr['exists']
        assert '20200101' in str(attr['local_path'])
        attr = local_cache.metadata_path(directory=test_directory,
                                         file_name='metadata_file')
        assert attr['exists']
        assert '20200101' in str(attr['local_path'])

        local_cache.load_manifest('releases/20220101/manifest.json')
        attr = local_cache.data_path(directory=test_directory,
                                     file_name='data_file/log2')
        assert attr['exists']
        assert '20220101' in str(attr['local_path'])
        attr = local_cache.metadata_path(directory=test_directory,
                                         file_name='metadata_file')
        assert attr['exists']
        assert '20220101' in str(attr['local_path'])