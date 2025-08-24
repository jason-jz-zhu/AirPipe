"""
Tests for data artifact system.
"""

import unittest
import pandas as pd
from pathlib import Path
import tempfile
import shutil

from airpipe.artifacts.data_artifact import DataArtifact, DataFormat, ArtifactStore


class TestDataArtifact(unittest.TestCase):
    """Test DataArtifact class."""
    
    def test_create_dataframe_artifact(self):
        """Test creating artifact with pandas DataFrame."""
        df = pd.DataFrame({
            'a': [1, 2, 3],
            'b': [4, 5, 6]
        })
        
        artifact = DataArtifact(data=df, name="test_df")
        
        self.assertEqual(artifact.name, "test_df")
        self.assertEqual(artifact.metadata.format, DataFormat.PANDAS_DATAFRAME)
        self.assertEqual(artifact.metadata.row_count, 3)
        self.assertEqual(artifact.metadata.column_count, 2)
        self.assertIsNotNone(artifact.metadata.checksum)
    
    def test_create_dict_artifact(self):
        """Test creating artifact with dictionary."""
        data = {'key1': 'value1', 'key2': 'value2'}
        
        artifact = DataArtifact(data=data, name="test_dict")
        
        self.assertEqual(artifact.metadata.format, DataFormat.DICT)
        self.assertEqual(artifact.metadata.row_count, 2)
    
    def test_create_list_artifact(self):
        """Test creating artifact with list."""
        data = [1, 2, 3, 4, 5]
        
        artifact = DataArtifact(data=data, name="test_list")
        
        self.assertEqual(artifact.metadata.format, DataFormat.LIST)
        self.assertEqual(artifact.metadata.row_count, 5)
    
    def test_transform_artifact(self):
        """Test transforming artifact data."""
        df = pd.DataFrame({'a': [1, 2, 3]})
        artifact = DataArtifact(data=df, name="original")
        
        # Apply transformation
        new_artifact = artifact.transform(lambda x: x * 2)
        
        self.assertEqual(new_artifact.name, "original_transformed")
        self.assertTrue((new_artifact.data['a'] == [2, 4, 6]).all())
        self.assertIn("original", new_artifact.metadata.lineage)
    
    def test_lineage_tracking(self):
        """Test lineage tracking."""
        artifact = DataArtifact(data=[1, 2, 3], name="test")
        
        artifact.add_lineage("extractor_1")
        artifact.add_lineage("transformer_1")
        
        self.assertEqual(artifact.metadata.lineage, ["extractor_1", "transformer_1"])
    
    def test_tags(self):
        """Test adding tags to artifact."""
        artifact = DataArtifact(data={'a': 1}, name="test")
        
        artifact.add_tag("source", "api")
        artifact.add_tag("version", "1.0")
        
        self.assertEqual(artifact.metadata.tags["source"], "api")
        self.assertEqual(artifact.metadata.tags["version"], "1.0")
    
    def test_conversions(self):
        """Test data format conversions."""
        # DataFrame to dict
        df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        artifact = DataArtifact(data=df, name="test")
        
        dict_data = artifact.as_dict()
        self.assertIsInstance(dict_data, list)
        self.assertEqual(len(dict_data), 2)
        
        # Dict to DataFrame
        artifact2 = DataArtifact(data={'a': 1, 'b': 2}, name="test2")
        df_data = artifact2.as_dataframe()
        self.assertIsInstance(df_data, pd.DataFrame)
        self.assertEqual(len(df_data), 1)
        
        # List to DataFrame
        artifact3 = DataArtifact(data=[{'a': 1}, {'a': 2}], name="test3")
        df_data = artifact3.as_dataframe()
        self.assertIsInstance(df_data, pd.DataFrame)
        self.assertEqual(len(df_data), 2)


class TestArtifactStore(unittest.TestCase):
    """Test ArtifactStore class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.store = ArtifactStore(base_path=Path(self.temp_dir))
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)
    
    def test_put_and_get(self):
        """Test storing and retrieving artifacts."""
        df = pd.DataFrame({'a': [1, 2, 3]})
        artifact = DataArtifact(data=df, name="test_artifact")
        
        self.store.put(artifact)
        
        retrieved = self.store.get("test_artifact")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.name, "test_artifact")
        self.assertTrue((retrieved.data['a'] == [1, 2, 3]).all())
    
    def test_exists(self):
        """Test checking artifact existence."""
        artifact = DataArtifact(data=[1, 2, 3], name="test")
        
        self.assertFalse(self.store.exists("test"))
        
        self.store.put(artifact)
        
        self.assertTrue(self.store.exists("test"))
    
    def test_list_artifacts(self):
        """Test listing artifacts."""
        artifact1 = DataArtifact(data={'a': 1}, name="artifact1")
        artifact2 = DataArtifact(data={'b': 2}, name="artifact2")
        
        self.store.put(artifact1)
        self.store.put(artifact2)
        
        artifacts = self.store.list_artifacts()
        self.assertEqual(len(artifacts), 2)
        self.assertIn("artifact1", artifacts)
        self.assertIn("artifact2", artifacts)
    
    def test_delete(self):
        """Test deleting artifacts."""
        artifact = DataArtifact(data=[1, 2, 3], name="test")
        self.store.put(artifact)
        
        self.assertTrue(self.store.exists("test"))
        
        self.store.delete("test")
        
        self.assertFalse(self.store.exists("test"))
    
    def test_persistence(self):
        """Test artifact persistence to disk."""
        df = pd.DataFrame({'a': [1, 2, 3]})
        artifact = DataArtifact(
            data=df,
            name="persistent",
            persist=True,
            persist_path=Path(self.temp_dir) / "persistent"
        )
        
        artifact.save_to_disk()
        
        # Load from disk
        loaded = DataArtifact.load_from_disk(
            "persistent",
            Path(self.temp_dir) / "persistent"
        )
        
        self.assertEqual(loaded.name, "persistent")
        self.assertTrue((loaded.data['a'] == [1, 2, 3]).all())
        self.assertEqual(loaded.metadata.format, DataFormat.PANDAS_DATAFRAME)


if __name__ == "__main__":
    unittest.main()