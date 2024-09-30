# tests/test_pipeline.py

import unittest
import pandas as pd
from cryptography.fernet import Fernet
from pipeline import encrypt_data, is_valid_url, sanitize_table_name

class TestPipelineFunctions(unittest.TestCase):
    
    def setUp(self):
        # Set up test data for encryption
        self.data = pd.DataFrame({
            'Name': ['John Doe', 'Jane Doe'],
            'Referee': ['Ref1', 'Ref2']
        })
        # Encryption key setup
        self.encryption_key = Fernet.generate_key()
        self.cipher_suite = Fernet(self.encryption_key)
    
    def test_encrypt_data(self):
        # Test encrypt_data function
        sensitive_columns = ['Referee']
        encrypted_df = encrypt_data(self.data.copy(), sensitive_columns)
        
        # Check that sensitive columns are encrypted
        self.assertNotEqual(self.data['Referee'].iloc[0], encrypted_df['Referee'].iloc[0])
        self.assertNotEqual(self.data['Referee'].iloc[1], encrypted_df['Referee'].iloc[1])
        
        # Check that non-sensitive columns remain unchanged
        self.assertEqual(self.data['Name'].iloc[0], encrypted_df['Name'].iloc[0])
        self.assertEqual(self.data['Name'].iloc[1], encrypted_df['Name'].iloc[1])
    
    def test_is_valid_url(self):
        # Test URL validation function
        self.assertTrue(is_valid_url("https://example.com"))
        self.assertFalse(is_valid_url("invalid-url"))
        
        # Edge cases for URL validation
        self.assertFalse(is_valid_url(""))
        self.assertFalse(is_valid_url("ftp://example.com"))

    def test_sanitize_table_name(self):
        # Test table name sanitization
        self.assertEqual(sanitize_table_name('Test-File 2020.csv'), 'Test_File_2020')
        self.assertEqual(sanitize_table_name('My/File:Name.txt'), 'My_File_Name')

if __name__ == '__main__':
    unittest.main()
