import boto3

def get_second_string_from_s3_file(bucket_name, object_key):
    # Initialize the S3 client
    s3_client = boto3.client('s3')

    try:
        # Download the file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        line = response['Body'].read().decode('utf-8').strip()

        # Split the line by comma and get the second string
        strings = line.split(',')
        if len(strings) >= 2:
            second_string = strings[1].strip()  # Remove leading/trailing whitespace
            return second_string
        else:
            print("Invalid file format: Unable to find second string separated by comma")
            return None
    except Exception as e:
        print(f"Error reading file from S3: {e}")
        return None

# Replace 'your_bucket_name' with the name of your S3 bucket
bucket_name = 'your_bucket_name'

# Replace 'your_object_key' with the key (path) of the file in S3
object_key = 'your_object_key'

# Call the function to get the second string from the file
second_string = get_second_string_from_s3_file(bucket_name, object_key)
print("Second string:", second_string)

import unittest
from unittest.mock import MagicMock, patch
from your_module import get_second_string_from_s3_file

class TestGetSecondStringFromS3File(unittest.TestCase):

    @patch('boto3.client')
    def test_valid_file_format(self, mock_s3_client):
        # Mock the get_object method to return a single line with two strings separated by comma
        mock_response = {
            'Body': MagicMock(),
            'Body.return_value.read.return_value.decode.return_value': 'first_string,second_string'
        }
        mock_s3_client.return_value.get_object.return_value = mock_response

        # Call the function to get the second string from the mock file
        second_string = get_second_string_from_s3_file('test_bucket', 'test_object_key')

        # Assert that the correct second string is returned
        self.assertEqual(second_string, 'second_string')

    @patch('boto3.client')
    def test_invalid_file_format(self, mock_s3_client):
        # Mock the get_object method to return a single line with only one string
        mock_response = {
            'Body': MagicMock(),
            'Body.return_value.read.return_value.decode.return_value': 'only_one_string'
        }
        mock_s3_client.return_value.get_object.return_value = mock_response

        # Call the function to get the second string from the mock file
        second_string = get_second_string_from_s3_file('test_bucket', 'test_object_key')

        # Assert that None is returned for invalid file format
        self.assertIsNone(second_string)

if __name__ == '__main__':
    unittest.main()
