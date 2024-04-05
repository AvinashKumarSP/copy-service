# service.py
import logging
from aiobotocore.session import AioSession

logger = logging.getLogger(__name__)

async def copy_s3_data(source_bucket: str, source_prefix: str, dest_bucket: str, dest_prefix: str):
    """
    Copies all files from a source S3 bucket folder to a destination S3 bucket folder.

    Args:
        source_bucket (str): The source S3 bucket name.
        source_prefix (str): The source S3 bucket prefix (folder).
        dest_bucket (str): The destination S3 bucket name.
        dest_prefix (str): The destination S3 bucket prefix (folder).

    Raises:
        Exception: If an error occurs during the S3 copy operation.

    """
    try:
        async with AioSession() as session:
            async with session.create_client('s3') as s3_client:
                copy_source = {'Bucket': source_bucket, 'Key': f"{source_prefix}/*"}
                dest_prefix = dest_prefix.rstrip('/') + '/'
                await s3_client.copy_object(
                    Bucket=dest_bucket,
                    Key=dest_prefix,
                    CopySource=copy_source
                )
                logger.info(f"Copy operation from {source_bucket}/{source_prefix}/* to {dest_bucket}/{dest_prefix} successful.")
    except Exception as e:
        logger.error(f"An error occurred during S3 copy operation: {str(e)}")
        raise

# test_service.py
import asyncio
from unittest.mock import MagicMock, patch
import pytest
from aiobotocore.session import AioSession
from service import copy_s3_data

@pytest.mark.asyncio
async def test_copy_folder_success():
    source_bucket = "source-bucket"
    source_prefix = "source-folder"
    dest_bucket = "dest-bucket"
    dest_prefix = "dest-folder"
    mock_s3_client = MagicMock()
    mock_s3_client.copy_object.return_value = asyncio.Future()
    mock_s3_client.copy_object.return_value.set_result({})
    with patch.object(AioSession, "create_client", return_value=mock_s3_client):
        await copy_s3_data(source_bucket, source_prefix, dest_bucket, dest_prefix)
    mock_s3_client.copy_object.assert_called_once_with(
        Bucket=dest_bucket,
        Key=dest_prefix.rstrip('/') + '/',
        CopySource={'Bucket': source_bucket, 'Key': f"{source_prefix}/*"}
    )

@pytest.mark.asyncio
async def test_copy_folder_exception():
    source_bucket = "source-bucket"
    source_prefix = "source-folder"
    dest_bucket = "dest-bucket"
    dest_prefix = "dest-folder"
    mock_s3_client = MagicMock()
    mock_s3_client.copy_object.return_value = asyncio.Future()
    mock_s3_client.copy_object.return_value.set_exception(Exception("Connection error"))
    with patch.object(AioSession, "create_client", return_value=mock_s3_client):
        with pytest.raises(Exception):
            await copy_s3_data(source_bucket, source_prefix, dest_bucket, dest_prefix)

# models.py
from pydantic import BaseModel

class S3CopyRequest(BaseModel):
    """
    Represents the request model for initiating an S3 copy operation.
    """
    source_bucket: str
    source_key: str
    dest_bucket: str
    dest_key: str

# test_models.py
from models import S3CopyRequest

def test_s3_copy_request_model():
    request_data = {
        "source_bucket": "source-bucket",
        "source_key": "source-key",
        "dest_bucket": "dest-bucket",
        "dest_key": "dest-key"
    }
    request = S3CopyRequest(**request_data)
    assert request.source_bucket == request_data["source_bucket"]
    assert request.source_key == request_data["source_key"]
    assert request.dest_bucket == request_data["dest_bucket"]
    assert request.dest_key == request_data["dest_key"]

# routes.py
import logging
from fastapi import APIRouter, HTTPException, status
from service import copy_s3_data
from models import S3CopyRequest

logger = logging.getLogger(__name__)

router = APIRouter()

@router.post("/copy-s3-data/", status_code=status.HTTP_202_ACCEPTED)
async def initiate_copy_s3_data(request: S3CopyRequest):
    """
    Initiates the S3 copy operation.

    Args:
        request (S3CopyRequest): The request containing source and destination S3 bucket paths.

    Returns:
        dict: A message indicating successful initiation of the copy operation.

    """
    try:
        await copy_s3_data(request.source_bucket, request.source_key, request.dest_bucket, request.dest_key)
    except Exception as e:
        logger.exception("Error occurred while initiating S3 copy operation.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred. Please try again later.")
    return {"message": "Copy operation initiated successfully."}

# test_routes.py
import json
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch
import pytest
from main import app
from models import S3CopyRequest

client = TestClient(app)

@pytest.mark.asyncio
async def test_initiate_copy_s3_data_success():
    request_data = {
        "source_bucket": "source-bucket",
        "source_key": "source-key",
        "dest_bucket": "dest-bucket",
        "dest_key": "dest-key"
    }
    response = client.post("/copy-s3-data/", json=request_data)
    assert response.status_code == 202

@pytest.mark.asyncio
async def test_initiate_copy_s3_data_exception():
    request_data = {
        "source_bucket": "source-bucket",
        "source_key": "source-key",
        "dest_bucket": "dest-bucket",
        "dest_key": "dest-key"
    }
    with patch("routes.copy_s3_data", side_effect=Exception("Internal Server Error")):
        response = client.post("/copy-s3-data/", json=request_data)
    assert response.status_code == 500

# main.py
import logging
from fastapi import FastAPI
from routes import router

logger = logging.getLogger(__name__)

app = FastAPI()
app.include_router(router)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run("main:app", host="0.0.0.0", port=8000, workers=4)

# test_main.py
def test_main():
    import main  # noqa



    import s3fs
import fastparquet

async def aggregate_row_count_s3_folder(bucket: str, prefix: str) -> int:
    fs = s3fs.S3FileSystem(anon=False)
    total_row_count = 0
    # List all Parquet files in the S3 bucket with the specified prefix
    parquet_files = fs.glob(f"{bucket}/{prefix}/*.parquet")
    for file_path in parquet_files:
        # Open each Parquet file and read its metadata (without loading the entire file)
        with fs.open(file_path, "rb") as f:
            metadata = fastparquet.ParquetFile(f).metadata
            total_row_count += metadata.num_rows
    return total_row_count


import pytest
from unittest.mock import MagicMock, patch
from service import aggregate_row_count_s3_folder

@pytest.mark.asyncio
async def test_aggregate_row_count_s3_folder():
    # Mock the S3FileSystem and ParquetFile objects
    mock_fs = MagicMock()
    mock_open = MagicMock()
    mock_metadata = MagicMock()
    mock_metadata.num_rows = 100  # Mock metadata with 100 rows
    mock_parquet_file = MagicMock()
    mock_parquet_file.metadata = mock_metadata

    # Configure the mock objects to return the desired values
    mock_fs.glob.return_value = ["bucket/prefix/file1.parquet", "bucket/prefix/file2.parquet"]
    mock_fs.open.side_effect = [mock_open, mock_open]  # Mock opening Parquet files
    mock_parquet_file = MagicMock()
    mock_parquet_file.metadata = mock_metadata
    mock_open.__enter__.return_value = mock_parquet_file

    # Patch the s3fs.S3FileSystem and fastparquet.ParquetFile classes
    with patch("service.s3fs.S3FileSystem", return_value=mock_fs), \
         patch("service.fastparquet.ParquetFile", return_value=mock_parquet_file):
        total_row_count = await aggregate_row_count_s3_folder("bucket", "prefix")

    assert total_row_count == 200  # Expecting 100 rows from each file


async def write_row_count_to_s3(bucket: str, prefix: str, row_count: int):
    # Get today's date in YYYYMMDD format
    today_date = datetime.datetime.now().strftime("%Y%m%d")

    # Prepare the file content
    file_content = f"{today_date}, {row_count}"

    # Define the file name
    file_name = f"{prefix}/{today_date}_row_count.txt"

    # Initialize an S3 client
    async with aiobotocore.get_session().create_client("s3") as s3_client:
        # Encode the file content using UTF-8 encoding
        encoded_content = file_content.encode('utf-8')

        # Upload the file to S3
        await s3_client.put_object(Bucket=bucket, Key=file_name, Body=encoded_content)


import pytest
from unittest.mock import MagicMock, patch
from service import write_row_count_to_s3

@pytest.mark.asyncio
async def test_write_row_count_to_s3():
    # Mock the aiobotocore.get_session().create_client method
    mock_client = MagicMock()
    mock_put_object = MagicMock()
    mock_client.put_object = mock_put_object

    # Patch the aiobotocore.get_session().create_client method
    with patch("service.aiobotocore.get_session") as mock_get_session:
        mock_get_session.return_value.__aenter__.return_value.create_client.return_value = mock_client

        # Call the function
        await write_row_count_to_s3("test_bucket", "test_prefix", 100)

    # Assert that put_object was called with the correct arguments
    mock_put_object.assert_called_once_with(Bucket="test_bucket", Key="test_prefix/row_count.txt", Body=b'20220406, 100')