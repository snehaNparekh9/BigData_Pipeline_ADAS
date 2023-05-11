from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from pyspark.sql import SparkSession

# Initialize Azure Blob Storage client
account_url = "<your_blob_storage_account_url>"
container_name = "<your_container_name>"
credential = DefaultAzureCredential()
blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
container_client = blob_service_client.get_container_client(container_name)

# Initialize Spark session
spark = SparkSession.builder.appName("ADASBigDataPipeline").getOrCreate()

# Define your data processing and analytics tasks using Spark
def process_data(data):
    # Apply your data processing logic
    processed_data = data

    # Apply your analytics and machine learning algorithms
    analyzed_data = processed_data

    return analyzed_data

try:
    # Iterate through the blobs in the container
    for blob in container_client.list_blobs():
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob.name)

        # Download the blob data
        blob_data = blob_client.download_blob().readall()

        # Perform data processing and analytics tasks
        analyzed_data = process_data(blob_data)

        # Save the analyzed data or perform further actions
        # e.g., write to another storage system, send to a database, etc.

except ResourceNotFoundError:
    print(f"Container '{container_name}' not found.")
