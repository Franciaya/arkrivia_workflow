
import os
import base64
from databricks.sdk import WorkspaceClient
# from databricks.sdk.service import files

# Create a WorkspaceClient instance
workspace_client = WorkspaceClient()

# Define local folder and DBFS folder
local_folder = "data/delta/patients_transformed"  # Path to local folder containing your Parquet files
dbfs_folder = "dbfs:/dbfs/my-folder"  # The DBFS destination folder where the files will be uploaded

# Loop through the local folder and upload Parquet files
for file_name in os.listdir(local_folder):
    if file_name.endswith(".parquet"):  # Check if the file is a Parquet file
        local_file_path = os.path.join(local_folder, file_name)
        dbfs_file_path = f"{dbfs_folder}/{file_name}"

        try:
            # Open the local file and read the content
            with open(local_file_path, "rb") as f:
                file_content = f.read()

            # Convert the file content from bytes to a base64 encoded string
            base64_content = base64.b64encode(file_content).decode("utf-8")

            # Upload the file to DBFS
            workspace_client.dbfs.put(
                path=dbfs_file_path, contents=base64_content, overwrite=True
            )
            print(f"Successfully uploaded {file_name} to {dbfs_file_path}")

        except FileNotFoundError:
            print(f"Error: The file {file_name} was not found in the local folder.")
            break
        except PermissionError:
            print(f"Error: Permission denied while accessing {file_name}.")
            break
        except Exception as e:
            print(f"An unexpected error occurred while uploading {file_name}: {e}")
            break
