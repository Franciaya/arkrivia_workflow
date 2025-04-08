# import os
# from databricks.sdk import WorkspaceClient
# from databricks.sdk.errors import DatabricksError

# # Initialize client
# w = WorkspaceClient()

# local_folder = "data/delta/patients_transformed"
# dbfs_folder = "dbfs:/user/francisbade1225@gmail.com/patients_transformed"  # Replace with your Databricks username

# # Upload each parquet file
# for file_name in os.listdir(local_folder):
#     if file_name.endswith(".parquet"):
#         local_file_path = os.path.join(local_folder, file_name)
#         dbfs_file_path = f"{dbfs_folder}/{file_name}"

#         try:
#             with open(local_file_path, "rb") as f:
#                 w.dbfs.upload(path=dbfs_file_path, src=f, overwrite=True)
#                 print(f"Uploaded {file_name} to {dbfs_file_path}")
#         except FileNotFoundError:
#             print(f"File not found: {local_file_path}")
#             break
#         except DatabricksError as e:
#             print(f"Upload failed for {file_name}: {e}")
#             break
#         except Exception as e:
#             print(f"Unexpected error during upload of {file_name}: {e}")
#             break

import os
import base64
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import files

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
            base64_content = base64.b64encode(file_content).decode('utf-8')

            # Upload the file to DBFS
            workspace_client.dbfs.put(path=dbfs_file_path, contents=base64_content, overwrite=True)
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