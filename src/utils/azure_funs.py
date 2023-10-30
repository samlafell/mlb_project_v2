# For azure connection
from dotenv import load_dotenv

##### Load the the environment variables #####
import os
load_dotenv()

# Get environment variables
account_name = os.getenv('ACCOUNT_NAME')
storage_account_key = os.getenv('STORAGE_ACCOUNT_KEY')
container = os.getenv('CONTAINER')

# Set Azure details
def set_azure_details(path:str):
    """
    Sets the Azure details for the given path.
    """
    storage_options = {
        "AZURE_STORAGE_ACCOUNT_NAME":account_name,
        "AZURE_STORAGE_ACCOUNT_KEY": storage_account_key,
        "AZURE_CONTAINER_NAME":container
    }

    full_path = f'abfs://{container}/{path}'
    return full_path, storage_options