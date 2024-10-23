import json
import os
from pathlib import Path
import re
import requests
import warnings
import tarfile
import zipfile
from tabulate import tabulate


def validate_url(url):
    """validates if URL is formatted correctly

    Returns:
        bool: True is URL is acceptable False if not acceptable
    """
    regex = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    return re.match(regex, url) is not None


def make_tarfile(output_file, source_dir):
    """tar a directory
    args
    -----
    output_file: path to output file
    source_dir: path to source directory

    returns
    -----
    tarred directory will be in output_file
    """
    with tarfile.open(output_file, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))


def make_zipfile(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file),
                       os.path.relpath(os.path.join(root, file),
                                       os.path.join(path, '..')))


class BearerAuth(requests.auth.AuthBase):
    """Bearer Authentication"""

    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


class Client(object):
    """Zenodo Client object

    Use this class to instantiate a zenodopy object
    to interact with your Zenodo account

        ```
        import zenodopy
        zeno = zenodopy.Client()
        zeno.help()
        ```

    Setup instructions:
        ```
        zeno.setup_instructions
        ```
    """

    def __init__(self, title=None, bucket=None, deposition_id=None, sandbox=None, token=None):
        """initialization method"""
        if sandbox:
            self._endpoint = "https://sandbox.zenodo.org/api"
            self._doi_pattern = r'^10\.5072/zenodo\.\d+$'
        else:
            self._endpoint = "https://zenodo.org/api"
            self._doi_pattern = r'^10\.5281/zenodo\.\d+$'

        self.title = title
        self.bucket = bucket
        self.deposition_id = deposition_id
        self.sandbox = sandbox
        self._token = self._read_from_config if token is None else token
        self._bearer_auth = BearerAuth(self._token)
        self.concept_id = None
        self.associated = False
        # 'metadata/prereservation_doi/doi'

    def __repr__(self):
        return f"zenodoapi('{self.title}','{self.bucket}','{self.deposition_id}')"

    def __str__(self):
        return f"{self.title} --- {self.deposition_id}"

    # ---------------------------------------------
    # hidden functions
    # ---------------------------------------------

    @staticmethod
    def _get_upload_types():
        """Acceptable upload types

        Returns:
            list: contains acceptable upload_types
        """
        return [
            "publication",
            "poster",
            "presentation",
            "dataset",
            "image",
            "video",
            "software",
            "lesson",
            "physicalobject",
            "other"
        ]

    @staticmethod
    def _read_config(path=None):
        """reads the configuration file

        Configuration file should be ~/.zenodo_token

        Args:
            path (str): location of the file with ACCESS_TOKEN

        Returns:
            dict: dictionary with API ACCESS_TOKEN
        """

        if path is None:
            print("You need to supply a path")

        full_path = os.path.expanduser(path)
        if not Path(full_path).exists():
            print(f"{path} does not exist. Please check you entered the correct path")

        config = {}
        with open(path) as file:
            for line in file.readlines():
                if ":" in line:
                    key, value = line.strip().split(":", 1)
                    if key in ("ACCESS_TOKEN", "ACCESS_TOKEN-sandbox"):
                        config[key] = value.strip()
        return config

    @property
    def _read_from_config(self):
        """reads the web3.storage token from configuration file
        configuration file is ~/.web3_storage_token
        Returns:
            str: ACCESS_TOKEN to connect to web3 storage
        """
        if self.sandbox:
            dotrc = os.environ.get("ACCESS_TOKEN-sandbox", os.path.expanduser("~/.zenodo_token"))
        else:
            dotrc = os.environ.get("ACCESS_TOKEN", os.path.expanduser("~/.zenodo_token"))

        if os.path.exists(dotrc):
            config = self._read_config(dotrc)
            key = config.get("ACCESS_TOKEN-sandbox") if self.sandbox else config.get("ACCESS_TOKEN")
            return key
        else:
            print(' ** No token was found, check your ~/.zenodo_token file ** ')


    def get_all_depositions(self):
        """
        Retrieves all depositions for the client, but only the last version with all metadata.

        Returns:
            list: A list of dictionaries containing the latest version of each deposition with full metadata.
        """
        url = f"{self._endpoint}/deposit/depositions"
        params = {
            "access_token": self._token,
            "size": 1000,  # Adjust this value based on your needs
            "sort": "mostrecent",
            "all_versions": True
        }

        all_depositions = []
        concept_ids_processed = set()

        while True:
            response = requests.get(url, params=params)
            response.raise_for_status()
            depositions = response.json()

            if not depositions:
                break

            for deposition in depositions:
                concept_id = deposition['conceptrecid']
                if concept_id not in concept_ids_processed:
                    # This is the latest version of this deposition
                    all_depositions.append(deposition)
                    concept_ids_processed.add(concept_id)

            # Check if there are more pages
            links = response.links
            if 'next' not in links:
                break
            url = links['next']['url']

        return all_depositions

    

    def set_deposition(self, deposition_id=None):
        """
        Sets the client to a specific deposition's latest version using a given deposition_id.

        Args:
            deposition_id (int): The ID of the deposition to set.

        Raises:
            ValueError: If no valid deposition is found for the given ID.
        """
        if not deposition_id:
            raise ValueError("You must provide a deposition_id.")

        # Retrieve the specific deposition by its ID
        deposition = self.get_deposition_by_id(deposition_id)

        # Get the concept ID from the retrieved deposition
        concept_id = deposition.get('conceptrecid')
        if not concept_id:
            raise ValueError(f"No concept ID found for deposition ID {deposition_id}.")

        # Retrieve the latest version of the deposition using concept ID
        url = f"{self._endpoint}/deposit/depositions"
        params = {
            "access_token": self._token,
            "q": f"conceptrecid:{concept_id}",
            "sort": "mostrecent",
            "size": 1
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        depositions = response.json()

        if not depositions:
            raise ValueError(f"No depositions found for concept ID {concept_id}.")
        
        latest_deposition = depositions[0]

        # Set class variables based on the latest version of the deposition
        self.title = latest_deposition['metadata'].get('title',None)
        self.bucket = latest_deposition['links'].get('bucket', 'N/A')
        
        self.deposition_id = latest_deposition['id']
        """
        Sets the client to a specific deposition's latest version using a given deposition_id.

        Args:
            deposition_id (int): The ID of the deposition to set.

        Raises:
            ValueError: If no valid deposition is found for the given ID.
        """
        if not deposition_id:
            raise ValueError("You must provide a deposition_id.")

        # Retrieve the specific deposition by its ID
        deposition = self.get_deposition_by_id(deposition_id)

        # Get the concept ID from the retrieved deposition
        concept_id = deposition.get('conceptrecid')
        if not concept_id:
            raise ValueError(f"No concept ID found for deposition ID {deposition_id}.")

        # Retrieve the latest version of the deposition using concept ID
        url = f"{self._endpoint}/deposit/depositions"
        params = {
            "access_token": self._token,
            "q": f"conceptrecid:{concept_id}",
            "sort": "mostrecent",
            "size": 1
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        depositions = response.json()

        if not depositions:
            raise ValueError(f"No depositions found for concept ID {concept_id}.")
        
        latest_deposition = depositions[0]

        # Set class variables based on the latest version of the deposition
        self.title = latest_deposition['metadata'].get('title',None)
        self.bucket = latest_deposition['links'].get('bucket', 'N/A')
        self.deposition_id = latest_deposition['id']
        self.concept_id = latest_deposition['conceptrecid']
        self.associated = True

    def unset_deposition(self):
        """
        Unsets the current deposition settings, resetting related attributes.
        """
        self.title = None
        self.bucket = None
        self.deposition_id = None
        self.concept_id = None
        self.associated = False

    def get_deposition_by_id(self, deposition_id):
        """
        Retrieves a specific deposition by its ID.

        Args:
            deposition_id (int): The ID of the deposition to retrieve.

        Returns:
            dict: A dictionary containing the full metadata of the deposition.
        """
        url = f"{self._endpoint}/deposit/depositions/{deposition_id}"
        params = {"access_token": self._token}

        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    

    def pretty_print_depositions(self, depositions=None):
        """
        Pretty prints all depositions with their metadata in a table format.

        Args:
            depositions (list): A list of deposition dictionaries to print.
        """
        if depositions is None: depositions = self.get_all_depositions()

        table_data = []

        for deposition in depositions:
            title = deposition['metadata'].get('title',None)
            cid = deposition['conceptrecid']
            dep_id = deposition['id']
            published = 'Yes' if deposition['submitted'] else 'No'
            doi = deposition.get('doi', 'N/A')
            
            # Check if this is the currently set deposition
            flag = '*' if self.deposition_id == dep_id else ''
            
            table_data.append([title, cid, dep_id, published, doi, flag])

        headers = ["Title", "CID", "ID", "Published", "DOI", "Set"]
        print(tabulate(table_data, headers=headers, tablefmt="pretty"))


    def create_new_deposition(self):
        """
        Creates a new deposition.

        Returns:
            dict: The newly created deposition data.
        """
        url = f"{self._endpoint}/deposit/depositions"
        headers = {"Content-Type": "application/json"}
        params = {"access_token": self._token}

        response = requests.post(url, params=params, json={}, headers=headers)
        response.raise_for_status()

        return response.json().get("id",None)

    def delete_deposition(self, deposition_id=None):
        """
        Deletes a deposition.

        Args:
            deposition_id (int): The ID of the deposition to delete.
        """

        if deposition_id is None : deposition_id = self.deposition_id

        url = f"{self._endpoint}/deposit/depositions/{deposition_id}"
        params = {"access_token": self._token}

        response = requests.delete(url, params=params)
        response.raise_for_status()

    def create_metadata(self, metadata):
        """
        Creates or updates metadata for a deposition.

        Args:
            deposition_id (int): The ID of the deposition.
            metadata (dict): The metadata to set.

        Returns:
            dict: The updated deposition data.
        """
        url = f"{self._endpoint}/deposit/depositions/{self.deposition_id}"
        headers = {"Content-Type": "application/json"}
        params = {"access_token": self._token}
        data = {"metadata": metadata}

        response = requests.put(url, params=params, data=json.dumps(data), headers=headers)
        response.raise_for_status()

        self.set_deposition(self.deposition_id)
        return response.json()

    def upload_file(self,file_path, remote_filename=None, file_id=None):
        """
        Uploads a file to a Zenodo deposition using PUT (if file_id is provided) or POST (for new files).

        Args:
            file_path (str): The local path of the file to upload.
            remote_filename (str, optional): The filename to use on Zenodo. If None, uses the local filename.
            file_id (str, optional): The ID of an existing file to update. If None, a new file will be created.

        Returns:
            dict: The uploaded file data, including the file_id.
        """
        deposition_id = self.deposition_id

        if remote_filename is None:
            remote_filename = os.path.basename(file_path)

        params = {"access_token": self._token}

        if file_id:
            # Update existing file using PUT
            url = f"{self._endpoint}/deposit/depositions/{deposition_id}/files/{file_id}"
            with open(file_path, "rb") as file:
                response = requests.put(url, params=params, data=file)
        else:
            # Upload new file using POST
            url = f"{self._endpoint}/deposit/depositions/{deposition_id}/files"
            with open(file_path, "rb") as file:
                data = {"name": remote_filename}
                files = {"file": file}
                response = requests.post(url, params=params, data=data, files=files)

        response.raise_for_status()
        file_data = response.json()

        return file_data

    def get_file_ids(self, deposition_id):
        """
        Retrieves the file IDs for all files in a deposition.

        Args:
            deposition_id (int): The ID of the deposition.

        Returns:
            dict: A dictionary mapping filenames to their file IDs.
        """
        deposition = self.get_deposition_by_id(deposition_id)
        return {file['filename']: file['id'] for file in deposition.get('files', [])}   

    def publish_deposition(self):
        """
        Publishes a deposition.

        Args:
            deposition_id (int): The ID of the deposition to publish.

        Returns:
            dict: The published deposition data.
        """
        url = f"{self._endpoint}/deposit/depositions/{self.deposition_id}/actions/publish"
        params = {"access_token": self._token}

        response = requests.post(url, params=params)
        response.raise_for_status()
        return response.json()

    def modify_metadata(self, metadata_updates):
        """
        Modifies metadata of a deposition (published or not).

        Args:
            deposition_id (int): The ID of the deposition.
            metadata_updates (dict): The metadata fields to update.

        Returns:
            dict: The updated deposition data.
        """
        # First, get the current metadata
        current_deposition = self.get_deposition_by_id(self.deposition_id)

        current_metadata = current_deposition['metadata']

        # Update the metadata
        current_metadata.update(metadata_updates)

        # Use the create_metadata method to update
        return self.create_metadata(current_metadata)

    def update_file(self, file_id, new_file_path):
        """
        Updates a file in a deposition (published or not).

        Args:
            deposition_id (int): The ID of the deposition.
            file_id (str): The ID of the file to update.
            new_file_path (str): The path to the new file.

        Returns:
            dict: The updated file data.
        """
        # First, delete the existing file
        delete_url = f"{self._endpoint}/deposit/depositions/{self.deposition_id}/files/{file_id}"
        params = {"access_token": self._token}
        requests.delete(delete_url, params=params).raise_for_status()

        # Then, upload the new file
        return self.upload_file(self.deposition_id, new_file_path)

    def get_file_ids(self):
        """
        Retrieves the file IDs for all files in a deposition.

        Args:
            deposition_id (int): The ID of the deposition.

        Returns:
            dict: A dictionary mapping filenames to their file IDs.
        """

        deposition = self.get_deposition_by_id(self.deposition_id)
        return {file['filename']: file['id'] for file in deposition.get('files', [])}

if __name__ == '__main__':
    zeno = Client(sandbox=True)

    # Create a new deposition
    deposition_id = zeno.create_new_deposition()
    
    # Create metadata
    metadata = {
        'title': 'My New Dataset',
        'description': 'This is a test dataset',
        'creators': [{'name': 'Doe, John', 'affiliation': 'Zenodo'}]
    }


    zeno.set_deposition(deposition_id)

    zeno.create_metadata(metadata)

    # Upload a file
    zeno.upload_file('/tmp/eos.zip', 'remote_filename.zip')

    # Publish the deposition
    #zeno.publish_deposition()

    # Modify metadata (even after publishing)
    zeno.modify_metadata({'title': 'Updated Dataset Title'})

    # Update a file (you need the file_id, which you can get from the deposition details)
    file_id = '...'  # You need to retrieve this
    file_ids = zeno.get_file_ids()
    for filename, file_id in file_ids.items():
        print(f"File: {filename}, ID: {file_id}")
        zeno.update_file(file_id, '/tmp/eos2.zip')


    # Delete a deposition (only works for unpublished depositions)
    zeno.delete_deposition(deposition_id)