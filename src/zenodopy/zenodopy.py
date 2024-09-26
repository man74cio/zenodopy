import json
import os
from pathlib import Path
import re
import requests
import warnings
import tarfile
import zipfile


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
            "Publication",
            "Poster",
            "Presentation",
            "Dataset",
            "Image",
            "Video/Audio",
            "Software",
            "Lesson",
            "Physical object",
            "Other"
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

    def _get_depositions(self):
        """gets the current project deposition

        this provides details on the project, including metadata

        Returns:
            dict: dictionary containing project details
        """
        # get request, returns our response
        r = requests.get(f"{self._endpoint}/deposit/depositions",
                         auth=self._bearer_auth)
        if r.ok:
            return r.json()
        else:
            return r.raise_for_status()

    def _get_depositions_by_id(self, dep_id):
        """gets the deposition based on project id

        this provides details on the project, including metadata

        Args:
            dep_id (str): project deposition ID

        Returns:
            dict: dictionary containing project details
        """
        # get request, returns our response
        # if dep_id is not None:
        r = requests.get(f"{self._endpoint}/deposit/depositions/{dep_id}",
                         auth=self._bearer_auth)

        if r.ok:
            return r.json()
        else:
            return r.raise_for_status()

    def _get_depositions_files(self):
        """gets the file deposition

        ** not used, can safely be removed **

        Returns:
            dict: dictionary containing project details
        """
        # get request, returns our response
        r = requests.get(f"{self._endpoint}/deposit/depositions/{self.deposition_id}/files",
                         auth=self._bearer_auth)

        if r.ok:
            return r.json()
        else:
            return r.raise_for_status()

    #def _get_bucket_by_title(self, title=None):
    # removed because it does not work correctly and unused

    def _get_bucket_by_id(self, dep_id=None):
        """gets the bucket URL by project deposition ID

        This URL is what you upload files to

        Args:
            dep_id (str): project deposition ID

        Returns:
            str: the bucket URL to upload files to
        """
        # get request, returns our response
        r = requests.get(f"{self._endpoint}/deposit/depositions/{dep_id}",
                         auth=self._bearer_auth)

        if r.ok:
            return r.json()['links']['bucket']
        else:
            return r.raise_for_status()

    def _get_api(self):
        # get request, returns our response
        r = requests.get(f"{self._endpoint}", auth=self._bearer_auth)

        if r.ok:
            return r.json()
        else:
            return r.raise_for_status()

    # ---------------------------------------------
    # user facing functions/properties
    # ---------------------------------------------
    @property
    def setup_instructions(self):
        """instructions to setup zenodoPy
        """
        print(
            '''
            # ==============================================
            # Follow these steps to setup zenodopy
            # ==============================================
            1. Create a Zenodo account: https://zenodo.org/

            2. Create a personal access token
                2.1 Log into your Zenodo account: https://zenodo.org/
                2.2 Click on the drop down in the top right and navigate to "application"
                2.3 Click "new token" in "personal access token"
                2.4 Copy the token into ~/.zenodo_token using the following terminal command

                    { echo 'ACCESS_TOKEN: YOUR_KEY_GOES_HERE' } > ~/.zenodo_token

                2.5 Make sure this file was creates (tail ~/.zenodo_token)

            3. Now test you can access the token from Python

                import zenodopy
                zeno = zenodopy.Client()
                zeno._token # this should display your ACCESS_TOKEN
            '''
        )

    @property
    def list_projects(self):
        """list projects connected to the supplied ACCESS_KEY

        prints to the screen the "Project Name" and "ID"
        """
        tmp = self._get_depositions()

        if isinstance(tmp, list):
            print('--- Project Name '+ '-'*28 + ' ID ------ CID - Status --- Latest ----- Published ID ' +'-'*10)
            print('-'*109)
            for file in tmp:
                status = {}  # just to rename the file outputs and deal with exceptions
                if file['submitted']:
                    status['submitted'] = 'Yes'
                else:
                    status['submitted'] = 'No '

                concept_id = self.get_conceptid_from_depo(file['id'])
                status["latest"] = self._get_latest_record(file['id'])

                out_string = f"{file['title']:35s}  {file['id']:9d}    {concept_id:9s} {status['submitted']:3s}      {status['latest']:6s} "

                # associated deposition
                start_string = ' * ' if str(self.deposition_id) == str(file['id']) else '   '

                out_string =  start_string + out_string

                if 'doi' in file: out_string += f"  DOI: {file['doi']}"
                
                print(out_string)
        else:
            print(' ** need to setup ~/.zenodo_token file ** ')
        

    def _is_published(self,deposition_id=None):
        """
        Property to retrieve if the deposition is published.

        Returns:
            published True (False): deposition is published (no)
        """
        if deposition_id is None :   deposition_id = self.deposition_id

        url = f"{self._endpoint}/deposit/depositions/{deposition_id}"
        
        response = requests.get(url, auth=self._bearer_auth)
        
        if response.status_code == 200:
            deposition_data = response.json()
            if deposition_data['submitted']:
                return True
            else:
                return False
        else:
            print(f"Failed to retrieve deposition status. Status code: {response.status_code}")
            return None
    
    @property
    def is_published(self):
        return self._is_published()


    @property
    def list_files(self):
        """list files in current project

        prints filenames to screen
        """
        dep_id = self.deposition_id
        dep = self._get_depositions_by_id(dep_id)
        if dep is not None:
            print('Files')
            print('------------------------')
            for file in dep['files']:
                print(file['filename'])
        else:
            print(" ** the object is not pointing to a project. Use either .set_project() or .create_project() before listing files ** ")
            # except UserWarning:
            # warnings.warn("The object is not pointing to a project. Either create a project or explicity set the project'", UserWarning)

    def create_project(self, title=None, upload_type=None, description=None):
        """Creates a new project

        After a project is creates the zenodopy object
        willy point to the project

        title is required. If upload_type or description
        are not specified, then default values will be used

        Args:
            title (str): new title of project
            upload_type (str, optional): new upload type
            description (str, optional): new description
        """

        if upload_type is None:
            upload_types = self._get_upload_types()
            warnings.warn(f"upload_type not set, so defaulted to 'other', possible choices include {upload_types}",
                          UserWarning)
            upload_type = 'other'

        # get request, returns our response
        r = requests.post(f"{self._endpoint}/deposit/depositions",
                          auth=self._bearer_auth,
                          data=json.dumps({}),
                          headers={'Content-Type': 'application/json'})

        if r.ok:
            deposition_id = r.json()['id']

            self.change_metadata(dep_id=deposition_id,
                                 title=title,
                                 upload_type=upload_type,
                                 description=description,
                                 )

            self.deposition_id = r.json()['id']
            self.bucket = r.json()['links']['bucket']
            self.title = title
        else:
            print("** Project not created, something went wrong. Check that your ACCESS_TOKEN is in ~/.zenodo_token ")

    def set_project(self,dep_id: str):
        '''set the project by id'''

        # get all projects 
        projects = self._get_depositions_by_id(dep_id)

        if projects is not None:
            self.title = projects['title']
            self.bucket = self._get_bucket_by_id(dep_id)
            self.deposition_id = dep_id
            self.concept_id = projects["conceptrecid"]
        else:
            print(f' ** Deposition ID: {dep_id} does not exist in your projects  ** ')
       
    def change_metadata(self, dep_id=None,
                        title=None,
                        upload_type=None,
                        description=None,
                        creator=None,
                        **kwargs
                        ):
        """change projects metadata

        ** warning **
        This changes everything. If nothing is supplied then
        uses default values are used.

        For example. If you do not supply an upload_type
        then it will default to "other"

        Args:
            dep_id (str): deposition to change
            title (str): new title of project
            upload_type (str): new upload type
            description (str): new description
            **kwargs: dictionary to update default metadata

        Returns:
            dict: dictionary with new metadata
        """
        if upload_type is None:
            upload_type = 'other'

        if description is None:
            description = "description goes here"

        if creator is None:
            creator = "creator goes here"

        data = {
            "metadata": {
                "title": f"{title}",
                "upload_type": f"{upload_type}",
                "description": f"{description}",
                "creators": [{"name": f"{creator}"}]
            }
        }
        # update metadata with a new metadata dictionary
        data['metadata'].update(kwargs)

        url = f"{self._endpoint}/deposit/depositions/{dep_id}"
        print(url)
        r = requests.put(url,
                         auth=self._bearer_auth,
                         data=json.dumps(data),
                         headers={'Content-Type': 'application/json'})

        if r.ok:
            return r.json()
        else:
            return r.raise_for_status()

    def upload_file(self, file_path=None, publish=False):
        """upload a file to a project

        Args:
            file_path (str): name of the file to upload
            publish (bool): whether implement publish action or not
        """
        if file_path is None:
            print("You need to supply a path")

        if not Path(os.path.expanduser(file_path)).exists():
            print(f"{file_path} does not exist. Please check you entered the correct path")

        if self.bucket is None:
            print("You need to create a project with zeno.create_project() "
                  "or set a project zeno.set_project() before uploading a file")
        else:
            bucket_link = self.bucket

            with open(file_path, "rb") as fp:
                # text after last '/' is the filename
                filename = file_path.split('/')[-1]
                r = requests.put(f"{bucket_link}/{filename}",
                                 auth=self._bearer_auth,
                                 data=fp)
                print(f"{file_path} successfully uploaded!") if r.ok else print("Oh no! something went wrong")

            if publish:
                return self.publish()
    


    def update_zenodo_deposition(self,file_path, publish=False):
        deposition_id = self.deposition_id
        url = f"{self._endpoint}/deposit/depositions/"

        # Step 1: Create a new version of the deposition
        new_version_url = f"{url}/{deposition_id}/actions/newversion"
        response = requests.post(new_version_url, auth=self._bearer_auth)

        if response.status_code != 201:
            raise Exception(f"Failed to create new version. Status code: {response.status_code}")


        draft_data = response.json()
        draft_id = draft_data['links']['latest_draft'].split('/')[-1]

        # Step 2: Retrieve the file list of the new version
        files_url = f"{url}/{draft_id}/files"
        response = requests.get(files_url, auth=self._bearer_auth)

        if response.status_code != 200:
            raise Exception(f"Failed to retrieve file list. Status code: {response.status_code}")

        files = response.json() 

        # Step 3: Delete the old file (assuming there's only one file to update)
        if files:
            delete_url = f"{url}/{draft_id}/files/{files[0]['id']}"
            response = requests.delete(delete_url, auth=self._bearer_auth)

            if response.status_code != 204:
                raise Exception(f"Failed to delete old file. Status code: {response.status_code}")  

        # Step 4: Upload the new file
        with open(file_path, 'rb') as file:
            data = {'name': os.path.basename(file_path)}
            files = {'file': file}
            response = requests.post(files_url,auth=self._bearer_auth, data=data, files=files)

            if response.status_code != 201:
                raise Exception(f"Failed to upload new file. Status code: {response.status_code}")  


        # Step 5: Update metadata (increment version number)
        metadata_url = f"{url}/{draft_id}"
        response = requests.get(metadata_url, auth=self._bearer_auth)
        
        if response.status_code != 200:
            raise Exception(f"Failed to retrieve metadata. Status code: {response.status_code}")
        
        metadata = response.json()['metadata']
        
        # Increment version number
        current_version = metadata.get('version', '1.0.0')
        version_parts = current_version.split('.')
        version_parts[-1] = str(int(version_parts[-1]) + 1)
        new_version = '.'.join(version_parts)
        metadata['version'] = new_version

        # Update metadata
        response = requests.put(metadata_url, json={'metadata': metadata},
                                headers={'Content-Type': 'application/json'}, 
                                auth=self._bearer_auth)
        
        if response.status_code != 200:
            raise Exception(f"Failed to update metadata. Status code: {response.status_code}")

        # Step 6: Publish the new version
        if publish:
            #return self.publish()
            publish_url = f"{url}/{draft_id}/actions/publish"
            response = requests.post(publish_url, auth=self._bearer_auth)
        
            if response.status_code != 202:
                raise Exception(f"Failed to publish new version. Status code: {response.status_code}")

            published_data = response.json()
            print(f"Successfully updated deposition {deposition_id} with new file version.")
            print(f"New version DOI: {published_data['doi']}")
            print(f"Concept DOI: {published_data['conceptdoi']}")
        
            return published_data
        else:
            unpublished_data = response.json()
            return unpublished_data

    def upload_zip(self, source_dir=None, output_file=None, publish=False):
        """upload a directory to a project as zip

        This will:
            1. zip the directory,
            2. upload the zip directory to your project
            3. remove the zip file from your local machine

        Args:
            source_dir (str): path to directory to tar
            output_file (str): name of output file (optional)
                defaults to using the source_dir name as output_file
            publish (bool): whether implement publish action or not, argument for `upload_file`
        """
        # make sure source directory exists
        source_dir = os.path.expanduser(source_dir)
        source_obj = Path(source_dir)
        if not source_obj.exists():
            raise FileNotFoundError(f"{source_dir} does not exist")

        # acceptable extensions for outputfile
        acceptable_extensions = ['.zip']

        # use name of source_dir for output_file if none is included
        if not output_file:
            output_file = f"{source_obj.stem}.zip"
            output_obj = Path(output_file)
        else:
            output_file = os.path.expanduser(output_file)
            output_obj = Path(output_file)
            extension = ''.join(output_obj.suffixes)  # gets extension like .tar.gz
            # make sure extension is acceptable
            if extension not in acceptable_extensions:
                raise Exception(f"Extension must be in {acceptable_extensions}")
            # add an extension if not included
            if not extension:
                output_file = os.path.expanduser(output_file + '.zip')
                output_obj = Path(output_file)

        # check to make sure outputfile doesn't already exist
        if output_obj.exists():
            raise Exception(f"{output_obj} already exists. Please chance the name")

        # create tar directory if does not exist
        if output_obj.parent.exists():
            with zipfile.ZipFile(output_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
                make_zipfile(source_dir, zipf)
        else:
            os.makedirs(output_obj.parent)
            with zipfile.ZipFile(output_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
                make_zipfile(source_dir, zipf)

        # upload the file
        self.upload_file(file_path=output_file, publish=publish)

        # remove tar file after uploading it
        os.remove(output_file)

    def upload_tar(self, source_dir=None, output_file=None, publish=False):
        """upload a directory to a project

        This will:
            1. tar the directory,
            2. upload the tarred directory to your project
            3. remove the tar file from your local machine

        Args:
            source_dir (str): path to directory to tar
            output_file (str): name of output file (optional)
                defaults to using the source_dir name as output_file
            publish (bool): whether implemente publish action or not, argument for `upload_file`
        """
        # output_file = './tmp/tarTest.tar.gz'
        # source_dir = '/Users/gloege/test'

        # make sure source directory exists
        source_dir = os.path.expanduser(source_dir)
        source_obj = Path(source_dir)
        if not source_obj.exists():
            raise FileNotFoundError(f"{source_dir} does not exist")

        # acceptable extensions for outputfile
        acceptable_extensions = ['.tar.gz']

        # use name of source_dir for output_file if none is included
        if not output_file:
            output_file = f"{source_obj.stem}.tar.gz"
            output_obj = Path(output_file)
        else:
            output_file = os.path.expanduser(output_file)
            output_obj = Path(output_file)
            extension = ''.join(output_obj.suffixes)  # gets extension like .tar.gz
            # make sure extension is acceptable
            if extension not in acceptable_extensions:
                raise Exception(f"Extension must be in {acceptable_extensions}")
            # add an extension if not included
            if not extension:
                output_file = os.path.expanduser(output_file + '.tar.gz')
                output_obj = Path(output_file)

        # check to make sure outputfile doesn't already exist
        if output_obj.exists():
            raise Exception(f"{output_obj} already exists. Please chance the name")

        # create tar directory if does not exist
        if output_obj.parent.exists():
            make_tarfile(output_file=output_file, source_dir=source_dir)
        else:
            os.makedirs(output_obj.parent)
            make_tarfile(output_file=output_file, source_dir=source_dir)

        # upload the file
        self.upload_file(file_path=output_file, publish=publish)

        # remove tar file after uploading it
        os.remove(output_file)

    def update(self, source=None, output_file=None, publish=False):
        """update an existed record

        Args:
            source (str): path to directory or file to upload
            output_file (str): name of output file (optional)
                defaults to using the source_dir name as output_file
            publish (bool): whether implemente publish action or not, argument for `upload_file`
        """
        # create a draft deposition
        url_action = self._get_depositions_by_id(self.deposition_id)['links']['newversion']
        r = requests.post(url_action, auth=self._bearer_auth)
        r.raise_for_status()

        # parse current project to the draft deposition
        new_dep_id = r.json()['links']['latest_draft'].split('/')[-1]
        self.set_project(new_dep_id)

        # invoke upload funcions
        if not source:
            print("You need to supply a path")

        if Path(source).exists():
            if Path(source).is_file():
                self.upload_file(source, publish=publish)
            elif Path(source).is_dir():
                if not output_file:
                    self.upload_zip(source, publish=publish)
                elif '.zip' in ''.join(Path(output_file).suffixes).lower():
                    self.upload_zip(source, output_file, publish=publish)
                elif '.tar.gz' in ''.join(Path(output_file).suffixes).lower():
                    self.upload_tar(source, output_file, publish=publish)
        else:
            raise FileNotFoundError(f"{source} does not exist")


    def publish(self):
        """ Publish a record """
        try:
            deposition_data = self._get_depositions_by_id(self.deposition_id)
            url_action = deposition_data['links'].get('publish')

            if not url_action:
                print("Publish link not found. The deposition might not be ready for publishing.")
                return None

            if  self.is_published :     
                print("This deposition was published yet. ")
                print("To publish it again please remove 'doi' entry from metadata.")
                return None

            # Try to publish
            r = requests.post(url_action, auth=self._bearer_auth)
            r.raise_for_status()

            print("Deposition published successfully!")
            return r.json()

        except requests.exceptions.HTTPError as err:
            print(f"HTTP error occurred: {err}")
            print(f"Response: {r.text}")  # For more detailed error information
        except KeyError:
            print("KeyError: Failed to retrieve 'publish' link. Check if the deposition is ready for publication.")
        except Exception as e:
            print(f"An error occurred: {e}")

    def download_file(self, filename=None, dst_path=None):
        """download a file from project

        Args:
            filename (str): name of the file to download
            dst_path (str): destination path to download the data (default is current directory)
        """
        if filename is None:
            print(" ** filename not supplied ** ")

        bucket_link = self.bucket

        if bucket_link is not None:
            if validate_url(bucket_link):
                r = requests.get(f"{bucket_link}/{filename}",
                                 auth=self._bearer_auth)

                # if dst_path is not set, set download to current directory
                # else download to set dst_path
                if dst_path:
                    if os.path.isdir(dst_path):
                        filename = dst_path + '/' + filename
                    else:
                        raise FileNotFoundError(f'{dst_path} does not exist')

                if r.ok:
                    with open(filename, 'wb') as f:
                        f.write(r.content)
                else:
                    print(f" ** Something went wrong, check that {filename} is in your poject  ** ")

            else:
                print(f' ** {bucket_link}/{filename} is not a valid URL ** ')

    def _is_doi(self, string=None):
        """
        #Zenodo DOI pattern: 10.5281/zenodo.XXXXXXX
        #Zenodo sandbox DOI pattern: 10.5072/zenodo.XXXXXXX
        
        # Check if the string matches the Zenodo DOI pattern
        """
        if re.match(self._doi_pattern, string):
            return True
        else:
            return False
        
    def _get_record_id_from_doi(self, doi=None):
        """return the record id for given doi

        Args:
            doi (string, optional): the zenodo doi. Defaults to None.

        Returns:
            str: the record id from the doi (just the last numbers)
        """
        return doi.split('.')[-1]

    def get_urls_from_doi(self, doi=None):
        """the files urls for the given doi

        Args:
            doi (str): the doi you want the urls from. Defaults to None.

        Returns:
            list: a list of the files urls for the given doi
        """
        if self._is_doi(doi):
            record_id = self._get_record_id_from_doi(doi)
        else:
            print(f"{doi} must be of the form: 10.5281/zenodo.[0-9]+")

        # get request (do not need to provide access token since public
        r = requests.get(f"{self._endpoint}/records/{record_id}")  # params={'access_token': ACCESS_TOKEN})
        return [f['links']['self'] for f in r.json()['files']]

    def _get_latest_record(self, record_id=None):
        """return the latest record id for given record id

        Args:
            record_id (str or int): the record id you known. Defaults to None.

        Returns:
            str: the latest record id or 'None' if not found
        """
        try:
            record = self._get_depositions_by_id(record_id)['links']['latest'].split('/')[-1]
        except:
            record = 'None'
        return record

    def delete_file(self, filename=None):
        """delete a file from a project

        Args:
            filename (str): the name of file to delete
        """
        bucket_link = self.bucket

        # with open(file_path, "rb") as fp:
        _ = requests.delete(f"{bucket_link}/{filename}",
                            auth=self._bearer_auth)

    def _delete_project(self, dep_id=None):
        """delete a project from repository by ID

        Args:
            dep_id (str): The project deposition ID
        """
        print('')
        # if input("are you sure you want to delete this project? (y/n)") == "y":
        # delete requests, we are deleting the resource at the specified URL
        r = requests.delete(f'{self._endpoint}/deposit/depositions/{dep_id}',
                            auth=self._bearer_auth)
        # response status
        print(r.status_code)

        # reset class variables to None
        self.title = None
        self.bucket = None
        self.deposition_id = None
        # else:
        #    print(f'Project title {self.title} is still available.')


    def _set_edit(self,dep_id=None):
        """Set the edit mode if the deposition is published
        
        Args:
            dep_id (str): The project deposition ID
        """
        if dep_id is None :
                dep_id = self.deposition_id
        if self.is_published :     
            url = f"{self._endpoint }/deposit/depositions/{dep_id}"
            r =  requests.post(f"{url}/actions/edit",auth=self._bearer_auth)


    def get_conceptid_from_depo(self,dep_id=None):
        """
        Retrieves the concept recid of a deposition on Zenodo.

        """
        if dep_id is None : dep_id = self.deposition_id

        url = f"{self._endpoint}/deposit/depositions/{dep_id}"

        # Fetch the existing metadata
        response = requests.get(url, auth=self._bearer_auth)

        if response.status_code == 200:
            return response.json()["conceptrecid"]
        else:
            print(f"Failed to fetch concept recid. Status code: {response.status_code}")
            return None


    def get_depo_ids(self,concept_id,all=True):
        # get the list of deposition_id associated with the concept.id
        params = {
            "q": f"conceptrecid:{concept_id}",
            "size": 100,  # Adjust this value based on the expected number of records
            "sort": "version",
        }
        if all : params["all_versions"] = True

        deposit_ids = []
        url = f'{self._endpoint}/records'
        response = requests.get(url, auth=self._bearer_auth, params=params)
        if response.status_code == 200:
            data = response.json()
            for hit in data.get("hits", {}).get("hits", []):
                deposit_id = hit.get("id")
                if deposit_id:
                    deposit_ids.append(deposit_id)
        else:
            print(f"Error: {response.status_code} - {response.text}")
        return deposit_ids

    def get_last_depo_id(self,concept_id):
        return self.get_depo_ids(concept_id,all=False)


    def _get_metadata(self,dep_id: str=None):
        """
        Retrieves the current metadata of a deposition on Zenodo.

        Args:
            dep_id (str): The ID of the deposition.

        Returns:
            response (dict): The current metadata from the Zenodo API.
        """
        if dep_id is None :
            dep_id = self.deposition_id
        url = f"{self._endpoint}/deposit/depositions/{dep_id}"

        # Fetch the existing metadata
        response = requests.get(url, auth=self._bearer_auth)

        if response.status_code == 200:
            return response.json()["metadata"]
        else:
            print(f"Failed to fetch metadata. Status code: {response.status_code}")
            return None


    def _set_metadata(self, new_metadata: dict, dep_id: str=None):
        """
        Modifies the metadata of a deposition on Zenodo, merging new metadata with the existing one.

        Args:
            dep_id (str): The ID of the deposition to modify.
            new_metadata (dict): The new metadata fields to update.

        Returns:
            response (dict): The response from the Zenodo API.
        """
        if dep_id is None :   dep_id = self.deposition_id
        print(f"Setting metadata for deposition : {dep_id}") 

        # First, retrieve the current metadata
        current_metadata = self._get_metadata(dep_id)
       
        if current_metadata is None:
                current_metadata = new_metadata
        else:
            # Merge the current metadata w  ith the new metadata
            current_metadata.update(new_metadata)
        
        url = f"{self._endpoint}/deposit/depositions/{dep_id}"
        
        data = json.dumps({"metadata": current_metadata})
        print(data)
        print(url)
        # Update the metadata without overwriting everything
        response = requests.put(url, 
                                auth=self._bearer_auth,
                                data=data,
                                headers={'Content-Type': 'application/json'})
        
        # Check if the request was successful
        if response.status_code == 200:
            print("Metadata updated successfully!")
            return response.json()
        else:
            print(f"Failed to update metadata. Status code: {response.status_code}")
            return response.json()
            