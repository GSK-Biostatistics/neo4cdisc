import os
import pandas as pd
from github import Github, UnknownObjectException, BadCredentialsException
import requests
from pathlib import Path

# ------------------- DATA LOAD -------------------
domains = ['DM', 'SUPPDM', 'AE', 'SUPPAE', 'VS']
# domains = ['ae', 'cm', 'dm', 'ds', 'ex', 'lb', 'mh', 'qs', 'relrec', 'sc', 'se', 'suppae', 'suppdm', 'suppds', 'supplb', 'sv', 'ta', 'te', 'ti', 'ts', 'tv', 'vs']
download_ok = []
download_error = []

def download_file_from_github(url_file, save_folder):
    url = f"https://github.com/phuse-org/phuse-scripts/raw/master/{url_file}"
    print("Download from url:", url)
    r = requests.get(url)
    # TODO: Get a proper temp location for downloaded files
    save_file = f"{save_folder}/{domain.lower()}.xpt"
    with open(save_file, 'wb') as f:
        f.write(r.content)
    return save_file

try:
    g = Github()
except BadCredentialsException:
    g = None
if g:
    save_folder = "/temp/data/sdtm/cdiscpilot01"
    Path(save_folder).mkdir(parents=True, exist_ok=True)
    assert os.path.exists(save_folder)

    repo = g.get_repo(full_name_or_id='phuse-org/phuse-scripts')
    for domain in domains:
        print("\n--------\nGetting domain:", domain)
        try:
            url_file = f"data/sdtm/cdiscpilot01/{domain.lower()}.xpt"
            # This is only here to check that the file exist in the repository
            # Actual file contents cannot be downloaded if file is >1M in size
            contents = repo.get_contents(path=url_file, ref='master')
            # This is what downloads the file
            save_file = download_file_from_github(url_file, save_folder)
            # TODO: Check if it is a xpt file?
            df = pd.read_sas(save_file)
            print(df.head())
            download_ok.append(domain)
        except UnknownObjectException:
            print("File not found")
            download_error.append(domain)
        except Exception as e:
            print("Error", e)
            download_error.append(domain)

print("\nDownloaded: ", download_ok)
print("\nError: ", download_error)
print("\nDone")
