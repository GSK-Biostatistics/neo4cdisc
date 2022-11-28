import os
import requests
from github import Github, UnknownObjectException, BadCredentialsException
from pathlib import Path

def download_file_from_github(domain, url_file, save_folder, file_name):
    url = f"https://github.com/phuse-org/phuse-scripts/raw/master/{url_file}"
    print("Download from url:", url)
    r = requests.get(url)
    save_file = f"{save_folder}/{file_name}"
    with open(save_file, 'wb') as f:
        f.write(r.content)
    return save_file


def get_cdisc_pilot_data(domains):
    try:
        g = Github()
    except BadCredentialsException:
        g = None
    if g:
        downloads = {'ok': [], 'error': []}
        save_folder = "temp/data/sdtm/cdiscpilot01"
        Path(save_folder).mkdir(parents=True, exist_ok=True)
        assert os.path.exists(save_folder)

        repo = g.get_repo(full_name_or_id='phuse-org/phuse-scripts')
        for domain in domains:
            print("\n--------\nGetting domain:", domain)
            file_name = f"{domain.lower()}.xpt"
            try:
                url_file = f"data/sdtm/cdiscpilot01/{file_name}"
                # N.B! This is only here to check that the file exist in the repository.
                # If the file is larger then >1M the github API will not decode the file and you get a "corrupt" file.
                contents = repo.get_contents(path=url_file, ref='master')
                assert contents
                # This is what downloads the file
                download_file_from_github(domain, url_file, save_folder, file_name)
                # TODO: Check if it is a xpt file?
                # df = pd.read_sas(save_file)
                # print(df.head())
                downloads['ok'].append({'folder': save_folder, 'file': file_name})
            except UnknownObjectException:
                print("File not found")
                downloads['error'].append(f"File not found: {url_file}")
            except Exception as e:
                print("Error", e)
                downloads['error'].append(f"Error: {url_file} {e}")
        return downloads