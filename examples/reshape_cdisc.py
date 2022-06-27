import os
import time
from cdisc_model_managers.cdisc_standard_loader import CdiscStandardLoader
from model_managers.model_manager import ModelManager
from data_loaders import file_data_loader
from model_appliers import ModelApplier
from data_providers import DataProvider
from github import Github, UnknownObjectException, BadCredentialsException
import requests
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
        save_folder = "/temp/data/sdtm/cdiscpilot01"
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


standards_folder = "cdisc_data"
standards_model = "SDTM_v1.4.csv"
standards_file = "SDTMIG_v3.2.csv"
sdtm_terminology = "CT2022Q1.csv"

csl = CdiscStandardLoader(standards_folder=standards_folder, sdtm_file=standards_model, sdtmig_file=standards_file, terminology_file=sdtm_terminology)
mm = ModelManager(rdf=True)
ma = ModelApplier(mode="schema_CLASS")
fdl = file_data_loader.FileDataLoader()
standard_label = os.path.basename(csl.sdtmig_file)

csl.clean_slate()
csl.load_standard()

mm.generate_excel_based_model()

# REMOVING data
ma.delete_reshaped()
fdl.delete_source_data()

# ------------------- DATA LOAD -------------------
# TODO: Which domains?
#domains = ['DM', 'SUPPDM', 'AE', 'SUPPAE', 'VS', 'DS']
domains = ['DM', 'EX', 'AE', 'LB', 'VS', 'DS']
downloads = get_cdisc_pilot_data(domains)
print(downloads['ok'])
if (downloads['error']):
    print("Some files did not download:")
    print(downloads['error'])
for download in downloads['ok']:
    df = fdl.load_file(download['folder'], download['file'])

# -------------------- AUTOMAPPING ---------------------
mm.automap_excel_based_model(domain=domains, standard=standard_label)  # all the columns that match column names in the excel standard will map automatically

mm.remove_unmapped_classes()

# -------------------- RESHAPING ---------------------
ma.delete_reshaped(batch_size=10000)

start_time = time.time()
ma.refactor_all()
print(f"Reshaping done in: {(time.time() - start_time):.2f} seconds")

# #In order to enable Methods to work with already existing Relationships (btw parent and a child's neighbour)
# #created during refactoring, we explicitly create Relationship nodes
mm.propagate_rels_to_parent_class()

# labels from the domains that were not loaded may be confusing
mm.remove_auxilary_term_labels()

# checking DataProvider works
dp = DataProvider()
df = dp.get_data_generic(
    labels=['Subject', 'Disposition', 'Dictionary-Derived Term'],
    where_map={'Dictionary-Derived Term': {'rdfs:label':'ADVERSE EVENT'}},
    infer_rels=True,
    return_nodeid=False,
    use_shortlabel=True,
    use_rel_labels=True,
    return_propname=False
)
print(df.head())
assert not(df.empty)

#extra:
"""
// with this model we can detect which of the loaded data is not compliant with the CT
MATCH (c:Class)<-[:IS_A]-(instance)
WHERE (c)-[:HAS_CONTROLLED_TERM]->(:Term)
AND NOT (c)-[:HAS_CONTROLLED_TERM]->(:Term)<-[:Term]-(instance)
RETURN * limit 10
"""
