import os
import time
import requests
# Start tab2neo
from data_loaders import file_data_loader
from model_appliers import ModelApplier
from data_providers import DataProvider
# End tab2neo
from github import Github, UnknownObjectException, BadCredentialsException
from cdisc_model_managers.cdisc_standard_loader import CdiscStandardLoader
from cdisc_model_managers.cdisc_model_manager import CdiscModelManager
from pathlib import Path

from utils.utils import download_file_from_github, get_cdisc_pilot_data


standards_folder = "cdisc_data"
standards_model = "SDTM_v1.4.csv"
standards_file = "SDTMIG_v3.2.csv"
sdtm_terminology = "CT2022Q1.csv"

csl = CdiscStandardLoader(standards_folder=standards_folder, sdtm_file=standards_model, sdtmig_file=standards_file, terminology_file=sdtm_terminology)
cdmm = CdiscModelManager(rdf=True)
ma = ModelApplier(mode="schema_CLASS")
fdl = file_data_loader.FileDataLoader()
standard_label = os.path.basename(csl.sdtmig_file)

csl.clean_slate()
csl.load_standard()

cdmm.generate_excel_based_model()

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
cdmm.automap_excel_based_model(domain=domains, standard=standard_label)  # all the columns that match column names in the excel standard will map automatically

cdmm.remove_unmapped_classes()

# -------------------- RESHAPING ---------------------
ma.delete_reshaped(batch_size=10000)

start_time = time.time()
ma.refactor_all()
print(f"Reshaping done in: {(time.time() - start_time):.2f} seconds")

# #In order to enable Methods to work with already existing Relationships (btw parent and a child's neighbour)
# #created during refactoring, we explicitly create Relationship nodes
cdmm.propagate_rels_to_parent_class()

# labels from the domains that were not loaded may be confusing
cdmm.remove_auxilary_term_labels()

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
