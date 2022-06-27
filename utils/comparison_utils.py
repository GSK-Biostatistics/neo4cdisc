import os
from neointerface.neointerface import NeoInterface

def get_compare_tables(folder: str, domains: list):
    assert os.path.exists(folder)
    assert type(domains) == list
    domains = [domain.upper() for domain in domains]
    files = {}
    all_files = os.listdir(folder)
    for file in all_files:
        current_domain = file.split('.')[0].upper()
        if current_domain in domains:
            files[current_domain] = file
    return files

def sorting_varible(table: str, standard: str, neo=None):
    assert isinstance(neo, NeoInterface) and 'driver' in neo.__dict__ and 'query' in dir(neo)
    sorting = neo.query(
        """
        MATCH (sdf:`Data Extraction Standard`{_tag_:$standard})-[:HAS_TABLE]->(sdt:`Source Data Table`{_domain_:$domain})
        RETURN sdt.SortOrder as sorting
        """, {'domain': table, 'standard': standard})[0]['sorting']
    return sorting

def neo_get_comp_tables(table, neo=None):
    assert neo is None or ('driver' in neo.__dict__ and 'query' in dir(neo))
    if not neo:
        neo = NeoInterface()
    q = """
    MATCH (sdt:`Source Data Table`)    
    WHERE sdt._domain_ = $domain and exists (sdt._filename_)
    //TODO: add WHERE statement for study so >1 study in the database is allowed
    RETURN sdt._folder_ as folder, sdt._filename_ as filename  
    """
    params = {'domain': table}
    return neo.query(q, params)
