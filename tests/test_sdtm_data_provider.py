import pytest
from data_providers import sdtm_data_provider
from data_loaders import file_data_loader
from model_appliers import model_applier
from model_managers import ModelManager
import pandas as pd
import os


# Provide a DataProvider object (which contains a database connection) that can be used by the various tests that need it
@pytest.fixture(scope="module")
def dp():
    dp = sdtm_data_provider.SDTMDataProvider(debug = True, check_for_refarctored = True)
    yield dp

@pytest.fixture(scope="module")
def data_folder():
    testfile_path = os.path.dirname(__file__)
    data_folder = os.path.join(testfile_path, "data")
    yield data_folder


def test_get_data_sdtm(dp, data_folder):
    # checking content of columns
    dp.mode = 'schema_CLASS'
    df_extracted = prepare_get_data_sdtm(dp, data_folder, metadata_version = "v007")
    result1 = df_extracted[['STUDYID', 'USUBJID', 'AGE', 'SITEID', 'RFSTDTC', 'SEX']]
    dl = file_data_loader.FileDataLoader(domain_dict={'testdata.xls': 'DM'})
    df = dl.read_file(folder=data_folder, filename="testdata.xls")[0]
    print(result1.compare(df))
    assert df.equals(result1), "Values in ['STUDYID', 'USUBJID', 'AGE', 'SITEID', 'RFSTDTC', 'SEX'] are different in the result returned" \
                               "from the dataframe loaded to neo4j"

    # checking columns names
    q = f"""
        MATCH (c:`Source Data Column`) where c.uri CONTAINS '#Metadata' and c._domain_='DM'
        Return c.`_columnname_` as column order by c.Order
        """
    params = {}
    meta_col = pd.DataFrame(dp.query(q, params))
    data_col = pd.DataFrame(df_extracted.columns.values.tolist(), columns=['column'])
    print(data_col.compare(meta_col))
    assert data_col.equals(meta_col), "Set of columns is different"



# helper functions
def prepare_get_data_sdtm(dp, data_folder, metadata_version):
    dp.clean_slate()
    dl = file_data_loader.FileDataLoader(domain_dict={'testdata.xls': 'DM'})
    df = dl.load_file(folder=data_folder, filename="testdata.xls")

    # Refactor data
    neo = model_applier.ModelApplier(
        rdf=True,
        mode=("schema_PROPERTY" if metadata_version == "v4" else "schema_CLASS")
    )
    import_reshaping_metadata_ttl(data_folder, metadata_version)
    neo.define_refactor_indexes()
    neo.delete_classes_entities()
    neo.refactor_all()

    # Extract data
    import_extracting_metadata_ttl(data_folder, metadata_version)
    # TODO sorting: Bug in neo_get_meta
    df_extracted = dp.get_data_sdtm(standard='MDR3_2', domain='DM', where_map=None)
    return df_extracted

def import_reshaping_metadata_ttl(data_folder:str, metadata_version):
    uri_dct1 = {key: item for key, item in ModelManager.URI_MAP.items() if key in ['Class', 'Property', 'Relationship']}
    uri_dct2 = {
        "Source Data Table": {
            "properties": "_domain_",
            "where": "WHERE NOT (exists(x.uri) and x.uri CONTAINS '#Metadata')"
        },
        "Source Data Column": {
            "properties": ["_domain_", "_columnname_"],
            "where": "WHERE NOT (exists(x.uri) and x.uri CONTAINS '#Metadata')"
        }
    }
    uri_dct = {**uri_dct1, **uri_dct2}
    neo = model_applier.ModelApplier(rdf=True)
    with open(os.path.join(data_folder, f'Map Columns to Properties_{metadata_version}_example_2domains_117106.ttl'), "r", encoding='utf-8') as f:
        neo.rdf_generate_uri(uri_dct)
        rdf = f.read()
    neo.delete_nodes_by_label(delete_labels=['Class', 'Property'])
    res = neo.rdf_import_subgraph_inline(rdf)
    print("Importing metadata:", res)
    classes = uri_dct2.keys()
    for class_ in classes:
        q = f"""
        MATCH (x:`{class_}`:Resource)
        remove x:Resource
        """
        params = {}
        neo.query(q, params)


def import_extracting_metadata_ttl(data_folder:str, metadata_version):
    uri_map1 = {
        key: item
        for key, item in ModelManager.URI_MAP.items()
        if key in ["Class", "Property", "Relationship"]
    }
    uri_map2 = {
        "Data Extraction Standard": {
            "properties": "_tag_",
            "where": "WHERE (exists(x.uri) and x.uri CONTAINS '#Metadata')"},
        "Source Data Folder": {"properties": "_folder_"},
        "Source Data Table": {
            "properties": "_domain_",
            "where": "WHERE (exists(x.uri) and x.uri CONTAINS '#Metadata')"
        },
        "Source Data Column": {
            "properties": ["_domain_", "_columnname_"],
            "where": "WHERE (exists(x.uri) and x.uri CONTAINS '#Metadata')"
        }
    }
    uri_map = {**uri_map1, **uri_map2}
    neo = model_applier.ModelApplier(rdf=True)
    with open(os.path.join(data_folder, f'export_sdtm_{metadata_version}_2domains.ttl'), "r", encoding='utf-8') as f:
        neo.rdf_generate_uri(dct={k: i for k, i in uri_map.items() if k in ['Class', 'Property']})
        rdf = f.read()
        res =neo.rdf_import_subgraph_inline(rdf)
        print("Importing metadata:", res)
        classes = uri_map
        for class_ in classes:
            q = f"""
               MATCH (x:`{class_}`:Resource)
               remove x:Resource
               """
            params = {}
            neo.query(q, params)
        f.close()

def test_neo_validate_access(dp):
    dp.clean_slate()
    classes = ["End Date/Time of Observation", "Date/Time of Reference Time Point", "Date/Time of Collection",
               "Subject", "Race"]
    dp.query(
        """     
        UNWIND $classes as class
        MERGE (c:Class{label:class})
        """,
        {'classes': classes}
    )
    dp.query("""
    WITH [['ak956494', 'Study Lead'], ['external', 'External Researcher']] as coll
    UNWIND coll as pair
    WITH pair[0] as user_id, pair[1] as role_name
    MERGE (user:User{id:user_id})-[r1:HAS_ROLE]->(role:`User Role`{name:role_name})    
    WITH *
    WHERE role.name = 'External Researcher'    
    MATCH (class:Class)
    WHERE class.label in $rclasses    
    MERGE (role)-[:ACCESS_RESTRICTED]->(class)    
    """,
    {'rclasses': classes[:3]}
             )
    #External Researcher
    res_has_access, res_no_access = dp.neo_validate_access(classes=classes, user_role="External Researcher")
    print(res_has_access, res_no_access)
    assert set(res_has_access) == {'Subject', 'Race'}
    assert set(res_no_access) == {'Date/Time of Reference Time Point', 'Date/Time of Collection', 'End Date/Time of Observation'}
    #Study Lead
    res_has_access, res_no_access = dp.neo_validate_access(classes=classes, user_role="Study Lead")
    assert set(res_has_access) == {'Subject', 'Race', 'Date/Time of Reference Time Point', 'Date/Time of Collection',
                                  'End Date/Time of Observation'}
    assert len(res_no_access) == 0
    #Not specified
    res_has_access, res_no_access = dp.neo_validate_access(classes=classes)
    assert set(res_has_access) == {'Subject', 'Race', 'Date/Time of Reference Time Point', 'Date/Time of Collection',
                                  'End Date/Time of Observation'}
    assert len(res_no_access) == 0
    #Role does not exist
    try:
        dp.neo_validate_access(classes=classes, user_role="Directors")
    except Exception as e:
        print(e)




