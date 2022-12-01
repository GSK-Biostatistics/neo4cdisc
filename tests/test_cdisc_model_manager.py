import json
import os

filepath = os.path.dirname(__file__)
import pytest
from cdisc_model_managers.cdisc_model_manager import CdiscModelManager


# Provide a DataProvider object (which contains a database connection)
# that can be used by the various tests that need it
@pytest.fixture(scope="module")
def cdmm():
    cdmm = CdiscModelManager(verbose=False)
    yield cdmm


def test_set_sort_order(cdmm):
    cdmm.clean_slate()

    q1 = '''
    MERGE (sdf:`Data Extraction Standard`{_tag_:'standard1'})-[:HAS_TABLE]->(sdt:`Source Data Table`{_domain_:'Domain 1'})
    MERGE (sdt)-[:HAS_COLUMN]->(sdc1:`Source Data Column`{_columnname_: 'Col 1', Order: 2})
    MERGE (sdt)-[:HAS_COLUMN]->(sdc2:`Source Data Column`{_columnname_: 'Col 2', Order: 3})
    MERGE (sdt)-[:HAS_COLUMN]->(sdc3:`Source Data Column`{_columnname_: 'Col 3', Order: 1})
    '''
    cdmm.query(q1)

    cdmm.set_sort_order(domain=['Domain 1'], standard='standard1')

    q1 = '''
    MATCH (sdt:`Source Data Table`)
    WHERE sdt._domain_ = 'Domain 1'
    RETURN sdt.SortOrder as SortOrder
    '''
    res = cdmm.query(q1)[0]['SortOrder']
    assert res == ['Col 3', 'Col 1', 'Col 2']


def test_extend_extraction_metadata(cdmm):
    cdmm.clean_slate()

    q1 = '''
    MERGE (sdf:`Data Extraction Standard`{_tag_:'standard1'})-[:HAS_TABLE]->(sdt:`Source Data Table`{_domain_:'Domain 1'})
    MERGE (sdt)-[:HAS_COLUMN]->(sdc1:`Source Data Column`{_columnname_: 'Col 1', Order: 2})
    MERGE (sdt)-[:HAS_COLUMN]->(sdc2:`Source Data Column`{_columnname_: 'Col 2', Order: 3})
    MERGE (sdt)-[:HAS_COLUMN]->(sdc3:`Source Data Column`{_columnname_: 'Col 3', Order: 1})

    MERGE (sdc1)-[:MAPS_TO_CLASS]->(c1:Class{label: 'Class 1'})
    MERGE (sdc2)-[:MAPS_TO_CLASS]->(c2:Class{label: 'Class 2'})
    MERGE (c1)<-[:TO]-(r1:Relationship)-[:FROM]->(c3:Class{short_label:'Domain 1'})
    MERGE (c2)<-[:TO]-(r2:Relationship)-[:FROM]->(c4:Class{short_label:'Domain 1'})
    '''

    cdmm.query(q1)

    cdmm.extend_extraction_metadata(domain=['Domain 1'], standard='standard1')

    q2 = '''
    MATCH (x)-[:MAPS_TO_CLASS]->(y)
    RETURN x, y
    '''

    res = cdmm.query(q2)
    expected_res = [{'x': {'Order': 2, '_columnname_': 'Col 1'}, 'y': {'label': 'Class 1'}},
                    {'x': {'Order': 3, '_columnname_': 'Col 2'}, 'y': {'label': 'Class 2'}}]
    assert res == expected_res
