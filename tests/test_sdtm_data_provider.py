import pytest
from cdisc_data_providers import sdtm_data_provider
import pandas as pd
import json
import os

filepath = os.path.dirname(__file__)


# Provide a DataProvider object (which contains a database connection)
@pytest.fixture(scope="module")
def dp():
    dp = sdtm_data_provider.SDTMDataProvider(debug=True, check_for_refarctored=True)
    yield dp


def test_get_data_sdtm_dm1(dp):
    dp.clean_slate()
    with open(os.path.join(filepath, 'data', 'test_data_sdtm.json')) as jsonfile:
        dct = json.load(jsonfile)
    dp.load_arrows_dict(dct)
    dp.verbose = True

    standard = 'test_standard'
    table = 'DM'
    res = dp.get_data_sdtm(standard=standard, domain=table)
    expected = pd.DataFrame({'TC4': ['test_data_2', 'test_data_1'], 'TC5': [None, None], 'TC3': ['test_study_1', 'test_study_2']})

    pd.testing.assert_frame_equal(res, expected)


def test_get_data_sdtm_dm2(dp):
    dp.clean_slate()
    with open(os.path.join(filepath, 'data', 'test_data_sdtm.json')) as jsonfile:
        dct = json.load(jsonfile)
    dp.load_arrows_dict(dct)
    dp.verbose = True

    standard = 'test_standard'
    table = 'AE'
    res = dp.get_data_sdtm(standard=standard, domain=table)
    expected = pd.DataFrame({'TC1': ['test_data_1', 'test_data_2'], 'TC2': ['test_study_1', 'test_study_2']})

    pd.testing.assert_frame_equal(res, expected)


def test_get_data_restricted_access(dp):
    dp.clean_slate()
    with open(os.path.join(filepath, 'data', 'test_data_sdtm_restricted.json')) as jsonfile:
        dct = json.load(jsonfile)
    dp.load_arrows_dict(dct)
    dp.verbose = True

    standard = 'test_standard'
    table = 'AE'
    res = dp.get_data_sdtm(standard=standard, domain=table, user_role='test_role')
    expected = pd.DataFrame({'TC1': [None, None], 'TC2': ['test_study_3', 'test_study_4']})

    pd.testing.assert_frame_equal(res, expected)


def test_neo_get_meta_dm1(dp):
    dp.clean_slate()
    with open(os.path.join(filepath, 'data', 'test_data_sdtm.json')) as jsonfile:
        dct = json.load(jsonfile)
    dp.load_arrows_dict(dct)

    # AE domain
    standard = 'test_standard'
    table = 'AE'
    res = dp.neo_get_meta(standard=standard, table=table)[0]

    rels = res.get('rels')
    expected_rels = [
        {'short_label': 'STUDYID', 'from': 'Adverse Events', 'to': 'Study', 'type': 'test_rel_1'},
        {'short_label': 'tc2', 'from': 'Adverse Events', 'to': 'test_class_2', 'type': 'test_rel_2'}
    ]
    assert [i for i in expected_rels if i not in rels] == []
    assert len(rels) == len(expected_rels)

    classes = res.get('classes')
    expected_classes = ['Study', 'Adverse Events', 'test_class_2']
    assert sorted(classes) == sorted(expected_classes)

    assert res.get('req_classes') == ['Study']
    assert sorted(res.get('order_dct')) == sorted({'TC1': 1, 'TC2': None})
    assert res.get('sorting') == ['TC1', 'TC2']


def test_neo_get_meta_dm2(dp):
    dp.clean_slate()
    with open(os.path.join(filepath, 'data', 'test_data_sdtm.json')) as jsonfile:
        dct = json.load(jsonfile)
    dp.load_arrows_dict(dct)

    # AE domain
    standard = 'test_standard'
    table = 'DM'
    res = dp.neo_get_meta(standard=standard, table=table)[0]

    rels = res.get('rels')
    expected_rels = [
        {'short_label': 'STUDYID', 'from': 'Demographics', 'to': 'Study', 'type': 'test_rel_3'},
        {'short_label': 'tc7', 'from': 'Demographics', 'to': 'test_class_7', 'type': 'test_rel_5'},
        {'short_label': 'tc6', 'from': 'Demographics', 'to': 'test_class_6', 'type': 'test_rel_4'}
    ]
    assert [i for i in expected_rels if i not in rels] == []
    assert len(rels) == len(expected_rels)

    classes = res.get('classes')
    expected_classes = ['Study', 'test_class_6', 'test_class_7', 'Demographics']
    assert sorted(classes) == sorted(expected_classes)

    assert sorted(res.get('req_classes')) == sorted(['test_class_6'])
    assert sorted(res.get('order_dct')) == sorted({'TC5': 3, 'TC4': 2, 'TC3': None})
    assert res.get('sorting') == ['TC3', 'TC4', 'TC5']


def test_neo_get_mapped_classes(dp):
    dp.clean_slate()
    with open(os.path.join(filepath, 'data', 'test_data_sdtm_mapped_classes.json')) as jsonfile:
        dct = json.load(jsonfile)
    dp.load_arrows_dict(dct)
    res = dp.neo_get_mapped_classes()
    expected = ['test_label_1', 'test_label_2']
    difference = set(res) ^ set(expected)
    assert not difference


def test_neo_validate_classes_to_extract(dp):
    dp.clean_slate()

    valid_labels = ['test_label_1', 'test_label_2']
    invalid_labels = ['test_label_3', 'test_label_4']
    q = """
    MERGE (:Class {label: 'test_label_1', count: 10})
    MERGE (:Class {label: 'test_label_2', count: 1})
    MERGE (:Class {label: 'test_label_3', count: 0})
    MERGE (:Class {label: 'test_label_4'})
    """
    dp.query(q)

    # All valid
    assert dp.neo_validate_classes_to_extract(classes=valid_labels) == (valid_labels, [])
    # Some valid
    assert dp.neo_validate_classes_to_extract(classes=valid_labels+invalid_labels) == (valid_labels, invalid_labels)
    # None valid
    assert dp.neo_validate_classes_to_extract(classes=invalid_labels) == ([], invalid_labels)


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
    WITH [['my_login', 'Study Lead'], ['external', 'External Researcher']] as coll
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

    # External Researcher
    res_has_access, res_no_access = dp.neo_validate_access(classes=classes, user_role="External Researcher")
    print(res_has_access, res_no_access)
    assert set(res_has_access) == {'Subject', 'Race'}
    assert set(res_no_access) == {'Date/Time of Reference Time Point', 'Date/Time of Collection', 'End Date/Time of Observation'}

    # Study Lead
    res_has_access, res_no_access = dp.neo_validate_access(classes=classes, user_role="Study Lead")
    assert set(res_has_access) == {'Subject', 'Race', 'Date/Time of Reference Time Point', 'Date/Time of Collection',
                                  'End Date/Time of Observation'}
    assert len(res_no_access) == 0

    # Not specified
    res_has_access, res_no_access = dp.neo_validate_access(classes=classes)
    assert set(res_has_access) == {'Subject', 'Race', 'Date/Time of Reference Time Point', 'Date/Time of Collection',
                                  'End Date/Time of Observation'}
    assert len(res_no_access) == 0

    # Role does not exist
    with pytest.raises(Exception):
        dp.neo_validate_access(classes=classes, user_role="Directors")
