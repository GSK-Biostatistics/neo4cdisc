import datacompy

from data_providers import DataProvider
import logging


class SDTMDataProvider(DataProvider):
    """
    A wrapper-class over DataProvider to extract specifically SDTM data
    """

    RDFSLABEL = "rdfs:label"

    def __init__(self, check_for_refarctored=True, *args, **kwargs):
        self.check_for_refarctored = check_for_refarctored
        super().__init__(*args, **kwargs)

    def get_data_sdtm(self, standard: str, domain: str, study=None, where_map=None, user_role=None):
        assert where_map is None or isinstance(where_map, dict)
        if not where_map:
            where_map = {}
        assert study is None or isinstance(study, str)
        if study:
            where_map = {**where_map, **{
                'Study': {'rdfs:label': study}}}
        # TODO: add assert statements to check prerequisites - e.g. extraction model contains all required nodes and properties
        meta = self.neo_get_meta(standard=standard, table=domain)
        if self.debug:
            print("meta", meta)
        if meta:
            classes = (['Study'] if 'Study' not in meta[0]['classes'] else []) + meta[0]['classes']
            non_valid, no_access = [], []

            if self.check_for_refarctored:
                classes, non_valid = self.neo_validate_classes_to_extract(classes)
                if non_valid:
                    print(
                        f"ERROR: the following classes were excluded as those were never created during refactoring: {non_valid}")
            if user_role:
                classes, no_access = self.neo_validate_access(classes, user_role=user_role)
                print(
                    f"WARNING: the following classes were excluded as the user_role {user_role} access is restricted: {no_access}")
            if meta[0]['req_classes']:
                classes = [(class_ + self.OCLASS_MARKER if class_ not in meta[0]['req_classes'] else class_) for
                           class_ in classes]
            if self.debug:
                print(f"Getting classes: {classes}")

            rels = meta[0]['rels']
            excluded_classes = no_access + non_valid
            if excluded_classes:
                print(f'Excluding the following classes from rels: {excluded_classes}')
                rels = self.filter_classes_from_rels(rels, excluded_classes)

            df = self.get_data_generic(labels=classes,
                                       rels=rels,
                                       where_map=where_map,
                                       infer_rels=False,
                                       return_nodeid=False,
                                       return_propname=False,
                                       use_shortlabel=True,
                                       only_props=[self.RDFSLABEL],
                                       limit=None)

            # renaming and re-ordering columns (according to metadata):
            rename_dct = {}
            for key, item in meta[0]['rename_dct'].items():
                if not item in rename_dct.values():  # to avoid 2 columns with the same name
                    if key != item:
                        rename_dct[key] = item
            df = df.rename(rename_dct, axis=1)

            def _sorter(x):
                # Some columns don't have an order set
                none_count = 1000
                if not x[1]:
                    order = none_count
                    none_count += 1
                else:
                    order = x[1]
                return order

            _col_order = [(k, v) for k, v in sorted(meta[0]['order_dct'].items(), key=lambda x: x[0])]
            col_order = [k for k, v in sorted(_col_order, key=lambda x: _sorter(x))]

            for col in col_order:
                if col not in df.columns:
                    df[col] = None
            df = df[col_order]

            # Sorting
            # check that variables for sorting in meta actaully exist
            sorting, sorting_excluded = [], []
            if meta[0]['sorting']:
                if isinstance(meta[0]['sorting'], list):
                    l_sorting = meta[0]['sorting']
                elif isinstance(meta[0]['sorting'], str):
                    if "," in meta[0]['sorting']:
                        l_sorting = meta[0]['sorting'].split(",")
                    else:
                        l_sorting = [meta[0]['sorting']]
                else:
                    l_sorting = []
                for col in l_sorting:
                    if col in df.columns:
                        sorting.append(col)
                    else:
                        sorting_excluded.append(col)
                    if sorting_excluded:
                        print(
                            f"ERROR: the following columns were excluded from sort-by-group as those have not been extracted"
                            f"from the graph: {sorting_excluded}")

            # sorting dataframe:
            if sorting:
                df = df.sort_values(by=sorting, ignore_index=True)
            else:
                print(f"WARNING: no sort-by-group metadata(`Source Data Table`.SortOrder) was provided")

            return df

    def neo_get_meta(self, standard: str, table: str):

        q = """
        MATCH (sdf:`Data Extraction Standard`{_tag_:$standard})-[:HAS_TABLE]->(sdt:`Source Data Table`{_domain_:$table}),
        (sdt)-[:HAS_COLUMN]->(sdc:`Source Data Column`), (class:Class)<-[:TO]-(rel:Relationship)-[:FROM]->(fromclass:Class)
        WHERE
          EXISTS(
            (sdc)<-[:MAPS_TO_COLUMN]-(rel)-[:TO]->(class)
          )
          OR
          EXISTS(
            (sdt)-[:MAPS_TO_CLASS]->(class)
          )
        WITH *
        OPTIONAL MATCH (sdc)-[r:MAPS_TO_CLASS]->(class)   
        WITH *
        ORDER BY sdf, sdt, sdc.Order, sdc
        RETURN
        collect({from: fromclass.label, to: class.label, type: rel.relationship_type, short_label: class.short_label}) as rels,
        collect(distinct fromclass.label) + collect(distinct class.label) as classes,
        apoc.coll.toSet(
                [triple in [ 
                    triple in collect([class.label, sdc.Core, class.create]) 
                    where triple[1] = "Req" or triple[2]] | triple[0]
                ] + 
                [pair in [
                    pair in collect([fromclass.label, fromclass.create]) 
                    where pair[1]] | pair[0]
                ] // Ensure Domain is in required classes
            ) as req_classes,         
        apoc.map.fromPairs(
            [y in   
                [x in collect(distinct {class:class, sdc:sdc, r:r}) WHERE NOT x['r'] IS NULL] | //filtering for classes with existing Column MAPS_TO_CLASS relationship
                [
                    //y['class'].label + '.' + y['class'].short_label,    //to be used as key of the dict
                    y['class'].short_label,    //to be used as key of the dict
                    y['sdc']._columnname_                               // to be used as value of the dict
                ]
            ]
        ) as rename_dct,
        apoc.map.fromPairs(collect([sdc._columnname_, sdc.Order])) as order_dct,
        sdt['SortOrder'] as sorting
        """

        params = {'standard': standard, 'table': table}
        if self.debug:
            logging.debug(f"""
                               query: {q}
                               parameters: {params}
                           """)
        res = self.query(q, params)
        return res

    def neo_get_mapped_classes(self):
        q = """
        MATCH (sdf:`Source Data Folder`)-[:HAS_TABLE]->(sdt:`Source Data Table`),
        (sdt)-[:HAS_COLUMN]->(sdc:`Source Data Column`),
        (sdc)-[:MAPS_TO_PROPERTY]->(property:Property),
        (class:Class)-[:HAS_PROPERTY]->(property:Property)               
        RETURN DISTINCT class.label as Class
        """
        params = {}
        if self.debug:
            logging.debug(f"""
                               query: {q}
                               parameters: {params}
                           """)
        res = self.query(q, params)
        if res:
            return [r['Class'] for r in res]
        else:
            return []

    def filter_classes_from_rels(self, rels, classes: list):
        clean_rels = []
        excluded_rels = []
        for rels_dict in rels:
            intersect = False
            for class_ in classes:
                if class_ in rels_dict.values():
                    intersect = True

            if not intersect:
                clean_rels.append(rels_dict)
            else:
                excluded_rels.append(rels_dict)

        if excluded_rels:
            print(f'The following rels were excluded: {excluded_rels}')

        return clean_rels

    def neo_validate_classes_to_extract(self, classes: list) -> ([], []):
        """
        :param classes: list of classes to validate (nodes with labels Class and property label must exist)
        :return: [], [] - list of valid classes and list if invalid classes
        Identifies non-valid classes with count==0 or no count property (at least 1 instance got extracted during reshaping)
        """
        q = """
        MATCH (n:Class) 
        WHERE n.count = 0 OR NOT EXISTS (n.count)
        RETURN DISTINCT n.label as class
        """
        pre_non_valid = [res['class'] for res in self.query(q, {'classes': classes})]
        non_valid = []
        for class_ in pre_non_valid:
            if class_ in classes:
                non_valid.append(class_)

        valid = [class_ for class_ in classes if class_ not in non_valid]
        return valid, non_valid

    def neo_validate_access(self, classes: list, user_role=None) -> ([], []):
        """
        :param classes: list of classes to validate (nodes with labels Class and property label must exist)
        :param user_role: when None no access restrictions are accounted for, otherwise the classes with
        (:`User Role`{name:$user_role})-[:RESTRICTED_ACCESS]->(:Class) would be considered invalid
        :return:
        Identifies non-valid classes to which the user_role(if provided does not have access to)
        """
        no_access = []
        if user_role:
            q = """
                MATCH (role:`User Role`{name:$user_role})
                OPTIONAL MATCH (role)-[:ACCESS_RESTRICTED]->(c:Class)
                RETURN DISTINCT role.name as role, c.label as class
                """
            pre_no_access = [res['class'] for res in self.query(q, {'user_role': user_role})]
            if pre_no_access == []:
                raise Exception(f"User Role {user_role} does not exist")
            for class_ in pre_no_access:
                if class_ in classes:
                    no_access.append(class_)
        has_access = [class_ for class_ in classes if class_ not in no_access]
        return has_access, no_access

    @staticmethod
    def check_dataframes_equal(df1, df2):
        compare = datacompy.Compare(df1.reset_index(), df2.reset_index(), join_columns=['index'])
        return compare.report()
