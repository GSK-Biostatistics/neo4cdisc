import os
from model_appliers.model_applier import ModelApplier
import json
from data_loaders import file_data_loader
import re


class CdiscStandardLoader(ModelApplier):

    def __init__(self, standards_folder: str = None, sdtm_file: str = None, sdtmig_file: str = None, terminology_file: str = None,
                 *args, **kwargs):
        """
        :param standards_folder: Directory where standard files are stored
        :param sdtm_file: Name of file containing SDTM Model metadata
        :param sdtmig_file: Name of file containing SDTMIG metadata
        :param terminology_file: Name of file containing SDTM Terminology
        """
        super().__init__(rdf=True, *args, **kwargs)
        assert os.path.exists(standards_folder)
        assert os.path.exists(os.path.join(standards_folder, sdtm_file))
        assert os.path.exists(os.path.join(standards_folder, sdtmig_file))
        assert os.path.exists(os.path.join(standards_folder, terminology_file))
        self.standards_folder = standards_folder
        self.sdtm_file = sdtm_file
        self.sdtmig_file = sdtmig_file
        self.terminology_file = terminology_file

    def load_standard(self, extract_terms: bool = True, extract_vld: bool = True):
        print("Loading content")
        print("Standards folder:", self.standards_folder)
        print("Standards model:", self.sdtm_file)
        print("Standards Implementation Guide:", self.sdtmig_file)
        print("Terminology file:", self.terminology_file)

        fdl = file_data_loader.FileDataLoader()
        df = fdl.load_file(self.standards_folder, self.sdtm_file)
        q = f"""
        MATCH (n:`Source Data Row`)
        REMOVE n:`Source Data Row`
        SET    n:`{self.sdtm_file}`
        """
        self.query(q)

        df = fdl.load_file(self.standards_folder, self.sdtmig_file)
        q = f"""
        MATCH (n:`Source Data Row`)
        REMOVE n:`Source Data Row`
        SET    n:`{self.sdtmig_file}`
        """
        self.query(q)

        df = fdl.load_file(self.standards_folder, self.terminology_file)
        q = f"""
        MATCH (n:`Source Data Row`)
        REMOVE n:`Source Data Row`
        SET    n:`{self.terminology_file}`
        """
        self.query(q)

        with open(os.path.join(self.standards_folder, "sdtmig3_2_domain_sort_order.json"), 'r') as json_file:
            json_data = json.load(json_file)
        self.DOMAIN_SORT_ORDER = json_data["domain_sort_order"]
        with open(os.path.join(self.standards_folder, "sdtmig3_3_domain_labels.json"), 'r') as json_file:
            json_data = json.load(json_file)
        self.DOMAIN_LABELS = json_data["domain_labels"]
        self.reshape_model()
        self.reshape_sdtmig()
        self.reshape_terminology()
        self.link_cdisc()
        self.load_link_sdtm_ttl(local=False)

    def reshape_model(self):
        # Create nodes for General Observation Classes
        q = f"""
        MATCH (n:`{self.sdtm_file}`)
        WITH collect(distinct(n.Class)) as gocs
        UNWIND gocs as goc
        MERGE (d:ObservationClass {{Class: toUpper(goc), label:goc}})
        RETURN count(d)
        """
        self.query(q)

        # Change label on general observation class variables
        q = f"""
        MATCH (n:`{self.sdtm_file}`)
        WHERE n.`Dataset Name` = ""
        REMOVE n:`{self.sdtm_file}`
        SET    n:GOC
        """
        self.query(q)

        # Change label on special purpose class variables
        q = f"""
        MATCH (n:`{self.sdtm_file}`)
        WHERE n.`Dataset Name` <> ""
        REMOVE n:`{self.sdtm_file}`
        SET    n:Special_Purpose_Variable
        """
        self.query(q)

        # Link General Observation Class to variables GOC-[CLASS_SPECIFIC_VARIABLE_GROUPING]-(VariableGrouping)
        # First create variable grouping node
        q = """
        MATCH (oc:ObservationClass)
        MERGE (vg:VariableGrouping {contextLabel: oc.label + " Observation Class Variables"})
        SET vg.contextLabel = oc.label + " Observation Class Variables"
        MERGE (oc)-[:CLASS_SPECIFIC_VARIABLE_GROUPING]->(vg)
        RETURN count(vg)
        """
        self.query(q)

        # Link General Observation Class to variable grouping node
        q = """
        MATCH (oc:ObservationClass)-[:CLASS_SPECIFIC_VARIABLE_GROUPING]->(vg:VariableGrouping)
        MATCH (v:GOC)
        WHERE v.Class = oc.label
        MERGE (v)-[:context]->(vg)
        return count(v)
        """
        self.query(q)

    def reshape_sdtmig(self):
        # Change label on domain variables
        # Adaptions for load_link_sdtm_ttl
        # - Add property Variable (Variable Name) (lls)
        # Adaption for generate model
        # - Add property Label (Variable Label)
        q = f"""
        MATCH (n:`{self.sdtmig_file}`)
        REMOVE n:`{self.sdtmig_file}`
        SET    n:Variable
        SET    n.Variable = n.`Variable Name` 
        SET    n.Label = n.`Variable Label` 
        """
        self.query(q)

        # Add domains/dataset
        # Adaptions for load_link_sdtm_ttl
        # - Add label Dataset
        q = f"""
        MATCH (n:Variable)
        WITH collect(distinct(n.`Dataset Name`)) as domains
        UNWIND domains as domain
        MERGE (d:Domain:Dataset {{Domain: domain, Dataset: domain}})
        """
        self.query(q)

        # Relate variables to domain and set class of domain
        q = f"""
        MATCH (d:Domain)
        MATCH (v:Variable)
        WHERE d.Domain = v.`Dataset Name`
        MERGE (d)-[:HAS_VARIABLE]->(v)
        SET d.Class = toUpper(v.Class)
        RETURN count(v)
        """
        self.query(q)

        # Remove empty value list
        q = """
        MATCH (n:Variable)
        WHERE n.`Value List` = ""
        REMOVE n.`Value List`
        RETURN count(n)
        """
        self.query(q)

        # Add property for codelist to variable
        # Adapt for generate_excel_based_model
        # - Add property Label (Variable Label)
        q = """
        MATCH (v:Variable)
        WHERE v.`CDISC CT Codelist Code(s)` <> ""
        SET v.`Codelist Code` = v.`CDISC CT Codelist Code(s)`
        SET v.Label = v.`Variable Label`
        RETURN count(v)
        """
        self.query(q)

        # Add domain labels
        # Adapt for generate_excel_based_model
        # - Add property Description (Label)
        for [domain, label] in self.DOMAIN_LABELS.items():
            q = f"""
            MATCH (d:Domain)
            WHERE d.Domain = $domain
            SET d.label = $label
            SET d.Description = $label
            RETURN d.label
            """
            if (self.debug == True):
                print(domain, label)
                print(q)
            self.query(q, {'domain': domain, 'label': label})

        # Link domains to General Observation Classes
        q = f"""
        MATCH (d:Domain)
        MATCH (g:ObservationClass)
        WHERE g.Class = d.Class
        MERGE (g)-[:HAS_DATASET]->(d)
        RETURN count(d)
        """
        self.query(q)

    def reshape_terminology(self):
        # Change label on codelists
        q = f"""
        MATCH (n:`{self.terminology_file}`)
        WHERE n.`Codelist Code` = ""
        REMOVE n:`{self.terminology_file}`
        SET    n:Codelist
        """
        self.query(q)

        # Change label on codelist items
        # Adapt for generate_excel_based_model
        # - Add property Term (CDISC Submission Value)
        # - Add property `NCI Codelist Code` (Codelist Code)
        # - Add property `NCI Term Code` (Code)
        q = f"""
        MATCH (n:`{self.terminology_file}`)
        WHERE n.`Codelist Code` <> ""
        REMOVE n:`{self.terminology_file}`
        SET    n:Term
        SET    n.Term = n.`CDISC Submission Value`
        SET    n.`NCI Codelist Code` = n.`Codelist Code`
        SET    n.`NCI Term Code` = n.Code
        """
        self.query(q)

        # Link codelist item to codelist
        q = f"""
        MATCH (c:Codelist)
        MATCH (t:Term)
        WHERE t.`Codelist Code` = c.Code
        MERGE (c)-[:HAS_TERM]->(t)
        RETURN count(t)
        """
        self.query(q)

    def link_cdisc(self, extract_terms: bool = True, extract_vld: bool = True):
        print("Linking CDISC content")
        for [domain, sort_order] in self.DOMAIN_SORT_ORDER.items():
            # hardcode adding --SEQ into the 'Sort Order'
            if (self.debug == True):
                print(domain, sort_order)
            q = f"""
            MATCH (n:Domain)-[:HAS_VARIABLE]->(v:Variable)
            WHERE v.`Variable Name` ends with "SEQ" AND n.Domain = "{domain}"
            WITH  n, v.`Variable Name` as seq
            SET n.`Sort Order` = CASE WHEN seq in apoc.text.split("{sort_order}", ",") THEN "{sort_order}" ELSE "{sort_order}" + "," + seq END
            RETURN n["Sort Order"]
            """
            self.query(q)

            # Variables need to have a property Order = sort order
            variables = sort_order.split(",")
            # Add --SEQ if not mentioned.
            add_seq = [var for var in variables if re.findall('SEQ', var.upper())]
            # N.B! Only to applicable to domains that can have --SEQ variable. Not trial design DM and SV.
            if not add_seq and not domain in ['DM', 'SV', 'TA', 'TE', 'TV', 'TS', 'TI']:
                variables.append(domain+'SEQ')

            for i, var in enumerate(variables, start=1):
                q = f"""
                MATCH (v:Variable)
                WHERE v.`Dataset Name` = '{domain}' AND v.`Variable Name` = '{var}'
                SET v.Order = {i}
                RETURN v.Order
                """
                self.query(q)

        # Add relationship to Codelist for variable
        # CREATE (v:Variable)-[:HAS_CONTROLLED_TERM]->(t:Term) Info on exact terms does not exist
        q = """
        MATCH (v:Variable)
        MATCH (c:Codelist)
        WHERE v.`Codelist Code` = c.Code
        MERGE (v)-[:HAS_CODELIST]->(c)
        WITH *
        MATCH (c)-[:HAS_TERM]->(t:Term)
        MERGE (v)-[:HAS_CONTROLLED_TERM]->(t)
        RETURN COUNT(v)
        """
        self.query(q)

        # Add relationship to terms for variable
        # TODO: This might not be needed. Adds variable.`Value List` relationship to terms
        # 1. Add for when VALUE LIST is a semi-colon separated list
        # NB: Extra space inserted before semi-colon, so splitting on "; "
        q = """
        MATCH (v:Variable)
        WHERE v.`Value List` contains ";"
        WITH v, apoc.text.split(v.`Value List`, "; ") as  terms
        UNWIND terms as trm
        MATCH (t:Term) WHERE t.`CDISC Submission Value` = trm
        MERGE (v)-[:HAS_CONTROLLED_TERM]->(t)
        RETURN count(v), count(t)
        """
        self.query(q)
        # 2. Add for when VALUE LIST is just a singe term
        # NB: The only values in SDTMIG 3.2 are domain codes (e.g. DM, AE etc.) and they do not exist in CT downloaded from CDISC Library
        #     So this statement has no effect at all at the moment.
        q = """
        MATCH (v:Variable)
        WHERE NOT v.`Value List` contains ";"
        MATCH (t:Term)
        WHERE t.`CDISC Submission Value` = v.`Value List`
        MERGE (v)-[:HAS_CONTROLLED_TERM]->(t)
        RETURN v, t
        """
        self.query(q)

        if extract_terms:
            # SET t.`Codelist Code` = t.Code # Codelist Code is already assigned earlier
            q = """
            MATCH (t:Term)       
            SET t.`Term Code` = t.Code
            """
            self.query(q)

            # TODO: No Term is linked to Variable (HAS_CONTROLLED_TERM), so this will not do anything
            # # #merging duplicate Terms together
            # q = """
            # MATCH (t:Term)
            # WITH t.`Codelist Code` as cl, t.`Term Code` as trm, collect(t) as coll
            # CALL apoc.refactor.mergeNodes(coll) YIELD node
            # REMOVE node.ID
            # WITH node
            # MATCH (node)<-[r:HAS_CONTROLLED_TERM]-(c)
            # WITH c, node, collect(r) as coll
            # WHERE size(coll)>1
            # WITH coll[1..] as coll
            # UNWIND coll as r
            # DELETE r
            # """
            # self.query(q)

        # TODO:
        #  1. As above, HAS_CONTROLLED_TERM relationship does not exist in CDISC metadata
        #  2. CDISC CT does not have a property Order for terms
        # # link related Terms sequentially according to their Order property
        # q = """
        # MATCH (v:Variable)-[:HAS_CONTROLLED_TERM]->(t:Term)
        # WITH v,t ORDER BY v.label, t.Order, t.Term ASC
        # WITH v, COLLECT(t) AS terms
        # FOREACH (n IN RANGE(0, SIZE(terms)-2) |
        #     FOREACH (prev IN [terms[n]] |
        #         FOREACH (next IN [terms[n+1]] |
        #             MERGE (prev)-[:NEXT]->(next))))
        # """
        # self.query(q)

        print("CDISC Data Link Complete")

    def load_link_sdtm_ttl(self, local=True):
        self.rdf_config()
        if local:
            with open(os.path.join(self.standards_folder, 'sdtm-1-3.ttl')) as f:
                rdf = f.read()
                print(
                    self.rdf_import_subgraph_inline(rdf, "Turtle")
                )
        else:
            print(
                self.rdf_import_fetch(
                    "https://raw.githubusercontent.com/phuse-org/rdf.cdisc.org/master/std/sdtm-1-3.ttl",
                    "Turtle"
                )
            )

        self.create_index(label="DataElement", key="dataElementName")

        # linking ObservationClass to VariableGrouping (only class specific)
        q = """
        MATCH (oc:ObservationClass), (vg:VariableGrouping)
        WHERE vg.contextLabel = 
            CASE oc.Class 
                WHEN "FINDINGS" THEN "Findings Observation Class Variables"
                WHEN "EVENTS" THEN "Event Observation Class Variables"
                WHEN "INTERVENTIONS" THEN "Interventions Observation Class Variables"
            END
        MERGE (oc)-[:CLASS_SPECIFIC_VARIABLE_GROUPING]->(vg)
        """
        self.query(q)

        # adding properties dataElementName
        q = """ 
        MATCH (v:Variable), (de:DataElement)
        WHERE v.Variable =~ apoc.text.replace(de.dataElementName, '-', '.')
           AND NOT (v.Variable STARTS WITH 'RF' and v.Dataset = 'DM')
        WITH DISTINCT v, de.dataElementName as dataElementName, de.dataElementLabel as dataElementLabel
        SET v.dataElementName = dataElementName, v.dataElementLabel = dataElementLabel 
        """
        self.query(q)

        # linking Variable to DataElements (on dataElementName there might be >1 DataElement per Variable -
        # need to filter on VariableGrouping~ObservationClass)
        # (1) WHERE size(coll)=1
        q = """
        MATCH (v:Variable)<-[r:HAS_VARIABLE]-(ds:Dataset)    
        OPTIONAL MATCH (da:DataElement)
        WHERE da.dataElementName = v.dataElementName        
        WITH v, collect(da) as coll
        WHERE size(coll)=1
        UNWIND coll as da
        MERGE (v)-[:IS_DATA_ELEMENT]->(da)
        """
        self.query(q)

        # 2 else
        q = """
        MATCH (v:Variable)<-[r:HAS_VARIABLE]-(ds:Dataset)<-[:HAS_DATASET]-(oc:ObservationClass)
        , (da:DataElement)-[:context]->(vg:VariableGrouping)<-[:CLASS_SPECIFIC_VARIABLE_GROUPING]-(oc)        
        WHERE NOT EXISTS ((v)-[:IS_DATA_ELEMENT]->()) AND da.dataElementName = v.dataElementName        
        MERGE (v)-[:IS_DATA_ELEMENT]->(da)               
        """
        self.query(q)

        # saveing counts of data elements with the same name
        q = """
        MATCH (da:DataElement)
        OPTIONAL MATCH (da:DataElement)-[:context]->(vg:VariableGrouping)
        WITH *, 
        {
          `Interventions Observation Class Variables`: "Interventions",
          `General Observation Class Timing Variables`: "GO Timing",
          `Findings Observation Class Variables`: "Findings",
          `General Observation Class Identifier Variables`: "GO Identifier",
          `Event Observation Class Variables`: "Events",
          `Findings About Events or Interventions Variables`: "FA"
        } as vg_mapping 
        SET da.vg = vg.contextLabel                
        SET da.vg_short = vg_mapping[vg.contextLabel]
        WITH DISTINCT da.dataElementName as name, collect(da) as coll
        WITH *, size(coll) as sz
        UNWIND coll as da
        SET da.n_with_same_name = sz 
        """
        self.query(q)

        # saveing counts of variables with the same label
        q = """
        MATCH (v:Variable)
        WITH v.Label as lbl, collect(v) as coll
        WITH *, size(coll) as sz
        UNWIND coll as v
        SET v.n_with_same_label = sz
        """
        self.query(q)

        # saveing counts of variables with the same name
        q = """
        MATCH (v:Variable)
        WITH v.Variable as name, collect(v) as coll
        WITH *, size(coll) as sz
        UNWIND coll as v
        SET v.n_with_same_name = sz
        """
        self.query(q)

        print("SDTM TTL Loaded and Linked")

    def propagate_relationships(self, on_children=True, on_parents=True):  # not used kept for code reference
        la = ('' if (on_children and on_parents) or not on_children else '<')
        ra = ('' if (on_children and on_parents) or not on_parents else '>')
        q = f"""
        //propagate_relationships_of_parents_on_children
        MATCH (c:Class)
        OPTIONAL MATCH path = (c){la}-[:SUBCLASS_OF*1..50]-{ra}(parent)<-[r1:FROM]-(r:Relationship)-[r2:TO]->(fromto)
        WITH c, collect(path) as coll
        OPTIONAL MATCH path = (c){la}-[:SUBCLASS_OF*1..50]-{ra}(parent)<-[r1:TO]-(r:Relationship)-[r2:FROM]->(fromto)
        WITH c, coll + collect(path) as coll
        UNWIND coll as path
        WITH 
            c, 
            nodes(path)[-1] as fromto, 
            nodes(path)[-2] as r,
            relationships(path)[-1] as r2, 
            relationships(path)[-2] as r1
        WITH *, apoc.text.join([k in [x in keys(r) WHERE x <> 'uri'] | '`' + k + '`: "' + r[k] + '"'], ", ") as r_params  
        WITH *, CASE WHEN r_params = '' THEN '' ELSE '{' + r_params + '}' END as r_params
        WITH *, 
         '
                WITH $c as c
                MATCH (fromto) WHERE id(fromto) = $id_fromto
                MERGE (c)<-[:`'+type(r1)+'`]-(:Relationship' + r_params +')-[:`'+type(r2)+'`]->(fromto) 
         ' as q 
        WITH c, fromto, q
        call apoc.do.when(
            NOT EXISTS ( (c)--(:Relationship)--(fromto) ), 
            q,
            '',    
            {{c:c, id_fromto:id(fromto)}}
        ) YIELD value
        RETURN value 
        """
        self.query(q)
