from model_managers.model_manager import ModelManager
import pandas as pd


class CdiscModelManager(ModelManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.verbose:
            print(f"---------------- {self.__class__} initialized -------------------")

    ## ---------------------------- Generate model from excel SDTM spec ----------------------------- ##
    def generate_excel_based_model(self, label_terms: bool = False, create_term_indexes: bool = False):
        """
        Run ExcelStandardLoader.load_standard() to prepare metadata from excel (and SDTM ontology form GitHub)
        in Neo4j
        :param create_term_indexes: weather to create indexes for each Class that HAS_CONTROLLED_TERM (can be done later
        during reshaping)
        :return: None
        """
        print("Creating indexes on Class and Term")
        self.create_index(label="Class", key="label")
        self.create_index(label="Class", key="short_label")
        self.create_index(label="Relationship", key="relationship_type")
        self.create_index(label="Term", key="Codelist Code")
        self.create_index(label="Term", key="Term Code")
        self.create_index(label="Term", key=ModelManager.RDFSLABEL)
        print("Creating indexes on Source Data Table and Source Data Column")
        self.create_index(label="Source Data Table", key="_domain_")
        self.create_index(label="Source Data Column", key="_columnname_")

        # Terms
        print("Mapping Term GSK Codes and NCI Codes together")
        q = f"""
           MATCH (t:Term)
           SET t.`{ModelManager.RDFSLABEL}` = t.Term
           SET t.`Codelist Code` = 
               CASE WHEN t.`NCI Codelist Code` IS NULL OR t.`NCI Codelist Code` = '' THEN 
                   t.GSK_Codelist_Code
               ELSE
                   t.`NCI Codelist Code`
               END
           SET t.`Term Code` = 
               CASE WHEN t.`NCI Term Code` IS NULL OR t.`NCI Term Code` = '' THEN 
                   t.GSK_Term_Code
               ELSE
                   t.`NCI Term Code`
               END
           """
        self.query(q)

        print("Creating Classes from Dataset and ObservationClass")
        # Datasets to Classes
        q = """
               MATCH (d:Dataset)
               SET d:Class
               SET d.label = d.Description
               SET d.short_label = d.Dataset
               SET d.create = True
               WITH *
               OPTIONAL MATCH (d)<-[:HAS_DATASET]-(oc:ObservationClass)
               WITH *, {
                   INTERVENTIONS: 'Intervention',
                   EVENTS: 'Event',  
                   `FINDINGS ABOUT`: 'Finding About',
                   FINDINGS: 'Finding',
                   RELATIONSHIP: 'Relationship (SDTM)',
                   `SPECIAL PURPOSE`: 'Special Purpose',
                   `TRIAL DESIGN`: 'Trial Design'                             
               } as map
               SET oc:Class
               SET oc.label = map[oc.Class]
               SET oc.short_label = toUpper(map[oc.Class])
               MERGE (d)-[:SUBCLASS_OF]->(oc)
               //MERGE (sdt:`Source Data Table`{_domain_: d.Dataset}) //This is outsourced to automap_excel_based_model
               //MERGE (sdt)-[:MAPS_TO_CLASS]->(d)
               WITH DISTINCT oc
               MERGE (rec_class:Class{label: 'Record', short_label: 'RECORD'})
               MERGE (oc)-[:SUBCLASS_OF]->(rec_class)
               """
        self.query(q)

        print("Creating Class from Variable")  # when no DataElement exists
        q = """
           MATCH (d:Dataset)-[:HAS_VARIABLE]->(v:Variable)
           WHERE NOT 
               (
                   (v.Variable starts with 'COVAL' AND v.Variable <> 'COVAL') 
                       OR
                   (v.Variable starts with 'TSVAL' AND NOT v.Variable in ['TSVAL', 'TSVALCD', 'TSVALNF'])
               )// - should be handled separately
           AND NOT EXISTS 
               (
                   (v)-[:IS_DATA_ELEMENT]->(:DataElement)
               )        
           SET v:Class
           SET v.label =                           
               CASE WHEN v.n_with_same_label > 1 THEN 
                   d.Description + ' ' + v.Label
               ELSE
                   v.Label
               END                
           SET v.short_label =             
               CASE WHEN v.n_with_same_name > 1 THEN 
                   d.Dataset + v.Variable
               ELSE
                   v.Variable
               END
           SET v.create = False
           // for the DM table, like all the variable to the (soon to be) subject class, otherwise link to the dataset class
           WITH d, v
           CALL apoc.do.when(
              d.Dataset in $datasets
              ,
              '
              MERGE (d)<-[:FROM]-(:Relationship{relationship_type:v.Label})-[:TO]->(v)
              WITH d, v
              MATCH (s:Variable)
              WHERE s.Dataset in $datasets AND s.Label = "Unique Subject Identifier"
              MERGE (s)<-[:FROM]-(:Relationship{relationship_type:v.Label})-[:TO]->(v) 
              '
              ,
              '
              MERGE (d)<-[:FROM]-(:Relationship{relationship_type:v.Label})-[:TO]->(v) 
              '
              ,
              {d:d, v:v, datasets:$datasets}
           )
           YIELD value
           RETURN value           
           """
        self.query(q, {'datasets': ['DM']})

        print("Creating Class from dataElement and Relationship from Variable")
        q = """
               MATCH (de:DataElement)
               OPTIONAL MATCH (de)-[:dataElementRole]->(dar:DataElementRole)
               WITH DISTINCT de,                                   
               {
                   label: 
                       CASE de.dataElementName
                           WHEN 'USUBJID' THEN 'Subject'
                           WHEN 'STUDYID' THEN 'Study'
                       ELSE
                           CASE 
                               WHEN de.dataElementName = '--DECOD' 
                               THEN 'Dictionary-Derived Term' 
                           ELSE 
                               CASE 
                                   WHEN de.dataElementLabel = 'Class' 
                                   THEN de.vg  // rename label of 'Class' as causes problems in extract_class_entities() when reshaping
                               ELSE de.dataElementLabel
                               END
                           END
                       END,
                   short_label: de.dataElementName,
                   create: 
                       CASE WHEN dar.label IN ['Identifier Variable', 'Result Qualifier']
                       AND NOT de.dataElementName in ['DOMAIN', 'STUDYID', 'USUBJID', 'EPOCH'] THEN
                           True                
                       ELSE
                           False
                       END
               } as map
               SET de:Class //nothing will be mapped to these classes - only to the parent class which is created below
               SET de.label = apoc.text.join([x in [de.vg, map['label']] WHERE NOT x IS NULL], " ")                   
               SET de.short_label = apoc.text.join([x in [de.vg_short, map['short_label']] WHERE NOT x IS NULL], " ")           
               SET de.create = map['create']
               WITH *
               CALL apoc.merge.node(['Class'], map, {}, {}) YIELD node as dehl 
               MERGE (de)-[:SUBCLASS_OF]->(dehl)
               WITH *      
               MATCH (d:Dataset)-[:HAS_VARIABLE]->(v:Variable)-[:IS_DATA_ELEMENT]->(de:DataElement)
               SET v:Relationship
               SET v.label = v.Label
               SET v.short_label = v.Variable
               SET v.relationship_type = map.label
               MERGE (dehl)<-[:TO]-(v)-[:FROM]->(d)  
               WITH *
               MATCH (v)-[:HAS_CONTROLLED_TERM]->(t:Term)
               MERGE (dehl)-[:HAS_CONTROLLED_TERM]->(t)
               """
        self.query(q)

        # In the DM dateset, migrate the relationships going TO variables FROM the Unique Subject Identifier
        # variable/relationship to the Subject Class (created from the USI variable above)
        q = """
        MATCH (var:Variable:Relationship)
        WHERE var.Dataset = 'DM' AND var.Label = "Unique Subject Identifier"
        MATCH (var)<-[from_rel:FROM]-(rel:Relationship)
        DELETE from_rel
        WITH rel
        MATCH (subject:Class{label:'Subject'})
        MERGE (rel)-[:FROM]->(subject)
        """
        self.query(q)

        # Merging duplicate dehl Terms:
        # Note: this is rather a workaround
        # In the graph world for the case where 'Weight' is part of 4 codelists CVTEST, MOTEST, PCTEST, VSTEST
        # it would make more sense to have 1 codelist for all tests and value-level metadata based on the DOMAIN value
        q = """
           MATCH path=(dehl:Class)-[r:HAS_CONTROLLED_TERM]->(t:Term)
           WHERE NOT dehl:Variable
           WITH *
           ORDER BY dehl, t.`rdfs:label`, t.`Codelist Code`, t.`Term Code`
           WITH dehl, t.`rdfs:label` as Term, collect([r, t]) as coll
           WHERE size(coll) > 1
           WITH *, coll[0][-1] as template
           MERGE (dehl_term:Term{`Codelist Code`: 'P' + template.`Codelist Code`, `Term Code`: 'P' + template.`Term Code`})
           SET dehl_term.`rdfs:label` = template.`rdfs:label`
           WITH *
           MERGE (dehl)-[:HAS_CONTROLLED_TERM]->(dehl_term)
           WITH *
           UNWIND coll as pair
           WITH *, pair[0] as r, pair[1] as t
           MERGE (t)-[:TERM_POOLED_INTO]->(dehl_term)
           DELETE r
           """
        self.query(q)

        # Link Domain Abbreviation to all dehl classes
        q = """
           MATCH (de:DataElement)-[:SUBCLASS_OF]->(dehl:Class), (domain_class:Class{label:'Domain Abbreviation'})
           WHERE dehl <> domain_class AND NOT dehl.label in ['Subject', 'Study']
           MERGE (dehl)<-[:FROM]-(:Relationship{relationship_type:'DOMAIN'})-[:TO]->(domain_class)
           """
        self.query(q)

        if label_terms:
            # Labelling Terms
            q = """
               MATCH (c:Class)
               WHERE NOT (c:DataElement)-[:SUBCLASS_OF]->()
                   AND NOT c.create //NOT labelling the classes that get always created (e.g. --ORRES), otherwise duplicates appear
                   AND NOT (c:Variable AND c.Dataset STARTS WITH 'SUPP')
               WITH * 
               MATCH (c)-[:HAS_CONTROLLED_TERM]->(t:Term)
               WITH c, collect(t) AS coll
               WITH *
               CALL apoc.create.addLabels(coll, [c.label]) 
               YIELD node
               RETURN count(*)
               """
            self.query(q)

            if create_term_indexes:
                # Creating indexes for each Term label
                print("Creating indexes for each Term label")
                q = f"""
                   MATCH (c:Class) 
                   WHERE EXISTS ( (:Term)<-[:HAS_CONTROLLED_TERM]-(c) )
                   RETURN c.label as label        
                   """
                res = self.query(q)
                for r in res:
                    # print(f"Creating index for {r['label']}")
                    self.create_index(r['label'], ModelManager.RDFSLABEL)

        # Creating classes from SUPP domain Terms (only SUPPDM for now)
        print("Creating Class from SUPP domain Terms")
        q = """
           MATCH path=(t:Term)<-[:HAS_CONTROLLED_TERM]-(qnam:Variable),
           (qnam)<-[:HAS_VARIABLE]-(suppd:Dataset)<-[:SUPP_DATASET]-(d:Dataset)        
               WHERE qnam.Dataset STARTS WITH 'SUPP' AND qnam.Variable = 'QNAM'
               AND d.Dataset = 'DM' // TODO: to be removed when generalized for all SUPP domains
               //chellenges - (1) no 1:1 btw Term and `Decoded Value`; (2) no uniqueness of Term/`Decoded Value`: sz>1            
           //WITH t.`Decoded Value` as label
           WITH t.`Decoded Value` + ' (' + t.Term + ')' as label
           ,collect({t: t, qnam: qnam, suppd: suppd, d: d}) as coll         
           WITH *, size(coll) as sz
           UNWIND coll as map
           WITH label, map['t'] as t, map['qnam'] as qnam, map['suppd'] as suppd, map['d'] as d, sz
           FOREACH(_ IN CASE WHEN sz=1 THEN [1] ELSE [] END | 
               SET t:Class
               SET t.label = label
               SET t.short_label = t.Term
               SET t.create = False
               MERGE (d)<-[:FROM]-(:Relationship{relationship_type:t.label})-[:TO]->(t)             
           )                                  
           WITH *                
           MATCH path2 =(qnam)<-[:HAS_VARIABLE]-(ds:Dataset)-[:HAS_VARIABLE]->(qval:Variable)
               ,path3 = (qval)-[:HAS_VALUE_LEVEL_METADATA]->(vl:Valuelevel)-[:HAS_WHERE_CLAUSE]->(wc:`Where Clause`)
               ,path4 = (wc)-[:ON_VARIABLE]->(qnam)
               ,path5 = (wc)-[:ON_VALUE]->(t)
               ,path6 = (vl)-[:HAS_VL_TERM]->(vlterm:Term)
               WHERE qval.Variable = 'QVAL'
           MERGE (t)-[:HAS_CONTROLLED_TERM]->(vlterm)                       
           """
        self.query(q)

        # ------------------ LINKING-----------------------
        # (0)

        # -------- Creating  'qualifies' Relationship--------
        print("Creating Relationships based on SDTM ontology")
        q = """
           MATCH p=(x:Class)<-[:SUBCLASS_OF*0..1]-(:DataElement)-[:qualifies]->(:DataElement)-[:SUBCLASS_OF*0..1]->(y:Class)
           MERGE (subj)<-[:FROM]-(:Relationship{relationship_type:'QUALIFIES'})-[:TO]->(core)
           """
        self.query(q)

        # --------------- Custom links (business experience) -------------------:
        # additional 'qualifies' rel for business purposes:
        print("Creating additional Relationships based on business need")
        data = []
        data.append({'left': 'Subject', 'right': 'Study', 'rel': 'Study'})
        data.append({'left': 'Body System or Organ Class', 'right': 'Dictionary-Derived Term', 'rel': 'QUALIFIES'})
        data.append({'left': 'Visit Name', 'right': 'Visit Number', 'rel': 'QUALIFIES'})
        q = """
           UNWIND $data as row
           MATCH (left_c:Class), (right_c:Class)
           WHERE left_c.label = row['left'] and right_c.label = row['right']
           MERGE (left_c)<-[:FROM]-(:Relationship{relationship_type:row['rel']})-[:TO]->(right_c)
           """
        self.query(q, {'data': data})

        # (2)
        # -------- getting topics -------
        q = """
           MATCH (de:DataElement)-[:dataElementRole]->(der:DataElementRole)
           WHERE der.label = 'Topic Variable'
           RETURN de.dataElementLabel as topic_class
           """
        topics = self.query(q)
        # extending and updating topics:
        df_topics = pd.DataFrame(
            topics + [{"topic_class": "Dictionary-Derived Term"}]
        )
        # we rather use the long name as topic than the short name
        df_topics["topic_class"] = df_topics["topic_class"].replace({
            "Short Name of Measurement, Test or Examination": "Name of Measurement, Test or Examination"})
        topics = list(df_topics["topic_class"])

        # ------- getting Result Qualifiers and findings topics -------
        q = """
           MATCH (de:DataElement)-[:dataElementRole]->(der:DataElementRole),
           (de2:DataElement)-[:context]->(ctx:VariableGrouping)
           WHERE
             der.label = 'Result Qualifier'
             AND de2.dataElementLabel in $topics
             AND ctx.contextLabel = 'Findings Observation Class Variables'
           RETURN de.dataElementLabel as rq_class, de2.dataElementLabel as topic
           """
        df_resqs = pd.DataFrame(self.query(q, {"topics": topics}))

        # ------- linking Result Qualifiers to topics (Findings) -------
        print("Linking Result Qualifiers to Finding Topics")
        q = """
           UNWIND $data as row
           MATCH (c:Class), (c_topic:Class)
           WHERE c.label = row['rq_class'] AND c_topic.label = row['topic']
           MERGE (c_topic)<-[:FROM]-(r:Relationship)-[:TO]->(c)
           SET r.relationship_type = 'HAS_RESULT'
           """
        self.query(q, {"data": df_resqs.to_dict(orient="records")})

        # (3)
        # linking grouping classes to topics
        print("Linking grouping classes to topics")
        q = """
           MATCH (de:DataElement)-[:dataElementRole]->(der:DataElementRole)
           WHERE der.label = 'Grouping Qualifier'
           RETURN DISTINCT de.dataElementLabel as groupping_class
           """
        groupings = [res["groupping_class"] for res in self.query(q)]

        q = """
           MATCH (topic:Class), (gr:Class)
           WHERE topic.label in $topics and gr.label in $groupings
           MERGE (topic)<-[:FROM]-(:Relationship{relationship_type:'IN_CATEGORY'})-[:TO]->(gr)
           """
        self.query(q, {"topics": topics, "groupings": groupings})

        # (4)
        # category to subcategory
        print("Linking Category to Sub-Category")
        self.query("""
           MATCH (cat:Class), (scat:Class)
           WHERE cat.label = 'Category' and scat.label = 'Subcategory'
           MERGE (cat)<-[:FROM]-(:Relationship{relationship_type:'HAS_SUBCATEGORY'})-[:TO]->(scat)
           """)

    def automap_excel_based_model(self, domain: list, standard: str):
        # mapping to Dataset and Variable/DataElement classes
        q = """
           MATCH (sdt:`Source Data Table`), (ds:Dataset)
           WHERE sdt._domain_ = ds.Dataset
           MERGE (sdt)-[:MAPS_TO_CLASS]->(ds)
           WITH *
           MATCH (ds)-[:HAS_VARIABLE]->(v:`Variable`),
                 (sdt)-[:HAS_COLUMN]->(sdc:`Source Data Column`)
           WHERE sdc._columnname_ = v.Variable
           CALL apoc.do.when(
               v:Class,
               '
               WITH sdc, v        
               MERGE (sdc)-[:MAPS_TO_CLASS]->(v)  
               '
               ,
               '
               WITH sdc, v
               MATCH (v)-[:IS_DATA_ELEMENT]->()-[:SUBCLASS_OF]->(dehl)
               MERGE (sdc)-[:MAPS_TO_CLASS]->(dehl)  
               '
               ,
               {sdc:sdc, v:v}
           ) YIELD value               
           RETURN *
           """
        self.query(q)

        # mapping to SUPP-- Term Classes
        q = """                
           MATCH (t:Term:Class), (sdc:`Source Data Column`)
           WHERE t.short_label = sdc._columnname_
           MERGE (sdc)-[:MAPS_TO_CLASS]->(t) 
           """
        self.query(q)

        q = f"""
        MATCH (sdt:`Source Data Table`)-[:HAS_COLUMN]->(sdc:`Source Data Column`), (d:Dataset)-->(v:Variable)
        WHERE sdt._domain_ = d.Dataset and sdc._columnname_ = v.Variable
        SET sdc.Order = v.Order
        SET sdc.Core = v.Core
        """

        self.query(q)

        # add the Data Extraction Standard node to the db and attach it to the Source Data Tables
        q = """
               MERGE (sdf:`Data Extraction Standard`{_tag_:$standard})
               WITH sdf
               MATCH (sdt:`Source Data Table`)
               MERGE (sdf)-[:HAS_TABLE]->(sdt)
               """
        params = {'standard': standard}
        self.query(q, params)

        # set the SortOrder property for each source data table in the graph
        self.set_sort_order(domain=domain, standard=standard)
        # Extend the extraction metadata with MAPS_TO_COLUMN rel between relationship and source data column nodes
        self.extend_extraction_metadata(domain=domain, standard=standard)

    def set_sort_order(self, domain: list, standard: str):

        for dom in domain:
            q = """
               MATCH (sdf:`Data Extraction Standard`{_tag_:$standard})-[:HAS_TABLE]->(sdt:`Source Data Table`{_domain_:$domain})-[:HAS_COLUMN]->(sdc:`Source Data Column`)
               WITH sdc, sdt
               ORDER BY sdc.Order
               WITH collect(sdc._columnname_) AS col_order, sdt
               SET sdt.SortOrder = col_order
               """
            params = {'domain': dom, 'standard': standard}

            self.query(q, params)

            q = """
               MATCH (sdf:`Data Extraction Standard`{_tag_:$standard})-[:HAS_TABLE]->(sdt:`Source Data Table`{_domain_:$domain})-[:HAS_COLUMN]->(sdc:`Source Data Column`)
               WITH sdc, sdt
               ORDER BY sdc.Order
               WITH collect(sdc._columnname_) AS col_order, sdt
               SET sdt.SortOrder = col_order
               """
            params = {'domain': dom, 'standard': standard}

            self.query(q, params)

    def extend_extraction_metadata(self, domain: list, standard: str):
        # Adds the relationship MAPS_TO_COLUMN between the source data column node and the relationship that
        # sdc node's variable is pointing 'TO'. WHERE that relationship is 'FROM' a core class (ie FA, EX, VS, ... etc)
        for dom in domain:
            q = """
               MATCH (sdf:`Data Extraction Standard`{_tag_:$standard})-[:HAS_TABLE]->(sdt:`Source Data Table`{_domain_:$table})
               , (sdt)-[:HAS_COLUMN]->(sdc:`Source Data Column`)-[:MAPS_TO_CLASS]->(c:Class)<-[:TO]-(r:Relationship)
               , (r)-[:FROM]-(c2:Class)
               WHERE c2.short_label = $table
               MERGE (r)-[:MAPS_TO_COLUMN]-(sdc)
               """
            params = {'standard': standard, 'table': dom}
            self.query(q, params)
