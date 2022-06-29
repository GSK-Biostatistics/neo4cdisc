# cdisc_standard_loader

CdiscStandardLoader is a subclass of the *ModelApplier* class ([model_applier.py](https://github.com/GSK-Biostatistics/tab2neo/model_appliers/model_applier.py)) which has methods for restructuring data loaded into Neo4j. 

Methods:
- load_standard - Loads the standards files: SDTM Model metadata, SDTM Implementation Guide metadata and SDTM Controlled Terminology into Neo4j.
  Also loads metadata included in the repository ([domain sort order](../cdisc_data/sdtmig3_2_domain_sort_order.json) and [domain labels](../cdisc_data/sdtmig3_3_domain_labels.json))
  After metadata is loaded it calls the other methods.
- reshape_model - Reshapes/Harmonises SDTM model loaded into Neo4j, such as changing labels on nodes from column names from the imported CSV file
- reshape_sdtmig - Reshapes/Harmonises SDTM IG loaded into Neo4j, such as changing labels on nodes from column names from the imported CSV file
- reshape_terminology - Reshapes/Harmonises SDTM CT loaded into Neo4j,, such as changing labels on nodes from column names from the imported CSV file
- link_cdisc - Adds relationships between metadata
- load_link_sdtm_ttl - Adds relationsips and properties found in RDF [sdtm-1-3.ttl](../cdisc_data/sdtm-1-3.ttl)

[cdisc_standard_loader](cdisc_standard_loader.py).




