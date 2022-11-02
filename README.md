# neo4cdisc


[Example](examples/reshape_cdisc.py) of loading the FDA CDISC pilot study into Neo4J using the [tab2neo](https://github.com/GSK-Biostatistics/tab2neo) python package.
Note: the script might require long time to run. 

It uses a simple approach on importing column based metadata and data into Neo4j using rows as nodes and columns as properties for the nodes.
The imported nodes are then reshaped to show relationships found in metadata and also relate the metadata with data.
 
It has been tested with csv-files downloaded from the CDISC Library. Excel files should be possible to use as well, if the column headers match as they are used as default name for properties created in Neo4j.

_CDISC metadata used_
- Study Data Tabulation Model (SDTM)
- SDTM Implementation Guide (SDTMIG)
- SDTM Controlled Terminology
- SDTM 

*CDISC data used*
- cdiscpilot01 - from [Phuse Scripts on Github](https://github.com/phuse-org/phuse-scripts/tree/master/data/sdtm/cdiscpilot01)

## CdiscStandardLoader
Python class to import CDISC metadata into Neo4j [details](cdisc_model_managers/README.md)

[source code](cdisc_model_managers/cdisc_standard_loader.py)

##

Parameters:
- standards_folder - path to folder containing metadata files
- sdtm_file - name of file containing Study Data Tabulation Model (SDTM) metadata
- sdtmig_file: name of file containing SDTM Implementation Guide metadata
- terminology_file: name of file containing SDTM Controlled Terminology


**Example call**

```
standards_folder = "cdisc_data"
standards_model  = "SDTM_v1.4.csv"
standards_file   = "SDTMIG_v3.2.csv"
sdtm_terminology = "CT2022Q1.csv"
csl = CdiscStandardLoader(standards_folder=standards_folder, sdtm_file=standards_model, sdtmig_file=standards_file, terminology_file=sdtm_terminology)
```




