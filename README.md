# neo4cdisc


[Example](CDISC Example.ipynb) of loading the FDA CDISC pilot study into Neo4J using the [tab2neo](https://github.com/GSK-Biostatistics/tab2neo) python package. 

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

## Installation Instructions
A detailed description of the installation steps can be found [here](README_install.md).



