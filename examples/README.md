# Examples

reshape_cdisc.py

comparison_cdiscpilot.py

download_cdisc_pilot.py

load_sdtm_metadata.py







**Example call**

```
standards_folder = "cdisc_data"
standards_model  = "SDTM_v1.4.csv"
standards_file   = "SDTMIG_v3.2.csv"
sdtm_terminology = "CT2022Q1.csv"
csl = CdiscStandardLoader(standards_folder=standards_folder, sdtm_file=standards_model, sdtmig_file=standards_file, terminology_file=sdtm_terminology)
```




