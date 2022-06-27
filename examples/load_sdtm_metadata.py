from data_loaders import file_data_loader
import time


def main():
    start_time = time.time()

    #loading data
    #dl = file_data_loader.FileDataLoader(host ="neo4j://10.40.225.48:17687", credentials = None)
    dl = file_data_loader.FileDataLoader()
    dl.clean_slate(keep_labels=["Message"])
    dl.load_file(folder="cdisc_data", filename="SDTMIG_v3.2.csv", load_to_neo=True,
                 colcharsbl=r'[^A-Za-z0-9_]+')
    # dl.load_file(folder="../cdisc_data", filename="ct-sdtm-ncievs-2013-12-20_codes.xls", load_data_to_neo=True,
    #              colcharsbl=r'[^A-Za-z0-9_]+')
    # dl.load_file(folder="../cdisc_data", filename="ct-sdtm-ncievs-2013-12-20_codelists.xls", load_data_to_neo=True,
    #              colcharsbl=r'[^A-Za-z0-9_]+')
    dl.link_nodes_on_matching_property(label1='Source Data Table', label2='Source Data Row',
                                                property1='_domain_', property2='_domain_',
                                                rel='HAS_DATA')


    #cleaning trailing spaces at the end of text in the following columns
    for key in ["Role", "Variable_Name", "Variable_Label", "Observation_Class", "Domain_Prefix"]:
        dl.query("""
        MATCH (d:`Source Data Row`) WHERE EXISTS (d[$key])
        CALL apoc.create.setProperty(d, $key, apoc.text.regreplace(d[$key], '\s+$', '')) YIELD node
        RETURN node 
        """, {"key": key})

    print(f"--- {(time.time() - start_time):.3f}' seconds ---")

if __name__ == "__main__":
    main()
