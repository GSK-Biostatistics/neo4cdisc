import utils.comparison_utils
import datacompy
import numpy as np
import re
import time
from cdisc_data_providers.sdtm_data_provider import SDTMDataProvider
from colorama import init, Fore, Back
from data_loaders import file_data_loader
import logging

init(autoreset=True)
log = logging.getLogger("neo4j")

# Call instances
dp = SDTMDataProvider(rdf=True, verbose=False)
ul = utils.comparison_utils
dl = file_data_loader.FileDataLoader()

def main(data_folder, domains):
    start_time = time.time()
    overall_col = []
    overall_row = []

    # Generate dataset and comparison report
    tables = ul.get_compare_tables(folder=data_folder, domains=domains)
    # TODO: Supplemental domains not supported. Remove domains starting with SUPP
    tables = {k: v for (k,v) in tables.items() if not k.startswith('SUPP')}
    for table in tables.items():
        print(Fore.GREEN + Back.BLUE + f'Extracting Data: {table} \n')
        df_output_raw = dp.get_data_sdtm(standard=standard_label, domain=table[0], where_map=None)

        print(Fore.RED + Back.YELLOW + f'Processing table: {table} \n')
        df_input = dl.read_file(folder=data_folder, filename=table[1])[0]

        sorting_variable = ul.sorting_varible(table=table[0], standard=standard_label, neo=dp)
        print(f'Sorting Var: {sorting_variable}')
        sorting_var = [col for col in sorting_variable if col in df_input.columns]
        print(f'Sorting variables: {sorting_var}\n')

        meta = dp.neo_get_meta(standard=standard_label, table=table[0])

        def _sorter(x):
            # Some columns don't have an order set, so create an artificial one
            none_count = 1000
            if not x[1]:
                order = none_count
                none_count += 1
            else:
                order = x[1]
            return order

        _col_order = [(k, v) for k, v in sorted(meta[0]['order_dct'].items(), key=lambda x: x[0])]
        col_order = [k for k, v in sorted(_col_order, key=lambda x: _sorter(x))]

        expected_cols = [col for col in list(df_input.columns) if col in col_order]
        print(f'Expected columns: {expected_cols}\n')
        # sorting input dataset based on standard sorting varibles
        df_input_raw = df_input.sort_values(by=sorting_var, ignore_index=True)

        # Excluding extra columns from both input and output datasets
        df_input = df_input_raw[expected_cols]
        df_output = df_output_raw[expected_cols]

        Input_col_excl = datacompy.Compare(df_input_raw, df_input, on_index=True).df1_unq_columns()
        Output_col_excl = datacompy.Compare(df_output_raw, df_output, on_index=True).df1_unq_columns()
        message = "Excluding extra columns prior to comparison\n------------------------------------------\nA total of {0} extra columns are excluded from input dataset: {1}\nA total of {2} extra columns are excluded from output dataset: {3}"
        print(message.format(len(Input_col_excl), Input_col_excl, len(Output_col_excl), Output_col_excl))
        print('---------------------------------------------------\n\n')
        compare = datacompy.Compare(df_input, df_output, on_index=True, df1_name='InputData', df2_name='OutputData')
        rep = compare.report()
        print(Fore.BLUE + rep)

        # Metrics calculation and report
        total_ncol = df_input.shape[1]
        match_ncol = int(re.findall('Number of columns compared with all values equal: (\d+)', rep)[0])
        colmatch_per = match_ncol * 100 / total_ncol
        total_nrow = df_input.shape[0]
        match_nrow = total_nrow - int(re.findall('Number of rows with some compared columns unequal: (\d+)', rep)[0])
        rowmatch_per = match_nrow * 100 / total_nrow

        message = "Domain {0}: \n----------\nColumns: total--{1}; exact match--{2}, matched percentage-{3:.3f}%\nRows: total--{4}; exact match--{5}, matched percentage--{6:.3f}%\n"
        print(Fore.MAGENTA + message.format(table[0], total_ncol, match_ncol, colmatch_per, total_nrow, match_nrow,
                                            rowmatch_per))

        overall_col.append(colmatch_per)
        overall_row.append(rowmatch_per)

    col_avg = np.mean(overall_col)
    row_avg = np.mean(overall_row)
    message = "\nOverall summary for {0} domains:\n------------------------------\nColumns: matched percentage--{1:.3f}%\nRows: matched percentage--{2:.3f}%\n"
    print(Fore.MAGENTA + message.format(len(domains), col_avg, row_avg))
    print(f"Total execution time: {(time.time() - start_time):.2f}' seconds")


if __name__ == "__main__":
    # Set Neo4j label for the metadata standard loaded
    # If it does not exist, add node in Neo4j
    # q = """
    # MERGE (des:`Data Extraction Standard`{_tag_:'SDTM 3.2.csv'})
    # MATCH (sdt:`Source Data Table`)
    # MERGE (des)-[:HAS_TABLE]->(sdt)
    # """
    # res = dp.query(q=q)
    standard_label = "SDTMIG_v3.2.csv"
    # Folder where datasets exist
    data_folder = '/temp/data/sdtm/cdiscpilot01'
    # Select domains to compare
    # N.B! Supplemental domains cannot be compared. Only SUPPQUAL exist in CDISC metadata, not specific supplemental domains, e.g. SUPPDM, SUPPAE
    domains = ['dm', 'ae', 'vs']
    main(data_folder, domains)


