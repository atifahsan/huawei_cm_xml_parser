import os
import csv
import time
import fnmatch
import concurrent.futures
from lxml import etree


INPUT = '.\\Input'
OUTPUT = '.\\Output'


def process_file(xmlfile):
    """
    XML tree structure: 
    > bulkCmConfigDataFile(root) 
    > configData 
    > class {bts} 
    > object {bts_version} 
    > class {MO} 
    > object {rows} 
    > parameters
    """
    input_filename = xmlfile.split('\\')[-1]
    tree = etree.parse(xmlfile)
    root = tree.getroot()
    date_time = tree.xpath('/bulkCmConfigDataFile/fileFooter')[0].attrib['dateTime']
    output_filename = input_filename.strip('.xml') +'_xml_'+ date_time.replace(':','') +'.csv'    
    
    config_data = root.getchildren()[1]
    bts = config_data.getchildren()[0]
    bts_version = bts.getchildren()[0]

    # Loop over MOs
    for mo in bts_version:
        folder_name = mo.attrib['name']
        create_folder(folder_name)
        rows = extract_para(mo, date_time, input_filename)
        write_data(rows, folder_name, output_filename)


def write_data(rows, folder_name, output_filename):
    all_keys = list(set().union(*(d.keys() for d in rows)))
    with open(os.path.join(OUTPUT, folder_name, output_filename), 'w') as csv_file:
        dict_writer = csv.DictWriter(csv_file, all_keys, lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(rows)


def extract_para(mo, date_time, input_filename):
    rows = []
    for node in mo:
        para_dict = {'dateTime': date_time, 'INPUT_FILENAME': input_filename}
        for row in node:
            para_dict[row.attrib['name']] = row.attrib['value']
        rows.append(para_dict)
    return rows


def create_folder(folder_name):
    folder_path = os.path.join(OUTPUT, folder_name)
    while not os.path.exists(folder_path):
        try:
            os.makedirs(folder_path)
        except:
            pass


def init_multiprocess():
    start = time.time()
    with concurrent.futures.ProcessPoolExecutor() as executor:
        files = []
        for root, dirnames, filenames in os.walk(INPUT):
            for filename in fnmatch.filter(filenames, '*.xml'):
                files.append(os.path.join(root, filename))
        count = 0

        for output_file in executor.map(process_file, files):
            count = count + 1
            print(f'Output file {count} of {len(files)} processed')
    print("\nTotal time taken: ", time.time() - start)


if __name__ == "__main__":
    init_multiprocess()