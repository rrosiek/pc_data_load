from config import CSV_HEADERS, ROOT_DIRECTORY
import os

def run(directory):
    for name in os.listdir(directory):
        file_path = os.path.join(directory, name)
        if os.path.isfile(file_path) and name in CSV_HEADERS:
            header = CSV_HEADERS[name]
            add_header(header, name, file_path)


def add_header(header, file_name, file_path):
    print 'Adding header', header, file_name, file_path

    file_name = os.path.basename(file_path)
    directory = os.path.dirname(file_path)

    output_file_name = 'header_' + file_name
    output_file_path = os.path.join(directory, output_file_name)
    try:
        output_file = open(output_file_path, 'r')
        print output_file_path, 'exists, skipping clean...'
        return output_file_path
    except Exception as e:
        print e

    output_file = open(output_file_path, 'w')
    output_file.write(header + '\n')

    with open(file_path, "r") as ins:
        line_count = 0
        for line in ins:
            if line is not None and len(line) > 0:
                line_count += 1
                line_comps = line.split('\x00')
                clean_line = ' '.join(line_comps)
                if line_count % 500000:
                    print 'Processing line:', str(line_count)
                output_file.write(clean_line + '\n')

    output_file.close()
    return output_file_path


run('/data/data_loading/irdb_2018_06/')