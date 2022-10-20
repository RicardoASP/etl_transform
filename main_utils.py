import os
import json
from io import StringIO
from log_config import logger
import shutil
from datetime import datetime

class Files:
    def __init__(self):
        self.local_path = os.path.abspath(os.path.dirname(__file__))
        self.data_files_path = self.local_path + "/data_files/"
        self.spec_files_path = self.local_path + "/spec_files/"
        self.output_files_path = self.local_path + "/output_files/"
        self.processed_files_path = self.local_path + "/processed_files/"
        self.test_files = self.local_path + "/tests/test_files/"
        self.processed_files = self.get_processed_file()
        self.output = list()

    def reset_output(self):
        '''
         reset output attribute after payload dump into ndjson file
        '''
        self.output = list()
        return

    def read_file(self, path, filename, skip_header=False):
        '''
            get data from any file and skip header if needed
        '''
        try:
            with open(path+filename, 'r') as data_file:
                if skip_header:
                    payload = data_file.readlines()[1:] #skip file header
                else:
                    payload = data_file.readlines()

                return payload
        except Exception as error:
            logger.logError(f"File name: {filename} | Error: {error}")
            return None

    def get_schema_filename(self, data_filename):
        '''
            get schema file name for data file to be processed
        '''
        split_data_filename = data_filename.split('_')
        return split_data_filename[0]+'.csv'

    def get_schema(self, filename):
        '''
           get schema for data file to be processed
        '''
        try:
            schema = list()
            spec_file = self.read_file(self.spec_files_path, filename, skip_header=True)
            current_byte = 0
            for row in spec_file:
                field = dict()
                split_row = row.split(',')
                field['field_name'] = str(split_row[0])
                field['start'] = current_byte
                field['end'] = current_byte + int(split_row[1])
                field['data_type'] = str(split_row[2]).replace("\n",'')
                schema.append(field)
                current_byte = field['end']
            return schema
        except Exception as error:
            logger.logError(f"File name: {filename} | Error: {error}")
            return None

    def to_json(self, payload):
        '''
            transform data into json format
        '''
        output_json = json.dumps(payload, indent=2)
        return output_json

    def to_ndjson(self, payload, filename):
        '''
           transform json into ndjson format
        '''
        try:
            json_str = StringIO(f"{payload}")
            output_ndjson = [json.dumps(row) for row in json.load(json_str)]
            return output_ndjson
        except Exception as error:
            logger.logError(f"File name: {filename} | Error: {error}")
            return None

    def write_file(self,payload, data_filename):
        '''
            create output file in .ndjson format
        '''
        try:
            split_data_filename = data_filename.split('.')
            output_file = open(self.output_files_path+split_data_filename[0]+".ndjson","w")
            output_file.write('\n'.join(payload))
            output_file.close()
            return True
        except Exception as error:
            logger.logError(f"File name: {data_filename} | Error: {error}")
            return False

    def get_processed_file(self):
        '''
            get list of files already processed
        '''
        filename = "processed.txt"
        payload = self.read_file(self.processed_files_path, filename, skip_header=False)
        if payload:
            processed_file = [x.replace("\n","").strip() for x in payload]
            return processed_file
        else:
            return list()

    def add_to_processed_file(self,filename):
        '''
            add processed file name to processed_file file
        '''
        processed_file = open(self.processed_files_path + "processed.txt", "a")
        processed_file.write(f"{filename} \n")
        processed_file.close()
        return

    #moving files can be implemented instead of a filename log or db to improve processing time
    def move_processed_file(self, filename):
        '''
            this method can be used instead of log file or db. could improve processing time
        '''
        os.rename(self.data_files_path+filename,self.processed_files_path+filename)
        return

class MapFiles:
    def apply_data_type(self, data, data_type, filename):
        '''
            apply correct data type to data based on schema directions
        '''
        if data_type == 'TEXT':
            return str(data).strip() #remove spaces if any
        elif data_type == 'BOOLEAN':
            return bool(int(data))
        elif data_type == 'INTEGER':
            return int(data)
        elif data_type == 'FLOAT':
            return float(data)
        elif data_type == 'DATE':
            return str(data) #if formated needed datetime.strptime(date_time_str, '%d/%m/%y %H:%M:%S')
        else:
            logger.logError(f"File name: {filename} | Data type: {data_type} | Error: Unknown data type")
            return data

    def map_data(self, data, schema, filename):
        '''
            map data point based on schema
        '''
        mapped_data = dict()
        try:
            for field in schema:
                mapped_data[field['field_name']] = self.apply_data_type(data[field['start']:field['end']], field['data_type'], filename)
            return mapped_data
        except Exception as error:
            logger.logError(f"File name: {filename} | Error: {error}")
            return None