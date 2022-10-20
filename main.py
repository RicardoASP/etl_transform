import os
from log_config import logger
from main_utils import Files, MapFiles

files = Files()
mapping = MapFiles()

logger.logInfo("process start")
for filename in os.listdir(files.data_files_path):
    #check if file was processed already
    if filename in files.processed_files:
        logger.logInfo(f"file:{filename} processed already")
        continue
    logger.logInfo(f"processing file:{filename}")

    logger.logInfo(f"get schema for file:{filename}")
    schema = files.get_schema(files.get_schema_filename(filename))

    if not schema:
        logger.logError(f"File name:{filename} | Error: schema do not exists")
        logger.logInfo(f"Schema for File:{filename} do not exists")
        continue

    logger.logInfo(f"read extract data from file:{filename}")
    payload = files.read_file(files.data_files_path, filename, skip_header=False)

    if not payload:
        logger.logError(f"File name:{filename} | Error: cannot be read or was empty")
        logger.logInfo(f"File:{filename} cannot be read or file was empty")
        continue

    logger.logInfo(f"map data rows of file: {filename}")
    for row in payload:
        mapped_row = mapping.map_data(row, schema, filename)
        files.output.append(mapped_row)

    if not files.output:
        logger.logError(f"File name:{filename} | Error: cannot be mapped")
        logger.logInfo(f"File:{filename} cannot be mapped")
        continue

    logger.logInfo(f"create ndjson file for data from file: {filename}")
    output_file = files.to_ndjson(files.to_json(files.output), filename)

    if not output_file:
        logger.logError(f"File name:{filename} | Error: ndjson payload can not be generated")
        logger.logInfo(f"File:{filename} ndjson payload can not be generated")
        continue

    if files.write_file(output_file, filename):
        logger.logInfo(f"add file: {filename} to processed file")
        files.add_to_processed_file(filename)
        files.reset_output()  # reseting output class attribute
        logger.logInfo(f"ndjson file created successfully for source file: {filename}")
    else:
        files.reset_output()  # reseting output class attribute
        logger.logError(f"File name: {filename} | Error: ndjson file was not created")
        logger.logInfo(f"ndjson file was not created for source file: {filename}")

logger.logInfo(f"process completed")