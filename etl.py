from extract import etl_extract
from load import etl_load
from transform import etl_transform
from utils import read_config


def main():
    # Input values
    config_file = 'dl.cfg'
    src = 'src_s3'  # 'src_local' or 'src_s3'

    # Read input
    config = read_config(config_file)
    base_path = config[src]['base_path']
    song_path = config[src]['song_path']
    log_path = config[src]['log_path']
    output_path = config[src]['output_path']
    aws_creds = dict(config['AWS'])  # the keys become lowercase

    # Read JSON files (EXTRACT)
    song_log_data = etl_extract(base_path, song_path, log_path, aws_creds)
    song_data = song_log_data['song_data']
    log_data = song_log_data['log_data']

    # Process data (TRANSFORM)
    tables = etl_transform(song_data, log_data)

    # Save processed data (LOAD)
    etl_load(output_path=output_path, tables=list(tables.values()))


if __name__ == '__main__':
    main()
