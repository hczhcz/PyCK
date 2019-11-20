import pathlib


dir_path = pathlib.Path(__file__).parent
binary_path = dir_path.joinpath('clickhouse')
config_path = dir_path.joinpath('config.xml')
