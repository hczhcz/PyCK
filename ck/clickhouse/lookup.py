import pathlib


def default_data_path():
    return str(pathlib.Path.home().joinpath('.ck_data'))


def binary_path():
    return str(pathlib.Path(__file__).parent.joinpath('clickhouse'))


def config_path():
    return str(pathlib.Path(__file__).parent.joinpath('config.xml'))


if __name__ == '__main__':
    print(default_data_path())
    print(binary_path())
    print(config_path())
