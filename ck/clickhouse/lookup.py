import pathlib


def default_data_dir():
    return str(pathlib.Path.home().joinpath('.ck_data'))


def binary_file():
    return str(pathlib.Path(__file__).parent.joinpath('clickhouse'))


if __name__ == '__main__':
    print(default_data_dir())
    print(binary_file())
