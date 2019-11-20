import pathlib


def binary_path():
    return pathlib.Path(__file__).parent.joinpath('clickhouse')


def config_path():
    return pathlib.Path(__file__).parent.joinpath('config.xml')


if __name__ == '__main__':
    print(binary_path())
    print(config_path())
