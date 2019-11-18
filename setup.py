import setuptools


if __name__ == '__main__':
    setuptools.setup(
        name='ck',
        packages=['ck'],
        package_data={'ck': ['clickhouse']},
        zip_safe=False
    )
