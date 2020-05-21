import setuptools  # type: ignore[import]


if __name__ == '__main__':
    setuptools.setup(
        install_requires=['paramiko', 'pyarrow', 'typing_extensions'],
        name='ck',
        package_data={'ck.clickhouse': ['clickhouse']},
        packages=setuptools.find_packages(),
        zip_safe=False
    )
