import setuptools


if __name__ == '__main__':
    setuptools.setup(
        install_requires=['paramiko'],
        name='ck',
        package_data={'ck.clickhouse': ['clickhouse', 'config.xml']},
        packages=setuptools.find_packages(),
        zip_safe=False
    )
