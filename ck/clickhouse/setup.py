import ast
import os
import pathlib
import typing
import xml.etree.ElementTree


def create_config(
        tcp_port: int,
        http_port: int,
        user: str,
        password: str,
        data_dir: str,
        # notice: recursive type
        config: typing.Dict[str, typing.Any]
) -> None:
    path = pathlib.Path(data_dir)

    tmp_path = path.joinpath('tmp')
    format_schema_path = path.joinpath('format_schemas')
    user_files_path = path.joinpath('user_files')
    access_control_path = path.joinpath('access')
    log_path = path.joinpath('stdout.log')
    errorlog_path = path.joinpath('stderr.log')
    config_path = path.joinpath('config.xml')

    memory_size = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
    memory_bound_1 = int(0.6 * memory_size)
    memory_bound_2 = int(0.5 * memory_size)
    memory_bound_3 = int(0.1 * memory_size)

    # add server settings

    data = {
        'listen_host': '0.0.0.0',
        'tmp_path': str(tmp_path),
        'format_schema_path': str(format_schema_path),
        'user_files_path': str(user_files_path),
        'access_control_path': str(access_control_path),
        'mark_cache_size': '5368709120',
        'logger': {
            'log': str(log_path),
            'errorlog': str(errorlog_path),
        },
        'query_log': {
            'database': 'system',
            'table': 'query_log',
        },
        'profiles': {},
        'users': {},
        'quotas': {},
        **config.copy(),
        'tcp_port': str(tcp_port),
        'http_port': str(http_port),
        'path': str(path),
    }

    # add profile

    if not isinstance(data['profiles'], dict):
        raise TypeError()

    data['profiles'] = {
        'default': {},
        **data['profiles'],
    }

    if not isinstance(data['profiles']['default'], dict):
        raise TypeError()

    data['profiles']['default'] = {
        'max_memory_usage_for_all_queries': str(memory_bound_1),
        'max_memory_usage': str(memory_bound_1),
        'max_bytes_before_external_group_by': str(memory_bound_2),
        'max_bytes_before_external_sort': str(memory_bound_2),
        'max_bytes_in_distinct': str(memory_bound_2),
        'max_bytes_before_remerge_sort': str(memory_bound_3),
        'max_bytes_in_set': str(memory_bound_3),
        'max_bytes_in_join': str(memory_bound_3),
        'log_queries': '1',
        'join_use_nulls': '1',
        'join_algorithm': 'auto',
        'input_format_allow_errors_num': '100',
        'input_format_allow_errors_ratio': '0.01',
        'date_time_input_format': 'best_effort',
        **data['profiles']['default'],
    }

    # add user

    if not isinstance(data['users'], dict):
        raise TypeError()

    data['users'] = {
        user: {},
        **data['users'],
    }

    if not isinstance(data['users'][user], dict):
        raise TypeError()

    data['users'][user] = {
        'access_management': '1',
        'networks': {
            'ip': '::/0',
        },
        'profile': 'default',
        'quota': 'default',
        **data['users'][user],
        'password': password,
    }

    # add quota

    if not isinstance(data['quotas'], dict):
        raise TypeError()

    data['quotas'] = {
        'default': {},
        **data['quotas'],
    }

    # generate xml

    def build_xml(
            # notice: recursive type
            data: typing.Any,
            node: xml.etree.ElementTree.Element
    ) -> None:
        if isinstance(data, dict):
            for key, value in data.items():
                if not isinstance(key, str):
                    raise TypeError()

                subnode = xml.etree.ElementTree.SubElement(node, key)
                build_xml(value, subnode)
        elif isinstance(data, str):
            node.text = data
        else:
            raise TypeError()

    root = xml.etree.ElementTree.Element('yandex')

    for key, value in data.items():
        subnode = xml.etree.ElementTree.SubElement(root, key)
        build_xml(value, subnode)

    # write xml

    config_path.open('wb').write(xml.etree.ElementTree.tostring(root))


if __name__ == '__main__':
    create_config(**ast.literal_eval(input()))
