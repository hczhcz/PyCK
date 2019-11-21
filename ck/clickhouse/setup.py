import ast
import os
import pathlib
import xml.etree.ElementTree


def create_config(
    tcp_port,
    http_port,
    data_dir,
    config
):
    assert type(tcp_port) is int
    assert type(http_port) is int
    assert type(data_dir) is str
    # notice: recursive type checking
    assert type(config) is dict

    path = pathlib.Path(data_dir)

    tmp_path = path.joinpath('tmp')
    format_schema_path = path.joinpath('format_schema')
    user_files_path = path.joinpath('user_files')
    log_path = path.joinpath('stdout.log')
    errorlog_path = path.joinpath('stderr.log')
    config_path = path.joinpath('config.xml')

    memory_size = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
    memory_bound_1 = int(0.8 * memory_size)
    memory_bound_2 = int(0.75 * memory_size)

    data = config.copy()

    # add server settings

    data['tcp_port'] = str(tcp_port)
    data['http_port'] = str(http_port)

    if 'listen_host' not in data:
        data['listen_host'] = '0.0.0.0'

    if 'path' not in data:
        data['path'] = str(path)

    if 'tmp_path' not in data:
        data['tmp_path'] = str(tmp_path)

    if 'format_schema_path' not in data:
        data['format_schema_path'] = str(format_schema_path)

    if 'user_files_path' not in data:
        data['user_files_path'] = str(user_files_path)

    if 'mark_cache_size' not in data:
        data['mark_cache_size'] = '5368709120'

    if 'logger' not in data:
        data['logger'] = {
            'log': str(log_path),
            'errorlog': str(errorlog_path),
        }

    if 'query_log' not in data:
        data['query_log'] = {
            'database': 'system',
            'table': 'query_log',
        }

    # add default profile

    if 'profiles' in data:
        assert type(data['profiles']) is dict
    else:
        data['profiles'] = {}

    if 'default' in data['profiles']:
        assert type(data['profiles']['default']) is dict
    else:
        data['profiles']['default'] = {}

    profile = data['profiles']['default']

    if 'max_memory_usage_for_all_queries' not in profile:
        memory_limit = str(memory_bound_1)

    if 'max_memory_usage' not in profile:
        profile['max_memory_usage'] = str(memory_bound_1)

    if 'max_bytes_before_external_group_by' not in profile:
        profile['max_bytes_before_external_group_by'] = str(memory_bound_2)

    if 'max_bytes_before_external_sort' not in profile:
        profile['max_bytes_before_external_sort'] = str(memory_bound_2)

    if 'log_queries' not in profile:
        profile['log_queries'] = '1'

    if 'join_use_nulls' not in profile:
        profile['join_use_nulls'] = '1'

    if 'input_format_allow_errors_num' not in profile:
        profile['input_format_allow_errors_num'] = '100'

    if 'input_format_allow_errors_ratio' not in profile:
        profile['input_format_allow_errors_ratio'] = '0.01'

    if 'date_time_input_format' not in profile:
        profile['date_time_input_format'] = 'best_effort'

    # add default user

    if 'users' in data:
        assert type(data['users']) is dict
    else:
        data['users'] = {}

    if 'default' in data['users']:
        assert type(data['users']['default']) is dict
    else:
        data['users']['default'] = {}

    user = data['users']['default']

    if 'profile' not in user:
        user['profile'] = 'default'

    if 'quota' not in user:
        user['quota'] = 'default'

    if 'password' not in user:
        user['password'] = ''

    if 'networks' not in user:
        user['networks'] = {
            'ip': '::/0',
        }

    # add default quota

    if 'quotas' in data:
        assert type(data['quotas']) is dict
    else:
        data['quotas'] = {}

    if 'default' in data['quotas']:
        assert type(data['quotas']['default']) is dict
    else:
        data['quotas']['default'] = {}

    # generate xml

    def build_xml(data, node):
        # notice: recursive type checking
        assert type(data) is dict or type(data) is str

        if type(data) is dict:
            for key, value in data.items():
                subnode = xml.etree.ElementTree.SubElement(node, key)
                build_xml(value, subnode)
        else:
            node.text = data

    root = xml.etree.ElementTree.Element('yandex')

    for key, value in data.items():
        subnode = xml.etree.ElementTree.SubElement(root, key)
        build_xml(value, subnode)

    # write xml

    config_path.open('wb').write(xml.etree.ElementTree.tostring(root))


if __name__ == '__main__':
    create_config(**ast.literal_eval(input()))
