import os

SETUP_PATH = './setup.py'
INIT_PATH = './anduin/__init__.py'
COPY_RIGHT_FILE = './COPYING.txt'
ANDUIN_VER = '5.0.14'

def add_copy_right_and_version():
    # pass
    print('写入版权数据，生成版本信息')
    split_line = '\n# <=========>\n'
    cpright = '"""' + open(COPY_RIGHT_FILE).read() + '"""'
    code_content = open(INIT_PATH).read().split(split_line)
    if len(code_content) == 1:
        code = code_content[0]
    else:
        code = code_content[1]
    code_list = code.split('\n')
    code_data = ''
    for line in code_list:
        if '__version__' in line:
            line = line[:14] + '"%s"'%ANDUIN_VER
        code_data = code_data+line+'\n'
    open(INIT_PATH, 'w').write(cpright + split_line + code_data[:-1])
    setup_file = open(SETUP_PATH).read().split('\n')
    code_data = ''
    for line in setup_file:
        if 'VER = ' in line:
            line = line[:6] + '"%s"' % ANDUIN_VER
        code_data = code_data + line + '\n'
    open(SETUP_PATH, 'w').write(code_data[:-1])


def clean_package():
    print('清理打包缓存')
    try:
        query = 'rm dist/*'
        os.system(query)
    except Exception as e:
        print(str(e))


def packageandupload():
    print('打包...')
    query = 'python setup.py sdist'
    os.system(query)
    print('打包完成~')

    print('上传中...')
    query = 'python -m twine upload --repository pypi dist/*'
    os.system(query)


if __name__ == "__main__":
    clean_package()
    add_copy_right_and_version()
    packageandupload()
