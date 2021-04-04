import codecs
import os
from setuptools import setup

import anduin

INIT_PATH = './anduin/__init__.py'
COPY_RIGHT_FILE = './COPYING.txt'


def read(fname):
    return codecs.open(os.path.join(os.path.dirname(__file__), fname)).read()


def add_copy_right():
    # pass
    split_line = '\n# <=========>\n'
    cpright = '"""' + open(COPY_RIGHT_FILE).read() + '"""'
    code_content = open(INIT_PATH).read().split(split_line)
    if len(code_content) == 1:
        code = code_content[0]
    else:
        code = code_content[1]
    open(INIT_PATH, 'w').write(cpright + split_line + code)


if __name__ == '__main__':
    print(os.system('ls'))
    add_copy_right()
    setup(
        # 以下为必需参数
        name='anduin',  # 模块名
        version=anduin.__version__,  # 当前版本
        description='a lite mysql & sqlite3 connect engine, mapping table into k-v structure',  # 简短描述
        py_modules=["anduin"],  # 单文件模块写法
        # ckages=find_packages(exclude=['contrib', 'docs', 'tests']),  # 多文件模块写法
        license='MIT',
        long_description=read("README.rst"),
        author='campanula',
        author_email='421248329@qq.com',
        platforms='any',
        keywords="mysql , sqlite3 , sql engine",
        # packages = find_packages('anduin'),
        # package_dir = {'anduin':'*'},
        classifiers=[
            'License :: OSI Approved :: MIT License',
            'Intended Audience :: Developers',
            'Operating System :: OS Independent',
            'Programming Language :: Python :: 3',
        ],
        url='https://github.com/campanulamediuml/Anduin',
        install_requires=[
            'pymysql==0.10.1',
        ],
        include_package_data=True,
        zip_safe=True,
        packages=['anduin', 'anduin/dbserver'],
        python_requires='>=3.2',

    )
