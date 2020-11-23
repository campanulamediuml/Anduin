import codecs
import os
from setuptools import setup

def read(fname):
    return codecs.open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    # 以下为必需参数
    name='anduindata',  # 模块名
    version='1.2.5',  # 当前版本
    description='a mysql connector',  # 简短描述
    py_modules=["anduindata"],  # 单文件模块写法
    # ckages=find_packages(exclude=['contrib', 'docs', 'tests']),  # 多文件模块写法
    license='MIT',
    long_description=read("README.rst"),
    author='campanula',
    author_email='421248329@qq.com',
    platforms='any',
    keywords="mysql",
    # packages = find_packages('anduin'),
    # package_dir = {'anduin':'*'},
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
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
