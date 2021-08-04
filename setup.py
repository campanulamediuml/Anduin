import codecs
import os
from setuptools import setup

VER = "7.0.17"

def read(fname):
    return codecs.open(os.path.join(os.path.dirname(__file__), fname)).read()

if __name__ == '__main__':
    setup(
        # 以下为必需参数
        name='anduin',  # 模块名
        version=VER,  # 当前版本
        description='a lite mysql & sqlite3 connect engine, mapping table into k-v structure',  # 简短描述
        # py_modules=["anduin"],  # 单文件模块写法
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
            'PyMySQL<=0.9.3,>=0.9',
            'aiomysql==0.0.21'
        ],
        include_package_data=True,
        zip_safe=True,
        packages=['anduin', 'anduin/dbserver'],
        python_requires='>=3.2',
    )
