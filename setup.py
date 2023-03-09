import codecs
import os
from setuptools import setup, find_packages

VER = "8.2.5"

def read(fname):
    return codecs.open(os.path.join(os.path.dirname(__file__), fname),encoding='utf-8').read()

if __name__ == '__main__':
    setup(
        # 以下为必需参数
        name='anduin',  # 模块名
        version=VER,  # 当前版本
        description='a lite mysql & sqlite3 & redis connect engine, mapping table into k-v structure , support async work',  # 简短描述
        license='MIT',
        long_description=read("README.md"),
        author='campanula',
        author_email='campanulamediuml@gmail.com',
        platforms='any',
        keywords="mysql , sqlite3 , sql engine , orm , redis",
        classifiers=[
            'License :: OSI Approved :: MIT License',
            'Intended Audience :: Developers',
            'Operating System :: OS Independent',
            'Programming Language :: Python :: 3',
        ],
        url='https://github.com/campanulamediuml/Anduin',
        install_requires=[
            'pymysql>=0.9',
            'aredis>=1.1.0',
            'aiomysql>=0.0.21'
        ],
        include_package_data=True,
        zip_safe=True,
        packages=find_packages(),
        python_requires='>=3.6',
    )
