import codecs
import os

from setuptools import setup, find_packages

def read(fname):
    """
    定义一个read方法，用来读取目录下的长描述
    我们一般是将README文件中的内容读取出来作为长描述，这个会在PyPI中你这个包的页面上展现出来，
    你也可以不用这个方法，自己手动写内容即可，
    PyPI上支持.rst格式的文件。暂不支持.md格式的文件，<BR>.rst文件PyPI会自动把它转为HTML形式显示在你包的信息页面上。
    """
    return codecs.open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    # 以下为必需参数
    name='anduindata',  # 模块名
    version='1.0.8',  # 当前版本
    description='a mysql connector',  # 简短描述
    py_modules=["anduindata"], # 单文件模块写法
    # ckages=find_packages(exclude=['contrib', 'docs', 'tests']),  # 多文件模块写法
    license='MIT',
    long_description = read("README.rst"),
    author='campanula',
    author_email='421248329@qq.com',
    platforms = 'any',
    keywords = "mysql",
    # packages = find_packages('anduin'),
    # package_dir = {'anduin':'*'},
    classifiers = [
            'License :: OSI Approved :: MIT License',
            'Programming Language :: Python',
            'Intended Audience :: Developers',
            'Operating System :: OS Independent',
        ],
    url = 'https://github.com/campanulamediuml/Anduin',
    install_requires=[
          'pymysql==0.10.1',
      ],
    include_package_data=True,
    zip_safe=True,
    packages = ['anduin','anduin/dbserver'],
    python_requires='>=3.2',

)