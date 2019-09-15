from setuptools import setup, find_packages

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name='dasladen',
    version='0.1.9',
    description='Simple, tiny and ridiculus ETL made with Python',
    long_description=long_description,
    long_description_content_type="text/markdown",
    license='MIT License',
    packages=find_packages(),
    author='Vagner Pagotti',
    author_email='pagotti@gmail.com',
    url='https://github.com/pagotti/dasladen',
    keywords=['etl'],
    install_requires=[
        'schedule',
        'petl',
        'backports.tempfile',
        'ftputil',
        'xlrd',
        'xlwt-future',
        'requests'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)