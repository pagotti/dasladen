from setuptools import setup

with open("README.rst", "r") as f:
    long_description = f.read()

setup(
    name='dasladen',
    version='0.1.1',
    description='Simple, tiny and ridiculus ETL made with Python',
    long_description=long_description,
    long_description_content_type="text/markdown",
    license='MIT License',
    packages=['dasladen'],
    author='Vagner Pagotti',
    author_email='pagotti@gmail.com',
    url='https://github.com/pagotti/dasladen',
    keywords=['etl'],
    install_requires=[
        'schedule',
        'petl',
        'backports.tempfile',
        'ftputil'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)