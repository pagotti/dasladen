from setuptools import setup

setup(
    name='dasladen',
    version='0.1.0',
    description='Simple, tiny and ridiculus ETL made with Python',
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