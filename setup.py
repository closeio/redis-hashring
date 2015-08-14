from setuptools import setup

setup(
    name='redis-hashring',
    version='0.1',
    url='http://github.com/elasticsales/redis-hashring',
    license='MIT',
    description='Python library for distributed applications using a Redis hash ring',
    test_suite='tests',
    tests_require=['redis'],
    platforms='any',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
    ],
)
