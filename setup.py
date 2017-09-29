from setuptools import setup

setup(
    name='redis-hashring',
    version='0.1.3',
    url='http://github.com/closeio/redis-hashring',
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
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
    ],
    packages=[
        'redis_hashring',
    ],
)
