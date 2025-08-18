from setuptools import setup

setup(
    name="redis-hashring",
    version="0.6.0",
    author="Close Engineering",
    author_email="engineering@close.com",
    url="https://github.com/closeio/redis-hashring",
    license="MIT",
    description=(
        "Python library for distributed applications using a Redis hash ring"
    ),
    install_requires=["redis>=3"],
    extras_require={
        "xxhash": ["xxhash>=3.5.0"],
    },
    platforms="any",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
    packages=["redis_hashring"],
)
