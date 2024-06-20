from setuptools import setup, find_packages

with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='asyncpg_lite',
    version='0.3.1.3',
    packages=find_packages(),
    install_requires=[
        'SQLAlchemy>=2.0.31',
        'asyncpg>=0.29.0',
    ],
    author='Alexey Yakovenko',
    author_email='mr.mnogo@gmail.com',
    description='A simple asynchronous library based on SQLAlchemy, powered by asyncpg',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/Yakvenalex/asyncpg_lite',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
