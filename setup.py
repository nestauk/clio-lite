from setuptools import setup
from setuptools import find_namespace_packages

with open('requirements.txt') as f:
    required = f.read().splitlines()

common_kwargs = dict(
    version='0.1',
    license='MIT',
    install_requires=required,
    long_description=open('README.md').read(),
    url='https://github.com/nestauk/clio-lite',
    author='Joel Klinger',
    author_email='joel.klinger@nesta.org.uk',
    maintainer='Joel Klinger',
    maintainer_email='joel.klinger@nesta.org.uk',
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
        'Environment :: Web Environment'
        'Topic :: System :: Monitoring',
    ],
    python_requires='>3.6',
    include_package_data=True,
)

setup(name='clio_lite',
      packages=['clio_lite'],
      **common_kwargs)

