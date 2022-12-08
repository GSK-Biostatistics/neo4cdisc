import setuptools
import os

with open("README.md", "r") as fh:
    long_description = fh.read()

def read_text(file_name: str):
    return open(os.path.join(file_name)).read()

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

required = []
dependency_links = []

# Do not add to required lines pointing to Git repositories
EGG_MARK = '#egg='
for line in requirements:
    if line.startswith('-e git:') or line.startswith('-e git+') or \
            line.startswith('git:') or line.startswith('git+'):
        if EGG_MARK in line:
            package_name = line[line.find(EGG_MARK) + len(EGG_MARK):]
            required.append(package_name)
            dependency_links.append(line)
        else:
            print('Dependency to a git repository should have the format:')
            print('git+ssh://git@github.com/xxxxx/xxxxxx#egg=package_name')
    else:
        required.append(line)

setuptools.setup(
    name="neo4cdisc",                           # This is the name of the package
    version="1.0.1.1",                      # Release.Major Feature.Minor Feature.Bug Fix
    author="Alexey Kuznetsov",              # Full name of the author
    description="Clinical Linked Data: Example of loading the FDA CDISC pilot study into Neo4J using the tab2neo python package",
    #long_description=long_description,      # Long description read from the the readme file
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(include=[
        "cdisc_model_managers",
        "cdisc_data_providers",
    ]),    # List of all python modules to be installed
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],                                      # Information to filter the project on PyPi website
    license=read_text("LICENSE"),
    python_requires='>=3.8',                # Minimum version requirement of the package
    # package_dir={'':''},                  # Directory of the source code of the package
    install_requires=required,
    dependency_links=dependency_links
)
