from setuptools import setup, find_packages


with open('requirements.txt') as f:
    requirements = f.read()


setup(name='tectonic',
      version='0.0.0.0.0.1',
      install_requires=requirements,
      packages=find_packages())
