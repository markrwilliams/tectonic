from setuptools import setup, find_packages

__author__ = 'Mark Williams'
__version__ = '0.0'
__contact__ = 'markrwilliams@gmail.com'
__url__ = 'https://github.com/markrwilliams/tectonic'
__license__ = 'BSD'



with open('requirements.txt') as f:
    requirements = f.read()


setup(name='tectonic',
      author=__author__,
      author_email=__contact__,
      version=__version__,
      license=__license__,
      url=__url__,
      install_requires=requirements,
      packages=find_packages())
