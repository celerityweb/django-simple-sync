from setuptools import setup, find_packages
import simplesync

setup(
    name='django-simple-sync',
    version=simplesync.__version__,
    packages=find_packages(exclude=['test_project']),
    url='https://github.com/celerityweb/django-simple-sync',
    license='LGPLv3',
    author='Joshua "jag" Ginsberg',
    author_email='jginsberg@celerity.com',
    description='Simple content syncing between two databases'
)
