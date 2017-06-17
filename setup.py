from distutils.core import setup

setup(
    name='gpo_tools',
    version='1.0',
    packages=['gpo_tools'],
    requires=['bs4', 'pprocess', 'gensim', 'nltk', 'psycopg2'],
    url='',
    license='MIT',
    author='Robert Shaffer',
    author_email='rbshaffer@utexas.edu',
    description='Parsing and scraping tools for GPO hearings data.'
)
