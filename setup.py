from distutils.core import setup

setup(
    name='gpo_tools',
    version='1.0',
    packages=['gpo_tools'],
    requires=['bs4', 'pprocess', 'gensim', 'nltk', 'psycopg2'],
    url='https://github.com/rbshaffer/gpo_tools',
    license='MIT',
    author='Robert Shaffer',
    author_email='rbshaffer@utexas.edu',
    description='Parsing and scraping tools for GPO hearings data.',
    package_data={'': ['data/committee_data.csv', 'data/house_assignments_103-115-1.csv',
                       'data/senate_assignments_103-115-1.csv', 'data/stewart_notes.txt']}
)
