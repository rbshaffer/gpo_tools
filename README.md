# gpo_tools
The Government Publishing Office (GPO)'s 
[Congressional hearings collection](https://www.govinfo.gov/app/collection/CHRG) dataset (and accopanying 
[API](https://api.govinfo.gov/docs/)) offers an extensive array of hearing texts and metadata. Unfortunately, hearing 
transcripts offered by the site are offered as plain, raw text, with no embedded metadata denoting speaking turns or 
speaker-level information (e.g. political party or committee seniority). As a result, it is difficult to extract useful 
information from these texts at scale.
 
The **gpo_tools** library addresses this issue by offering a series of parsing and metadata management tools. 
The library has two primary classes:
 - ``gpo_tools.scrape.Scraper`` streamlines API queries and saves information to a PostgreSQL database for convenient querying and compact storage. 
 - ``gpo_tools.parse.Parser`` segments hearing transcripts into individual statements and, when possible, assigns speaker-level meta to each statement.
 
Combining these functions gives a dataset of congressional hearing statements suitable for large-scale content analysis. An example output line reads as follows:

```
[
  {
    'cleaned': "The subcommittee will now come to order.\n    I would like to first welcome Mr. Hale 
                  and Mr. Hockey of the EPA. We are delighted to have you here today.\n    And today's 
                  hearing focuses on ...",
    'committees': ('128',),
    'congress': 108,
    'hearing_chamber': 'HOUSE',
    'jacket': 'CHRG-108hhrg93977',
    'leadership': u'0',
    'majority': u'1',
    'member_id': 15604,
    'name_full': (u'gillmor, paul e',),
    'name_raw': 'Mr. Gillmor',
    'party': (u'R',),
    'party_seniority': u'6',
    'person_chamber': u'HOUSE',
    'state': None,
    'date': '05-20-2004'
  },
  {
    'cleaned': "I thank you, Mr. Chairman, for calling this \nhearing on a very important topic...",
    ...
  },
  ...
]
```
 
## Scraping
### Prerequisites
Before starting, you will need: 
 - A Python 3.6 installation
 - An empty PostgreSQL datbase
 - An API key from the [GPO API](https://api.data.gov/signup/)
 - [psycopg2](https://pypi.org/project/psycopg2/)

### Getting Started
For first-time users, initialize a ``Scraper`` instance as follows:
```
>> from gpo_tools.scrape import Scraper
>> scraper = Scraper(min_congress = '111', max_congress = '112', api_key = api_key, 
                     db = 'your_db', user = 'your_username', password = 'your_password', 
                     host = 'localhost', update_stewart_meta = True)
```
``min_congress`` and ``max_congress`` should give the first and last Congress from which you want to download data. 
Hearing texts are available through the GPO's website from as early as the 85th Congress, but coverage only becomes 
reasonably comprehensive starting around 2000. ``api_key`` should contain your [GPO API](https://api.data.gov/signup/) 
key.  

``db``, ``user``, and ``password`` should give valid credentials to an empty, preestablished PostgreSQL database, created through the method of your chosing and hosted at ``host`` (``'localhost'`` for most users). If PostgreSQL is not already available on your machine, installation instructions for OSX and Windows are available [here](https://www.postgresql.org/download/macosx/) and [here](https://www.postgresql.org/download/windows/). If you're not familiar with PostgreSQL, the [createdb](https://www.postgresql.org/docs/9.1/static/app-createdb.html) command line utility is a good option for generating an empty database.

If this is your first time setting up a database using ``gpo_tools``, you'll also need to read in Stewart's [Congressional committee membership data](http://web.mit.edu/17.251/www/data_page.html), which ``gpo_tools`` uses as a metadata source for members of Congress. Setting the flag ``update_stewart_meta = True`` during initialization will result in a prompt requesting paths to the CSV versions of Stewart's House and Senate membership data. For convenience, a version of each file is included with *gpo_tools* in the ``gpo_tools/data`` folder.

### Running the Scraper
Once the ``Scraper`` instance has been intialized, downloading data is straightforward:
```
>> scraper.scrape()
```
Unfortunately, the GPO's website slow, so scraping all available links may take some time. Make sure you have a stable internet connection, particularly if this is your first time running ``scrape()``. If possible, ``scrape()`` will attempt to restart itself in case of connection issues, but you may need to manually restart the function several times during this process.

To update an existing database, simply call ``Scraper.scrape()`` on a ``Scraper`` instance initialized with a preestablished database. 

## Parsing
### Initialization
Hearing transcripts are delivered by the GPO as plain-text documents, with no embedded metadata or structure. The ``gpo_tools.parse.Parser`` class handles both of these issues through a series of rules-based matching and segmentation functions. In the financial hearings dataset used in [Shaffer (2017)](https://rbshaffer.github.io/_includes/cognitive-load-issue.pdf), these matching functions successfully assigned metadata to approximately 87% of hearing statements.

To initialize the parser, simply provide the same login credentials used for the ``Scraper`` class:

```
>> from gpo_tools.parse import Parser
>> parser = Parser(db = 'your_db', user = 'your_username', 
                   password = 'your_password', host = 'localhost')
```

By default, the ``Parser`` class will attempt to parse all hearings in the dataset. To specify a subset instead, use the following optional argument:

```
>> ids_to_parse = ['CHRG-115hhrg24325', 'CHRG-115hhrg24324']
>> parser = Parser(db = 'your_db', user = 'your_username', 
                   password = 'your_password', host = 'localhost',
                   id_values = ids_to_parse)
```

ID values specified in this fashion should correspond to the jacket numbers used to identify hearings in the GPO's website.

### Processing
To run the parser, simply call the wrapper function:
```
>> parser.parse_gpo_hearings()
```
Outputs will be saved to the ``parser.results`` slot, which can be saved to disk using the method of your choice. 

**Important**: not all metadata fields will be filled for all individuals or  hearings. Because the *gpo_tools* package relies on the Stewart committee assignment information to assign individual-level metadata, metadata information is often missing for ad hoc or select committees. In addition, since the GPO (and *gpo_tools*) relies on Congressional committees for witness metadata, information on witnesses is also frequently unavailable. 



## Citation
If you use ``gpo_tools`` in your own work, please cite the following:

```
@article{shaffer2017cognitive,
  title={Cognitive load and issue engagement in congressional discourse},
  author={Shaffer, Robert},
  journal={Cognitive Systems Research},
  volume={44},
  pages={89--99},
  year={2017},
  publisher={Elsevier}
}
```
