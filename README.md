# gpo_tools
The Government Publishing Office (GPO)'s [congressional hearings](https://www.gpo.gov/fdsys/browse/collection.action?collectionCode=CHRG) dataset offers an extensive collection of hearing texts and metadata. Unfortunately, the GPO's website lacks batch-downloading and querying tools, and hearing transcripts offered by the site lack embedded metadata denoting statement breakpoints or speaker-level information (e.g. political party or committee seniority). As a result, analyzing the GPO's data *en masse* is impractical.

The **gpo_tools** library addresses both of these issues. The library has two primary classes:
 - ``gpo_tools.scrape.Scraper`` downloads data from GPO's individual hearing pages, and saves information to a PostgreSQL database for convenient querying and commpact storage. 
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
    'state': None
  },
  {
    'cleaned': "I thank you, Mr. Chairman, for calling this \nhearing on a very important topic...",
    ...
  },
  ...
]
```
 
## Scraping
### Getting Started
For first-time users, initialize a ``Scraper`` instance as follows:
```
>> from gpo_tools.scrape import Scraper
>> scraper = Scraper(db = 'your_db', user = 'your_username', password = 'your_password', 
                     host = 'localhost', update_stewart_meta = True)
```

``db``, ``user``, and ``password`` should give valid credentials to an empty, preestablished PostgreSQL database, created through the method of your chosing and hosted at ``host`` (``'localhost'`` for most users). If PostgreSQL is not already available on your machine, installation instructions for OSX and Windows are available [here](https://www.postgresql.org/download/macosx/) and [here](https://www.postgresql.org/download/windows/). If you're not familiar with PostgreSQL, the [createdb](https://www.postgresql.org/docs/9.1/static/app-createdb.html) command line utility is a good option for generating an empty database.

If this is your first time setting up a database using ``gpo_tools``, you'll also need to read in Stewart's [Congressional committee membership data](http://web.mit.edu/17.251/www/data_page.html), which ``gpo_tools`` uses as a metadata source for members of Congress. Setting the flag ``update_stewart_meta = True`` during initialization will result in a prompt requesting paths to the CSV versions of Stewart's House and Senate membership data. For convenience, a version of each file is included with **gpo_tools** in the ``gpo_tools/data`` folder.

### Running the Scraper
Once the ``Scraper`` instance has been intialized, downloading data is straightforward:
```
>> scraper.scrape()
```
Unfortunately, the GPO's website is complex and slow, so scraping all available links may take some time. Make sure you have a stable internet connection, particularly if this is your first time running ``scrape()``. If possible, ``scrape()`` will attempt to restart itself in case of connection issues, but you may need to manually restart the function several times during this process.

To update an existing database, simply call ``Scraper.scrape()`` on a ``Scraper`` instance initialized with a preestablished database. This will take some time, since ``scrape()`` will need to re-crawl the GPO's website, but should be relatively quick.

