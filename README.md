# gpo_tools

Like many government data sources, the Government Publishing Office (GPO)'s [congressional hearings](https://www.gpo.gov/fdsys/browse/collection.action?collectionCode=CHRG) dataset is both promising and frustrating. In particular, the GPO's website lacks batch-downloading and querying tools, and hearing transcripts offered by the site lack embedded metadata denoting statement breakpoints or speaker-level information (e.g. political party or committee seniority).

The ``gpo_tools`` library provides tools designed to solve both of these problems.  Briefly, the library offers two main classes: the ``Scraper`` class, which contains functions navigate and scrape the GPO's individual hearing pages, and the ``Parser`` class , which segment hearing transcripts into individual statements and, when possible, assigns speaker-level meta to each statement using Stewart's [committee membership data](http://web.mit.edu/17.251/www/data_page.html). Combining these functions gives a dataset of congressional hearing statements suitable for large-scale content analysis:

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
