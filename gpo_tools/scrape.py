import re
from urllib.request import urlopen

import psycopg2
from bs4 import BeautifulSoup
from psycopg2 import IntegrityError
from psycopg2.extras import Json


class Scraper:
    def __init__(self, db, user, password, host='localhost', update_stewart_meta=False):
        """
        GPO scraper class, which also handles database setup.
        """

        self.con = psycopg2.connect('dbname={} user={} password={} host={}'.format(db, user, password, host))
        self.cur = self.con.cursor(cursor_factory=psycopg2.extras.DictCursor)

        self._execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ")
        table_names = [t[0] for t in self.cur.fetchall()]

        if len(set(table_names)) == 0:
            self._execute("""
                             CREATE TABLE members(
                                id integer PRIMARY KEY,
                                metadata json,
                                committee_membership json);
                                
                             CREATE TABLE hearings(
                                id text PRIMARY KEY,
                                transcript text,
                                congress integer,
                                session integer,
                                chamber text,
                                date date,
                                committees text[],
                                subcommittees text[],
                                uri text,
                                url text,
                                sudoc text,
                                number text,
                                witness_meta json,
                                member_meta json,
                                parsed json);
                          """)

        elif set(table_names) != {'members', 'hearings'}:
            raise ValueError(""" Improperly configured postgresql database given! Please give either a blank database
                                 or one that has been previously configured by this package.
                             """)

        if update_stewart_meta:
            self._update_stewart_meta()

        self._execute('SELECT url FROM hearings;')
        self.searched = [e[0] for e in self.cur.fetchall()]

    def scrape(self):
        """
        Scrape data from the GPO website. Loops through the website until all links are exhausted.
        """

        initial_link = 'http://www.gpo.gov/fdsys/browse/collection.action?collectionCode=CHRG'

        new_links = [link for link in BeautifulSoup(urlopen(initial_link), 'lxml').find_all('a')
                     if link.get('onclick') is not None]

        print("Crawling and scraping the GPO website. As pages are scraped, page URLs will be printed in terminal. If "
              "you're running the scraper for the first time, the initial crawl will take some time.")

        while True:
            old_links = new_links
            new_links = []

            if old_links:
                for link in old_links:
                    print(link)
                    if link.get('onclick') is not None and 'Browse More Information' in link.get('onclick'):
                        meta_url = 'http://www.gpo.gov/fdsys/search/pagedetails.action?' + \
                            re.search('browsePath.*?(?=\')', link.get('onclick')).group(0)

                        if meta_url not in self.searched:
                            print(('Saving:' + meta_url))

                            self._save_data(meta_url)
                            self.searched.append(meta_url)

                    elif link.string is None:
                        new_links += self._extract_nav(link)
            else:
                break

    def _extract_nav(self, url_element):
        """ Helper function - grabs all unobserved links out of a given HTML element. """

        url = 'http://www.gpo.gov' + re.search('(?<=\').*?(?=\')', url_element.get('onclick')).group(0)

        if url not in self.searched:

            page = urlopen('http://www.gpo.gov' + re.search('(?<=\').*?(?=\')', url_element.get('onclick')).group(0))

            soup = BeautifulSoup(page.read(), 'lxml')
            elements = [l for l in soup.find_all('a') if l.get('onclick') is not None]
            self.searched.append(url)

            return elements
        else:
            return []

    def _save_data(self, url):
        """ Dumps scraped text and metadata to the appropriate location in the document file structure. """

        def extract_doc_meta(meta_html):
            """
            Function to extract hearing metadata from the metadata file. Program searches through the HTML metadata and
            locates various features, and combines them into a json object.
            """

            def locate_string(key, name=False):
                """ Helper function. Checks for a unique match on a given metadata element, and returns the value. """

                elements_from_meta = meta_html.find(key)
                if elements_from_meta is not None:
                    elements = list(set(elements_from_meta))

                    if len(elements) == 1 and name is False:
                        return elements[0].string
                    elif len(elements) == 1 and name is True:
                        return elements[0].find('name').string
                    else:
                        return ''
                else:
                    return ''

            # gathering a few unusual variables
            uri = [link.string for link in meta_html.find_all('identifier') if link.get('type') == 'uri'][0]
            congress = re.search('(?<=-)[0-9]+', uri).group(0)

            committee_meta = meta_html.find_all('congcommittee')
            committee_names = []
            subcommittee_names = []

            # first pass, using short committee names
            for committee in committee_meta:
                if committee.find('name', type='authority-short') is not None:
                    committee_names.append(committee.find('name', type='authority-short').string)
                    if committee.find('subcommittee') is not None:
                        try:
                            subcommittee = committee.find('subcommittee')
                            subcommittee_names.append(subcommittee.find('name', type='authority-short').string)
                        except:
                            pass

            # occasionally, short names are missing - fall back standard names if no short ones are found
            if len(committee_names) == 0:
                for committee in committee_meta:
                    if committee.find('name', type='authority-standard') is not None:
                        committee_names.append(committee.find('name', type='authority-standard').string)

            if meta_html.find('congserial') is not None:
                serials = meta_html.find_all('congserial')
                numbers = [serial.get('number') for serial in serials if serial.get('number') is not None]
            else:
                numbers = []

            # the main variable collection and output construction.
            meta_dictionary = {'Identifier': locate_string('recordidentifier'),
                               'Congress': congress,
                               'Session': locate_string('session'),
                               'Chamber': locate_string('chamber'),
                               'Date': locate_string('helddate'),
                               'Committees': committee_names,
                               'Subcommittees': subcommittee_names,
                               'Title': locate_string('title'),
                               'uri': uri,
                               'url': url,
                               'sudoc': locate_string('classification'),
                               'Number': numbers}

            return meta_dictionary

        def extract_member_meta(meta_html):
            """ Function to extract member metadata from the metadata file. This information is often absent. """
            import re

            member_dictionary = {}
            member_elements = [link for link in meta_html.find_all('congmember')]

            # loop over all of the member elements in a given page, and get relevant data
            for member in member_elements:
                party = member.get('party')
                state_short = member.get('state')
                chamber = member.get('chamber')
                bio_id = member.get('bioguideid')

                name_elements = member.find_all('name')
                name_parsed = [link.string for link in name_elements if link.get('type') == 'parsed'][0]
                state_long = re.search('(?<= of ).*', name_parsed).group(0)

                member_dictionary[name_parsed] = {'Name': name_parsed,
                                                  'State_Short': state_short,
                                                  'State_Long': state_long,
                                                  'Party': party,
                                                  'Chamber': chamber,
                                                  'GPO_ID': bio_id}

            return member_dictionary

        def extract_witness_meta(meta_html):
            """ Function to extract witness metadata from the metadata file. This information is often absent. """

            witness_list = [w.string for w in meta_html.find_all('witness') if w.string is not None]

            return witness_list

        page = urlopen(url)
        soup = BeautifulSoup(page.read(), 'lxml')

        meta_link = [l.get('href') for l in soup.find_all('a') if l.string == 'MODS'][0]
        transcript_link = [l.get('href') for l in soup.find_all('a') if l.string == 'Text'][0]

        transcript = urlopen(transcript_link).read()

        transcript = re.sub('\x00', '', transcript)

        meta_page = urlopen(meta_link)
        meta_soup = BeautifulSoup(meta_page.read(), 'lxml')

        # Metadata is divided into three pieces: hearing info, member info, and witness info.
        # See functions for details on each of these metadata elements.

        hearing_meta = extract_doc_meta(meta_soup)
        witness_meta = extract_witness_meta(meta_soup)
        member_meta = extract_member_meta(meta_soup)

        try:
            self._execute('INSERT INTO hearings VALUES (' + ','.join(['%s'] * 14) + ')',
                          (hearing_meta['Identifier'],
                           transcript,
                           hearing_meta['Congress'],
                           hearing_meta['Session'],
                           hearing_meta['Chamber'],
                           hearing_meta['Date'],
                           hearing_meta['Committees'],
                           hearing_meta['Subcommittees'],
                           hearing_meta['uri'],
                           hearing_meta['url'],
                           hearing_meta['sudoc'],
                           hearing_meta['Number'],
                           Json(witness_meta),
                           Json(member_meta)))
        except IntegrityError:
            print('Duplicate key. Link not included.')
            self.con.rollback()

    def _update_stewart_meta(self):
        """

        Generate the member table. The member table lists party seniority, majority status, leadership,
        committee membership, congress, and state. All member data are drawn from Stewart's committee assignments data
        (assumed to be saved as CSV files), which are available at the link below.

        http://web.mit.edu/17.251/www/data_page.html

        """
        import csv

        def update(inputs, table, chamber):
            """

            Helper function, which updates a given member table with metadata from Stewart's metadata. Given data from
            a csv object, the function interprets that file and adds the data to a json output. See Stewart's data and
            codebook for descriptions of the variables.

            """

            def update_meta(meta_entry, datum):
                meta_entry.append(datum)
                meta_entry = [e for e in list(set(meta_entry)) if e != '']

                return meta_entry

            for row in inputs:
                name = str(row[3].lower().decode('ascii', errors='ignore'))
                name = name.translate(str.maketrans(dict.fromkeys('!"#$%&\'()*+-./:;<=>?[\\]_`{|}~')))

                congress = row[0].lower()
                committee_code = row[1]
                member_id = row[2]
                majority = row[4].lower()
                party_seniority = row[5].lower()
                leadership = row[9]
                committee_name = row[15]
                state = row[18]

                if row[6] == '100':
                    party = 'D'
                elif row[6] == '200':
                    party = 'R'
                else:
                    party = 'I'

                entry = {'Party Seniority': party_seniority, 'Majority': majority, 'Leadership': leadership,
                         'Chamber': chamber, 'Party': party, 'State': state, 'Committee Name': committee_name}

                if committee_code != '' and member_id != '':
                    if member_id in table:
                        member_meta = table[member_id]['Metadata']
                        member_membership = table[member_id]['Membership']

                        member_meta['Name'] = update_meta(member_meta['Name'], name)
                        member_meta['State'] = update_meta(member_meta['State'], state)
                        member_meta['Chamber'] = update_meta(member_meta['Chamber'], chamber)
                        member_meta['Party'] = update_meta(member_meta['Party'], party)
                        member_meta['Committee'] = update_meta(member_meta['Committee'], committee_name)

                        if congress in table[member_id]['Membership']:
                            member_membership[congress][committee_code] = entry
                        else:
                            member_membership[congress] = {committee_code: entry}

                    else:
                        table[member_id] = {'Metadata': {'Name': [name],
                                                         'State': [state],
                                                         'Chamber': [chamber],
                                                         'Party': [party],
                                                         'Committee': [committee_name]},
                                            'Membership': {congress: {committee_code: entry}}}

        self._execute('DELETE FROM members;')
        member_table = {}

        house_path = eval(input('Path to Stewart\'s House committee membership data (as csv): '))
        senate_path = eval(input('Path to Stewart\'s Senate committee membership data (as csv): '))

        # Loop through the house and senate assignment files, and save the output.
        with open(house_path, 'rb') as f:
            house_inputs = list(csv.reader(f))[2:]
        with open(senate_path, 'rb') as f:
            senate_inputs = list(csv.reader(f))[2:]

        update(house_inputs, member_table, 'HOUSE')
        update(senate_inputs, member_table, 'SENATE')

        for k, v in list(member_table.items()):
            self._execute('INSERT INTO members VALUES (%s, %s, %s)', (k, Json(v['Metadata']), Json(v['Membership'])),
                          errors='strict')

    def _execute(self, cmd, data=None, errors='strict'):
        """ Wrapper function for pyscopg2 commands. """
        if errors not in ['strict', 'ignore']:
            raise ValueError("""errors argument must be \'strict\' (raise exception on bad command)
                                or \'ignore\' (return None on bad command). '""")

        self.cur = self.con.cursor()

        if errors == 'ignore':
            try:
                self.cur.execute(cmd, data)
            except:
                self.con.rollback()

        elif errors == 'strict':
            self.cur.execute(cmd, data)

        self.con.commit()
