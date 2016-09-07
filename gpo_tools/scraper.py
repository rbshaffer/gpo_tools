__author__ = 'rbshaffer'

import os
import re
import json
from urllib2 import urlopen
from bs4 import BeautifulSoup

class GPOManager:
    def __init__(self, pwd):
        """
        GPO data manager class, developed for the GPO's congressional hearings collection. Scrapes hearing text and
        metadata (where available), and saves to a local file structure.
        """

        # Working directory where files will be saved.
        self.pwd = pwd

        # Container for processed links. Links that are placed here are not followed or searched again.
        self.searched = []

    def scrape(self):
        """
        Scrape data from the GPO website. Loops through the website until all links are exhausted.
        """

        initial_link = 'http://www.gpo.gov/fdsys/browse/collection.action?collectionCode=CHRG'

        new_links = [link for link in BeautifulSoup(urlopen(initial_link)).find_all('a')
                     if link.get('onclick') is not None]

        while True:
            old_links = new_links
            new_links = []

            if old_links is not []:
                for link in old_links:
                    print link.get('onclick')
                    if link.get('onclick') is not None and 'Browse More Information' in link.get('onclick'):

                        meta_url = 'http://www.gpo.gov/fdsys/search/pagedetails.action?' + \
                            re.search('browsePath.*?(?=\')', link.get('onclick')).group(0)

                        if meta_url not in self.searched:
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
            soup = BeautifulSoup(page.read())
            elements = [l for l in soup.find_all('a') if l.get('onclick') is not None]
            self.searched.append(url)

            return elements
        else:
            return []

    def _save_data(self, url):
        """ Dumps scraped text and metadata to the appropriate location in the document file structure. """

        page = urlopen(url)
        soup = BeautifulSoup(page.read())

        meta_link = [l.get('href') for l in soup.find_all('a') if l.string == 'MODS'][0]
        transcript_link = [l.get('href') for l in soup.find_all('a') if l.string == 'Text'][0]
        transcript = urlopen(transcript_link).read()

        meta_page = urlopen(meta_link)
        meta_soup = BeautifulSoup(meta_page.read())

        # Metadata is divided into three pieces: hearing info, member info, and witness info.
        # See functions for details on each of these metadata elements.
        hearing_meta = {'Hearing Info': self._extract_doc_meta(meta_soup),
                        'Member Info': self._extract_member_meta(meta_soup),
                        'Witness Info': self._extract_witness_meta(meta_soup)}

        congress = hearing_meta['Hearing Info']['Congress']
        committee = hearing_meta['Hearing Info']['Committee']
        identifier = hearing_meta['Hearing Info']['Identifier']

        # Output file structure is organized by congress, committee, and finally hearing identifier
        out_path = self.pwd + congress + os.sep + committee[0] + os.sep + identifier

        # For each hearing, a transcript and a metadata file is saved.
        if os.path.exists(out_path) is False:
            os.makedirs(out_path)
            with open(out_path + os.sep + identifier + '.html', 'wb') as f:
                f.write(transcript)

            with open(out_path + os.sep + identifier + '.json', 'wb') as f:
                f.write(json.dumps(hearing_meta))

    @staticmethod
    def _extract_doc_meta(meta_html):
        """
        Function to extract hearing metadata from the metadata file. Program searches through the HTML metadata and
        locates various features, and combines them into a json object.
        """

        def locate_string(key, name=False):
            """ Helper function, which checks for a unique match on a given metadata element, and returns the value. """

            elements_from_meta = meta_html.find(key)
            if elements_from_meta is not None:
                elements = list(set(elements_from_meta))

                if len(elements) == 1 and name is False:
                    return elements[0].string
                elif len(elements) == 1 and name is True:
                    return elements[0].find('name').string
                else:
                    return None
            else:
                return None

        # gathering a few unusual variables
        uri = [l.string for l in meta_html.find_all('identifier') if l.get('type') == 'uri'][0]
        congress = re.search('(?<=-)[0-9]+', uri).group(0)

        committee_meta = meta_html.find_all('congcommittee')
        committee_names = []
        subcommittee_names = []

        for committee in committee_meta:
            committee_names.append(re.sub('(Joint |Special )?Committee on (the )?', '', committee.find('name').string))
            if committee.find('subcommittee') is not None:
                subcommittee = committee.find('subcommittee')
                subcommittee_names.append(re.sub('Subcommittee on ', '', subcommittee.find('name').string))

        if meta_html.find('congserial') is not None:
            serials = meta_html.find_all('congserial')
            numbers = [serial.get('number') for serial in serials if serial.get('number') is not None]
        else:
            numbers = []

        # the main variable collection and output construction.
        meta_dictionary = {'Identifier': locate_string('recordidentifier'),
                           'Congress': congress,
                           'Chamber': locate_string('chamber'),
                           'Session': locate_string('session'),
                           'Date': locate_string('helddate'),
                           'Committee': committee_names,
                           'Subcommittee': subcommittee_names,
                           'Title': locate_string('title'),
                           'uri': uri,
                           'sudoc': locate_string('classification'),
                           'Number': numbers}

        return meta_dictionary

    @staticmethod
    def _extract_member_meta(meta_html):
        """ Function to extract member metadata from the metadata file. Note that this information is often absent. """
        import re

        member_dictionary = {}
        member_elements = [l for l in meta_html.find_all('congmember')]

        # loop over all of the member elements in a given page, and get relevant data
        for member in member_elements:
            party = member.get('party')
            state_short = member.get('state')
            chamber = member.get('chamber')

            name_elements = member.find_all('name')
            name_parsed = [l.string for l in name_elements if l.get('type') == 'parsed'][0]
            state_long = re.search('(?<= of ).*', name_parsed).group(0)

            member_dictionary[name_parsed] = {'Name': name_parsed,
                                              'State_Short': state_short,
                                              'State_Long': state_long,
                                              'Party': party,
                                              'Chamber': chamber}

        return member_dictionary

    @staticmethod
    def _extract_witness_meta(meta_html):
        """ Function to extract witness metadata from the metadata file. Note that this information is often absent. """

        witness_list = [w.string for w in meta_html.find_all('witness') if w.string is not None]

        return witness_list
