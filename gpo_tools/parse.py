import codecs
import csv
import io
import os
import re
import string
import sys
from datetime import datetime
from itertools import repeat

import pkg_resources
import psycopg2
from gensim import corpora
from nltk.corpus import stopwords
from psycopg2.extras import DictCursor


class Parser:
    def __init__(self, db, user, password, host='localhost', id_values=None):
        """ 
        GPO parser class. Assumes that the given database has been formatted by gpotools.Scraper.
        """

        # authenticate the connection and check to make sure the database is properly configured

        def merge_two_dicts(x, y, id_to_add=None):
            """Given two dicts, merge them into a new dict as a shallow copy."""
            z = x.copy()
            z.update(y)
            if id_to_add:
                z['id'] = id_to_add
            return z

        self.credentials = {'dbname': db, 'user': user, 'password': password, 'host': host}
        con = psycopg2.connect('dbname={} user={} password={} host={}'.format(db, user, password, host))
        cur = con.cursor()

        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ")
        table_names = [t[0] for t in cur.fetchall()]
        if set(table_names) != {'members', 'hearings'}:
            raise ValueError(""" Improperly configured postgresql database given! Please give a database configured by
                                 the gpo_tools.Scraper class. """)

        # select and format member information (from Stewart's metadata)
        # note that this format is a little sketchy - could be better to re-organize later
        cur.execute('select * from members')
        self.member_table = {}
        for id_val, meta, membership in cur.fetchall():
            if meta['Name'][0] not in self.member_table:
                self.member_table[tuple(meta['Name'])] = merge_two_dicts(meta, membership, id_val)
            else:
                self.member_table[tuple(meta['Name'])].update(merge_two_dicts(meta, membership, id_val))

        # load the committee data file from the package resources
        with open(pkg_resources.resource_filename('gpo_tools', 'data/committee_data.csv')) as f:
            self.committee_data = {row[0]: {'Code': row[1], 'Chamber': row[2]} for row in csv.reader(f)}

        self.results = []

        # select ID values from the database.
        if not id_values:
            confirmation = eval(input('No ID values were given, so all IDs in the database will be processed. '
                                      'Proceed y/(n)?'))
            if confirmation == 'y':
                cur.execute('select id from hearings')
                self.id_values = [r[0] for r in cur.fetchall()]
            else:
                self.id_values = []

        else:
            # do some basic checking of user-inputted id values - not 100% comprehensive but should fairly broad
            if type(id_values) not in (list, tuple) or any([type(id_value) != str for id_value in id_values]) or \
                    any([re.search('CHRG-[0-9]+[a-z]+[0-9]+', id_value) is None for id_value in id_values]):

                raise ValueError(""" id_values should be a list of strings, following the naming convention used by the
                                     GPO (e.g. \'CHRG-113jhrg79942\'). """)
            else:
                self.id_values = id_values

    def parse_gpo_hearings(self, n_cores=4):
        """ Primary parser function. Wraps and parallelizes methods described elsewhere in this file. """

        # import pprocess

        def parse(data):
            """

            Wrapper for parser function, intended for parallel processing. Takes a data object with an initialized
            connection and a set of IDs to query and parse.

            """

            cur = data['con'].cursor(cursor_factory=psycopg2.extras.DictCursor)

            output = []

            for j in data['id_inds']:
                id_to_parse = self.id_values[j]

                cur.execute('select * from hearings where id = %s', (id_to_parse,))
                entry = cur.fetchone()
                if entry is not None:
                    parsed = ParseHearing(entry, committee_data=self.committee_data,
                                          member_table=self.member_table).parsed
                    output.append(parsed)
                else:
                    print((' Warning: id {} not found!'.format(id_to_parse)))

            # Returned value records whether the file was actually parsed.
            return output

        n_ids = len(self.id_values)

        # if n_ids is reasonably large (say >100), parallelize; if not, just do in serial
        # disabled for now because pprocess seems to be broken
        # if n_ids > 100:
        #     to_analyze = [{'con': psycopg2.connect(**self.credentials),
        #                    'id_inds': list(range(int(i * n_ids / n_cores), int((i + 1) * n_ids / n_cores)))}
        #                   for i in range(n_cores)]
        #
        #     self.results = [r for r in pprocess.pmap(parse, to_analyze, limit=n_cores)]
        #     self.results = list(chain(*self.results))
        # else:
        #     con = psycopg2.connect(**self.credentials)
        #     self.results = parse({'con': con, 'id_inds': list(range(len(self.id_values)))})

        con = psycopg2.connect(**self.credentials)
        self.results = parse({'con': con, 'id_inds': list(range(len(self.id_values)))})

    def create_dataset(self, out_dir, out_name='corpus', min_token_length=3, min_doc_length=5, min_dic_count=5,
                       additional_meta=None, additional_meta_labels=None):
        """

        Create the finished corpus file. Corpus is created using a subset of available documents, identified using the
        jacket_out_list argument. Preprocessing for the corpus file is conducted in this function as well.

        Output is saved to the working directory (named with today's date) using several output formats. The primary
        corpus output is in Blei's LDA-C format, although a flat (csv-type) output is also created. A dictionary of word
        indices for the LDA-C output is also generated. Also creates an index file, which contains one line for each
        document in the corpus. Index file contains both individual-level and hearing-level metadata entries, which
        apply to each document.

        """

        # some quick type-checking up-front
        if additional_meta and (type(additional_meta) not in (list, tuple) or
                                        len(additional_meta) != len(self.results)):
            raise ValueError('Additional metadata should be a list or tuple, with one entry for each parsed hearing!')

        if additional_meta_labels and additional_meta and \
                (type(additional_meta_labels) not in (list, tuple) or
                         len(additional_meta_labels) != len(additional_meta[0])):
            raise ValueError('Additional metadata labels should have the same number of entries as the metadata!')

        csv.field_size_limit(sys.maxsize)
        stop_list = stopwords.words('english')
        trans_table = str.maketrans(dict.fromkeys(string.punctuation))

        documents = []
        corpus = []
        index = []

        index_keys = ['name_raw', 'name_full', 'member_id', 'party', 'state', 'majority', 'party_seniority',
                      'jacket', 'committees', 'person_chamber', 'hearing_chamber', 'leadership', 'congress',
                      'date']

        # Preprocess, part 1. Documents are lower-cased, punctuation is stripped, words shorter than the cutoff are
        # dropped, stopwords are dropped. After preprocessing, documents shorter than the cutoff are dropped
        if not self.results:
            print('No parsed results found, so no processing will be done.')
        else:
            for i, content in enumerate(self.results):
                if len(content) > 1:
                    print((i, content[0]['jacket']))
                    for row in content:
                        doc = [w for w in row['cleaned'].lower().translate(trans_table).split()
                               if len(w) > min_token_length and w not in stop_list]

                        if len(doc) > min_doc_length:
                            documents.append(doc)
                            index_row = [','.join(row[key]) if type(row[key]) in (tuple, list) else row[key]
                                         for key in index_keys]
                            if additional_meta:
                                index_row += additional_meta[i]

                            index.append(index_row)

            dic = corpora.Dictionary(documents)
            dic.filter_extremes(no_below=min_dic_count, no_above=1)
            dic.compactify()
            print(dic)

            keep = []
            bow_list = []

            for i, doc in enumerate(documents):
                bow = dic.doc2bow(doc)
                if len(bow) > min_doc_length:
                    corpus.append([' '.join([' '.join(list(repeat(dic[k], times=v))) for k, v in bow])])
                    keep.append([str(val) for val in index[i]] + [str(len(bow))])
                    bow_list.append(bow)

            today = datetime.today().strftime('%Y-%m-%d')
            with open(out_dir + os.sep + out_name + '_' + today + '.csv', 'w') as f:
                UnicodeWriter(f).writerows(corpus)

            with open(out_dir + os.sep + out_name + '_index_' + today + '.csv', 'w') as f:
                header = index_keys
                if additional_meta_labels:
                    header += additional_meta_labels

                UnicodeWriter(f).writerow(header + ['word_count'])
                UnicodeWriter(f).writerows(keep)

            corpora.Dictionary.save(dic, out_dir + os.sep + out_name + '_' + today + '.lda-c.dic')
            corpora.BleiCorpus.serialize(fname=out_dir + os.sep + out_name + '_' + today + '.lda-c',
                                         corpus=bow_list, id2word=dic)


class ParseHearing:
    def __init__(self, entry, committee_data, member_table):
        """
        Class for parsing hearings. This class is not intended to be called directly; rather, it is only meant to be
        called in the parallelized parse() method of BuildDatabase. The parser breaks hearings into statements,
        links the speaker of each statement to metadata (wherever possible), and outputs a flat file, with one
        statement per line.
        
        Former variables: hearing_text, hearing_data, member_data=None, committee_data=None

        """

        def clean_hearing(text):
            """ Preprocessing function which strips out transcript components that aren't part of the conversation. """

            import re
            if re.search('\[Questions for the record with answers supplied follow:\]', text) is not None:
                text = text[0:re.search('\[Questions for the record ' +
                                        'with answers supplied follow:\]',
                                        text).start()]
            return text

        self.entry = entry
        self.entry['transcript'] = clean_hearing(self.entry['transcript'])

        self.member_table = member_table
        self.committee_data = committee_data

        # List of speaker prefixes. These are important for identifying the beginning and end of each statement.
        self.prefixes = ['Mr.', 'Mrs.', 'Ms.', 'Mr', 'Mrs', 'Ms', 'Chairman', 'Chairwoman', 'Dr.', 'Dr', 'Senator',
                         'Secretary', 'Director', 'Representative', 'Vice Chairman', 'Vice Chair', 'Admiral', 'General',
                         'Gen.', 'Judge', 'Commissioner', 'Lieutenant', 'Lt.', 'Trustee', 'Sergeant', 'Major',
                         'Colonel', 'Captain', 'Capt.', 'Commander', 'Specialist', 'Voice', 'The Chairman',
                         'The Chairwoman', 'Governor', 'Chair', 'The Clerk', 'Clerk', 'Mayor', 'Reverend', 'Justice',
                         'Ambassador', 'Chief']

        self.prefixes += [prefix.upper() for prefix in self.prefixes]

        # Constant for performance. Limits how far forward (number of characters) the script will search in order to
        # find certain pieces of information, such as the name of the chair of the committee.
        self.max_search_length = 75000
        self.delete_last = False

        meta_chamber = self.entry['chamber']

        # If there's at least one statement identified in the text, start parsing
        if self._name_search(self.entry['transcript']) is not None:
            print(self._name_search(self.entry['transcript']))
            self.session_cutpoints = self._find_sessions()
            self.statement_cutpoints = self._find_statements()
            self.parsed = self._segment_transcript()

            # If a committee name is missing from the committee_data.csv file, output a warning and skip the file
            if any(meta_chamber + '-' + c not in self.committee_data for c in self.entry['committees']) is True:
                print('Warning! One of the following committees is missing from the committee data file: ')

                for c in self.entry['committees']:
                    print((meta_chamber, c))

                print('--------')

            else:
                print('assigning metadata')
                self._assign_metadata()

        else:
            self.session_cutpoints = []
            self.statement_cutpoints = []
            self.parsed = []

        print((self.entry['id']))

    def _name_search(self, name_string):
        """ Helper function, which sorts through the hearing text and finds all names that start statements. """
        import re

        # VERY complicated name regex, which is tough to simplify, since names aren't consistent. Modify with care.
        # note additional second part of the regex, which adds an all-caps option for names
        # second additional part is shortened for performance, since it's a rare case
        matches = re.finditer('((?<= {4})|(?<=\t))[A-Z][a-z]+(\.)? ([A-Z][A-Za-z\'][-A-Za-z \[\]\']*?)*' +
                              '[A-Z\[\]][-A-Za-z\[\]]{1,100}(?=\.([- ]))' +
                              '|((?<= {4})|(?<=\t))[A-Z]+(\.)? [A-Z\[\]][-A-Z\[\]]{1,100}(?=\.([- ]))' +
                              # '|((?<= {4})|(?<=\t))[A-Z]+(\.)? ' +
                              # '[A-Z\[\]][-A-Z\[\]]{1,100}(?=\.([- ]))' +
                              '|((?<= {4})|(?<=\t))Voice(?=\.([- ]))' +
                              '|((?<= {4})|(?<=\t))The Chair(man|woman)(?=\.([- ]))',
                              name_string[0:self.max_search_length])
        for i, match in enumerate(matches):
            if match is not None and len(match.group(0).split()) <= 5 and \
                    re.search('^(' + '|'.join(self.prefixes) + ')', match.group(0)) is not None:

                return match

        return None

    def _find_sessions(self):
        """

        Helper function, which tries to find the opening and closing point for each session in a given hearing. If there
        isn't a clear endpoint for the hearing, the function sets a flag to drop the last statement in the hearing. This
        is because the last statement essentially becomes a "residual", which consumes all of the closing material in
        the hearing (e.g. documents submitted for the record, procedural information, etc.

        """
        import re

        o = list(re.finditer('The (Committee|Subcommittee)s? met', self.entry['transcript'], flags=re.I))
        if len(o) > 0 and o[0] is not None:
            openings = [regex.start() for regex in o]
        else:
            openings = [self._name_search(self.entry['transcript']).start() - 10]
        c = list(re.finditer('([\[(]?Whereupon[^\r\n]*?)?the\s+(Committee|Subcommittee|hearing|forum|panel)s?.*?' +
                             '(was|were)?\s+(adjourned|recessed)[\r\n]*?[\])]?|' +
                             '\[Additional material follows\.?\]', self.entry['transcript'], flags=re.I))

        if len(c) > 0:
            closings = [regex.start() for regex in c]
            self.delete_last = False
        else:
            closings = [len(self.entry['transcript'])]
            self.delete_last = True

        if len(closings) < len(openings):
            closings += openings[len(closings) + 1:]
            closings += [len(self.entry['transcript'])]
            self.delete_last = True
        elif len(openings) < len(closings):
            openings += closings[len(openings):]

        return list(zip(openings, closings))

    def _find_statements(self):
        """

        Helper function, which finds all statements in a given session. Statements are found using the _name_search
        function.

        """
        import re

        cuts = []
        for opening, closing in self.session_cutpoints:
            newlines = list(re.finditer('\n+( {4}|\t)', self.entry['transcript'][opening:closing]))
            for i, nl in enumerate(newlines):
                if i < len(newlines) - 1:
                    line = self.entry['transcript'][nl.start() + opening:newlines[i + 1].start() + opening]
                else:
                    line = self.entry['transcript'][nl.start() + opening:closing]

                s = self._name_search(line)

                # offset to get the indexing right
                offset = nl.start() + opening

                if s is not None:
                    cuts.append([s.start() + offset, s.end() + offset])

            cuts.append([closing])

        return cuts

    def _segment_transcript(self):
        import re

        def clean_statement(statement_string):
            """

            Helper function to clean undesired text out of statements. Currently cleans procedural text, with an option
            to remove prepared statements. Disabled by default.

            """

            s = re.search('([\[(].*?[\r\n]*.*?(prepared|opening)\s+statement.*?[\r\n]*.*?[\])]|' +
                          '[\[(].*?[\r\n]*.*?following.*?(was|were).*?[\r\n]*.*?[\r\n]*.*?[\])]|' +
                          '[\[(].*?[\r\n]*.*?follows?[:.].*?[\r\n]*[^<]*?[\])])' +
                          '(?!\s+[<|\[]GRAPHIC)',
                          statement_string, re.I)

            if s is not None:
                statement_string = statement_string[0:s.start()]

            statement_string = re.sub('---------+[\n\r]+.*?[\n\r]+---------+|\s*<[^\r\n]+>\s*', '', statement_string,
                                      flags=re.DOTALL)
            statement_string = re.sub('\[.*?[\n\r]*?.*?\]', '', statement_string)
            statement_string = re.sub('(OPENING )?STATEMENT.*', '', statement_string, flags=re.DOTALL)

            statement_string = statement_string.strip()

            return statement_string

        def process_name(name_str):
            name_str = re.sub('\s*\[[a-z ]*?\]\s*', '', name_str)
            state_matches = [st for st in states_long if st in name_str.lower()]
            if len(state_matches) == 1:
                state_str = state_matches[0]
                name_str = re.sub(' of ' + state_str, '', name_str, flags=re.I)
            else:
                state_str = None

            return name_str, state_str

        states_long = ['alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado', 'connecticut',
                       'delaware', 'district of columbia', 'florida', 'georgia', 'hawaii', 'idaho', 'illinois',
                       'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana', 'maine', 'maryland',
                       'massachusetts', 'michigan', 'minnesota', 'mississippi', 'missouri', 'montana',
                       'nebraska', 'nevada', 'new hampshire', 'new jersey', 'new mexico', 'new york',
                       'north carolina', 'north dakota', 'ohio', 'oklahoma', 'oregon', 'pennsylvania',
                       'rhode island', 'south carolina', 'south dakota', 'tennessee', 'texas', 'utah', 'vermont',
                       'virginia', 'washington', 'west virginia', 'wisconsin', 'wyoming']

        output = []

        # Loop over statement cutpoints. Note that the last set of cutpoints is length 1 (since it's just the end of the
        # hearing), so we can skip that.
        for i, cut in enumerate(self.statement_cutpoints):
            if len(cut) == 2:

                # Grab the name, and strip state names and editorial marks if present
                name, state = process_name(self.entry['transcript'][cut[0]:cut[1]])

                # Grab the chamber from the metadata
                meta_chamber = self.entry['chamber']

                # if committee data is given, get formal committee names and their chambers
                if self.committee_data:
                    committees = [self.committee_data[meta_chamber + '-' + c]['Code'] for c in self.entry['committees']]
                    hearing_chamber = list(set([self.committee_data[meta_chamber + '-' + c]['Chamber'] for c in
                                                self.entry['committees']]))

                    if len(hearing_chamber) > 1:
                        hearing_chamber = 'JOINT'
                    else:
                        hearing_chamber = hearing_chamber[0]

                else:
                    committees = []
                    hearing_chamber = None

                congress = self.entry['congress']
                date = self.entry['date']

                statement = self.entry['transcript'][cut[1] + 2:self.statement_cutpoints[i + 1][0]]
                cleaned = clean_statement(statement)

                output.append({'name_raw': name, 'name_full': None, 'member_id': None, 'state': state, 'party': None,
                               'committees': committees, 'person_chamber': None, 'date': date.strftime('%m-%d-%Y'),
                               'hearing_chamber': hearing_chamber, 'majority': None, 'party_seniority': None,
                               'leadership': None, 'congress': congress, 'jacket': self.entry['id'],
                               'cleaned': cleaned})

        if self.delete_last is True:
            del output[-1]

        return output

    def _assign_metadata(self):
        """

        Nasty function to create the output for the parser. For each statement, this function attempts to identify the
        speaker, assign  appropriate metadata to that speaker, and add the metadata to the output. Metadata are assigned
        using a multistep process, which is described below.

        """
        import re
        from string import punctuation

        def find_last_name(name_string):
            """ Helper function to find last names in name strings. """
            import re

            punctuation_list = '!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~'

            for pre in self.prefixes:
                name_string = re.sub('^' + pre, '', name_string)

            name_string = re.sub('\[.*?\]', '', name_string)
            name_string = re.sub('^\s*|\s*$', '', name_string)
            name_string = name_string.translate(str.maketrans(dict.fromkeys(punctuation_list)))
            name_string = name_string.strip()

            return name_string

        def find_member_list():
            """

            Helper function to parse preliminary member information. In many (though not all) hearing transcripts, the
            transcript begins with a list of members that are present at the hearing, which is useful for identifying
            members.

            """
            results = re.finditer('( {4}|\t)(Members |Also )?(present[^.]*?:)(.*?)\.[\n\r]',
                                  self.entry['transcript'][0:self.max_search_length], flags=re.I | re.S)
            out = []
            for result in results:
                result = re.sub('\s+', ' ', result.group(3))
                if re.search('staff', result, flags=re.I) is None:
                    out.append(result)

            if len(out) > 0:
                return ' '.join(out)
            else:
                return None

        def find_chair():
            """

            Helper function to find the name of the chairperson (from the introductory material in the hearing).
            Often, committee chairs (acting or otherwise) aren't referred to by name; so, need to be able to find that
            information separately.

            """
            start = self.statement_cutpoints[0][0]
            chair_search = re.search('([-A-Za-z\'\n]+)[,]?( (jr|[ivx]+))?[,. \n]*?\s+' +
                                     '[(\[]?(chairman|chairwoman)\s*(of|\)|\]|,)',
                                     self.entry['transcript'][start - 1000:start], flags=re.I)
            if chair_search is not None:
                return re.sub('\s', '', chair_search.group(1))
            else:
                return None

        trans_table = str.maketrans(dict.fromkeys(punctuation))

        states_long = ['alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado', 'connecticut',
                       'delaware', 'district of columbia', 'florida', 'georgia', 'hawaii', 'idaho', 'illinois',
                       'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana', 'maine', 'maryland',
                       'massachusetts', 'michigan', 'minnesota', 'mississippi', 'missouri', 'montana',
                       'nebraska', 'nevada', 'new hampshire', 'new jersey', 'new mexico', 'new york',
                       'north carolina', 'north dakota', 'ohio', 'oklahoma', 'oregon', 'pennsylvania',
                       'rhode island', 'south carolina', 'south dakota', 'tennessee', 'texas', 'utah', 'vermont',
                       'virginia', 'washington', 'west virginia', 'wisconsin', 'wyoming']

        states_abbrev = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', 'GA', 'HI', 'ID',
                         'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO',
                         'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA',
                         'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']

        chair = find_chair()
        present_members = find_member_list()
        if chair is not None and present_members is not None:
            present_members = find_member_list() + ' ' + chair
        else:
            present_members = find_member_list()

        for i in range(len(self.parsed)):
            name = self.parsed[i]['name_raw']
            state = self.parsed[i]['state']

            if re.search('the chair(man|woman)', name, re.I) is not None and chair is not None:
                name_last = chair
            else:
                name_last = find_last_name(name)

            committees = self.parsed[i]['committees']
            hearing_chamber = self.parsed[i]['hearing_chamber']
            congress = str(self.parsed[i]['congress'])

            person_chamber = ''

            #####################################################################
            # Ugly pile of logic to match names to metadata - modify with care! #
            #####################################################################
            # First, check to see if there's a member in the member table with a matching name, who also served on
            # the same committee in the same congress

            member_table_matches = [n_tuple for n_tuple in self.member_table
                                    if any([re.sub('\s|jr\.?', '',
                                                   str(name_last).lower()).translate(trans_table) ==
                                            re.sub('\s|jr\.?', '', str(n.split(',')[0]).lower()) for n in n_tuple])
                                    and congress in self.member_table[n_tuple]
                                    and any([c in self.member_table[n_tuple][congress] for c in committees]) is True]

            # If the state is specified in the transcript, do some additional matching
            if state is not None:
                abbrev = states_abbrev[states_long.index(state)]
                member_table_matches = [m for m in member_table_matches if abbrev in self.member_table[m]['State']]

            # Same process for witnesses
            witness_name_matches = [n for n in self.entry['witness_meta']
                                    if name_last.lower().translate(trans_table)
                                    in str(n).lower().translate(trans_table)]

            # Same process for "guest" members who happen to be present at that hearing, matching on Congress and
            # list of members in the present_members list
            guest_matches = [n_tuple for n_tuple in self.member_table
                             if any([re.sub('\s|jr\.?', '', str(name_last).lower()).translate(trans_table) ==
                                     re.sub('\s|jr\.?', '', str(n.split(',')[0]).lower()) for n in n_tuple])
                             and hearing_chamber in self.member_table[n_tuple]['Chamber']
                             and congress in self.member_table[n_tuple]
                             and present_members is not None
                             and any([n.split(',')[0].lower() in present_members.lower() for n in n_tuple])]

            # If there's a unique match on the member name, take that as the match
            if len(member_table_matches) == 1:
                name_full = member_table_matches[0]

                first_committee = list(self.member_table[name_full][congress].keys())[0]
                party = self.member_table[name_full][congress][first_committee]['Party']

                member_id = self.member_table[name_full]['id']
                current_committees = [c for c in committees if c in self.member_table[name_full][congress]]
                person_chamber = self.member_table[name_full][congress][current_committees[0]]['Chamber']

                if len(current_committees) == 1:
                    c = current_committees[0]
                    majority = self.member_table[name_full][congress][c]['Majority']
                    party_seniority = self.member_table[name_full][congress][c]['Party Seniority']
                    leadership = self.member_table[name_full][congress][c]['Leadership']

                else:
                    majority = 'NA'
                    party_seniority = 'NA'
                    leadership = 'NA'

            # else, if there's a unique match in the witness list (and the witness isn't a member of congress),
            # use that
            elif len(witness_name_matches) == 1 and 'Representative in Congress' not in witness_name_matches[0] \
                    and 'Senator' not in witness_name_matches[0]:
                name_full = (witness_name_matches[0],)
                member_id = 'NA'
                party = 'WITNESS'
                majority = 'NA'
                party_seniority = 'NA'
                leadership = 'NA'
                person_chamber = hearing_chamber

            # else, if there's a unique match in the list of guest members, take that
            elif len(guest_matches) == 1:
                name_full = guest_matches[0]
                member_id = self.member_table[guest_matches[0]]['id']

                first_committee = list(self.member_table[guest_matches[0]][congress].keys())[0]
                party = self.member_table[guest_matches[0]][congress][first_committee]['Party']
                person_chamber = self.member_table[guest_matches[0]][congress][first_committee]['Chamber']

                majority = 'NA'
                party_seniority = 'NA'
                leadership = 'NA'

            # if all else fails, check the member data table and see if there's a member of Congress with a matching
            # name who served on the given committee in the given Congress - if so, take that as a match
            else:
                rep_list = [n_tuple for n_tuple in self.member_table
                            if any([re.sub('\s|jr\.?', '', str(name_last).lower()).translate(trans_table) ==
                                    re.sub('\s|jr\.?', '', str(n.split(',')[0]).lower()) for n in n_tuple])
                            and congress in self.member_table[n_tuple]]

                if len(rep_list) == 1:
                    try:
                        name_line = re.search('.* ' + name_last + '[ ,.].*|^' + name_last + '[ ,.].*',
                                              self.entry['transcript'][:self.statement_cutpoints[0][0]],
                                              flags=re.I | re.M).group(0).strip()

                        first_word = re.search('^[^\s]*', name_line).group(0)
                        if first_word in ['Representative ', 'Senator '] or 'Representative in Congress' in \
                                name_line or 'U.S. Senator' in name_line:
                            name_full = rep_list[0]
                            member_id = self.member_table[name_full]['id']
                            party = self.member_table[name_full]['Party']
                            current_committees = [c for c in committees
                                                  if c in self.member_table[name_full][congress]]
                            person_chamber = self.member_table[name_full]['Chamber']

                            if len(current_committees) == 1:
                                c = current_committees[0]
                                majority = self.member_table[name_full][congress][c]['Majority']
                                party_seniority = self.member_table[name_full][congress][c]['Party Seniority']
                                leadership = self.member_table[name_full][congress][c]['Leadership']
                            else:
                                majority = 'NA'
                                party_seniority = 'NA'
                                leadership = 'NA'

                        else:
                            name_full = ('NA',)
                            member_id = 'NA'
                            party = 'NA'
                            majority = 'NA'
                            party_seniority = 'NA'
                            leadership = 'NA'

                    except AttributeError:
                        name_full = ('NA',)
                        member_id = 'NA'
                        party = 'NA'
                        majority = 'NA'
                        party_seniority = 'NA'
                        leadership = 'NA'

                else:
                    name_full = ('NA',)
                    member_id = 'NA'
                    party = 'NA'
                    majority = 'NA'
                    party_seniority = 'NA'
                    leadership = 'NA'

            if person_chamber == '':
                person_chamber = hearing_chamber

            self.parsed[i].update(
                {'name_full': name_full, 'member_id': member_id, 'party': (party,), 'majority': majority,
                 'person_chamber': person_chamber, 'party_seniority': party_seniority,
                 'leadership': leadership, 'committees': (committees,)})


class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = io.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)
