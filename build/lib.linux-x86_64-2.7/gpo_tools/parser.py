import csv
import pkg_resources
from itertools import chain
import os
import psycopg2
from psycopg2.extras import DictCursor
import re


class Parser:
    def __init__(self, db, user, password, host='localhost', id_values=None):
        """ 
        GPO parser class. Assumes that the given database has been formatted by gpotools.Scraper.
        """

        # authenticate the connection and check to make sure the database is properly configured
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
        self.member_table = {meta['Name'][0]: meta.update(membership)
                             for id_val, meta, membership in cur.fetchall()}

        # load the committee data file from the package resources
        print pkg_resources.resource_filename('gpo_tools', 'data/committee_data.csv')
        with open(pkg_resources.resource_filename('gpo_tools', 'data/committee_data.csv')) as f:
            reader = csv.reader(f)
            reader.next()
            self.committee_data = {row[0]: {'Code': row[1], 'Chamber': row[2]} for row in csv.reader(f)}

        self.results = []

        # select ID values from the database.
        if not id_values:
            confirmation = input('No ID values were given, so all IDs in the database will be processed. '
                                 'Proceed y/(n)?')
            if confirmation == 'y':
                cur.execute('select id from hearings')
                self.id_values = [r[0] for r in cur.fetchall()]
            else:
                self.id_values = []

        else:
            # do some basic checking of user-inputted id values - not 100% comprehensive but should fairly broad
            if type(id_values) != list or any([type(id_value) != str for id_value in id_values]) or \
                    any([re.search('CHRG-[0-9]+[a-z]+[0-9]+', id_value) is None for id_value in id_values]):

                raise ValueError(""" id_values should be a list of strings, following the naming convention used by the
                                     GPO (e.g. \'CHRG-113jhrg79942\'). """)
            else:
                self.id_values = id_values

    def parse_gpo_hearings(self, n_cores=4):
        """ Primary parser function. Wraps and parallelizes methods described elsewhere in this file. """
        import pprocess

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
                parsed = ParseHearing(entry, committee_data=self.committee_data, member_table=self.member_table).parsed

                output.append(parsed)

            # Returned value records whether the file was actually parsed.
            return output

        n_ids = len(self.id_values)

        # if n_ids is reasonably large (say >1000), parallelize; if not, just do in serial
        if n_ids > 100000:
            to_analyze = [{'con': psycopg2.connect(**self.credentials),
                           'id_inds': range(i*n_ids/n_cores, (i+1)*n_ids/n_cores)}
                          for i in range(n_cores)]

            self.results = [r for r in pprocess.pmap(parse, to_analyze, limit=n_cores)]
            self.results = list(chain(*self.results))
        else:
            con = psycopg2.connect(**self.credentials)
            self.results = parse({'con': con, 'id_inds': range(len(self.id_values))})

    def _update_tables(self):
        """ Wrapper function for creating and updating metadata tables. See functions for details. """
        self._update_gpo_tables()
        self._manual_hearing_table_update()

    def _update_gpo_tables(self):
        """

        Function for creating hearing-level metadata files. These files provide a map between sudoc and jacket numbers
        for hearings. Jacket numbers are contained in the file names (and resulting URLs) for each hearing. Sudocs are
        contained in the hearing metadata files (but note that these data are often missing).

        """
        import re
        import json
        from itertools import chain

        # Get the current tables
        sudoc_table = self._get_current_data(self.pwd + os.sep + 'sudoc_table.json')
        jacket_table = self._get_current_data(self.pwd + os.sep + 'jacket_table.json')

        gpo_file_list = list(chain(*[[direc[0] + os.sep + fi for fi in direc[2] if 'json' in fi]
                                     for direc in self.gpo_walked]))

        for i, file_name in enumerate(gpo_file_list):
            with open(file_name, 'rb') as f:
                content = json.loads(f.read())
            print file_name

            meta = content['Hearing Info']

            # If a sudoc is present, loop over sudocs and create a map between the sudoc and the hearing jacket
            # Note that a single metadata file can have multiple sudocs, or none!
            if meta['sudoc'] is not None:
                sudocs = [meta['sudoc'] + ':' + meta['Congress'] + '-' + n for n in meta['Number']]
                jacket = re.search('[0-9]+(?=\.json)', file_name).group(0)
                for s in sudocs:
                    sudoc_number = re.sub(' ', '', s)
                    if sudoc_number not in sudoc_table:
                        sudoc_table[sudoc_number] = {'jacket': jacket}
                        jacket_table[jacket] = {'sudoc': sudoc_number}

        with open(self.pwd + os.sep + 'sudoc_table.json', 'wb') as f:
            f.write(json.dumps(sudoc_table))

        with open(self.pwd + os.sep + 'jacket_table.json', 'wb') as f:
            f.write(json.dumps(jacket_table))

    def _manual_hearing_table_update(self):
        """

        Update the hearing table according to a manually-constructed file which matches CIS numbers to sudocs, jackets,
        and sudocs. See accompanying data files and documentation for details.

        """
        import csv
        import json

        with open(self.pwd + os.sep + 'manual_sudoc_table.csv') as f:
            manual_table = list(csv.reader(f))[1:]

        sudoc_table = self._get_current_data(self.pwd + os.sep + 'sudoc_table.json')
        jacket_table = self._get_current_data(self.pwd + os.sep + 'jacket_table.json')
        cis_number_table = self._get_current_data(self.pwd + os.sep + 'cis_number_table.json')

        for row in manual_table:
            cis_number = row[0]
            pap_code = row[1]
            sudoc_number = row[2]
            jacket = row[3]

            if '' not in [cis_number, sudoc_number, jacket]:
                jacket_table[jacket] = {'CIS': cis_number, 'sudoc': sudoc_number, 'PAP_Code': pap_code}
                sudoc_table[sudoc_number] = {'CIS': cis_number, 'jacket': jacket, 'PAP_Code': pap_code}
                cis_number_table[cis_number] = {'sudoc': sudoc_number, 'jacket': jacket, 'PAP_Code': pap_code}

        with open(self.pwd + os.sep + 'sudoc_table.json', 'wb') as f:
            f.write(json.dumps(sudoc_table))

        with open(self.pwd + os.sep + 'jacket_table.json', 'wb') as f:
            f.write(json.dumps(jacket_table))

        with open(self.pwd + os.sep + 'cis_number_table.json', 'wb') as f:
            f.write(json.dumps(cis_number_table))

    def create_dataset(self, jackets_dates=(), types_dates=(), committee_list=(),
                       out_name='financial', min_words=10):
        """

        Create the finished corpus file. Corpus is created using a subset of available documents, identified using the
        jacket_out_list argument. Preprocessing for the corpus file is conducted in this function as well.

        Output is saved to the working directory (named with today's date) using several output formats. The primary
        corpus output is in Blei's LDA-C format, although a flat (csv-type) output is also created. A dictionary of word
        indices for the LDA-C output is also generated. Also creates an index file, which contains one line for each
        document in the corpus. Index file contains both individual-level and hearing-level metadata entries, which
        apply to each document.

        """
        import re
        import csv
        import sys
        import json
        import string
        from gensim import corpora
        from itertools import chain
        from datetime import datetime
        from itertools import repeat
        from nltk.corpus import stopwords

        csv.field_size_limit(sys.maxsize)
        stopwords = stopwords.words('english')

        # Get the list of available files, and some preliminary data ready for metadata extraction
        file_list = list(chain(*[[direc[0] + os.sep + fi for fi in direc[2] if 'csv' in fi]
                                 for direc in self.gpo_walked]))

        with open(self.pwd + os.sep + 'committee_data.csv', 'rb') as f:
            committee_table = {row[0]: {'Code': row[1], 'Chamber': row[2]} for row in csv.reader(f)}

        today = datetime.now().strftime('%m%d%Y')

        if len(jackets_dates) > 0:
            jackets_to_parse = zip(*jackets_dates)[1]
        else:
            jackets_to_parse = ()

        with open(self.pwd + os.sep + 'cis_number_table.json', 'rb') as f:
            cis_table = json.loads(f.read())
            jackets = [cis_table[k]['jacket'] for k in cis_table]
            pap_codes = [cis_table[k]['PAP_Code'] for k in cis_table]

        documents = []
        corpus = []
        index = []

        for i, in_file in enumerate(file_list):
            print i, in_file
            if '#' not in in_file:
                data_file = re.sub('csv', 'json', in_file)

                with open(data_file, 'rb') as f:
                    data = json.loads(f.read())

                date = data['Hearing Info']['Date']
                committee = data['Hearing Info']['Committee']

                committee_codes = [committee_table[data['Hearing Info']['Chamber'] + '-' + c_name]['Code']
                                   for c_name in committee]

                jacket = re.search('([0-9]+)\.csv', in_file).group(1)

                # Parse if the given jacket is in the jacket table and in the list identified by jackets_to_parse.
                if any(c_name in committee_list for c_name in committee_codes) or \
                      (jacket in jackets and jacket in jackets_to_parse):

                    if len(jackets_dates) > 0 and len(types_dates) > 0:
                        jacket_row = [row for row in jackets_dates if row[1] == jacket][0]
                        cis = jacket_row[0]

                        cis_row = [row for row in types_dates if row[0] == cis][0]
                        cis_year = cis_row[1]
                        pap_code = pap_codes[jackets.index(jacket)]
                    else:
                        cis_year = None
                        pap_code = None

                    # apparently, jacket numbers are re-used fairly frequently (5-8% re-use or so)
                    # this at least cuts the more recent
                    if cis_year is None or cis_year == date[0:4]:
                        with open(in_file, 'rb') as f:
                            text = list(csv.reader(f))

                        if len(text) > 1 and len(text[0]) > 2:
                            for row in text:
                                # Get metadata from the CSV parsed text
                                speaker_name = row[0]
                                member_id = row[2]
                                state = row[3]
                                speaker_type = row[4]
                                person_chamber = row[6]
                                speaker_chamber = row[7]
                                majority = row[8]
                                party_seniority = row[9]
                                leadership = row[10]

                                hearing_identifier = re.search('([0-9]+)\.csv', in_file).group(1)

                                # Preprocess, part 1. Documents are lower-cased, punctuation is stripped, words < 3
                                # characters are dropped, stopwords are dropped. After preprocessing words, documents
                                # <= 5 words are also dropped.

                                doc = [w for w in row[11].lower().translate(None, string.punctuation).split()
                                       if len(w) > 3 and w not in stopwords]
                                if len(doc) > 5:
                                    index.append([speaker_name, speaker_type, member_id, date, state,
                                                  hearing_identifier, ' '.join(committee), ' '.join(committee_codes),
                                                  person_chamber,  speaker_chamber, majority, party_seniority,
                                                  leadership, pap_code])
                                    documents.append(doc)

        # Preprocess, part 2. Words that occur in fewer than 10 documents, or in every document, are dropped. After
        # these rare words are dropped, documents with less than 5 words are dropped. All other words are retained.
        dic = corpora.Dictionary(documents)
        dic.filter_extremes(no_below=min_words, no_above=1)
        dic.compactify()
        print dic
        keep = []
        bow_list = []
        for i, doc in enumerate(documents):
            bow = dic.doc2bow(doc)
            if len(bow) > 5:
                corpus.append([' '.join([' '.join(list(repeat(dic[k], times=v))) for k, v in bow])])
                keep.append(index[i]+[len(bow)])
                bow_list.append(bow)

        # Save outputs.
        with open(self.pwd + os.sep + 'corpus_' + today + '.csv', 'wb') as f:
            csv.writer(f).writerows(corpus)
        with open(self.pwd + os.sep + 'corpus_index_' + today + '.csv', 'wb') as f:
            csv.writer(f).writerows(keep)

        corpora.Dictionary.save(dic, self.pwd + os.sep + out_name + '_' + today + '.lda-c.dic')
        corpora.BleiCorpus.serialize(fname=self.pwd + os.sep + out_name + '_' + today + '.lda-c',
                                     corpus=bow_list, id2word=dic)

    def update_tables_from_file(self, data_path):
        """

        Extra method to update sudoc/CIS/jacket indices from file, if desired. Assumed input for data_path is
        a flat (csv) file with three items per row: [cis, sudoc, jacket]. Optionally, a fourth row with pap_code
        can also be included.

        """

        import csv
        import json

        sudoc_table = self._get_current_data(self.pwd + os.sep + 'sudoc_table.json')
        jacket_table = self._get_current_data(self.pwd + os.sep + 'jacket_table.json')
        cis_number_table = self._get_current_data(self.pwd + os.sep + 'cis_number_table.json')

        with open(data_path, 'rb') as f:
            content = list(csv.reader(f))

        for row in content:
            cis_number = row[0]
            sudoc_number = row[1]
            jacket_number = row[2]
            if len(row) > 3:
                pap_code = row[3]

            cis_number_table[cis_number] = {'sudoc': sudoc_number, 'jacket': jacket_number, 'PAP_Code': pap_code}
            jacket_table[jacket_number] = {'sudoc': sudoc_number, 'CIS': cis_number, 'PAP_Code': pap_code}
            sudoc_table[sudoc_number] = {'CIS': cis_number, 'jacket': jacket_number, 'PAP_Code': pap_code}

        with open(self.pwd + os.sep + 'sudoc_table.json', 'wb') as f:
            f.write(json.dumps(sudoc_table))

        with open(self.pwd + os.sep + 'jacket_table.json', 'wb') as f:
            f.write(json.dumps(jacket_table))

        with open(self.pwd + os.sep + 'cis_number_table.json', 'wb') as f:
            f.write(json.dumps(cis_number_table))

    @staticmethod
    def _get_current_data(path):
        """

        Helper function, which checks to see if a path (to a json object) exists. If it exists, read in the file;
        if not, create the file, and return an empty dictionary

        """
        import os
        import json

        if os.path.exists(path) is False:
            with open(path, 'wb') as f:
                current_data = {}
                f.write(json.dumps(current_data))
        else:
            with open(path, 'rb') as f:
                current_data = json.loads(f.read())

        return current_data

    @staticmethod
    def _syschar():
        """ Helper function, which returns the appropriate slash character for the native operating system. """
        import sys

        if sys.platform in ['win32', 'cygwin']:
            return '\\'
        else:
            return '/'

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
                         'The Chairwoman', 'Governor', 'Chair', 'The Clerk', 'Clerk', 'Mayor', 'Reverend', 'Justice']

        # Constant for performance. Limits how far forward (number of characters) the script will search in order to
        # find certain pieces of information, such as the name of the chair of the committee.
        self.max_search_length = 75000
        self.delete_last = False

        meta_chamber = self.entry['chamber']

        # If there's at least one statement identified in the text, start parsing
        if self._name_search(self.entry['transcript']) is not None:
            self.session_cutpoints = self._find_sessions()
            self.statement_cutpoints = self._find_statements()
            # self.parsed = self._segment_transcript()

            # If a committee name is missing from the committee_data.csv file, output a warning and skip the file
            if any(meta_chamber + '-' + c not in self.committee_data for c in self.entry['committees']) is True:
                print 'Warning! One of the following committees is missing from the committee data file: '

                for c in self.entry['committees']:
                    print meta_chamber, c

                print '--------'
                x = raw_input('')
                if x:
                    raise

            else:
                print 'assigning metadata'
                #print self.committee_data
                #print self.entry['uri']
                #raise
                #self._assign_metadata()

        else:
            self.session_cutpoints = None
            self.statement_cutpoints = None
            self.parsed = None

        self.parsed = None

    def _name_search(self, string):
        """ Helper function, which sorts through the hearing text and finds all names that start statements. """
        import re

        # VERY complicated name regex, which is tough to simplify, since names aren't consistent. Modify with care.
        matches = re.finditer('(?<=    )[A-Z][a-z]+(\.)? ([A-Z][A-Za-z\'][-A-Za-z \[\]\']*?)*' +
                              '[A-Z\[\]][-A-Za-z\[\]]{1,100}(?=\.([- ]))' +
                              '|(?<=    )Voice(?=\.([- ]))' +
                              '|(?<=    )The Chair(man|woman)(?=\.([- ]))', string[0:self.max_search_length])
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
            openings = [self._name_search(self.entry['transcript']).start()-10]
        c = list(re.finditer('([\[\(]?Whereupon[^\r\n]*?)?the\s+(Committee|Subcommittee|hearing|forum|panel)s?.*?' +
                             '(was|were)?\s+(adjourned|recessed)[\r\n]*?[\]\)]?', self.entry['transcript'], flags=re.I))

        if len(c) > 0:
            closings = [regex.start() for regex in c]
            self.delete_last = False
        else:
            closings = [len(self.entry['transcript'])]
            self.delete_last = True

        if len(closings) < len(openings):
            closings += openings[len(closings)+1:]
            closings += [len(self.entry['transcript'])]
            self.delete_last = True
        elif len(openings) < len(closings):
            openings += closings[len(openings):]

        return zip(openings, closings)

    def _find_statements(self):
        """

        Helper function, which finds all statements in a given session. Statements are found using the _name_search
        function.

        """
        import re

        cutpoints = []
        for opening, closing in self.session_cutpoints:
            newlines = list(re.finditer('\n+    ', self.entry['transcript'][opening:closing]))
            for i, nl in enumerate(newlines):
                if i < len(newlines)-1:
                    line = self.entry['transcript'][nl.start() + opening:newlines[i+1].start() + opening]
                else:
                    line = self.entry['transcript'][nl.start() + opening:closing]

                s = self._name_search(line)

                # offset to get the indexing right
                offset = nl.start() + opening

                if s is not None:
                    cutpoints.append([s.start() + offset, s.end() + offset])

            cutpoints.append([closing])

        return cutpoints

    def _segment_transcript(self):
        import re

        def clean_statement(string):
            """

            Helper function to clean undesired text out of statements. Currently cleans titles, some prepared
            statements, and some procedural text.

            """
            s = re.search('([\[(].*?[\r\n]*.*?(prepared|opening)\s+statement.*?[\r\n]*.*?[\])]|' +
                          '[\[(].*?[\r\n]*.*?following.*?(was|were).*?[\r\n]*.*?[\r\n]*.*?[\])]|' +
                          '[\[(].*?[\r\n]*.*?follows?\.:.*?[\r\n]*[^<]*?[\])])' +
                          '(?!\s+[<|\[]GRAPHIC)',
                          string, re.I)

            if s is not None:
                string = string[0:s.start()]
            string = re.sub('---------+[\n\r]+.*?[\n\r]+---------+|\s*<[^\r\n]+>\s*', '', string, flags=re.S)
            string = re.sub('\[.*?\]', '', string)
            string = re.sub('(OPENING )?STATEMENT.*', '', string, flags=re.DOTALL)
            string = string.strip()

            return string

        def process_name(string):
            name_str = string
            name_str = re.sub('\s*\[[a-z ]*?\]\s*', '', name_str)
            state_matches = [st for st in states_long if st in name_str.lower()]
            if len(state_matches) == 1:
                state_str = state_matches[0]
                name_str = re.sub(' of ' + state_str, '', name_str, flags=re.I)
            else:
                state_str = None

            return name_str, state_str

        states_long = [u'alabama', u'alaska', u'arizona', u'arkansas', u'california', u'colorado', u'connecticut',
                       u'delaware', u'district of columbia', u'florida', u'georgia', u'hawaii', u'idaho', u'illinois',
                       u'indiana', u'iowa', u'kansas', u'kentucky', u'louisiana', u'maine', u'maryland',
                       u'massachusetts', u'michigan', u'minnesota', u'mississippi', u'missouri', u'montana',
                       u'nebraska', u'nevada', u'new hampshire', u'new jersey', u'new mexico', u'new york',
                       u'north carolina', u'north dakota', u'ohio', u'oklahoma', u'oregon', u'pennsylvania',
                       u'rhode island', u'south carolina', u'south dakota', u'tennessee', u'texas', u'utah', u'vermont',
                       u'virginia', u'washington', u'west virginia', u'wisconsin', u'wyoming']

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
                    committee_chamber = list(set([self.committee_data[meta_chamber + '-' + c]['Chamber'] for c in
                                                  self.entry['committees']]))

                    if len(committee_chamber) > 1:
                        committee_chamber = 'JOINT'
                    else:
                        committee_chamber = committee_chamber[0]

                else:
                    committees = []
                    committee_chamber = None

                congress = self.entry['congress']

                statement = self.entry['transcript'][cut[1] + 2:self.statement_cutpoints[i + 1][0]]
                cleaned = clean_statement(statement)

                output.append({'name_raw': name, 'name_full': None, 'member_id': None, 'state': state, 'party': None,
                               'committees': ','.join(committees), 'person_chamber': None,
                               'hearing_chamber': committee_chamber, 'majority': None, 'party_seniority': None,
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

        def find_last_name(string):
            """ Helper function to find last names in name strings. """
            import re

            punctuation_list = '!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~'

            for pre in self.prefixes:
                string = re.sub('^' + pre, '', string)

            string = re.sub('\[.*?\]', '', string)
            string = re.sub('^\s*|\s*$', '', string)
            string = string.translate(None, punctuation_list)
            string = string.strip()

            return string

        def clean_statement(string):
            """

            Helper function to clean undesired text out of statements. Currently cleans titles, some prepared
            statements, and some procedural text.

            """
            s = re.search('(\[\(.*?[\r\n]*.*?(prepared|opening)\s+statement.*?[\r\n]*.*?\]\)|' +
                          '\[\(.*?[\r\n]*.*?following.*?(was|were).*?[\r\n]*.*?[\r\n]*.*?\]\)|' +
                          '\[\(.*?[\r\n]*.*?follows?\.:.*?[\r\n]*[^<]*?\]\))' +
                          '(?!\s+[<|\[]GRAPHIC)',
                          string, re.I)
            if s is not None:
                string = string[0:s.start()]
            string = re.sub('---------+[\n\r]+.*?[\n\r]+---------+|\s*<[^\r\n]+>\s*', '', string, flags=re.S)
            string = re.sub('\[.*?\]', '', string)
            return string

        def find_member_list():
            """

            Helper function to parse preliminary member information. In many (though not all) hearing transcripts, the
            transcript begins with a list of members that are present at the hearing, which is useful for identifying
            members.

            """
            results = re.finditer('    (Members |Also )?(present[^.]*?:)([^.]+)',
                                  self.hearing_text[0:self.max_search_length], flags=re.I)
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
            chair_search = re.search('([-A-Za-z\'\n]+)[,]?( (jr|[ivx]+))?[,\. \n]*?\s+' +
                                     '[\(\[]?(chairman|chairwoman)( of|\)|\]|,)',
                                     self.hearing_text[start-1000:start], flags=re.I)
            if chair_search is not None:
                return re.sub('\s', '', chair_search.group(1))
            else:
                return None

        states_long = [u'alabama', u'alaska', u'arizona', u'arkansas', u'california', u'colorado', u'connecticut',
                       u'delaware', u'district of columbia', u'florida', u'georgia', u'hawaii', u'idaho', u'illinois',
                       u'indiana', u'iowa', u'kansas', u'kentucky', u'louisiana', u'maine', u'maryland',
                       u'massachusetts', u'michigan', u'minnesota', u'mississippi', u'missouri', u'montana',
                       u'nebraska', u'nevada', u'new hampshire', u'new jersey', u'new mexico', u'new york',
                       u'north carolina', u'north dakota', u'ohio', u'oklahoma', u'oregon', u'pennsylvania',
                       u'rhode island', u'south carolina', u'south dakota', u'tennessee', u'texas', u'utah', u'vermont',
                       u'virginia', u'washington', u'west virginia', u'wisconsin', u'wyoming']

        states_abbrev = [u'AL', u'AK', u'AZ', u'AR', u'CA', u'CO', u'CT', u'DE', u'DC', u'FL', u'GA', u'HI', u'ID',
                         u'IL', u'IN', u'IA', u'KS', u'KY', u'LA', u'ME', u'MD', u'MA', u'MI', u'MN', u'MS', u'MO',
                         u'MT', u'NE', u'NV', u'NH', u'NJ', u'NM', u'NY', u'NC', u'ND', u'OH', u'OK', u'OR', u'PA',
                         u'RI', u'SC', u'SD', u'TN', u'TX', u'UT', u'VT', u'VA', u'WA', u'WV', u'WI', u'WY']

        output = []
        chair = find_chair()
        present_members = find_member_list()
        if chair is not None and present_members is not None:
            present_members = find_member_list() + ' ' + chair
        else:
            present_members = find_member_list()

        # Loop over statement cutpoints. Note that the last set of cutpoints is length 1 (since it's just the end of the
        # hearing), so we can skip that.
        for i, cut in enumerate(self.statement_cutpoints):
            if len(cut) == 2:

                # Grab the name, and strip state names and editorial marks if present
                name = self.hearing_text[cut[0]:cut[1]]
                name = re.sub('\s*\[[a-z ]*?\]\s*', '', name)
                state_matches = [st for st in states_long if st in name.lower()]
                if len(state_matches) == 1:
                    state = state_matches[0]
                    name = re.sub(' of ' + state, '', name, flags=re.I)
                else:
                    state = None

                # Gather metadata
                if re.search('the chair(man|woman)', name, re.I) is not None and chair is not None:
                    name_last = chair
                else:
                    name_last = find_last_name(name)

                meta_chamber = self.hearing_data['Hearing Info']['Chamber']
                committees = [self.committee_data[meta_chamber + '-' + c]['Code'] for c in
                              self.hearing_data['Hearing Info']['Committee']]

                hearing_chamber = list(set([self.committee_data[meta_chamber + '-' + c]['Chamber'] for c in
                                            self.hearing_data['Hearing Info']['Committee']]))

                person_chamber = ''

                if len(hearing_chamber) > 1:
                    hearing_chamber = 'JOINT'
                else:
                    hearing_chamber = hearing_chamber[0]

                congress = self.hearing_data['Hearing Info']['Congress']

                #####################################################################
                # Ugly pile of logic to match names to metadata - modify with care! #
                #####################################################################

                # First, check to see if there's a member in the member table with a matching name, who also served on
                # the same committee in the same congress
                member_table_matches = [n for n in self.member_table if str(name_last).lower() ==
                                        str(n.split(',')[0]).lower()
                                        and congress in self.member_table[n]
                                        and any([c in self.member_table[n][congress]
                                                 for c in committees]) is True]

                # If the state is specified in the transcript, do some additional matching
                if state is not None:
                    abbrev = states_abbrev[states_long.index(state)]
                    member_table_matches = [m for m in member_table_matches if self.member_table[m]['State'] == abbrev]

                # Same process for witnesses
                witness_name_matches = [n for n in self.hearing_data['Witness Info'] if
                                        name_last.lower() in str(n).lower().translate(None, punctuation)]

                # Same process for "guest" members who happen to be present at that hearing, matching on Congress and
                # list of members in the present_members list
                guest_matches = [n for n in self.member_table if str(name_last).lower() in str(n).lower()
                                 and hearing_chamber in self.member_table[n]['Chamber']
                                 and congress in self.member_table[n]
                                 and present_members is not None
                                 and n.split(',')[0].lower() in present_members.lower()]

                # If there's a unique match on the member name, take that as the match
                if len(member_table_matches) == 1:
                    name_full = member_table_matches[0]
                    party = self.member_table[name_full]['Party']
                    member_id = self.member_table[name_full]['ID']
                    current_committees = [c for c in committees if c in self.member_table[name_full][congress]]
                    person_chamber = self.member_table[name_full][congress]['Chamber']

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
                    name_full = witness_name_matches[0]
                    member_id = 'NA'
                    party = 'WITNESS'
                    majority = 'NA'
                    party_seniority = 'NA'
                    leadership = 'NA'
                    person_chamber = hearing_chamber

                # else, if there's a unique match in the list of guest members, take that
                elif len(guest_matches) == 1:
                    name_full = guest_matches[0]
                    member_id = self.member_table[guest_matches[0]]['ID']
                    party = self.member_table[guest_matches[0]]['Party']
                    person_chamber = self.member_table[guest_matches[0]][congress]['Chamber']
                    majority = 'NA'
                    party_seniority = 'NA'
                    leadership = 'NA'

                # if all else fails, check the member data table and see if there's a member of Congress with a matching
                # name who served on the given committee in the given Congress - if so, take that as a match
                else:
                    rep_list = [n for n in self.member_table if str(name_last).lower() ==
                                str(n.split(',')[0]).lower()
                                and congress in self.member_table[n]]

                    if len(rep_list) == 1:
                        try:
                            name_line = re.search('.* ' + name_last + '[ ,.].*|^' + name_last + '[ ,.].*',
                                                  self.hearing_text[:self.statement_cutpoints[0][0]],
                                                  flags=re.I | re.M).group(0).strip()
                            first_word = re.search('^[^\s]*', name_line).group(0)
                            if first_word in ['Representative ', 'Senator '] or 'Representative in Congress' in \
                                    name_line or 'U.S. Senator' in name_line:
                                name_full = rep_list[0]
                                member_id = self.member_table[name_full]['ID']
                                party = self.member_table[name_full]['Party']
                                current_committees = [c for c in committees
                                                      if c in self.member_table[name_full][congress]]
                                person_chamber = self.member_table[name_full][congress]['Chamber']

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
                                name_full = 'NA'
                                member_id = 'NA'
                                party = 'NA'
                                majority = 'NA'
                                party_seniority = 'NA'
                                leadership = 'NA'

                        except AttributeError:
                            name_full = 'NA'
                            member_id = 'NA'
                            party = 'NA'
                            majority = 'NA'
                            party_seniority = 'NA'
                            leadership = 'NA'

                    else:
                        name_full = 'NA'
                        member_id = 'NA'
                        party = 'NA'
                        majority = 'NA'
                        party_seniority = 'NA'
                        leadership = 'NA'

                # Append all of that matched data and the statement text to the output file
                statement = self.hearing_text[cut[1] + 2:self.statement_cutpoints[i+1][0]]
                cleaned = clean_statement(statement)

                if person_chamber == '':
                    person_chamber = hearing_chamber

                output.append([name, name_full, member_id, state, party, ','.join(committees), person_chamber,
                               hearing_chamber, majority, party_seniority, leadership, cleaned])

        if self.delete_last is True:
            del output[-1]

        return output

#
# import csv
# from datetime import datetime
#
# start = datetime.now()
# print start
#
# with open('/home/rbshaffer/Desktop/Financial_Replication/Hearing_Data/manual_sudoc_table.csv', 'rb') as f:
#     manual_sudoc_table = list(csv.reader(f))
#     jackets_dates = zip(zip(*manual_sudoc_table)[0], zip(*manual_sudoc_table)[3])
#
# with open('/home/rbshaffer/Desktop/Financial_Replication/Hearing_Data/dates and hearing types.csv', 'rb') as f:
#     types_dates = list(csv.reader(f))
#     types_dates = [[row[0]] + row[2:5] for row in types_dates]
#
#
# build = BuildDatabase('/home/rbshaffer/Desktop/Financial_Replication/Hearing_Data')
# # build.parse_gpo_hearings()
# # results = build.results
# # build.create_dataset(jackets_dates, types_dates)
# build.create_dataset(committee_list=('196', '156', '251'),
#                      out_name='comparing_committees',
#                      min_words=50)
# print 'dataset created'
#
# print datetime.now() - start
