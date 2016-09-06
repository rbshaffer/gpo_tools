__author__ = 'rbshaffer'

import os

class BuildDatabase:
    def __init__(self, pwd):
        """ Base class for constructing the financial hearings dataset. """
        # slash separator character, which depends on system
        self.slash = self._syschar()

        # path to working directory (where inputs will be stored).
        # Assumed to be created by gpo_scraper.py, with appropriate file structure. See that file for details.
        self.pwd = pwd
        self.gpo_walked = list(os.walk(self.pwd + self.slash + 'scraped_hearings'))
        self.results = []

    def parse_gpo_hearings(self):
        """ Primary parser function. Wraps and parallelizes methods described elsewhere in this file. """
        from itertools import chain
        import pprocess
        import json
        import csv
        import re

        def parse(in_path):
            """

            Wrapper for parser function, intended for parallel processing. Opens metadata and HTML file objects for a
            given hearing, parses them, and saves them.

            """

            with open(in_path, 'rb') as file_object:
                content = file_object.read()
            json_path = re.sub('html', 'json', in_path)

            with open(json_path, 'rb') as file_object:
                hearing_table = json.loads(file_object.read())

            out = ParseHearing(content, member_table, hearing_table, committee_table).parsed

            # Write the output. Returned value records whether the file was actually parsed.
            if out is not None:
                csv_path = re.sub('html', 'csv', in_path)
                with open(csv_path, 'wb') as file_object:
                    csv.writer(file_object).writerows(out)
                return 1
            else:
                return 0

        # Update metadata tables
        self._update_tables()

        gpo_file_list = list(chain(*[[direc[0] + self.slash + fi for fi in direc[2] if 'htm' in fi]
                             for direc in self.gpo_walked]))

        # Get data from metadata tables. Member data is drawn from the member_table.json file, which is created and
        # updated with the _update_tables() method and its subfunctions. Committee data is drawn from the
        # committee_data.csv file, which is a hand-created mapping between Stewart's committee codes and committee names
        # as saved in the dataset.
        with open(self.pwd + self.slash + 'member_table.json', 'rb') as f:
            member_table = json.loads(f.read())

        with open(self.pwd + self.slash + 'committee_data.csv', 'rb') as f:
            committee_table = {row[0]: {'Code': row[1], 'Chamber': row[2]} for row in csv.reader(f)}

        self.results = [r for r in pprocess.pmap(parse, gpo_file_list, limit=5)]

    def _update_tables(self):
        """ Wrapper function for creating and updating metadata tables. See functions for details. """
        self._update_gpo_tables()
        self._update_member_table()
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
        sudoc_table = self._get_current_data(self.pwd + self.slash + 'sudoc_table.json')
        jacket_table = self._get_current_data(self.pwd + self.slash + 'jacket_table.json')

        gpo_file_list = list(chain(*[[direc[0] + self.slash + fi for fi in direc[2] if 'json' in fi]
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

        with open(self.pwd + self.slash + 'sudoc_table.json', 'wb') as f:
            f.write(json.dumps(sudoc_table))

        with open(self.pwd + self.slash + 'jacket_table.json', 'wb') as f:
            f.write(json.dumps(jacket_table))

    def _update_member_table(self):
        """

        Generate the member table. The member table lists party seniority, majority status, leadership,
        committee membership, congress, and state. All member data are drawn from Stewart's committee assignments data,
        which are available at the link below.

        http://web.mit.edu/17.251/www/data_page.html

        """
        import json
        import csv

        def update(inputs, table, chamber):
            """

            Helper function, which updates a given member table with metadata from Stewart's metadata. Given data from
            a csv object, the function interprets that file and adds the data to a json output. See Stewart's data and
            codebook for descriptions of the variables.

            """

            for row in inputs:
                name = str(row[3].lower().decode('ascii', errors='ignore')).translate(None,
                                                                                      '!"#$%&\'()*+-./:;<=>?[\\]_`{|}~')
                congress = row[0].lower()
                committee_code = row[1]
                member_id = row[2]
                majority = row[4].lower()
                party_seniority = row[5].lower()
                leadership = row[9]
                state = row[18]

                if row[6] == '100':
                    party = 'D'
                elif row[6] == '200':
                    party = 'R'
                else:
                    party = 'I'

                entry = {'Party Seniority': party_seniority, 'Majority': majority, 'Leadership': leadership}

                if committee_code != '':
                    if name in table:
                        if congress in table[name]:
                            table[name][congress][committee_code] = entry

                            if chamber != table[name][congress]['Chamber']:
                                table[name][congress]['Chamber'] = 'JOINT'

                        else:
                            table[name][congress] = {committee_code: entry, 'Chamber': chamber}
                    else:
                        table[name] = {congress: {committee_code: entry, 'Chamber': chamber}}

                    table[name]['Party'] = party
                    table[name]['State'] = state
                    table[name]['ID'] = member_id

                    if 'Chamber' not in table[name]:
                        table[name]['Chamber'] = [chamber, 'JOINT']
                    else:
                        table[name]['Chamber'].append(chamber)
                        table[name]['Chamber'] = list(set(table[name]['Chamber']))

        member_table = {}

        # Loop through the house and senate assignment files, and save the output.
        with open(self.pwd + self.slash + 'house_assignments.csv', 'rb') as f:
            house_inputs = list(csv.reader(f))[2:]
        with open(self.pwd + self.slash + 'senate_assignments.csv', 'rb') as f:
            senate_inputs = list(csv.reader(f))[2:]

        update(house_inputs, member_table, 'HOUSE')
        update(senate_inputs, member_table, 'SENATE')

        with open(self.pwd + self.slash + 'member_table.json', 'wb') as f:
            f.write(json.dumps(member_table))

    def _manual_hearing_table_update(self):
        """

        Update the hearing table according to a manually-constructed file which matches CIS numbers to sudocs, jackets,
        and sudocs. See accompanying data files and documentation for details.

        """
        import csv
        import json

        with open(self.pwd + self.slash + 'manual_sudoc_table.csv') as f:
            manual_table = list(csv.reader(f))[1:]

        sudoc_table = self._get_current_data(self.pwd + self.slash + 'sudoc_table.json')
        jacket_table = self._get_current_data(self.pwd + self.slash + 'jacket_table.json')
        cis_number_table = self._get_current_data(self.pwd + self.slash + 'cis_number_table.json')

        for row in manual_table:
            cis_number = row[0]
            pap_code = row[1]
            sudoc_number = row[2]
            jacket = row[3]

            if '' not in [cis_number, sudoc_number, jacket]:
                jacket_table[jacket] = {'CIS': cis_number, 'sudoc': sudoc_number, 'PAP_Code': pap_code}
                sudoc_table[sudoc_number] = {'CIS': cis_number, 'jacket': jacket, 'PAP_Code': pap_code}
                cis_number_table[cis_number] = {'sudoc': sudoc_number, 'jacket': jacket, 'PAP_Code': pap_code}

        with open(self.pwd + self.slash + 'sudoc_table.json', 'wb') as f:
            f.write(json.dumps(sudoc_table))

        with open(self.pwd + self.slash + 'jacket_table.json', 'wb') as f:
            f.write(json.dumps(jacket_table))

        with open(self.pwd + self.slash + 'cis_number_table.json', 'wb') as f:
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
        file_list = list(chain(*[[direc[0] + self.slash + fi for fi in direc[2] if 'csv' in fi]
                                 for direc in self.gpo_walked]))

        with open(self.pwd + self.slash + 'committee_data.csv', 'rb') as f:
            committee_table = {row[0]: {'Code': row[1], 'Chamber': row[2]} for row in csv.reader(f)}

        today = datetime.now().strftime('%m%d%Y')

        if len(jackets_dates) > 0:
            jackets_to_parse = zip(*jackets_dates)[1]
        else:
            jackets_to_parse = ()

        with open(self.pwd + self.slash + 'cis_number_table.json', 'rb') as f:
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
        with open(self.pwd + self.slash + 'corpus_' + today + '.csv', 'wb') as f:
            csv.writer(f).writerows(corpus)
        with open(self.pwd + self.slash + 'corpus_index_' + today + '.csv', 'wb') as f:
            csv.writer(f).writerows(keep)

        corpora.Dictionary.save(dic, self.pwd + self.slash + out_name + '_' + today + '.lda-c.dic')
        corpora.BleiCorpus.serialize(fname=self.pwd + self.slash + out_name + '_' + today + '.lda-c',
                                     corpus=bow_list, id2word=dic)

    def update_tables_from_file(self, data_path):
        """

        Extra method to update sudoc/CIS/jacket indices from file, if desired. Assumed input for data_path is
        a flat (csv) file with three items per row: [cis, sudoc, jacket]. Optionally, a fourth row with pap_code
        can also be included.

        """

        import csv
        import json

        sudoc_table = self._get_current_data(self.pwd + self.slash + 'sudoc_table.json')
        jacket_table = self._get_current_data(self.pwd + self.slash + 'jacket_table.json')
        cis_number_table = self._get_current_data(self.pwd + self.slash + 'cis_number_table.json')

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

        with open(self.pwd + self.slash + 'sudoc_table.json', 'wb') as f:
            f.write(json.dumps(sudoc_table))

        with open(self.pwd + self.slash + 'jacket_table.json', 'wb') as f:
            f.write(json.dumps(jacket_table))

        with open(self.pwd + self.slash + 'cis_number_table.json', 'wb') as f:
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


class ParseHearing:
    def __init__(self, hearing_text, member_data, hearing_data, committee_data):
        """

        Class for parsing hearings. This class is not intended to be called directly; rather, it is only meant to be
        called in the parallelized parse() method of BuildDatabase. The parser breaks hearings into statements,
        links the speaker of each statement to metadata (wherever possible), and outputs a flat file, with one
        statement per line.

        """

        def clean_hearing(text):
            """ Preprocessing function whcih strips out transcript components that aren't part of the conversation. """

            import re
            if re.search('\[Questions for the record with answers supplied follow:\]', text) is not None:
                text = text[0:re.search('\[Questions for the record ' +
                                        'with answers supplied follow:\]',
                                        text).start()]
            return text

        self.member_data = member_data
        self.hearing_data = hearing_data
        self.committee_data = committee_data
        self.hearing_text = clean_hearing(hearing_text)

        # List of speaker prefixes. These are important for identifying the beginning and end of each statement.
        self.prefixes = ['Mr.', 'Mrs.', 'Ms.', 'Mr', 'Mrs', 'Ms', 'Chairman', 'Chairwoman', 'Dr.', 'Dr', 'Senator',
                         'Secretary', 'Director', 'Representative', 'Vice Chairman', 'Vice Chair', 'Admiral', 'General',
                         'Gen.', 'Judge', 'Commissioner', 'Lieutenant', 'Lt.', 'Trustee', 'Sergeant', 'Major', 'Colonel',
                         'Captain', 'Capt.', 'Commander', 'Specialist', 'Voice', 'The Chairman', 'The Chairwoman',
                         'Governor', 'Chair', 'The Clerk', 'Clerk', 'Mayor']

        # Constant for performance. Limits how far forward (number of characters) the script will search in order to
        # find certain pieces of information, such as the name of the chair of the committee.
        self.max_search_length = 75000
        self.delete_last = False

        meta_chamber = self.hearing_data['Hearing Info']['Chamber']

        # If there's at least one statement identified in the text, start parsing
        if self._name_search(self.hearing_text) is not None:

            # If a committee name is missing from the committee_data.csv file, output a warning and skip the file
            if any(meta_chamber + '-' + c not in self.committee_data
                   for c in self.hearing_data['Hearing Info']['Committee']) is True:

                print 'Warning! One of the following committees is missing from the committee data file: '
                for c in self.hearing_data['Hearing Info']['Committee']:
                    print c

                self.session_cutpoints = None
                self.statement_cutpoints = None
                self.parsed = None

                print '--------'

            else:
                self.session_cutpoints = self._find_sessions()
                self.statement_cutpoints = self._find_statements()
                self.parsed = self._create_output()
        else:
            self.session_cutpoints = None
            self.statement_cutpoints = None
            self.parsed = None

    def _name_search(self, string):
        """ Helper function, which sorts through the hearing text and finds all names that start statements. """
        import re

        # VERY complicated name regex, which is tough to simplify, since names aren't consistent. Modify with care.
        matches = re.finditer('(?<=    )[A-Z][a-z]+(\.)? ([A-Z][A-Za-z\'][-A-Za-z \[\]\']*?)*' +
                              '[A-Z\[\]][-A-Za-z\[\]]{1,100}(?=\.( |-))' +
                              '|(?<=    )Voice(?=\.( |-))' +
                              '|(?<=    )The Chair(man|woman)(?=\.( |-))', string[0:self.max_search_length])
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

        o = list(re.finditer('The (Committee|Subcommittee)s? met', self.hearing_text, flags=re.I))
        if len(o) > 0 and o[0] is not None:
            openings = [regex.start() for regex in o]
        else:
            openings = [self._name_search(self.hearing_text).start()-10]
        c = list(re.finditer('([\[\(]?Whereupon[^\r\n]*?)?the\s+(Committee|Subcommittee|hearing|forum|panel)s?.*?' +
                             '(was|were)?\s+(adjourned|recessed)[\r\n]*?[\]\)]?', self.hearing_text, flags=re.I))
        if len(c) > 0:
            closings = [regex.start() for regex in c]
            self.delete_last = False
        else:
            closings = [len(self.hearing_text)]
            self.delete_last = True

        if len(closings) < len(openings):
            closings += openings[len(closings)+1:]
            closings += [len(self.hearing_text)]
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
            newlines = list(re.finditer('\n+    ', self.hearing_text[opening:closing]))
            for i, nl in enumerate(newlines):
                if i < len(newlines)-1:
                    line = self.hearing_text[nl.start() + opening:newlines[i+1].start() + opening]
                else:
                    line = self.hearing_text[nl.start() + opening:closing]

                s = self._name_search(line)

                # offset to get the indexing right
                offset = nl.start() + opening

                if s is not None:
                    cutpoints.append([s.start() + offset, s.end() + offset])

            cutpoints.append([closing])

        return cutpoints

    def _create_output(self):
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
            s = re.search('([\[\(].*?[\r\n]*.*?(prepared|opening)\s+statement.*?[\r\n]*.*?[\]\)]|' +
                          '[\[\(].*?[\r\n]*.*?following.*?(was|were).*?[\r\n]*.*?[\r\n]*.*?[\]\)]|' +
                          '[\[\(].*?[\r\n]*.*?follows?[\.:].*?[\r\n]*[^<]*?[\]\)])' +
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
            results = re.finditer('    (Members |Also )?(present[^\.]*?:)([^\.]+)',
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
                  u'indiana', u'iowa', u'kansas', u'kentucky', u'louisiana', u'maine', u'maryland', u'massachusetts',
                  u'michigan', u'minnesota', u'mississippi', u'missouri', u'montana', u'nebraska', u'nevada',
                  u'new hampshire', u'new jersey', u'new mexico', u'new york', u'north carolina', u'north dakota',
                  u'ohio', u'oklahoma', u'oregon', u'pennsylvania', u'rhode island', u'south carolina', u'south dakota',
                  u'tennessee', u'texas', u'utah', u'vermont', u'virginia', u'washington', u'west virginia',
                  u'wisconsin', u'wyoming']

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
                state_matches = [s for s in states_long if s in name.lower()]
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
                member_table_matches = [n for n in self.member_data if str(name_last).lower() ==
                                        str(n.split(',')[0]).lower()
                                        and congress in self.member_data[n]
                                        and any([c in self.member_data[n][congress]
                                                 for c in committees]) is True]

                # If the state is specificed in the transcript, do some additional matching
                if state is not None:
                    abbrev = states_abbrev[states_long.index(state)]
                    member_table_matches = [m for m in member_table_matches if self.member_data[m]['State'] == abbrev]

                # Same process for witnesses
                witness_name_matches = [n for n in self.hearing_data['Witness Info'] if
                                        name_last.lower() in str(n).lower().translate(None, punctuation)]

                # Same process for "guest" members who happen to be present at that hearing, matching on Congress and
                # list of members in the present_members list
                guest_matches = [n for n in self.member_data if str(name_last).lower() in str(n).lower()
                                 and hearing_chamber in self.member_data[n]['Chamber']
                                 and congress in self.member_data[n]
                                 and present_members is not None
                                 and n.split(',')[0].lower() in present_members.lower()]

                # If there's a unique match on the member name, take that as the match
                if len(member_table_matches) == 1:
                    name_full = member_table_matches[0]
                    party = self.member_data[name_full]['Party']
                    member_id = self.member_data[name_full]['ID']
                    current_committees = [c for c in committees if c in self.member_data[name_full][congress]]
                    person_chamber = self.member_data[name_full][congress]['Chamber']

                    if len(current_committees) == 1:
                        c = current_committees[0]
                        majority = self.member_data[name_full][congress][c]['Majority']
                        party_seniority = self.member_data[name_full][congress][c]['Party Seniority']
                        leadership = self.member_data[name_full][congress][c]['Leadership']

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
                    member_id = self.member_data[guest_matches[0]]['ID']
                    party = self.member_data[guest_matches[0]]['Party']
                    person_chamber = self.member_data[guest_matches[0]][congress]['Chamber']
                    majority = 'NA'
                    party_seniority = 'NA'
                    leadership = 'NA'

                # if all else fails, check the member data table and see if there's a member of Congress with a matching
                # name who served on the given committee in the given Congress - if so, take that as a match
                else:
                    rep_list = [n for n in self.member_data if str(name_last).lower() ==
                                str(n.split(',')[0]).lower()
                                and congress in self.member_data[n]]

                    if len(rep_list) == 1:
                        try:
                            name_line = re.search('.* ' + name_last + '[ ,.].*|^' + name_last + '[ ,.].*',
                                              self.hearing_text[:self.statement_cutpoints[0][0]],
                                              flags=re.I|re.M).group(0).strip()
                            first_word = re.search('^[^\s]*', name_line).group(0)
                            if first_word in ['Representative ', 'Senator '] or 'Representative in Congress' in \
                                    name_line or 'U.S. Senator' in name_line:
                                name_full = rep_list[0]
                                member_id = self.member_data[name_full]['ID']
                                party = self.member_data[name_full]['Party']
                                current_committees = [c for c in committees
                                                      if c in self.member_data[name_full][congress]]
                                person_chamber = self.member_data[name_full][congress]['Chamber']

                                if len(current_committees) == 1:
                                    c = current_committees[0]
                                    majority = self.member_data[name_full][congress][c]['Majority']
                                    party_seniority = self.member_data[name_full][congress][c]['Party Seniority']
                                    leadership = self.member_data[name_full][congress][c]['Leadership']
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


import csv
from datetime import datetime

start = datetime.now()
print start

with open('/home/rbshaffer/Desktop/Financial_Replication/Hearing_Data/manual_sudoc_table.csv', 'rb') as f:
    manual_sudoc_table = list(csv.reader(f))
    jackets_dates = zip(zip(*manual_sudoc_table)[0], zip(*manual_sudoc_table)[3])

with open('/home/rbshaffer/Desktop/Financial_Replication/Hearing_Data/dates and hearing types.csv', 'rb') as f:
    types_dates = list(csv.reader(f))
    types_dates = [[row[0]] + row[2:5] for row in types_dates]


build = BuildDatabase('/home/rbshaffer/Desktop/Financial_Replication/Hearing_Data')
# build.parse_gpo_hearings()
# results = build.results
# build.create_dataset(jackets_dates, types_dates)
build.create_dataset(committee_list=('196', '156', '251'),
                     out_name='comparing_committees',
                     min_words=50)
print 'dataset created'

print datetime.now() - start