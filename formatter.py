import re
import timex
import datetime
import utilities
from dateutil import parser
from collections import Counter


def format_content(raw_content):
    """
    Function to process a given news story for further formatting. Calls
    a function that extract the story text minus the date and source line. Also
    splits the sentences using the ``sentence_segmenter()`` function.

    Parameters
    ----------

    raw_content: String.
                    Content of a news story as pulled from the web scraping
                    database.

    Returns
    -------

    sent_list: List.
                List of sentences.

    """
    content = _get_story(raw_content)
    split = sentence_segmenter(content)
    return split


def _get_story(story_all):
    """
    Function to extract story text without date and source line.

    Parameters
    ----------

    story_all: String.
                Content of a news story as pulled from the web scraping
                database.

    Returns
    -------

    story: String.
            Content of story with header/frontmatter removed.

    """

    if '(Reuters)' in story_all:
        story = story_all[story_all.find('(Reuters)') + 12:]
    elif '(IANS)' in story_all:
        story = story_all[story_all.find('(IANS)') + 7:]
    elif '(ANI)' in story_all:
        story = story_all[story_all.find('(ANI)') + 7:]
    elif '(Xinhua) -- ' in story_all:
        story = story_all[story_all.find('(Xinhua) -- ') + 12:]
    elif '(UPI) -- ' in story_all:
        story = story_all[story_all.find('(UPI) -- ') + 9:]
    if bool(re.search("\xe2\x80\x93", story_all[0:32])):
        try:
            story = story_all.split("\xe2\x80\x93 ", 1)[1]
        except IndexError:
            story = story_all
    else:
        story = story_all

    return story


def get_date(result_entry, process_date):
    """
    Function to extract date from a story. First checks for a date from the RSS
    feed itself. Then tries to pull a date from the first two sentences of a
    story. Finally turns to the date that the story was added to the database.
    For the dates pulled from the story, the function checks whether the
    difference is greater than one day from the date that the pipeline is
    parsing.

    Parameters
    ----------

    result_entry: Dictionary.
                    Record of a single result from the web scraper.

    process_date: datetime object.
                    Datetime object indicating which date the pipeline is
                    processing. Standard is date_running - 1 day.


    Returns
    -------

    date : String.
            Date string in the form YYMMDD.

    """
    date_obj = ''
    if result_entry['date']:
        try:
            date_obj = parser.parse(result_entry['date'])
        except TypeError:
            date_obj = ''
    else:
        date_obj = ''

    if not date_obj:
        tagged = timex.tag(result_entry['content'][:2])
        dates = re.findall(r'<TIMEX2>(.*?)</TIMEX2>', tagged)
        if dates:
            try:
                date_obj = parser.parse(dates[0])
                diff_check = _check_date(date_obj, process_date)
                if diff_check:
                    date_obj = ''
            except TypeError:
                date_obj = ''
        else:
            date_obj = ''

    if not date_obj:
        date_obj = result_entry['date_added']

    date = '{}{:02d}{:02d}'.format(str(date_obj.year)[2:], date_obj.month,
                                   date_obj.day)

    return date


def _check_date(date_object, process_date):
    """
    Function to check the gap between the parsed date and the actual date that
    the pipeline is processing.

    Parameters
    ----------

    date_object: datetime object.
                    Date that the _get_date function suggests as a candidate
                    date.

    process_date: datetime object.
                    Datetime object indicating which date the pipeline is
                    processing. Standard is date_running - 1 day.

    Returns
    -------

    too_big: Boolean.
                Whether the gap is one day or larger.

    """
    diff = date_object - process_date
    too_big = diff > datetime.timedelta(days=0)

    return too_big


def sentence_segmenter(paragr):
    """
    Function to break a string 'paragraph' into a list of sentences based on
    the following rules:

    1. Look for terminal [.,?,!] followed by a space and [A-Z]
    2. If ., check against abbreviation list ABBREV_LIST: Get the string
    between the . and the previous blank, lower-case it, and see if it is in
    the list. Also check for single-letter initials. If true, continue search
    for terminal punctuation
    3. Extend selection to balance (...) and "...". Reapply termination rules
    4. Add to sentlist if the length of the string is between MIN_SENTLENGTH
    and MAX_SENTLENGTH
    5. Returns sentlist

    Parameters
    ----------

    paragr: String.
            Content that will be split into constituent sentences.

    Returns
    -------

    sentlist: List.
                List of sentences.

    """
    # this is relatively high because we are only looking for sentences that
    # will have subject and object
    MIN_SENTLENGTH = 100
    MAX_SENTLENGTH = 512

    # sentence termination pattern used in sentence_segmenter(paragr)
    terpat = re.compile('[\.\?!]\s+[A-Z\"]')

    # source: LbjNerTagger1.11.release/Data/KnownLists/known_title.lst from
    # University of Illinois with editing
    ABBREV_LIST = ['mrs.', 'ms.', 'mr.', 'dr.', 'gov.', 'sr.', 'rev.', 'r.n.',
                   'pres.', 'treas.', 'sect.', 'maj.', 'ph.d.', 'ed. psy.',
                   'proc.', 'fr.', 'asst.', 'p.f.c.', 'prof.', 'admr.',
                   'engr.', 'mgr.', 'supt.', 'admin.', 'assoc.', 'voc.',
                   'hon.', 'm.d.', 'dpty.',  'sec.', 'capt.', 'c.e.o.',
                   'c.f.o.', 'c.i.o.', 'c.o.o.', 'c.p.a.', 'c.n.a.', 'acct.',
                   'llc.', 'inc.', 'dir.', 'esq.', 'lt.', 'd.d.', 'ed.',
                   'revd.', 'psy.d.', 'v.p.',  'senr.', 'gen.', 'prov.',
                   'cmdr.', 'sgt.', 'sen.', 'col.', 'lieut.', 'cpl.', 'pfc.',
                   'k.p.h.', 'cent.', 'deg.', 'doz.', 'Fahr.', 'Cel.', 'F.',
                   'C.', 'K.', 'ft.', 'fur.',  'gal.', 'gr.', 'in.', 'kg.',
                   'km.', 'kw.', 'l.', 'lat.', 'lb.', 'lb per sq in.', 'long.',
                   'mg.', 'mm.,, m.p.g.', 'm.p.h.', 'cc.', 'qr.', 'qt.', 'sq.',
                   't.', 'vol.',  'w.', 'wt.']

    sentlist = []
    # controls skipping over non-terminal conditions
    searchstart = 0
    terloc = terpat.search(paragr)
    while terloc:
        isok = True
        if paragr[terloc.start()] == '.':
            if (paragr[terloc.start() - 1].isupper() and
                    paragr[terloc.start() - 2] == ' '):
                        isok = False      # single initials
            else:
                # check abbreviations
                loc = paragr.rfind(' ', 0, terloc.start() - 1)
                if loc > 0:
                    if paragr[loc + 1:terloc.start() + 1].lower() in ABBREV_LIST:
                        isok = False
        if paragr[:terloc.start()].count('(') != paragr[:terloc.start()].count(')'):
            isok = False
        if paragr[:terloc.start()].count('"') % 2 != 0:
            isok = False
        if isok:
            if (len(paragr[:terloc.start()]) > MIN_SENTLENGTH and
                    len(paragr[:terloc.start()]) < MAX_SENTLENGTH):
                sentlist.append(paragr[:terloc.start() + 2])
            paragr = paragr[terloc.end() - 1:]
            searchstart = 0
        else:
            searchstart = terloc.start() + 2

        terloc = terpat.search(paragr, searchstart)

    # add final sentence
    if (len(paragr) > MIN_SENTLENGTH and len(paragr) < MAX_SENTLENGTH):
        sentlist.append(paragr)

    return sentlist


def main(results, file_details, process_date, thisday):
    """
    Main function to parse results from the web scraper to TABARI-formatted
    output.

    Parameters
    ----------

    results: pymongo.cursor.Cursor. Iterable.
                Iterable containing the results from the scraper.

    file_details: NamedTuple.
                    Container generated from the config file specifying file
                    stems and other relevant options.

    process_date: String.
                    Date for which the pipeline is running. Usually
                    current_date - 1.

    this_date: String.
                The current date the pipeline is running.

    Returns
    -------

    new_results: List.
                    List of dictionaries that contain the MongoDB records with
                    new, formatted content.
    """

    sourcecount = Counter()

    new_results = []
    for i, story in enumerate(list(results)):
        content = story['content'].encode('utf-8')
        formatted_content = format_content(content)
        story['content'] = ' '.join([sent for sent in formatted_content if
                                     sent[0] != '"'])

        story['date'] = get_date(story, process_date)

        source = story['source']
        sourcecount[source] += 1

        new_results.append(story)

    for source, count in sourcecount.iteritems():
        source_counts_string += '{}\t{}'.format(source, count)

#    with open(newsourcefile, 'w') as sauce:
#        sauce.write(source_counts_string)

    return new_results