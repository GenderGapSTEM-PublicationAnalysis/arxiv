import re
import urllib.request as req
import xml.etree.ElementTree as ET
from urllib.parse import urlencode
from config import *
import xmltodict
import json

"""General XML methods"""


def fetch_xml_root(url):
    data = req.urlopen(url).read()
    root = ET.fromstring(data)
    return root


def fetch_xml_tree(u):
    d = req.urlopen(u)
    tree = ET()
    tree.parse(d)
    tree.getroot()
    return tree


def convert_to_dict(url, xml_attribs=False):
    """Fetch XML data from a URL and convert it to a dictionary"""
    xml_file = req.urlopen(url).read()
    return xmltodict.parse(xml_file, xml_attribs=xml_attribs)


def dict_to_json(d):
    return json.dumps(d, indent=4)


"""Specific arXiv XML methods"""


def fetch_sets():
    r = fetch_xml_root(BASE_URL + '?verb=ListSets')
    sets = []
    for s in r.iter(TAG_SET_SPEC):
        sets.append(s.text)
    return sets


def extract_resumption_token(r):
    try:
        return list(r.iter(TAG_RESUMPTION_TOKEN))[0].text
    except:
        return None


def extract_reponse_date(r):
    return list(r.iter(BASE_TAG_OAI + 'responseDate'))[0].text


def count_articles(r):
    return len(list(r.iter(TAG_ID)))


def count_affiliations(r):
    return len(list(r.iter(TAG_AFFILIATION)))


def count_initials_and_full_names(r):
    n_initials = 0
    n_full_names = 0
    """regex to recognize initials, defined as names starting with 1 or 2 letters followed by a dot"""
    initial = re.compile('^[\w]{1,2}\.')

    for forename in r.iter(TAG_FORENAMES):
        if initial.match(forename.text):
            n_initials += 1
        else:
            n_full_names += 1

    return n_initials, n_full_names


def build_url(s, y, t=None):
    base_url = 'http://arXiv.org/oai2'
    query_string_params = {
        'verb': 'ListRecords',
        'from': str(y) + '-01-01',
        'until': str(y + 1) + '-01-01',
        'metadataPrefix': 'arXiv'
    }

    if t is None:
        return base_url + '?' + urlencode(query_string_params) + '&set=' + s
    else:
        return base_url + '?verb=ListRecords&resumptionToken=' + t
