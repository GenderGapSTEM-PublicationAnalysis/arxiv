import json
import urllib.request as req

import xmltodict


class ArxivXML(object):
    def __init__(self):
        self.json_data = None
        self.records = None
        self.metadata = None
        self.resumption_token = None
        self.missing_metadata = None

    def load_data_from_api(self, url, xml_attribs=False):
        xml_file = req.urlopen(url).read()
        self.json_data = xmltodict.parse(xml_file, xml_attribs=xml_attribs)

    def extract_resumption_token(self):
        try:
            self.resumption_token = self.json_data["OAI-PMH"]["ListRecords"]["resumptionToken"]
        except (TypeError, KeyError):
            print("collected data is complete")

    def extract_records(self):
        try:
            self.records = self.json_data["OAI-PMH"]["ListRecords"]["record"]
            # if the XML contains only 1 record then self.records is an OrderedDict and not a list
            # compare e.g. 'from=2017-09-11&until=2017-09-11' with 'from=2017-09-10&until=2017-09-10'
            if not isinstance(self.records, list):
                self.records = [self.records]
        except (TypeError, KeyError):
            try:
                print("The request resulted in the following error: ", self.json_data["OAI-PMH"]["error"])
            except:
                print("The response cannot be processed. Error unknown.")

    def extract_flat_metadata(self):
        """Store records with no metadata section in separate files since these
        correspond to deleted records, and those need to be handled separately when importing into a database.
        """
        metadata = []
        missing_metadata = []
        if self.records is not None:
            for r in self.records:
                try:
                    item = r["metadata"]["arXiv"]
                    item["authors"] = item["authors"]["author"]
                    item.update(r["header"])
                    metadata.append(item)
                except KeyError:
                    missing_metadata.append(r["header"])
        self.metadata = metadata
        self.missing_metadata = missing_metadata

    def process_xml(self, url):
        self.load_data_from_api(url)
        self.extract_records()
        self.extract_flat_metadata()
        self.extract_resumption_token()

    @staticmethod
    def dump_as_json(attribute, file_name):
        with open(file_name, 'w') as fp:
            json.dump(attribute, fp)
