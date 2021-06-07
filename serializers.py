import json
from xml.dom.minidom import parseString

from dicttoxml import dicttoxml
from loguru import logger


class Serializer:
    """Base class, that implements serialization Python object"""

    def __init__(self, json_file):
        self.json_file = json_file

    def to_format(self):
        raise NotImplementedError


class XMLSerializer(Serializer):
    """Class that implements serialization to XML."""

    def __init__(self, json_file):
        super().__init__(json_file)

    @logger.catch
    def to_format(self):
        """Converts a lists of dicts to XML string.

        :return: XML in string representation
        """

        xml_string = dicttoxml(self.json_file,
                               custom_root='students_in_room',
                               # dict_to_xml(item_func) need function,
                               # that generate the element name for
                               # items in a list. Default is 'item'.
                               item_func=lambda source_name: 'field',
                               attr_type=False)

        xml = parseString(xml_string).toprettyxml()

        logger.info('Data was serialized into XML format.')

        return xml


class JSONSerializer(Serializer):
    """Class that implements serialization to XML."""

    def __init__(self, json_file):
        super().__init__(json_file)

    @logger.catch
    def to_format(self):
        """Converts a list of dicts to JSON string.

        :return: JSON in string representation.
        """

        json_string = json.dumps(self.json_file, indent=2)

        logger.info('Data was serialized into JSON format.')

        return json_string
