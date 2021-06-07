from loguru import logger


class FileWriter:
    def __init__(self, serialized_structure):
        self.serialized_structure = serialized_structure

    def to_file(self):
        raise NotImplementedError


class JSONWriter(FileWriter):

    def __init__(self, serialized_structure):
        super().__init__(serialized_structure)

    @logger.catch
    def to_file(self, file_name):
        """Write information into json.

        :return: None
        """
        with open(file_name + '.json', 'w') as xml_file:
            xml_file.write(self.serialized_structure)

        logger.info('JSON structure was recorded.')


class XMLWriter(FileWriter):
    def __init__(self, serialized_structure):
        super().__init__(serialized_structure)

    @logger.catch
    def to_file(self, file_name):
        """Write information into XML"""
        with open(file_name + '.xml', 'w') as xml_file:
            xml_file.write(self.serialized_structure)

        logger.info('XML structure was recorded.')
