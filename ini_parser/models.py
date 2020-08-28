import json


class INIBlock(object):
    def __init__(self, config):
        self._config = config
        self.block_name = "DEFAULT"

    def get_multi_attr(self):
        names = []
        keys = self._config.sections()
        for k in keys:
            if k == self.block_name:
                continue
            parts = k.split('.')
            if self.block_name == parts[0]:
                names.append(k)
        return names

    def get_child(self, child_name):
        children = self.get_multi_attr()
        for child in children:
            split = child.split('.')
            if split[1] == child_name:
                return child
        return None

    def has_attr(self, attr):
        return self._config.has_option(self.block_name, attr)

    def get_str(self, attr):
        if not self.has_attr(attr): return None
        return self._config.get(self.block_name, attr).strip()

    def get_int(self, attr):
        if not self.has_attr(attr): return None
        return self._config.getint(self.block_name, attr)

    def get_arr(self, attr):
        if not self.has_attr(attr): return None
        v = self.get_str(attr)
        if v == '': return []
        return [s.strip() for s in v.split(',')]

    def get_json(self, attr):
        if not self.has_attr(attr): return None
        json.loads(self.get_str(attr))

class DatabaseCfg(INIBlock):
    def __init__(self, config):
        super(self.__class__, self).__init__(config)
        self.block_name = "database"
        self.user = self.get_str("user")
        self.password = self.get_str("password")
        self.host = self.get_str("host")
        self.name = self.get_str("name")
        self.port = self.get_int("port")
        #
        # if self.password == '':
        #     self.password = input("Enter password for %s: " % self.host)


class WorkspaceCfg(INIBlock):
    def __init__(self, config):
        super(self.__class__, self).__init__(config)
        self.block_name = "workspace"
        self.directory = self.get_str("directory")

# Export
class QueryFilterCfg(INIBlock):
    def __init__(self, config, block_name):
        super(self.__class__, self).__init__(config)
        self.block_name = block_name
        self.surveys = self.get_arr("surveys")
        self.flights = self.get_arr("flights")
        self.cameras = self.get_arr("cameras")
        self.reviewers = self.get_arr("reviewers")
        self.species = self.get_arr("species")
        self.species_mappings = self.get_json("species_mappings")

class ExportCfg(INIBlock):
    def __init__(self, config):
        super(self.__class__, self).__init__(config)
        self.block_name = "export"
        self.type = self.get_str("type")
        assert (self.type == "eo" or self.type == "ir" or self.type == "fused")

        # query filter
        qf_name = self.get_child('animal_filter')
        self.query_filter = QueryFilterCfg(config, qf_name)

# Transform
class ChipCfg(INIBlock):
    def __init__(self, config, block_name):
        super(self.__class__, self).__init__(config)
        self.block_name = block_name
        size = self.get_int("size")
        stride = self.get_int("stride")
        if size:
            self.width = size
            self.height = size
        else:
            self.width = self.get_int("width")
            self.height = self.get_int("height")

        if stride:
            self.stride_x = stride
            self.stride_y = stride
        else:
            self.stride_x = self.get_int("stride_x")
            self.stride_y = self.get_int("stride_y")

class TransformCfg(INIBlock):
    def __init__(self, config):
        super(self.__class__, self).__init__(config)
        self.block_name = "transform"

        # chip
        chip_name = self.get_child('chip')
        self.chip = ChipCfg(config, chip_name)