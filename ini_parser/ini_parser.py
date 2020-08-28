import json
import configparser

from models import DatabaseCfg, ExportCfg, WorkspaceCfg, TransformCfg


class IniConfig():
    def __init__(self, config):
        self.database = DatabaseCfg(config)
        self.workspace = WorkspaceCfg(config)
        self.export = ExportCfg(config)
        self.transform = TransformCfg(config)

def parse(fn):
    config = configparser.ConfigParser()
    config.read(fn)
    ini = IniConfig(config)
    return ini

parse('ini_parser/etl_example.cfg')