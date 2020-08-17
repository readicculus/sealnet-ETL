import os

from import_project.imports.import_models import StandardCSVRow
import pandas as pd

class KotzCSVRow(StandardCSVRow):
    def preprocess(self):
        species_map = {'unknown_seal': 'UNK Seal',
                       'unknown_pup': 'UNK Seal',
                       'ringed_seal': 'Ringed Seal',
                       'ringed_pup': 'Ringed Seal',
                       'bearded_seal': 'Bearded Seal',
                       'bearded_pup': 'Bearded Seal',
                       'animal': 'animal',
                       'Ringed Seal': 'Ringed Seal',
                       'Bearded Seal': 'Bearded Seal',
                       'Polar Bear': 'Polar Bear',
                       'incorrect': 'falsepositive'}
        is_pup = not pd.isnull(self.species) and 'pup' in self.species
        self.age_class = "Pup" if is_pup else "Adult"
        self.species = "UNK" if pd.isnull(self.species) else species_map[self.species]
        if self.species == 'falsepositive':
            self.age_class = None
        if not pd.isna(self.image_eo):
            self.image_eo = os.path.basename(self.image_eo)
        if not pd.isna(self.image_ir):
            self.image_ir = os.path.basename(self.image_ir)
