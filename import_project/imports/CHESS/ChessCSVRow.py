import os

from import_project.imports.import_models import StandardCSVRow
import pandas as pd
def is_number(n):
    try:
        float(n)   # Type-casting the string to `float`.
                   # If string is not a valid `float`,
                   # it'll raise `ValueError` exception
    except ValueError:
        return False
    return True

class ChessCSVRow(StandardCSVRow):
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
        # self.species = "UNK" if pd.isnull(self.species) else species_map[self.species]

        if is_number(self.confidence_eo):
            self.confidence_eo = float(self.confidence_eo)
        elif self.confidence_eo == "No":
            self.confidence_eo = 0
        elif self.confidence_eo == "Likely":
            self.confidence_eo = .9
        elif self.confidence_eo == "Guess":
            self.confidence_eo = .6
        elif self.confidence_eo is not None:
            self.confidence_eo = int(self.confidence_eo.replace("%", ""))/100.0

        if self.image_eo: self.image_eo = os.path.basename(self.image_eo)
        if self.image_ir: self.image_ir = os.path.basename(self.image_ir)
