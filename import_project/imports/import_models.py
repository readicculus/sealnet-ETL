import abc
import logging
import os
from datetime import datetime
import pandas as pd
from sqlalchemy.exc import IntegrityError

from noaadb.schema.models import Species, EOLabelEntry, IRLabelEntry, EOImage, EOIRLabelPair, IRImage

class StandardFlightDataset(object):
    @abc.abstractmethod
    def get_cam_transform(self, cam_name): return

    @abc.abstractmethod
    def get_cam_eo_detections_file(self, cam_name): return

    @abc.abstractmethod
    def get_cam_ir_detections_file(self, cam_name): return

    @abc.abstractmethod
    def get_cam_eo_detections(self, cam_name): return

    @abc.abstractmethod
    def get_cam_ir_detections(self, cam_name): return

    @abc.abstractmethod
    def get_eo_ir_merged_detections(self, cam_name): return

    @abc.abstractmethod
    def id(self): return

    @abc.abstractmethod
    def get_cam_names(self): return

    @abc.abstractmethod
    def get_cam_id(self, cam_name): return

    @abc.abstractmethod
    def get_cam_meta_files(self, cam_name): return

    @abc.abstractmethod
    def get_cam_ir_images(self, cam_name): return

    @abc.abstractmethod
    def get_cam_eo_images(self, cam_name): return

    @abc.abstractmethod
    def get_cam_eo_ir_meta_matches(self, cam_name): return


class StandardCSVRow(object):
    def __init__(self, species, image_eo, image_ir, x1_eo, x2_eo, y1_eo, y2_eo, x1_ir, x2_ir, y1_ir, y2_ir, confidence_eo, confidence_ir, hs_id=None):
        self.species = species
        self.image_eo = image_eo
        self.image_ir = image_ir
        self.x1_eo = x1_eo
        self.x2_eo = x2_eo
        self.y1_eo = y1_eo
        self.y2_eo = y2_eo
        self.x1_ir = x1_ir
        self.x2_ir = x2_ir
        self.y1_ir = y1_ir
        self.y2_ir = y2_ir
        self.confidence_eo = confidence_eo
        self.confidence_ir = confidence_ir
        self.age_class = None
        self.hs_id = hs_id
        self.preprocess()


    @abc.abstractmethod
    def preprocess(self):
        """ Implement preprocessing of the standard csv row data """
        return

    def record(self, s, eo_worker, ir_worker, job):
        """ record to database """
        self.insert_species(s)

        im_eo = s.query(EOImage).filter_by(file_name=self.image_eo).first()
        label_entry_eo, label_entry_ir = None, None

        eo_added = False
        ir_added = False

        # Add EO Image
        if im_eo is None:
            eo_added = False
            raise Exception("ERROR %s" % self.image_eo)
        else:
            label_entry_eo, eo_added = self.insert_eo_label(s, eo_worker, job)

        # Add IR Image
        if pd.isna(self.image_ir):  # no ir match for this image
            ir_added = False
        else:
            im_ir = s.query(IRImage).filter_by(file_name=self.image_ir).first()
            if im_ir is None:
                print("ERROR %s" % self.image_ir)
            else:
                label_entry_ir, ir_added = self.insert_ir_label(s, ir_worker, job)

        label_pair = EOIRLabelPair(
            eo_label=label_entry_eo,
            ir_label=label_entry_ir
        )
        s.add(label_pair)
        try:
            s.flush()
            logging.info(
                "SUCCESS: added sighting eo:%s ir:%s" % (label_entry_eo is not None, label_entry_ir is not None))
        except IntegrityError as e:
            logging.info(
                "FAILED: added sighting eo:%s ir:%s" % (label_entry_eo is not None, label_entry_ir is not None))
            logging.error(e)
            print(e)
            s.rollback()


        return eo_added, ir_added

    def query_species(self, session):
        return session.query(Species).filter_by(name=self.species).first()

    def insert_species(self, s):
        sp = self.query_species(s)
        if not sp:
            sp = Species(name=self.species)
            s.add(sp)
            try:
                s.flush()
            except:
                s.rollback()
                sp = self.query_species(s)
        return sp

    def get_existing_eo_label(self, session, label):
        return session.query(EOLabelEntry).filter_by(image_id=label.image_id,
                                                     x1=label.x1,
                                                     x2=label.x2,
                                                     y1=label.y1,
                                                     y2=label.y2).first()

    def get_existing_ir_label(self, session, label):
        return session.query(IRLabelEntry).filter_by(image=label.image,
                                                     x1=label.x1,
                                                     x2=label.x2,
                                                     y1=label.y1,
                                                     y2=label.y2).first()

    # insert record for the eo label
    def insert_eo_label(self, s, worker, job):
        species_obj = self.query_species(s)
        label_entry = EOLabelEntry(image_id=self.image_eo,x1=self.x1_eo,x2=self.x2_eo,y1=self.y1_eo,y2=self.y2_eo,species=species_obj,age_class=self.age_class, hotspot_id=self.hs_id)
        check = self.get_existing_eo_label(s, label_entry)
        if check is not None:
            return check, False # label already exists

        label_entry = EOLabelEntry(
            image_id=self.image_eo,
            x1=self.x1_eo,
            x2=self.x2_eo,
            y1=self.y1_eo,
            y2=self.y2_eo,
            species=species_obj,
            age_class=self.age_class,
            confidence=self.confidence_eo,
            start_date=datetime.now(),
            end_date=None,
            is_shadow=None,
            worker=worker,
            job=job,
            hotspot_id=self.hs_id,
        )
        s.add(label_entry)
        try:
            s.flush()
            logging.info("SUCCESS: added label entry im: %s %d %d %d %d" % (
                self.image_eo, label_entry.x1, label_entry.y1, label_entry.x2, label_entry.y2))
        except IntegrityError as e:
            logging.error("ERROR: adding label entry im: %s %d %d %d %d" % (
                self.image_eo, label_entry.x1, label_entry.y1, label_entry.x2, label_entry.y2))
            print(e)
            s.rollback()
            return None, False

        return label_entry, True


    # insert record for the ir label
    def insert_ir_label(self, s, worker, job):
        species_obj = self.query_species(s)
        label_entry = IRLabelEntry(image_id=self.image_ir,x1=self.x1_ir,x2=self.x2_ir,y1=self.y1_ir,y2=self.y2_ir,species=species_obj,age_class=self.age_class, hotspot_id=self.hs_id)
        check = self.get_existing_ir_label(s, label_entry)
        if check is not None:
            return check, False # label already exists

        label_entry = IRLabelEntry(
            image_id=self.image_ir,
            x1=self.x1_ir,
            x2=self.x2_ir,
            y1=self.y1_ir,
            y2=self.y2_ir,
            species=species_obj,
            age_class=self.age_class,
            confidence=self.confidence_ir,
            start_date=datetime.now(),
            end_date=None,
            is_shadow=None,
            worker=worker,
            job=job,
            hotspot_id=self.hs_id,
        )
        s.add(label_entry)
        try:
            s.flush()
            logging.info("SUCCESS: added label entry im: %s %d %d %d %d" % (
                self.image_ir, label_entry.x1, label_entry.y1, label_entry.x2, label_entry.y2))
        except IntegrityError as e:
            logging.error("ERROR: adding label entry im: %s %d %d %d %d" % (
                self.image_ir, label_entry.x1, label_entry.y1, label_entry.x2, label_entry.y2))
            print(e)
            s.rollback()
            return None, False

        return label_entry, True