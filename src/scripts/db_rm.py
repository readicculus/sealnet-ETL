from sqlalchemy.orm import aliased

from build.lib.noaadb import Session
from noaadb.schema.models import *
from sqlalchemy import inspect
# def drop_tables(s):
#     engine = create_engine(DATABASE_URI, echo=echo)
#     Base.metadata.drop_all(engine)
#     print("Success")

def remove_images_by_survey(s, survey):
    print("NOAAImages being deleted")
    s.query(NOAAImage).filter_by(survey=survey).delete()
    s.commit()
    print("NOAAImages deleted")

def remove_sightings_by_survey(s, survey):
    ir_label = aliased(LabelEntry)
    eo_label = aliased(LabelEntry)

    res1 = s.query(Sighting).filter(Sighting.ir_label_id.isnot(None)).join(ir_label, Sighting.ir_label).join(ir_label.image).filter_by(survey=survey).all()
    res2 = s.query(Sighting).filter(Sighting.eo_label_id.isnot(None)).join(eo_label, Sighting.eo_label).join(eo_label.image).filter_by(survey=survey).all()
    res = res1+res2
    for i, sighting in enumerate(res):
        s.delete(sighting)
        # s.delete(label)
        if i % 1000 == 0:
            s.commit()
    s.flush()
    s.commit()

def remove_labels_by_survey(s, survey):
    res = s.query(LabelEntry).join(LabelEntry.image).filter_by(survey=survey).all()
    num = len(res)
    print("%d labels being deleted" % num)
    for i, label in enumerate(res):

        s.delete(label)
        s.commit()

    s.commit()
    s.expunge_all()

    print("%d labels deleted" % num)




