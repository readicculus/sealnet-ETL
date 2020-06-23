from sqlalchemy.orm import aliased

from build.lib.noaadb import Session
from noaadb.schema.models import *

# def drop_tables(s):
#     engine = create_engine(DATABASE_URI, echo=echo)
#     Base.metadata.drop_all(engine)
#     print("Success")

def remove_images_by_survey(s, survey):
    print("NOAAImages being deleted")
    s.query(NOAAImage).filter_by(survey=survey).delete()
    s.commit()
    print("NOAAImages deleted")

def remove_hotspots_by_survey(s, survey):
    ir_label = aliased(TruePositiveLabels)
    eo_label = aliased(TruePositiveLabels)
    eo_image = aliased(NOAAImage)
    ir_image = aliased(NOAAImage)

    res_rgb = s.query(Sighting) \
        .join(eo_label.sighting, Sighting).join(eo_image, eo_label.image).filter_by(survey=survey) \
        .all()
    res_ir = s.query(Sighting) \
        .join(ir_label, Sighting.ir_label).join(ir_image, ir_label.image).filter_by(survey=survey) \
        .all()
    print("removing hotspots")
    num = 0
    for hs in res_rgb:
        num+=1
        s.delete(hs)
    s.commit()

    for hs in res_ir:
        s.delete(hs)
    s.commit()
    print("%d hotspots removed" % num)

def remove_sightings_by_survey(s, survey):
    ir_label = aliased(LabelEntry)
    eo_label = aliased(LabelEntry)
    eo_image = aliased(NOAAImage)
    ir_image = aliased(NOAAImage)

    res = s.query(Sighting) \
        .join(ir_label, Sighting.ir_label).join(eo_label, Sighting.eo_label).join(eo_image, eo_label.image).filter_by(survey=survey).join(ir_image, ir_label.image) \
        .all()
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
        # s.delete(label)
        if i % 1000 == 0:
            s.commit()
    s.flush()
    s.commit()

    print("%d labels deleted" % num)

def remove_fps_by_survey(s, survey):
    ir_label = aliased(TruePositiveLabels)
    eo_label = aliased(TruePositiveLabels)
    eo_image = aliased(NOAAImage)
    ir_image = aliased(NOAAImage)
    res_rgb_fp = s.query(FalsePositiveLabels) \
        .join(eo_label, FalsePositiveLabels.eo_label).join(eo_image, eo_label.image).filter_by(survey=survey) \
        .all()
    res_ir_fp = s.query(FalsePositiveLabels) \
        .join(ir_label, FalsePositiveLabels.ir_label).join(ir_image, ir_label.image).filter_by(survey=survey) \
        .all()

    print("FalsePositives being deleted")
    num = 0
    for hs in res_rgb_fp:
        num+=1
        s.delete(hs)
    s.commit()

    for hs in res_ir_fp:
        num+=1
        s.delete(hs)
    s.commit()
    print("%d false positives removed" % num)



