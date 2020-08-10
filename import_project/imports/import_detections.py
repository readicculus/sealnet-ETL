import mlflow

from import_project.utils.util import printProgressBar


def process_labels(s, labels, job, eo_worker, ir_worker):
    total = len(labels)
    j = 0
    eo_added_ct = 0
    ir_added_ct = 0
    for i, label in enumerate(labels):
        if j % 10 == 0:
            printProgressBar(j+1, total, prefix='Progress:', suffix='Complete', length=50)
        j += 1

        eo_added, ir_added = label.record(s, eo_worker, ir_worker, job)
        eo_added_ct += eo_added
        ir_added_ct += ir_added
        mlflow.log_metric('eo_labels', eo_added_ct)
        mlflow.log_metric('ir_labels', ir_added_ct)

        if j % 100 == 0:
            printProgressBar(j+1, total, prefix='Progress:', suffix='Committing', length=50)
            s.commit()
            s.flush()
    s.commit()
    s.flush()
    mlflow.log_metric('eo_labels', eo_added_ct)
    mlflow.log_metric('ir_labels', ir_added_ct)
    return eo_added_ct, ir_added_ct
    # print("%d images have no IR/not aligned" % num_ir_missing)