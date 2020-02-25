# sealnet-ETL
A datapipeline for @readicculus/sealnet using postgresql, S3, and Sagemaker/GroundTruth


pg_dump -a -F p  -U postgres noaa -f remote_copy.pgsql
