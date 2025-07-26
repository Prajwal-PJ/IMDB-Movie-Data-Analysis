import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
import concurrent.futures
import re

class GroupFilter:
      def __init__(self, name, filters):
        self.name = name
        self.filters = filters

def apply_group_filter(source_DyF, group):
    return(Filter.apply(frame = source_DyF, f = group.filters))

def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {executor.submit(apply_group_filter, source_DyF, gf): gf for gf in group_filters}
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print('%r generated an exception: %s' % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node S3-Data-Source
S3DataSource_node1732103711562 = glueContext.create_dynamic_frame.from_catalog(database="imdb-movies-datacatalog", table_name="imdb_movies_analysis", transformation_ctx="S3DataSource_node1732103711562")

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1732104155922_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
            IsComplete "imdb_rating",
        ColumnValues "imdb_rating" between 8.5 and 10.3
    ]
"""

EvaluateDataQuality_node1732104155922 = EvaluateDataQuality().process_rows(frame=S3DataSource_node1732103711562, ruleset=EvaluateDataQuality_node1732104155922_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1732104155922", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"performanceTuning.caching":"CACHE_NOTHING","observations.scope":"ALL"})

# Script generated for node ruleOutcomes
ruleOutcomes_node1732104440057 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1732104155922, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1732104440057")

# Script generated for node rowLevelOutcomes
rowLevelOutcomes_node1732104677738 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1732104155922, key="rowLevelOutcomes", transformation_ctx="rowLevelOutcomes_node1732104677738")

# Script generated for node Conditional Router
ConditionalRouter_node1732105007272 = threadedRoute(glueContext,
  source_DyF = rowLevelOutcomes_node1732104677738,
  group_filters = [GroupFilter(name = "Failed_records", filters = lambda row: (bool(re.match("Failed", row["DataQualityEvaluationResult"])))), GroupFilter(name = "default_group", filters = lambda row: (not(bool(re.match("Failed", row["DataQualityEvaluationResult"])))))])

# Script generated for node default_group
default_group_node1732105007580 = SelectFromCollection.apply(dfc=ConditionalRouter_node1732105007272, key="default_group", transformation_ctx="default_group_node1732105007580")

# Script generated for node Failed_records
Failed_records_node1732105007644 = SelectFromCollection.apply(dfc=ConditionalRouter_node1732105007272, key="Failed_records", transformation_ctx="Failed_records_node1732105007644")

# Script generated for node Change Schema
ChangeSchema_node1732105231679 = ApplyMapping.apply(frame=default_group_node1732105007580, mappings=[("overview", "string", "overview", "string"), ("gross", "string", "gross", "string"), ("director", "string", "director", "string"), ("certificate", "string", "certificate", "string"), ("star4", "string", "star4", "string"), ("runtime", "string", "runtime", "string"), ("star2", "string", "star2", "string"), ("star3", "string", "star3", "string"), ("no_of_votes", "long", "no_of_votes", "int"), ("series_title", "string", "series_title", "string"), ("meta_score", "long", "meta_score", "int"), ("star1", "string", "star1", "string"), ("genre", "string", "genre", "string"), ("released_year", "string", "released_year", "string"), ("poster_link", "string", "poster_link", "string"), ("imdb_rating", "double", "imdb_rating", "decimal")], transformation_ctx="ChangeSchema_node1732105231679")

# Script generated for node Amazon S3
AmazonS3_node1732104812713 = glueContext.write_dynamic_frame.from_options(frame=ruleOutcomes_node1732104440057, connection_type="s3", format="json", connection_options={"path": "s3://imdb-movies-analysis/rule-outcomes/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1732104812713")

# Script generated for node Amazon S3
AmazonS3_node1732105158584 = glueContext.write_dynamic_frame.from_options(frame=Failed_records_node1732105007644, connection_type="s3", format="json", connection_options={"path": "s3://imdb-movies-analysis/failed-records/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1732105158584")

# Script generated for node Redshift-load
Redshiftload_node1732105377367 = glueContext.write_dynamic_frame.from_catalog(frame=ChangeSchema_node1732105231679, database="imdb-movies-datacatalog", table_name="redshift-destination-imdb-movies-datacatalogdev_movies_imdb_movies_rating", redshift_tmp_dir="s3://imdb-movies-analysis/temp/",additional_options={"aws_iam_role": "arn:aws:iam::535002885128:role/redshift-role"}, transformation_ctx="Redshiftload_node1732105377367")

job.commit()
