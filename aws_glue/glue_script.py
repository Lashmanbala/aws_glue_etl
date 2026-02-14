import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, to_timestamp, year, month, dayofmonth,
    lit, when
)
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME'
    ]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

src_bucket = 'github-activity-bucket-123'
src_folder = 'landing'
tgt_bucket = 'github-activity-bucket-123'
tgt_folder = 'cleaned'


datasource = glueContext.create_dynamic_frame.from_options(
    format="json",
    format_options={"multiLine": "true"},
    connection_type="s3",
    connection_options={
        "paths": [f"s3://{src_bucket}/{src_folder}/"],
        "recurse": True
    },
    transformation_ctx="datasource"
)

df_raw = datasource.toDF()


EXPECTED_ROOT_COLUMNS = (
    "id", "type", "created_at", "actor",
    "org", "repo", "payload", "public"
)

def validate_schema_columns(df):
    present = set(df.columns)
    missing = [c for c in EXPECTED_ROOT_COLUMNS if c not in present]
    if missing:
        print(f"Schema drift detected. Missing columns: {missing}")
    else:
        print("All expected root columns present")

validate_schema_columns(df_raw)


def build_fact_events(df):
    fact_df = df.select(
        col("id").alias("event_id"),
        col("type").alias("event_type"),
        to_timestamp(col("created_at")).alias("created_at"),
        col("public").alias("is_public"),
        col("actor.id").alias("actor_id"),
        col("org.id").alias("org_id"),
        col("repo.id").alias("repo_id"),
        col("payload.action").alias("payload_action"),
        col("payload.ref").alias("ref"),
        col("payload.ref_type").alias("ref_type"),
        col("payload.push_id").alias("push_id"),
        col("payload.pull_request.number").alias("pr_number"),
        col("payload.issue.number").alias("issue_number"),
        col("payload.release.tag_name").alias("release_tag_name"),
        col("payload.forkee.full_name").alias("forkee_full_name"),
    )

    return (
        fact_df
        .withColumn("year", year(col("created_at")))
        .withColumn("month", month(col("created_at")))
        .withColumn("day", dayofmonth(col("created_at")))
    )


def build_dim_actor(df):
    return (
        df.select(
            col("actor.id").alias("actor_id"),
            col("actor.login"),
            col("actor.display_login"),
            col("actor.avatar_url"),
        )
        .filter(col("actor_id").isNotNull())
        .dropDuplicates(["actor_id"])
    )

def build_dim_org(df):
    return (
        df.select(
            col("org.id").alias("org_id"),
            col("org.login"),
            col("org.avatar_url"),
        )
        .filter(col("org_id").isNotNull())
        .dropDuplicates(["org_id"])
    )

def build_dim_repo(df):
    return (
        df.select(
            col("repo.id").alias("repo_id"),
            col("repo.name"),
            col("repo.url"),
        )
        .filter(col("repo_id").isNotNull())
        .dropDuplicates(["repo_id"])
    )

EVENT_TYPE_CATEGORY = {
    "PushEvent": "push",
    "PullRequestEvent": "pr",
    "PullRequestReviewEvent": "pr",
    "PullRequestReviewCommentEvent": "pr",
    "IssuesEvent": "issue",
    "IssueCommentEvent": "issue",
    "ReleaseEvent": "release",
    "ForkEvent": "fork",
    "CreateEvent": "create",
    "DeleteEvent": "delete",
    "WatchEvent": "watch",
    "MemberEvent": "member",
    "PublicEvent": "public",
}

def build_dim_event_type(df):
    distinct_types = (
        df.select(col("type").alias("event_type"))
        .filter(col("event_type").isNotNull())
        .distinct()
    )

    category_expr = lit("other")
    for event_type, category in reversed(list(EVENT_TYPE_CATEGORY.items())):
        category_expr = when(
            col("event_type") == event_type, lit(category)
        ).otherwise(category_expr)

    return distinct_types.withColumn("category", category_expr)


df_fact = build_fact_events(df_raw)
df_dim_actor = build_dim_actor(df_raw)
df_dim_org = build_dim_org(df_raw)
df_dim_repo = build_dim_repo(df_raw)
df_dim_event_type = build_dim_event_type(df_raw)


fact_dyf = DynamicFrame.fromDF(df_fact, glueContext, "fact_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=fact_dyf,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": f"s3://{tgt_bucket}/{tgt_folder}/fact_events/",
        "partitionKeys": ["year", "month", "day"]
    },
    transformation_ctx="fact_write"
)

    
def write_dimension(df, table_name):
    dyf = DynamicFrame.fromDF(df.coalesce(1), glueContext, f"{table_name}_dyf")

    glueContext.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="s3",
        format="glueparquet",
        connection_options={
            "path": f"s3://{tgt_bucket}/{tgt_folder}/{table_name}/"
        },
        transformation_ctx=f"{table_name}_write"
    )

write_dimension(df_dim_actor, "dim_actor")
write_dimension(df_dim_org, "dim_org")
write_dimension(df_dim_repo, "dim_repo")
write_dimension(df_dim_event_type, "dim_event_type")


job.commit()
