from iam_util import create_iam_policy, create_iam_role

bucket_name = 'github-activity-bucket'

policy_ARN = create_iam_policy(bucket_name)

res = create_iam_role(policy_ARN)
print(res)