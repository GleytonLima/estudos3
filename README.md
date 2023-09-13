aws --endpoint-url http://localhost:4566 s3api \
create-bucket --bucket my-bucket \
--create-bucket-configuration LocationConstraint=eu-central-1 \
--region us-east-1
{
    "Location": "http://my-bucket.s3.localhost.localstack.cloud:4566/"
}


aws --endpoint-url http://localhost:4566 s3api list-buckets