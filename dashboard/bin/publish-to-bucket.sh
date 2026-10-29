# For testing purposes, publish the package to an S3 bucket.
# The s3 bucket is deployed as a website to serve as a read-only npm package http location.
# Each package's own publish-s3 script builds, packs and uploads it (to the bucket's dynamodb/ prefix).

set -e

# integration-core and integration-huron-person sit next to the fargate repo
integration_dir="$(cd "$(dirname "$0")/../../.." && pwd)"

echo "Publishing to S3 bucket..."

for package in integration-core integration-huron-person ; do
  (cd "$integration_dir/$package" && npm run publish-s3)
done
