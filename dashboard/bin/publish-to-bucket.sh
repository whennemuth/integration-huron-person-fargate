# For testing purposes, publish the package to an S3 bucket.
# The s3 bucket is deployed as a website to serve as a read-only npm package http location.

echo "Publishing to S3 bucket..."

bucket_uri="s3://simple-npm-package-bucket"

# Build the integration-core package
cd ../integration-core || exit 1
npm run pack
package_file_core=$(ls *.tgz | head -n 1)

# Copy the integration-core package to the S3 bucket
aws s3 cp "$package_file_core" "$bucket_uri/"
cd - || exit 1

# Build the integration-huron-person package
cd ../integration-huron-person || exit 1
npm run pack
package_file_person=$(ls *.tgz | head -n 1)

# Copy the integration-huron-person package to the S3 bucket
aws s3 cp "$package_file_person" "$bucket_uri/"
cd - || exit 1
