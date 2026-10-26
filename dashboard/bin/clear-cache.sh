#!/bin/bash

if [ ! -d cdk.out ] ; then
  echo "No cdk.out directory found. Cancelling."
  exit 0
fi

(
  cd cdk.out/.cache
  for zip in $(ls -1t | tail -n +2) ; do
    base=$(echo $zip | cut -d'.' -f1)
    echo $base
    rm -f $zip
    rm -rf ../asset.${base}
  done
)