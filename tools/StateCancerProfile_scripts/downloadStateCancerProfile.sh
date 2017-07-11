#! /usr/bin/env bash

set -e
if [ $# -lt 1 ]; then
  echo "USAGE: $0 listOfLinks"
  exit 1
fi

fname=`basename $0`
cat $1|awk '{print "wget \"" $1 "\" -O", $2}' >/tmp/$fname
source /tmp/$fname
rm -f /tmp/$fname
