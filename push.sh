#!/bin/bash

if [[ "$*" == "" ]]; then
mess="Minor Changes!"
else
mess="$*"
fi

echo "Message: $mess"

git pull
git add .
git commit -m "$mess"
git push -u origin master