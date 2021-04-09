#!/bin/bash


if [[ "$*" == "" ]]; then
    mess="Minor Changes!"
else
    mess="$*"
fi
#git pull
git add .
echo "Message: $mess"
git commit -m "$mess"
git push -u origin master