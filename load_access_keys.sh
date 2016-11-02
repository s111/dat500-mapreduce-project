#!/bin/sh

source ~/rootkey.csv

export AWS_ACCESS_KEY_ID=${AWSAccessKeyId/%$'\r'/}
export AWS_SECRET_ACCESS_KEY=${AWSSecretKey/%$'\r'/}
