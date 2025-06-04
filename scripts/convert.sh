#!/bin/bash
# scripts/convert.sh
docker-compose run --rm pwconvert python3 convert.py "$@"